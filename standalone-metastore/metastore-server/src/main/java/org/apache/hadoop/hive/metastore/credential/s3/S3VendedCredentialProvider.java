/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.credential.s3;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.credential.StorageAccessRequest;
import org.apache.hadoop.hive.metastore.credential.VendedCredentialProvider;
import org.apache.hadoop.hive.metastore.credential.VendedStorageCredential;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A VendedCredentialProvider specified for Amazon S3.
 */
public class S3VendedCredentialProvider implements VendedCredentialProvider {
  private static final String PREFIXES_KEY = "aws.prefixes";
  private static final String REGION_KEY = "aws.region";
  private static final String ROLE_ARN_KEY = "aws.role-arn";
  private static final String EXTERNAL_ID_KEY = "aws.external-id";
  private static final String CREDENTIAL_EXPIRATION_KEY = "aws.expiration";
  private static final String SESSION_PREFIX = "hms_";

  private final Arn roleArn;
  private final String externalId;
  private final List<String> prefixes;
  private final int expirationInSeconds;
  private final StsClient stsClient;

  private static StsClient createStsClient(String region) {
    final var credentialsProvider = DefaultCredentialsProvider.builder().build();
    final var builder = StsClient.builder().credentialsProvider(credentialsProvider);
    return region == null ? builder.build() : builder.region(Region.of(region)).build();
  }

  private static List<String> createPrefixes(String[] prefixes) {
    if (prefixes == null) {
      return Collections.emptyList();
    }
    return Arrays.asList(prefixes);
  }

  public S3VendedCredentialProvider(String configKeyPrefix, Configuration conf) {
    this(
        Arn.fromString(Objects.requireNonNull(conf.get(configKeyPrefix + ROLE_ARN_KEY))),
        conf.get(configKeyPrefix + EXTERNAL_ID_KEY),
        createPrefixes(conf.getStrings(configKeyPrefix + PREFIXES_KEY, (String) null)),
        (int) Math.min(
            Integer.MAX_VALUE,
            conf.getTimeDuration(configKeyPrefix + CREDENTIAL_EXPIRATION_KEY, 3600, TimeUnit.SECONDS)
        ),
        createStsClient(conf.get(configKeyPrefix + REGION_KEY))
    );
  }

  @VisibleForTesting
  S3VendedCredentialProvider(Arn roleArn, String externalId, List<String> prefixes, int expirationSeconds,
      StsClient stsClient) {
    this.prefixes = prefixes;
    this.roleArn = Objects.requireNonNull(roleArn);
    this.externalId = externalId;
    this.expirationInSeconds = expirationSeconds;
    this.stsClient = Objects.requireNonNull(stsClient);
  }

  @Override
  public boolean supports(StorageAccessRequest request) {
    final var optionalLocation = S3Location.create(roleArn.partition(), request.location().toUri());
    if (optionalLocation.isEmpty()) {
      return false;
    }
    if (prefixes.isEmpty()) {
      // Accepts all legal S3 paths
      return true;
    }
    final var location = optionalLocation.orElseThrow();
    return prefixes.stream().anyMatch(location::matches);
  }

  @Override
  public List<VendedStorageCredential> vend(String username, List<StorageAccessRequest> accessRequests) {
    // This provider issues a single assume-role request and get a merged credential to reduce the number of requests
    final var assumeRoleRequest = AssumeRoleRequest.builder().externalId(externalId).roleArn(roleArn.toString())
        .roleSessionName(createRoleSessionName(username)).durationSeconds(expirationInSeconds)
        .policy(buildPolicy(accessRequests).toJson()).build();
    final var response = stsClient.assumeRole(assumeRoleRequest);
    final var awsCredentials = response.credentials();

    final var builder = new HashMap<String, String>();
    builder.put("s3.access-key-id", awsCredentials.accessKeyId());
    builder.put("s3.secret-access-key", awsCredentials.secretAccessKey());
    builder.put("s3.session-token", awsCredentials.sessionToken());
    final Instant expiredAt;
    if (awsCredentials.expiration() == null) {
      expiredAt = Instant.MAX;
    } else {
      expiredAt = awsCredentials.expiration();
      final var epochMillis = String.valueOf(awsCredentials.expiration().toEpochMilli());
      builder.put("s3.session-token-expires-at-ms", epochMillis);
    }
    final var credentials = Collections.unmodifiableMap(builder);

    return accessRequests.stream()
        .map(request -> new VendedStorageCredential(request.location(), credentials, expiredAt)).toList();
  }

  @Override
  public String toString() {
    return "S3VendedCredentialProvider{" + "role='" + roleArn + '\'' + ", prefixes=" + prefixes + '}';
  }

  private static String createRoleSessionName(String username) {
    final var builder = new StringBuilder(SESSION_PREFIX.length() + username.length());
    builder.append(SESSION_PREFIX);
    for (char c: username.toCharArray()) {
      if ("abcdefghijklmnopqrstuvwxyz0123456789,.@-".contains(
          Character.toString(c).toLowerCase(Locale.ENGLISH))) {
        builder.append(c);
      } else {
        builder.append('-');
      }
    }
    return builder.toString();
  }

  /**
   * Creates a down-scoped policy for the given request.
   */
  private IamPolicy buildPolicy(List<StorageAccessRequest> requests) {
    final Map<String, IamStatement.Builder> bucketLocationBuilder = new HashMap<>();
    final Map<String, IamStatement.Builder> listBuilder = new HashMap<>();
    final List<String> readResources = new ArrayList<>();
    final List<String> createResources = new ArrayList<>();
    final List<String> deleteResources = new ArrayList<>();

    requests.forEach(request -> {
      Preconditions.checkArgument(supports(request));
      final var s3Location = S3Location.create(roleArn.partition(), request.location().toUri()).orElseThrow();
      final var bucketArn = s3Location.getBucketArn().toString();
      final var wildCardArn = s3Location.getWildCardArn().toString();
      bucketLocationBuilder.computeIfAbsent(bucketArn,
          key -> IamStatement.builder().effect(IamEffect.ALLOW).addAction("s3:GetBucketLocation").addResource(key));
      request.operations().forEach(action -> {
        switch (action) {
        case LIST -> listBuilder.computeIfAbsent(bucketArn,
                key -> IamStatement.builder().effect(IamEffect.ALLOW).addAction("s3:ListBucket").addResource(key))
            .addCondition(IamConditionOperator.STRING_LIKE, "s3:prefix", s3Location.getWildCardPath());
        case READ -> readResources.add(wildCardArn);
        case CREATE -> createResources.add(wildCardArn);
        case DELETE -> deleteResources.add(wildCardArn);
        default -> throw new IllegalArgumentException("Unexpected action: " + action);
        }
      });
    });

    final var policyBuilder = IamPolicy.builder();
    bucketLocationBuilder.values().stream().map(IamStatement.Builder::build).forEach(policyBuilder::addStatement);
    listBuilder.values().stream().map(IamStatement.Builder::build).forEach(policyBuilder::addStatement);
    if (!readResources.isEmpty()) {
      policyBuilder.addStatement(builder -> {
        builder.effect(IamEffect.ALLOW).addAction("s3:GetObject").addAction("s3:GetObjectVersion");
        readResources.forEach(builder::addResource);
      });
    }
    if (!createResources.isEmpty()) {
      policyBuilder.addStatement(builder -> {
        builder.effect(IamEffect.ALLOW).addAction("s3:PutObject");
        createResources.forEach(builder::addResource);
      });
    }
    if (!deleteResources.isEmpty()) {
      policyBuilder.addStatement(builder -> {
        builder.effect(IamEffect.ALLOW).addAction("s3:DeleteObject");
        deleteResources.forEach(builder::addResource);
      });
    }
    return policyBuilder.build();
  }
}
