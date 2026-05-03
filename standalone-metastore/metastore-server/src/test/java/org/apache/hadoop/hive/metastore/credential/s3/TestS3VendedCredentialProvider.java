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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.credential.StorageAccessRequest;
import org.apache.hadoop.hive.metastore.credential.StorageOperation;
import org.apache.hadoop.hive.metastore.credential.VendedStorageCredential;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

@Category(MetastoreUnitTest.class)
public class TestS3VendedCredentialProvider {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSupportsWhenPrefixesAreEmpty() {
    var provider = new S3VendedCredentialProvider(
        Arn.fromString("arn:aws:iam::123456789012:role/test-role"),
        null,
        Collections.emptyList(),
        3600,
        Mockito.mock(StsClient.class));

    Assert.assertTrue(provider.supports(new StorageAccessRequest(
        new Path("s3://bucket-a/warehouse/table"),
        EnumSet.of(StorageOperation.READ))));
  }

  @Test
  public void testSupportsWithPrefix() {
    var provider = new S3VendedCredentialProvider(
        Arn.fromString("arn:aws:iam::123456789012:role/test-role"),
        null,
        List.of("bucket-a/warehouse/", "bucket-x/hive/"),
        3600,
        Mockito.mock(StsClient.class));

    var matched = new StorageAccessRequest(
        new Path("s3a://bucket-a/warehouse/table"),
        EnumSet.of(StorageOperation.READ));
    Assert.assertTrue(provider.supports(matched));
    var unmatched = new StorageAccessRequest(
        new Path("s3a://bucket-b/warehouse/table"),
        EnumSet.of(StorageOperation.READ));
    Assert.assertFalse(provider.supports(unmatched));
    Assert.assertThrows(IllegalArgumentException.class,
        () -> provider.vend("test-user", Collections.singletonList(unmatched)));
  }

  @Test
  public void testSupportsWithArnPrefix() {
    var provider = new S3VendedCredentialProvider(
        Arn.fromString("arn:aws:iam::123456789012:role/test-role"),
        null,
        List.of("arn:aws:s3:::bucket-a/warehouse/", "arn:aws:s3:::bucket-x/hive/"),
        3600,
        Mockito.mock(StsClient.class));

    var matched = new StorageAccessRequest(
        new Path("s3a://bucket-a/warehouse/table"),
        EnumSet.of(StorageOperation.READ));
    Assert.assertTrue(provider.supports(matched));
    var unmatched = new StorageAccessRequest(
        new Path("s3a://bucket-b/warehouse/table"),
        EnumSet.of(StorageOperation.READ));
    Assert.assertFalse(provider.supports(unmatched));
    Assert.assertThrows(IllegalArgumentException.class,
        () -> provider.vend("test-user", Collections.singletonList(unmatched)));
  }

  @Test
  public void testSupportsWithUnsupportedPaths() {
    var provider = new S3VendedCredentialProvider(
        Arn.fromString("arn:aws:iam::123456789012:role/test-role"),
        null,
        Collections.emptyList(),
        3600,
        Mockito.mock(StsClient.class));

    var nonSchema = new StorageAccessRequest(
        new Path("/bucket-a/warehouse/table"),
        EnumSet.of(StorageOperation.READ));
    Assert.assertFalse(provider.supports(nonSchema));

    var nonS3 = new StorageAccessRequest(
        new Path("hdfs://bucket-a/warehouse/table"),
        EnumSet.of(StorageOperation.READ));
    Assert.assertFalse(provider.supports(nonS3));

    var nonAuthority = new StorageAccessRequest(
        new Path("s3a:///warehouse/table"),
        EnumSet.of(StorageOperation.READ));
    Assert.assertFalse(provider.supports(nonAuthority));
  }

  @Test
  public void testVend() throws Exception {
    var stsClient = Mockito.mock(StsClient.class);
    var requestCaptor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    var accessKey = "dummy-access-key";
    var secretKey = "dummy-secret-key";
    var sessionToken = "dummy-session-token";
    var expiration = Instant.parse("2026-04-26T12:00:00Z");
    Mockito.when(stsClient.assumeRole(requestCaptor.capture())).thenReturn(
        AssumeRoleResponse.builder().credentials(
            Credentials.builder()
                .accessKeyId(accessKey)
                .secretAccessKey(secretKey)
                .sessionToken(sessionToken)
                .expiration(expiration)
                .build())
            .build());

    var provider = new S3VendedCredentialProvider(
        Arn.fromString("arn:aws-us-gov:iam::123456789012:role/test-role"),
        "external-id",
        Collections.emptyList(),
        1200,
        stsClient);
    var credentials = provider.vend(
        "User Name+1@example.com",
        List.of(
            new StorageAccessRequest(
                new Path("s3://bucket-realtime/warehouse/table"),
                EnumSet.of(StorageOperation.LIST, StorageOperation.READ, StorageOperation.CREATE,
                    StorageOperation.DELETE)),
            new StorageAccessRequest(
                new Path("s3n://bucket-archive/warehouse/table"),
                EnumSet.of(StorageOperation.LIST, StorageOperation.READ))
        )
    );

    var expected = List.of(
        new VendedStorageCredential(
            new Path("s3://bucket-realtime/warehouse/table"),
            Map.of(
                "s3.access-key-id", accessKey,
                "s3.secret-access-key", secretKey,
                "s3.session-token", sessionToken,
                "s3.session-token-expires-at-ms", "1777204800000"
            ),
            expiration
        ),
        new VendedStorageCredential(
            new Path("s3n://bucket-archive/warehouse/table"),
            Map.of(
                "s3.access-key-id", accessKey,
                "s3.secret-access-key", secretKey,
                "s3.session-token", sessionToken,
                "s3.session-token-expires-at-ms", "1777204800000"
            ),
            expiration
        )
    );
    Assert.assertEquals(expected, credentials);

    var request = requestCaptor.getValue();
    Assert.assertEquals("arn:aws-us-gov:iam::123456789012:role/test-role", request.roleArn());
    Assert.assertEquals("external-id", request.externalId());
    Assert.assertEquals(Integer.valueOf(1200), request.durationSeconds());
    Assert.assertEquals("hms_User-Name-1@example.com", request.roleSessionName());

    Assert.assertEquals(
        MAPPER.readTree("""
          {
            "Version": "2012-10-17",
            "Statement": [
              {
                "Effect": "Allow",
                "Action": "s3:GetBucketLocation",
                "Resource": "arn:aws-us-gov:s3:::bucket-archive"
              },
              {
                "Effect": "Allow",
                "Action": "s3:GetBucketLocation",
                "Resource": "arn:aws-us-gov:s3:::bucket-realtime"
              },
              {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws-us-gov:s3:::bucket-archive",
                "Condition": {
                  "StringLike": {
                    "s3:prefix": "warehouse/table/*"
                  }
                }
              },
              {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws-us-gov:s3:::bucket-realtime",
                "Condition": {
                  "StringLike": {
                    "s3:prefix": "warehouse/table/*"
                  }
                }
              },
              {
                "Effect": "Allow",
                "Action": [
                  "s3:GetObject",
                  "s3:GetObjectVersion"
                ],
                "Resource": [
                  "arn:aws-us-gov:s3:::bucket-realtime/warehouse/table/*",
                  "arn:aws-us-gov:s3:::bucket-archive/warehouse/table/*"
                ]
              },
              {
                "Effect": "Allow",
                "Action": "s3:PutObject",
                "Resource": "arn:aws-us-gov:s3:::bucket-realtime/warehouse/table/*"
              },
              {
                "Effect": "Allow",
                "Action": "s3:DeleteObject",
                "Resource": "arn:aws-us-gov:s3:::bucket-realtime/warehouse/table/*"
              }
            ]
          }
          """),
        MAPPER.readTree(request.policy()));
  }

  @Test
  public void testVendWithoutExpiration() throws Exception {
    var stsClient = Mockito.mock(StsClient.class);
    var requestCaptor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    var accessKey = "dummy-access-key";
    var secretKey = "dummy-secret-key";
    var sessionToken = "dummy-session-token";
    Mockito.when(stsClient.assumeRole(requestCaptor.capture())).thenReturn(
        AssumeRoleResponse.builder().credentials(
            Credentials.builder()
                .accessKeyId(accessKey)
                .secretAccessKey(secretKey)
                .sessionToken(sessionToken)
                .build())
            .build());

    var provider = new S3VendedCredentialProvider(
        Arn.fromString("arn:aws:iam::123456789012:role/test-role"),
        null,
        Collections.emptyList(),
        3600,
        stsClient);
    var credentials = provider.vend(
        "user",
        List.of(
            new StorageAccessRequest(
                new Path("s3a://bucket-a/warehouse/table"),
                EnumSet.of(StorageOperation.READ))
        )
    );

    var expected = new VendedStorageCredential(
        new Path("s3a://bucket-a/warehouse/table"),
        Map.of(
            "s3.access-key-id", accessKey,
            "s3.secret-access-key", secretKey,
            "s3.session-token", sessionToken
        ),
        Instant.MAX
    );
    Assert.assertEquals(List.of(expected), credentials);

    var request = requestCaptor.getValue();
    Assert.assertEquals("arn:aws:iam::123456789012:role/test-role", request.roleArn());
    Assert.assertNull(request.externalId());
    Assert.assertEquals(Integer.valueOf(3600), request.durationSeconds());
    Assert.assertEquals("hms_user", request.roleSessionName());

    Assert.assertEquals(
        MAPPER.readTree("""
          {
            "Version": "2012-10-17",
            "Statement": [
              {
                "Effect": "Allow",
                "Action": "s3:GetBucketLocation",
                "Resource": "arn:aws:s3:::bucket-a"
              },
              {
                "Effect": "Allow",
                "Action": [
                  "s3:GetObject",
                  "s3:GetObjectVersion"
                ],
                "Resource": "arn:aws:s3:::bucket-a/warehouse/table/*"
              }
            ]
          }
          """),
        MAPPER.readTree(request.policy()));
  }
}
