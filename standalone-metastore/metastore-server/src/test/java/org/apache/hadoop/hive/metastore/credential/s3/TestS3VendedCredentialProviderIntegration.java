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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.annotation.MetastoreExternalTest;
import org.apache.hadoop.hive.metastore.credential.StorageAccessRequest;
import org.apache.hadoop.hive.metastore.credential.StorageOperation;
import org.apache.hadoop.hive.metastore.credential.VendedStorageCredential;
import org.apache.hadoop.hive.metastore.testutils.AwsS3IntegrationTestConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AccessDeniedException;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sts.StsClient;

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

@Category(MetastoreExternalTest.class)
public class TestS3VendedCredentialProviderIntegration {
  private static final String READABLE_FILE_CONTENT = "this-is-read-only";
  private static final String WRITABLE_FILE_CONTENT = "this-is-deletable";

  private AwsS3IntegrationTestConfig config;
  private S3Client adminS3;
  private StsClient stsClient;
  private String readOnlyPrefix;
  private String readableKey;
  private String readableKeyVersion;
  private String writablePrefix;
  private String writableKey;
  private String writableKeyVersion;
  private String deniedPrefix;
  private String deniedKey;

  private StsClient createStsClient() {
    stsClient = StsClient.builder().region(config.region()).build();
    return stsClient;
  }

  private static S3Client createSessionS3Client(Region region, VendedStorageCredential credential) {
    var sessionCredentials = AwsSessionCredentials.create(
        credential.credentials().get("s3.access-key-id"),
        credential.credentials().get("s3.secret-access-key"),
        credential.credentials().get("s3.session-token"));
    return S3Client.builder()
        .region(region)
        .credentialsProvider(StaticCredentialsProvider.create(sessionCredentials))
        .build();
  }

  private static void deleteObjectIfExists(S3Client s3, String bucket, String key) {
    try {
      s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
    } catch (S3Exception ignored) {
    }
  }

  private void assertForbidden(Runnable command) {
    Assert.assertThrows(AccessDeniedException.class, command::run);
  }

  private void assertNotFound(Runnable command) {
    Assert.assertThrows(NoSuchKeyException.class, command::run);
  }

  @Before
  public void setUp() {
    Assume.assumeTrue("Test configurations are not available", AwsS3IntegrationTestConfig.isConfigured());

    config = AwsS3IntegrationTestConfig.fromEnvironment();
    adminS3 = S3Client.builder().region(config.region()).build();

    var rootPrefix = "%s/%s".formatted(config.basePath(), UUID.randomUUID());
    readOnlyPrefix = rootPrefix + "/read-only";
    readableKey = readOnlyPrefix + "/readable.txt";
    writablePrefix = rootPrefix + "/read-write";
    writableKey = writablePrefix + "/writable.txt";
    deniedPrefix = rootPrefix + "/denied";
    deniedKey = deniedPrefix + "/denied/unreadable.txt";

    readableKeyVersion = adminS3.putObject(
        PutObjectRequest.builder().bucket(config.bucket()).key(readableKey).build(),
        RequestBody.fromString(READABLE_FILE_CONTENT)).versionId();
    Assert.assertNotNull(readableKeyVersion);
    writableKeyVersion = adminS3.putObject(
        PutObjectRequest.builder().bucket(config.bucket()).key(writableKey).build(),
        RequestBody.fromString(WRITABLE_FILE_CONTENT)).versionId();
    Assert.assertNotNull(writableKeyVersion);
    adminS3.putObject(
        PutObjectRequest.builder().bucket(config.bucket()).key(deniedKey).build(),
        RequestBody.fromString("outside-scope"));
  }

  @After
  public void tearDown() {
    if (adminS3 != null) {
      deleteObjectIfExists(adminS3, config.bucket(), readableKey);
      deleteObjectIfExists(adminS3, config.bucket(), writableKey);
      deleteObjectIfExists(adminS3, config.bucket(), deniedKey);
      adminS3.close();
    }
    if (stsClient != null) {
      stsClient.close();
    }
  }

  @Test
  public void testVend() {
    var provider = new S3VendedCredentialProvider(
        Arn.fromString(config.roleArn()),
        config.externalId(),
        List.of(),
        900,
        createStsClient());

    var requests = List.of(
        new StorageAccessRequest(
            new Path("s3://%s/%s".formatted(config.bucket(), readOnlyPrefix)),
            EnumSet.of(StorageOperation.LIST, StorageOperation.READ)),
        new StorageAccessRequest(
            new Path("s3a://%s/%s".formatted(config.bucket(), writablePrefix)),
            EnumSet.of(StorageOperation.LIST, StorageOperation.READ, StorageOperation.CREATE, StorageOperation.DELETE))
    );

    var credentials = provider.vend("integration-test-user", requests);
    Assert.assertEquals(2, credentials.size());
    // This provider gets a merged credential for all access requests
    Assert.assertEquals(credentials.get(0).credentials(), credentials.get(1).credentials());

    try (var sessionS3 = createSessionS3Client(config.region(), credentials.getFirst())) {
      var location = sessionS3.getBucketLocation(GetBucketLocationRequest.builder().bucket(config.bucket()).build());
      Assert.assertEquals("", location.locationConstraintAsString());

      // Readable path
      var listReadable = sessionS3 .listObjectsV2(
          ListObjectsV2Request.builder().bucket(config.bucket()).prefix(readOnlyPrefix + "/").build())
          .contents().stream().map(S3Object::key).toList();
      Assert.assertEquals(List.of(readableKey), listReadable);

      var getReadable = sessionS3.getObjectAsBytes(
          GetObjectRequest.builder().bucket(config.bucket()).key(readableKey).versionId(readableKeyVersion).build()
      ).asUtf8String();
      Assert.assertEquals(READABLE_FILE_CONTENT, getReadable);

      assertForbidden(() -> sessionS3 .putObject(
          PutObjectRequest.builder().bucket(config.bucket()).key(readOnlyPrefix + "/test-put-readable.txt").build(),
          RequestBody.fromString("test-put-readable")));

      assertForbidden(() -> sessionS3 .deleteObject(
          DeleteObjectRequest.builder().bucket(config.bucket()).key(readableKey).build()
      ));

      // Writable path
      var listWritable = sessionS3 .listObjectsV2(
              ListObjectsV2Request.builder().bucket(config.bucket()).prefix(writablePrefix + "/").build())
          .contents().stream().map(S3Object::key).toList();
      Assert.assertEquals(List.of(writableKey), listWritable);

      var getWritable = sessionS3.getObjectAsBytes(
          GetObjectRequest.builder().bucket(config.bucket()).key(writableKey).versionId(writableKeyVersion).build()
      ).asUtf8String();
      Assert.assertEquals(WRITABLE_FILE_CONTENT, getWritable);

      sessionS3.putObject(
          PutObjectRequest.builder().bucket(config.bucket()).key(writablePrefix + "/test-put-writable.txt").build(),
          RequestBody.fromString("test-put-writable")
      );
      Assert.assertEquals("test-put-writable", sessionS3.getObjectAsBytes(
          GetObjectRequest.builder().bucket(config.bucket()).key(writablePrefix + "/test-put-writable.txt").build()
      ).asUtf8String());

      sessionS3.deleteObject(
          DeleteObjectRequest.builder().bucket(config.bucket()).key(writableKey).build());
      assertNotFound(() -> sessionS3.getObject(
          GetObjectRequest.builder().bucket(config.bucket()).key(writableKey).build()));

      // Denied path
      assertForbidden(() -> sessionS3 .listObjectsV2(
          ListObjectsV2Request.builder().bucket(config.bucket()).prefix(deniedPrefix + "/").build()));
      assertForbidden(() -> sessionS3.getObject(
          GetObjectRequest.builder().bucket(config.bucket()).key(deniedKey).build()));
      assertForbidden(() -> sessionS3.putObject(
          PutObjectRequest.builder().bucket(config.bucket()).key(deniedPrefix + "/test-put-denied.txt").build(),
          RequestBody.fromString("test-put-denied")));
      assertForbidden(() -> sessionS3 .deleteObject(
          DeleteObjectRequest.builder().bucket(config.bucket()).key(deniedKey).build()));
    }
  }
}
