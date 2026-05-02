/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreExternalTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.credential.s3.S3VendedCredentialProvider;
import org.apache.hadoop.hive.metastore.testutils.AwsS3IntegrationTestConfig;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.apache.iceberg.rest.extension.MockHiveAuthorizer;
import org.apache.iceberg.types.Types;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AccessDeniedException;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

@Category(MetastoreExternalTest.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestCredentialVendingAws {
  private static final String ACCESS_DELEGATION_HEADER = "header.X-Iceberg-Access-Delegation";
  private static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
  private static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";
  private static final String AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN";

  private static final Namespace NAMESPACE = Namespace.of("ns");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "test");
  private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION = newServerExtension();

  private AwsS3IntegrationTestConfig config;
  private RESTCatalog adminCatalog;
  private S3Client adminS3;
  private String currentTableRoot;
  private String tableLocation;
  private String metadataLocation;

  private static HiveRESTCatalogServerExtension newServerExtension() {
    var builder = HiveRESTCatalogServerExtension.builder(AuthType.SIMPLE);
    if (!AwsS3IntegrationTestConfig.isConfigured()) {
      return builder.build();
    }

    var config = AwsS3IntegrationTestConfig.fromEnvironment();

    builder.configure(ConfVars.ICEBERG_CATALOG_VENDED_CREDENTIALS_ENABLED.getVarname(), "true");
    builder.configure(ConfVars.CATALOG_VENDED_CREDENTIALS_PROVIDERS.getVarname(), "my-s3");
    builder.configure(ConfVars.CATALOG_VENDED_CREDENTIALS_PROVIDERS.getVarname() + ".my-s3.class",
        S3VendedCredentialProvider.class.getName());
    builder.configure(ConfVars.CATALOG_VENDED_CREDENTIALS_PROVIDERS.getVarname() + ".my-s3.aws.role-arn",
        config.roleArn());
    builder.configure(ConfVars.CATALOG_VENDED_CREDENTIALS_PROVIDERS.getVarname() + ".my-s3.aws.prefixes",
        "%s/%s/".formatted(config.bucket(), config.basePath()));
    builder.configure(ConfVars.CATALOG_VENDED_CREDENTIALS_PROVIDERS.getVarname() + ".my-s3.aws.region",
        config.regionId());
    if (config.externalId() != null && !config.externalId().isBlank()) {
      builder.configure(ConfVars.CATALOG_VENDED_CREDENTIALS_PROVIDERS.getVarname() + ".my-s3.aws.external-id",
          config.externalId());
    }
    builder.configure("fs.s3a.impl", S3AFileSystem.class.getName());
    builder.configure("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
    builder.configure("fs.s3a.endpoint.region", config.regionId());
    configureS3aCredentials(builder);

    return builder.build();
  }

  private static void configureS3aCredentials(HiveRESTCatalogServerExtension.Builder builder) {
    var accessKey = System.getenv(AWS_ACCESS_KEY_ID);
    var secretKey = System.getenv(AWS_SECRET_ACCESS_KEY);
    var sessionToken = System.getenv(AWS_SESSION_TOKEN);

    if (accessKey == null || accessKey.isBlank() || secretKey == null || secretKey.isBlank()) {
      return;
    }

    builder.configure("fs.s3a.access.key", accessKey);
    builder.configure("fs.s3a.secret.key", secretKey);
    if (sessionToken != null && !sessionToken.isBlank()) {
      builder.configure("fs.s3a.session.token", sessionToken);
      builder.configure("fs.s3a.aws.credentials.provider", TemporaryAWSCredentialsProvider.class.getName());
    } else {
      builder.configure("fs.s3a.aws.credentials.provider", SimpleAWSCredentialsProvider.class.getName());
    }
  }

  private RESTCatalog newCatalog(String user, boolean requestVendedCredentials) {
    var properties = new HashMap<String, String>();
    properties.put("uri", REST_CATALOG_EXTENSION.getRestEndpoint());
    properties.put("header.x-actor-username", user);
    properties.put("io-impl", S3FileIO.class.getName());
    properties.put("client.region", config.regionId());
    if (requestVendedCredentials) {
      properties.put(ACCESS_DELEGATION_HEADER, "vended-credentials");
    }
    return RCKUtils.initCatalogClient(properties);
  }

  private void deletePrefix(String prefix) {
    String continuationToken = null;
    boolean truncated;
    do {
      var response = adminS3.listObjectsV2(ListObjectsV2Request.builder()
          .bucket(config.bucket())
          .prefix(prefix)
          .continuationToken(continuationToken)
          .build());
      response.contents().forEach(object -> adminS3.deleteObject(
          DeleteObjectRequest.builder().bucket(config.bucket()).key(object.key()).build()));
      continuationToken = response.nextContinuationToken();
      truncated = Boolean.TRUE.equals(response.isTruncated());
    } while (truncated);
  }

  @BeforeAll
  void setupAll() {
    Assumptions.assumeTrue(
        AwsS3IntegrationTestConfig.isConfigured(),
        "Set HIVE_IT_AWS_INTEGRATION_TEST_ENABLED=true and configure S3 integration environment variables");

    config = AwsS3IntegrationTestConfig.fromEnvironment();
    adminS3 = S3Client.builder().region(config.region()).build();
    adminCatalog = newCatalog("admin", false);

    Assertions.assertEquals(
        Collections.singletonList(Namespace.of("default")),
        adminCatalog.listNamespaces());
  }

  @BeforeEach
  void setup() {
    RCKUtils.purgeCatalogTestEntries(adminCatalog);
    adminCatalog.createNamespace(NAMESPACE);
    currentTableRoot = "%s/%s".formatted(config.basePath(), UUID.randomUUID());
    tableLocation = "s3a://%s/%s/table".formatted(config.bucket(), currentTableRoot);
    var table = adminCatalog.buildTable(TABLE, SCHEMA).withLocation(tableLocation).create();
    metadataLocation = ((BaseTable) table).operations().refresh().metadataFileLocation();
  }

  @AfterEach
  void teardown() {
    if (adminCatalog != null) {
      RCKUtils.purgeCatalogTestEntries(adminCatalog);
    }
    if (adminS3 != null && currentTableRoot != null) {
      deletePrefix(currentTableRoot);
    }
    currentTableRoot = null;
    tableLocation = null;
    metadataLocation = null;
  }

  @AfterAll
  void teardownAll() throws Exception {
    if (adminCatalog != null) {
      adminCatalog.close();
    }
    if (adminS3 != null) {
      adminS3.close();
    }
  }

  @Test
  void testWritableUser() throws IOException {
    try (var sessionCatalog = newCatalog("USER_1", true)) {
      var table = sessionCatalog.loadTable(TABLE);

      var metadataFile = table.io().newInputFile(metadataLocation);
      Assertions.assertTrue(metadataFile.exists());
      try (var input = metadataFile.newStream()) {
        Assertions.assertTrue(new String(input.readAllBytes(), StandardCharsets.UTF_8).contains(tableLocation));
      }

      var destination = tableLocation + "/credential-vending-it.txt";
      try (var output = table.io().newOutputFile(destination).createOrOverwrite()) {
        output.write("content".getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  void testReadOnlyUser() throws IOException {
    try (var sessionCatalog = newCatalog(MockHiveAuthorizer.READ_ONLY_USER, true)) {
      var table = sessionCatalog.loadTable(TABLE);

      var metadataFile = table.io().newInputFile(metadataLocation);
      Assertions.assertTrue(metadataFile.exists());
      try (var input = metadataFile.newStream()) {
        Assertions.assertTrue(new String(input.readAllBytes(), StandardCharsets.UTF_8).contains(tableLocation));
      }

      var destination = tableLocation + "/credential-vending-it.txt";
      Assertions.assertThrows(AccessDeniedException.class, () -> {
        try (var output = table.io().newOutputFile(destination).createOrOverwrite()) {
          output.write("content".getBytes(StandardCharsets.UTF_8));
        }
      });
    }
  }
}
