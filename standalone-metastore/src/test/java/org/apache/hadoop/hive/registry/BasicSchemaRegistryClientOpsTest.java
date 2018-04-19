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

package org.apache.hadoop.hive.registry;

import org.apache.hadoop.hive.registry.common.test.IntegrationTest;
import org.apache.hadoop.hive.registry.conf.SchemaRegistryTestProfileType;
import org.apache.hadoop.hive.registry.helper.SchemaRegistryTestServerClientWrapper;
import org.apache.hadoop.hive.registry.serdes.avro.AvroSchemaProvider;
import org.apache.hadoop.hive.registry.util.AvroSchemaRegistryClientUtil;
import org.apache.hadoop.hive.registry.util.CustomParameterizedRunner;
import org.apache.hadoop.hive.registry.util.SchemaRegistryTestName;
import org.apache.hadoop.hive.registry.client.SchemaRegistryClient;
import org.apache.hadoop.hive.registry.common.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.common.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 *
 */
@RunWith(CustomParameterizedRunner.class)
@Category(IntegrationTest.class)
public class BasicSchemaRegistryClientOpsTest {
    private SchemaRegistryClient schemaRegistryClient;
    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;

    @Rule
    public SchemaRegistryTestName TEST_NAME_RULE = new SchemaRegistryTestName();

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
    }

    @Before
    public void startServer() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        schemaRegistryClient = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.getClient(false);
    }

    @After
    public void stopServer() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.stopTestServer();
    }

    public BasicSchemaRegistryClientOpsTest(SchemaRegistryTestProfileType schemaRegistryTestProfileType) {
    }

    @Test
    public void testSchemaOpsWithValidationLevelAsLatest() throws Exception {
        doTestSchemaOps(SchemaValidationLevel.LATEST);
    }

    @Test
    public void testSchemaOpsWithValidationLevelAsAll() throws Exception {
        doTestSchemaOps(SchemaValidationLevel.ALL);
    }

    private void doTestSchemaOps(SchemaValidationLevel validationLevel) throws IOException, InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        String testName = TEST_NAME_RULE.getMethodName();
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(testName + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(testName + "-group")
                .description("Schema for " + testName)
                .validationLevel(validationLevel)
                .compatibility(SchemaCompatibility.BOTH)
                .build();

        Long id = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        Assert.assertNotNull(id);

        // registering a new schema
        String schemaName = schemaMetadata.getName();
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc");
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(schema1, "Initial version of the schema"));
        Assert.assertNotNull(v1.getSchemaMetadataId());
        Assert.assertEquals(1, v1.getVersion().intValue());

        SchemaMetadataInfo schemaMetadataInfoForId = schemaRegistryClient.getSchemaMetadataInfo(v1.getSchemaMetadataId());
        SchemaMetadataInfo schemaMetadataInfoForName = schemaRegistryClient.getSchemaMetadataInfo(schemaName);
        Assert.assertEquals(schemaMetadataInfoForId, schemaMetadataInfoForName);

        // adding a new version of the schema using uploadSchemaVersion API
        SchemaIdVersion v2 = schemaRegistryClient.uploadSchemaVersion(schemaMetadata.getName(),
                                                                      "second version",
                                                                      AvroSchemaRegistryClientTest.class.getResourceAsStream("/schema-2.avsc"));
        Assert.assertEquals(v1.getVersion() + 1, v2.getVersion().intValue());

        SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2
                .getVersion()));
        SchemaVersionInfo latest = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
        Assert.assertEquals(latest, schemaVersionInfo);

        Collection<SchemaVersionInfo> allVersions = schemaRegistryClient.getAllVersions(schemaName);
        Assert.assertEquals(2, allVersions.size());

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "already added schema"));
        Assert.assertEquals(version, v1);

        Collection<SchemaVersionKey> md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder()
                                                                                                               .name("md5")
                                                                                                               .build());
        Assert.assertEquals(2, md5SchemaVersionKeys.size());

        Collection<SchemaVersionKey> txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder()
                                                                                                                .name("txid")
                                                                                                                .build());
        Assert.assertEquals(1, txidSchemaVersionKeys.size());

        // checks we can update schema meta data.
        SchemaMetadata currentSchemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaName)
                                                                   .getSchemaMetadata();
        SchemaMetadata schemaMetadataToUpdateTo = new SchemaMetadata.Builder(currentSchemaMetadata).validationLevel(SchemaValidationLevel.LATEST)
                                                                                                   .build();
        SchemaMetadataInfo updatedSchemaMetadata = schemaRegistryClient.updateSchemaMetadata(schemaName, schemaMetadataToUpdateTo);

        Assert.assertEquals(SchemaValidationLevel.LATEST, updatedSchemaMetadata.getSchemaMetadata()
                                                                               .getValidationLevel());
    }


}
