/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.hadoop.hive.metastore.registry.client;

import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import mockit.Expectations;
import mockit.Tested;
import org.junit.Test;

import java.util.Collections;

import static mockit.Deencapsulation.invoke;

/**
 *
 */
public class SchemaRegistryClientTest {

    @Tested
    private SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), "some-url"));

    @Test
    public void testClientWithCache() throws Exception {

        final String schemaName = "foo";
        final SchemaMetadata schemaMetaData = new SchemaMetadata.Builder(schemaName).schemaGroup("group").type("type").build();
        final SchemaVersion schemaVersion = new SchemaVersion("schema-text", "desc");
        final SchemaIdVersion schemaIdVersion = new SchemaIdVersion(1L, 1);

        new Expectations(schemaRegistryClient) {{
            invoke(schemaRegistryClient, "registerSchemaMetadata", schemaMetaData);
            result = 1L;

            invoke(schemaRegistryClient, "doAddSchemaVersion", SchemaBranch.MASTER_BRANCH, schemaName, schemaVersion);
            result = schemaIdVersion;
            times = 1; // this should be invoked only once as this should have been cached

        }};

        Long metadataId = schemaRegistryClient.registerSchemaMetadata(schemaMetaData);
        schemaRegistryClient.addSchemaVersion(schemaMetaData, schemaVersion);
        schemaRegistryClient.addSchemaVersion(schemaMetaData, schemaVersion);
        schemaRegistryClient.addSchemaVersion(schemaName, schemaVersion);
    }
}
