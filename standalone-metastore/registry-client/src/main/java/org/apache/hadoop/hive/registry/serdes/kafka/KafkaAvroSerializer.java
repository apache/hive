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

package org.apache.hadoop.hive.registry.serdes.kafka;

import org.apache.hadoop.hive.registry.SchemaCompatibility;
import org.apache.hadoop.hive.registry.SchemaMetadata;
import org.apache.hadoop.hive.registry.serdes.avro.AvroSchemaProvider;
import org.apache.hadoop.hive.registry.client.ISchemaRegistryClient;
import org.apache.hadoop.hive.registry.serdes.avro.AvroSnapshotSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 *
 */
public class KafkaAvroSerializer implements Serializer<Object> {

    /**
     * Compatibility property to be set on configs for registering message's schema to schema registry.
     */
    public static final String SCHEMA_COMPATIBILITY = "schema.compatibility";

    public static final String SCHEMA_GROUP = "schema.group";
    public static final String SCHEMA_NAME_KEY_SUFFIX_ = "schema.name.key.suffix";
    public static final String SCHEMA_NAME_VALUE_SUFFIX_= "schema.name.value.suffix";

    public static final String DEFAULT_SCHEMA_GROUP = "kafka";
    public static final String DEFAULT_SCHEMA_NAME_KEY_SUFFIX = ":k";
    public static final String DEFAULT_SCHEMA_NAME_VALUE_SUFFIX = null;

    private final AvroSnapshotSerializer avroSnapshotSerializer;
    private boolean isKey;

    private SchemaCompatibility compatibility;

    private String schemaGroup;
    private String schemaNameKeySuffix;
    private String schemaNameValueSuffix;


    public KafkaAvroSerializer() {
        avroSnapshotSerializer = new AvroSnapshotSerializer();
    }

    public KafkaAvroSerializer(ISchemaRegistryClient schemaRegistryClient) {
        avroSnapshotSerializer = new AvroSnapshotSerializer(schemaRegistryClient);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        compatibility = (SchemaCompatibility) configs.get(SCHEMA_COMPATIBILITY);

        schemaGroup = getOrDefault(configs, SCHEMA_GROUP, DEFAULT_SCHEMA_GROUP);
        schemaNameKeySuffix = getOrDefault(configs, SCHEMA_NAME_KEY_SUFFIX_, DEFAULT_SCHEMA_NAME_KEY_SUFFIX);
        schemaNameValueSuffix = getOrDefault(configs, SCHEMA_NAME_VALUE_SUFFIX_, DEFAULT_SCHEMA_NAME_VALUE_SUFFIX);

        this.isKey = isKey;

        avroSnapshotSerializer.init(configs);
    }

    private static String getOrDefault(Map<String, ?> configs, String key, String defaultValue) {
        String value = (String) configs.get(key);
        if (value == null || value.trim().isEmpty()) {
            value = defaultValue;
        }
        return value;
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return avroSnapshotSerializer.serialize(data, createSchemaMetadata(topic));
    }

    private SchemaMetadata createSchemaMetadata(String topic) {
        SchemaMetadata schemaMetadata = getSchemaKey(topic, isKey);
        String description = "Schema registered by KafkaAvroSerializer for topic: [" + topic + "] iskey: [" + isKey + "]";
        return new SchemaMetadata.Builder(schemaMetadata).description(description).compatibility(compatibility).build();
    }

    public SchemaMetadata getSchemaKey(String topic, boolean isKey) {
        String name = topic;
        if(isKey) {
            if (schemaNameKeySuffix != null ) {
                name += schemaNameKeySuffix;
            }
        } else {
            if (schemaNameValueSuffix != null ) {
                name += schemaNameValueSuffix;
            }
        }
        // there wont be any naming collisions as kafka does not allow character `:` in a topic name.
        return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup(schemaGroup).build();
    }

    @Override
    public void close() {
        try {
            avroSnapshotSerializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
