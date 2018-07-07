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

import org.apache.hadoop.hive.registry.client.ISchemaRegistryClient;
import org.apache.hadoop.hive.registry.serdes.avro.AvroSnapshotDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Map;

/**
 * This class can be configured as key or value deserializer for kafka consumer. This can be used like below with kafka consumers.
 *
 * <pre>{@code
        // consumer configs
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        // key deserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // schema registry config
        props.putAll(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryUrl));

        // configure reader versions for topics to be consumed.
        Map<String, Integer> readerVersions = new HashMap<>();
        readerVersions.put("clicks", 2);
        readerVersions.put("users", 1);
        props.put(KafkaAvroDeserializer.READER_VERSIONS, readerVersions);

        // value deserializer
        // current props are passed to KafkaAvroDeserializer instance by invoking #configure(Map, boolean) method.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());


        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);

 * }</pre>
 *
 */
public class KafkaAvroDeserializer implements Deserializer<Object> {

    /**
     * This property represents the version of a reader schema to be used in deserialization for each topic in
     * {@link #deserialize(String, byte[])}. This property should be passed with configs argument to {@link #configure(Map, boolean)}.
     * <p>
     * For example, to set reader version of different topics which should be handled by this Deserializer.
     * <pre>{@code
     * Map<String, Object> configs = ...
     * Map<String, Integer> readerVersions = new HashMap<>();
     * readerVersions.put("clicks", 2);
     * readerVersions.put("users", 1);
     * configs.put(READER_VERSIONS, readerVersions);
     * }</pre>
     */
    public static final String READER_VERSIONS = "schemaregistry.reader.schema.versions";

    private final AvroSnapshotDeserializer avroSnapshotDeserializer;
    private Map<String, Integer> readerVersions;

    public KafkaAvroDeserializer() {
        avroSnapshotDeserializer = new AvroSnapshotDeserializer();
    }

    public KafkaAvroDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        avroSnapshotDeserializer = new AvroSnapshotDeserializer(schemaRegistryClient);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        avroSnapshotDeserializer.init(configs);
        Map<String, Integer> versions = (Map<String, Integer>) ((Map<String, Object>) configs).get(READER_VERSIONS);
        readerVersions = versions != null ? versions : Collections.emptyMap();
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(data), readerVersions.get(topic));
    }

    @Override
    public void close() {
        try {
            avroSnapshotDeserializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
