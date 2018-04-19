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

import static org.apache.hadoop.hive.registry.serdes.avro.AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.Resources;
import org.apache.hadoop.hive.registry.client.SchemaRegistryClient;
import org.apache.hadoop.hive.registry.serdes.SerDesProtocolHandlerRegistry;
import org.apache.hadoop.hive.registry.serdes.kafka.KafkaAvroDeserializer;
import org.apache.hadoop.hive.registry.serdes.kafka.KafkaAvroSerializer;
import org.apache.hadoop.hive.registry.webservice.LocalSchemaRegistryServer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ConfluentProtocolCompatibleTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentProtocolCompatibleTest.class);
    public static final String GENERIC_TEST_RECORD_SCHEMA =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"org.apache.hadoop.hive.registry.serdes.avro\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}";

    @Test
    public void testConfluentProduceRegistryConsume() throws Exception {
        String configPath = new File(Resources.getResource("schema-registry-test.yaml").toURI()).getAbsolutePath();
        LocalSchemaRegistryServer localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath);
        try {
            localSchemaRegistryServer.start();

            final String confluentUrl = String.format("http://localhost:%d/api/v1/confluent", localSchemaRegistryServer.getLocalPort());
            final String registryUrl = String.format("http://localhost:%d/api/v1", localSchemaRegistryServer.getLocalPort());
            
            Map<String, Object> confluentConfig = new HashMap<>();
            confluentConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, confluentUrl);

            Map<String, Object> registryConfig = new HashMap<>();
            registryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryUrl);

            Schema schema = new Schema.Parser().parse(GENERIC_TEST_RECORD_SCHEMA);

            GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();


            io.confluent.kafka.serializers.KafkaAvroSerializer kafkaAvroSerializer = new io.confluent.kafka.serializers.KafkaAvroSerializer();
            kafkaAvroSerializer.configure(confluentConfig, false);
            byte[] bytes = kafkaAvroSerializer.serialize("topic", record);

            io.confluent.kafka.serializers.KafkaAvroDeserializer confluentKafkaAvroDeserializer = new io.confluent.kafka.serializers.KafkaAvroDeserializer();
            confluentKafkaAvroDeserializer.configure(confluentConfig, false);

            GenericRecord confluentResult = (GenericRecord) confluentKafkaAvroDeserializer.deserialize("topic", bytes);
            LOG.info(confluentResult.toString());
            
            KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
            kafkaAvroDeserializer.configure(registryConfig, false);

            GenericRecord registryResult = (GenericRecord) kafkaAvroDeserializer.deserialize("topic", bytes);

            LOG.info(registryResult.toString());

            Assert.assertEquals(record, registryResult);
            Assert.assertEquals(record, confluentResult);

            Assert.assertEquals(registryResult, confluentResult);

        } finally {
            localSchemaRegistryServer.stop();
        }
    }

    @Test
    public void testRegistryProduceConfluentConsume() throws Exception {
        String configPath = new File(Resources.getResource("schema-registry-test.yaml").toURI()).getAbsolutePath();
        LocalSchemaRegistryServer localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath);
        try {
            localSchemaRegistryServer.start();

            final String confluentUrl = String.format("http://localhost:%d/api/v1/confluent", localSchemaRegistryServer.getLocalPort());
            final String registryUrl = String.format("http://localhost:%d/api/v1", localSchemaRegistryServer.getLocalPort());

            Map<String, Object> confluentConfig = new HashMap<>();
            confluentConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, confluentUrl);

            Map<String, Object> registryConfig = new HashMap<>();
            registryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryUrl);
            registryConfig.put(SERDES_PROTOCOL_VERSION, SerDesProtocolHandlerRegistry.CONFLUENT_VERSION_PROTOCOL);
            
            Schema schema = new Schema.Parser().parse(GENERIC_TEST_RECORD_SCHEMA);
            GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();


            KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
            kafkaAvroSerializer.configure(registryConfig, false);
            byte[] bytes = kafkaAvroSerializer.serialize("topic", record);

            KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
            kafkaAvroDeserializer.configure(registryConfig, false);

            GenericRecord registryResult = (GenericRecord) kafkaAvroDeserializer.deserialize("topic", bytes);
            LOG.info(registryResult.toString());


            io.confluent.kafka.serializers.KafkaAvroDeserializer confluentKafkaAvroDeserializer = new io.confluent.kafka.serializers.KafkaAvroDeserializer();
            confluentKafkaAvroDeserializer.configure(confluentConfig, false);

            GenericRecord confluentResult = (GenericRecord) confluentKafkaAvroDeserializer.deserialize("topic", bytes);
            LOG.info(confluentResult.toString());

            Assert.assertEquals(record, registryResult);
            Assert.assertEquals(record, confluentResult);

            Assert.assertEquals(registryResult, confluentResult);


        } finally {
            localSchemaRegistryServer.stop();
        }
    }
    
}
