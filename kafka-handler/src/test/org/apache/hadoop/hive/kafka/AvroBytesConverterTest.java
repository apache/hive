/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.kafka;

import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

/**
 * Test class for Hive Kafka Avro bytes converter.
 */
public class AvroBytesConverterTest {
  private static SimpleRecord simpleRecord1 = SimpleRecord.newBuilder().setId("123").setName("test").build();
  private static byte[] simpleRecord1AsBytes;

  /**
   * Emulate confluent avro producer that add 4 magic bits (int) before value bytes. The int represents the schema ID from schema registry.
   */
  @BeforeClass
  public static void setUp() {
    Map<String, String> config = Maps.newHashMap();
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(new MockSchemaRegistryClient());
    avroSerializer.configure(config, false);
    simpleRecord1AsBytes = avroSerializer.serialize("temp", simpleRecord1);
  }

  /**
   * Emulate - avro.serde.type = none (Default).
   */
  @Test
  public void convertWithAvroBytesConverter() {
    Schema schema = SimpleRecord.getClassSchema();
    KafkaSerDe.AvroBytesConverter conv = new KafkaSerDe.AvroBytesConverter(schema);
    AvroGenericRecordWritable simpleRecord1Writable = conv.getWritable(simpleRecord1AsBytes);

    Assert.assertNotNull(simpleRecord1Writable);
    Assert.assertEquals(SimpleRecord.class, simpleRecord1Writable.getRecord().getClass());

    SimpleRecord simpleRecord1Deserialized = (SimpleRecord) simpleRecord1Writable.getRecord();

    Assert.assertNotNull(simpleRecord1Deserialized);
    Assert.assertNotEquals(simpleRecord1, simpleRecord1Deserialized);
  }

  /**
   * Emulate - avro.serde.type = confluent.
   */
  @Test
  public void convertWithConfluentAvroBytesConverter() {
    Schema schema = SimpleRecord.getClassSchema();
    KafkaSerDe.AvroSkipBytesConverter conv = new KafkaSerDe.AvroSkipBytesConverter(schema, 5);
    AvroGenericRecordWritable simpleRecord1Writable = conv.getWritable(simpleRecord1AsBytes);

    Assert.assertNotNull(simpleRecord1Writable);
    Assert.assertEquals(SimpleRecord.class, simpleRecord1Writable.getRecord().getClass());

    SimpleRecord simpleRecord1Deserialized = (SimpleRecord) simpleRecord1Writable.getRecord();

    Assert.assertNotNull(simpleRecord1Deserialized);
    Assert.assertEquals(simpleRecord1, simpleRecord1Deserialized);
  }

  /**
   * Emulate - avro.serde.type = skip.
   */
  @Test
  public void convertWithCustomAvroSkipBytesConverter() {
    int offset = 2;
    byte[] simpleRecordAsOffsetBytes = Arrays.copyOfRange(simpleRecord1AsBytes, 5 - offset, simpleRecord1AsBytes.length);

    Schema schema = SimpleRecord.getClassSchema();
    KafkaSerDe.AvroSkipBytesConverter conv = new KafkaSerDe.AvroSkipBytesConverter(schema, offset);
    AvroGenericRecordWritable simpleRecord1Writable = conv.getWritable(simpleRecordAsOffsetBytes);

    Assert.assertNotNull(simpleRecord1Writable);
    Assert.assertEquals(SimpleRecord.class, simpleRecord1Writable.getRecord().getClass());

    SimpleRecord simpleRecord1Deserialized = (SimpleRecord) simpleRecord1Writable.getRecord();

    Assert.assertNotNull(simpleRecord1Deserialized);
    Assert.assertEquals(simpleRecord1, simpleRecord1Deserialized);
  }

  @Test
  public void enumParseTest() {
    Assert.assertEquals(KafkaSerDe.BytesConverterType.CONFLUENT, 
      KafkaSerDe.BytesConverterType.fromString("confluent"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.CONFLUENT, 
      KafkaSerDe.BytesConverterType.fromString("conFLuent"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.CONFLUENT, 
      KafkaSerDe.BytesConverterType.fromString("Confluent"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.CONFLUENT, 
      KafkaSerDe.BytesConverterType.fromString("CONFLUENT"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.SKIP, KafkaSerDe.BytesConverterType.fromString("skip"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.SKIP, KafkaSerDe.BytesConverterType.fromString("sKIp"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.SKIP, KafkaSerDe.BytesConverterType.fromString("SKIP"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.NONE, KafkaSerDe.BytesConverterType.fromString("SKIP1"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.NONE, KafkaSerDe.BytesConverterType.fromString("skipper"));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.NONE, KafkaSerDe.BytesConverterType.fromString(""));
    Assert.assertEquals(KafkaSerDe.BytesConverterType.NONE, KafkaSerDe.BytesConverterType.fromString(null));
  }
}
