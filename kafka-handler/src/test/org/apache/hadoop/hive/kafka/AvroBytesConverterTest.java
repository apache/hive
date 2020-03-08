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

package org.apache.hadoop.hive.kafka;

import com.google.common.collect.Maps;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.avro.Schema;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.SerDeException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Test class for Hive Kafka Avro SerDe with variable bytes skipped.
 */
public class AvroBytesConverterTest {
  private static SimpleRecord simpleRecord = SimpleRecord.newBuilder().setId("123").setName("test").build();
  private static byte[] simpleRecordConfluentBytes;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Use the KafkaAvroSerializer from Confluent to serialize the simpleRecord.
   */
  @BeforeClass
  public static void setUp() {
    Map<String, String> config = Maps.newHashMap();
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(new MockSchemaRegistryClient());
    avroSerializer.configure(config, false);
    simpleRecordConfluentBytes = avroSerializer.serialize("temp", simpleRecord);
  }

  private void runConversionTest(Properties tbl, byte[] serializedSimpleRecord) throws SerDeException {
    KafkaSerDe serde = new KafkaSerDe();
    Schema schema = SimpleRecord.getClassSchema();
    KafkaSerDe.AvroBytesConverter conv = (KafkaSerDe.AvroBytesConverter)serde.getByteConverterForAvroDelegate(
        schema, tbl);
    AvroGenericRecordWritable simpleRecordWritable = conv.getWritable(serializedSimpleRecord);

    Assert.assertNotNull(simpleRecordWritable);
    Assert.assertEquals(SimpleRecord.class, simpleRecordWritable.getRecord().getClass());

    SimpleRecord simpleRecordDeserialized = (SimpleRecord) simpleRecordWritable.getRecord();

    Assert.assertNotNull(simpleRecordDeserialized);
    Assert.assertEquals(simpleRecord, simpleRecordDeserialized);
  }

  /**
   * Tests the default case of no skipped bytes per record works properly.
   */
  @Test
  public void convertWithAvroBytesConverter() throws SerDeException {
    // Since the serialized version was created by Confluent,
    // let's remove the first five bytes to get the actual message.
    int recordLength = simpleRecordConfluentBytes.length;
    byte[] simpleRecordWithNoOffset = Arrays.copyOfRange(simpleRecordConfluentBytes, 5, recordLength);

    Properties tbl = new Properties();
    tbl.setProperty(AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_TYPE.getPropName(), "NONE");

    runConversionTest(tbl, simpleRecordWithNoOffset);
  }

  /**
   * Tests that the skip converter skips 5 bytes properly, which matches what Confluent needs.
   */
  @Test
  public void convertWithConfluentAvroBytesConverter() throws SerDeException {
    Integer offset = 5;

    Properties tbl = new Properties();
    tbl.setProperty(AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_TYPE.getPropName(), "SKIP");
    tbl.setProperty(AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_SKIP_BYTES.getPropName(), offset.toString());

    runConversionTest(tbl, simpleRecordConfluentBytes);
  }

  /**
   * Tests that the skip converter skips a custom number of bytes properly.
   */
  @Test
  public void convertWithCustomAvroSkipBytesConverter() throws SerDeException {
    Integer offset = 2;
    // Remove all but two bytes of the five byte offset which Confluent adds,
    // to simulate a message with only 2 bytes in front of each message.
    int recordLength = simpleRecordConfluentBytes.length;
    byte[] simpleRecordAsOffsetBytes = Arrays.copyOfRange(simpleRecordConfluentBytes, 5 - offset, recordLength);

    Properties tbl = new Properties();
    tbl.setProperty(AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_TYPE.getPropName(), "SKIP");
    tbl.setProperty(AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_SKIP_BYTES.getPropName(), offset.toString());

    runConversionTest(tbl, simpleRecordAsOffsetBytes);
  }

  /**
   * Test that when we skip more bytes than are in the message, we throw an exception properly.
   */
  @Test
  public void skipBytesLargerThanMessageSizeConverter() throws SerDeException {
    // The simple record we are serializing is two strings, that combine to be 7 characters or 14 bytes.
    // Adding in the 5 byte offset, we get 19 bytes. To make sure we go bigger than that, we are setting
    // the offset to ten times that value.
    Integer offset = 190;

    Properties tbl = new Properties();
    tbl.setProperty(AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_TYPE.getPropName(), "SKIP");
    tbl.setProperty(AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_SKIP_BYTES.getPropName(), offset.toString());

    exception.expect(RuntimeException.class);
    exception.expectMessage("org.apache.hadoop.hive.serde2.SerDeException: " +
        "Skip bytes value is larger than the message length.");
    runConversionTest(tbl, simpleRecordConfluentBytes);
  }

  /**
  * Test that we properly parse the converter type, no matter the casing.
  */
  @Test
  public void bytesConverterTypeParseTest() {
    Map<String, KafkaSerDe.BytesConverterType> testCases = new HashMap<String, KafkaSerDe.BytesConverterType>() {{
        put("skip", KafkaSerDe.BytesConverterType.SKIP);
        put("sKIp", KafkaSerDe.BytesConverterType.SKIP);
        put("SKIP", KafkaSerDe.BytesConverterType.SKIP);
        put("   skip   ", KafkaSerDe.BytesConverterType.SKIP);
        put("SKIP1", KafkaSerDe.BytesConverterType.NONE);
        put("skipper", KafkaSerDe.BytesConverterType.NONE);
        put("", KafkaSerDe.BytesConverterType.NONE);
        put(null, KafkaSerDe.BytesConverterType.NONE);
        put("none", KafkaSerDe.BytesConverterType.NONE);
        put("NONE", KafkaSerDe.BytesConverterType.NONE);
      }};

    for(Map.Entry<String, KafkaSerDe.BytesConverterType> entry: testCases.entrySet()) {
      Assert.assertEquals(entry.getValue(), KafkaSerDe.BytesConverterType.fromString(entry.getKey()));
    }
  }
}
