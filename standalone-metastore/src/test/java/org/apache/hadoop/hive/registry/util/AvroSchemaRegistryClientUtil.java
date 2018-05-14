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

package org.apache.hadoop.hive.registry.util;

import org.apache.hadoop.hive.registry.serdes.Device;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hive.registry.AvroSchemaRegistryClientTest;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Random;

public final class AvroSchemaRegistryClientUtil {

  private AvroSchemaRegistryClientUtil() {

  }

  public static void assertAvroObjs(Object expected, Object given) {
    if (expected instanceof byte[]) {
      Assert.assertArrayEquals((byte[]) expected, (byte[]) given);
    } else if(expected instanceof SpecificRecord) {
      Assert.assertTrue(SpecificData.get().compare(expected, given, ((SpecificRecord) expected).getSchema()) == 0);
    } else {
      Assert.assertEquals(expected, given);
    }
  }

  public static Object[] generatePrimitivePayloads() {
    Random random = new Random();
    byte[] bytes = new byte[4];
    random.nextBytes(bytes);
    return new Object[]{random.nextBoolean(), random.nextDouble(), random.nextLong(), random.nextInt(), "String payload:" + new Date(), null};
  }

  public static Object createGenericRecordForCompatDevice() throws IOException {
    Schema schema = new Schema.Parser().parse(getSchema("/device-compat.avsc"));

    GenericRecord avroRecord = new GenericData.Record(schema);
    long now = System.currentTimeMillis();
    avroRecord.put("xid", now);
    avroRecord.put("name", "foo-" + now);
    avroRecord.put("version", new Random().nextInt());
    avroRecord.put("timestamp", now);
    avroRecord.put("make", "make-" + now);

    return avroRecord;
  }

  public static Object createGenericRecordForIncompatDevice() throws IOException {
    Schema schema = new Schema.Parser().parse(getSchema("/device-incompat.avsc"));

    GenericRecord avroRecord = new GenericData.Record(schema);
    long now = System.currentTimeMillis();
    avroRecord.put("xid", now);
    avroRecord.put("name", "foo-" + now);
    avroRecord.put("version", new Random().nextInt());
    avroRecord.put("mfr", "mfr-" + now);

    return avroRecord;
  }

  public static Object createGenericRecordForDevice() throws IOException {
    Schema schema = new Schema.Parser().parse(getSchema("/device.avsc"));

    GenericRecord avroRecord = new GenericData.Record(schema);
    long now = System.currentTimeMillis();
    avroRecord.put("xid", now);
    avroRecord.put("name", "foo-" + now);
    avroRecord.put("version", new Random().nextInt());
    avroRecord.put("timestamp", now);

    return avroRecord;
  }

  public static String getSchema(String schemaFileName) throws IOException {
    InputStream avroSchemaStream = AvroSchemaRegistryClientTest.class.getResourceAsStream(schemaFileName);
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    return parser.parse(avroSchemaStream).toString();
  }

  public static Device createSpecificRecord() {
    return Device.newBuilder().setName("device-" + System.currentTimeMillis())
            .setTimestamp(System.currentTimeMillis())
            .setVersion(new Random().nextInt())
            .setXid(new Random().nextLong()).build();
  }
}
