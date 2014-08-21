/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestThatEvolvedSchemasActAsWeWant {

  @Test
  public void resolvedSchemasShouldReturnReaderSchema() throws IOException {
    // Need to verify that when reading a datum with an updated reader schema
    // that the datum then returns the reader schema as its own, since we
    // depend on this behavior in order to avoid re-encoding the datum
    // in the serde.
    String v0 = "{\n" +
        "    \"namespace\": \"org.apache.hadoop.hive\",\n" +
        "    \"name\": \"SomeStuff\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"v0\",\n" +
        "            \"type\":\"string\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
     String v1 = "{\n" +
        "    \"namespace\": \"org.apache.hadoop.hive\",\n" +
        "    \"name\": \"SomeStuff\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"v0\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"v1\",\n" +
        "            \"type\":\"string\",\n" +
        "            \"default\":\"v1_default\"" +
        "        }\n" +
        "    ]\n" +
        "}";

    Schema[] schemas = {AvroSerdeUtils.getSchemaFor(v0), AvroSerdeUtils.getSchemaFor(v1)};

    // Encode a schema with v0, write out.
    GenericRecord record = new GenericData.Record(schemas[0]);
    record.put("v0", "v0 value");
    assertTrue(GenericData.get().validate(schemas[0], record));

    // Write datum out to a stream
    GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<GenericRecord>(schemas[0]);
    DataFileWriter<GenericRecord> dfw = new DataFileWriter<GenericRecord>(gdw);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    dfw.create(schemas[0], baos);
    dfw.append(record);
    dfw.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>();
    gdr.setExpected(schemas[1]);
    DataFileStream<GenericRecord> dfs = new DataFileStream<GenericRecord>(bais, gdr);
    assertTrue(dfs.hasNext());
    GenericRecord next = dfs.next();
    assertEquals("v0 value", next.get("v0").toString());
    assertEquals("v1_default", next.get("v1").toString());

    // Now the most important check - when we query this record for its schema,
    // we should get back the latest, reader schema:
    assertEquals(schemas[1], next.getSchema());
  }
}
