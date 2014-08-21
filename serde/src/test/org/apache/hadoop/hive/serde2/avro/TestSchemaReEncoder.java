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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.junit.Test;

public class TestSchemaReEncoder {

  @Test
  public void schemasCanAddFields() throws SerDeException {
    String original = "{\n" +
        "    \"namespace\": \"org.apache.hadoop.hive\",\n" +
        "    \"name\": \"Line\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"text\",\n" +
        "            \"type\":\"string\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
    String evolved = "{\n" +
        "    \"namespace\": \"org.apache.hadoop.hive\",\n" +
        "    \"name\": \"Line\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"text\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"new_kid\",\n" +
        "            \"type\":\"string\",\n" +
        "            \"default\":\"Hi!\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
    Schema originalSchema = AvroSerdeUtils.getSchemaFor(original);
    Schema evolvedSchema = AvroSerdeUtils.getSchemaFor(evolved);

    GenericRecord record = new GenericData.Record(originalSchema);
    record.put("text", "it is a far better thing I do, yadda, yadda");
    assertTrue(GenericData.get().validate(originalSchema, record));
    AvroDeserializer.SchemaReEncoder schemaReEncoder = new AvroDeserializer.SchemaReEncoder(record.getSchema(), evolvedSchema);
    GenericRecord r2 = schemaReEncoder.reencode(record);

    assertTrue(GenericData.get().validate(evolvedSchema, r2));
    assertEquals("Hi!", r2.get("new_kid").toString());

    // Now make sure that we can re-use the re-encoder against a completely
    // different record to save resources
    String original2 = "{\n" +
        "    \"namespace\": \"somebody.else\",\n" +
        "    \"name\": \"something_else\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"a\",\n" +
        "            \"type\":\"int\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
    String evolved2 = "{\n" +
        "    \"namespace\": \"somebody.else\",\n" +
        "    \"name\": \"something_else\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"a\",\n" +
        "            \"type\":\"int\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"b\",\n" +
        "            \"type\":\"long\",\n" +
        "            \"default\":42\n" +
        "        }\n" +
        "    ]\n" +
        "}";
    Schema originalSchema2 = AvroSerdeUtils.getSchemaFor(original2);
    Schema evolvedSchema2 = AvroSerdeUtils.getSchemaFor(evolved2);

    record = new GenericData.Record(originalSchema2);
    record.put("a", 19);
    assertTrue(GenericData.get().validate(originalSchema2, record));

    schemaReEncoder = new AvroDeserializer.SchemaReEncoder(record.getSchema(), evolvedSchema2);
    r2 = schemaReEncoder.reencode(record);
    assertTrue(GenericData.get().validate(evolvedSchema2, r2));
    assertEquals(42l, r2.get("b"));
  }
}
