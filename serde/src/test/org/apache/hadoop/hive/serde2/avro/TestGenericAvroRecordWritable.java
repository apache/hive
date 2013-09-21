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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.rmi.server.UID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class TestGenericAvroRecordWritable {
  private static final String schemaJSON = "{\n" +
      "    \"namespace\": \"gallifrey\",\n" +
      "    \"name\": \"TestPerson\",\n" +
      "    \"type\": \"record\",\n" +
      "    \"fields\": [\n" +
      "        {\n" +
      "            \"name\":\"first\",\n" +
      "            \"type\":\"string\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\":\"last\",\n" +
      "            \"type\":\"string\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";

  @Test
  public void writableContractIsImplementedCorrectly() throws IOException {
    Schema schema = Schema.parse(schemaJSON);

    GenericRecord gr = new GenericData.Record(schema);
    gr.put("first", "The");
    gr.put("last", "Doctor");

    assertEquals("The", gr.get("first"));
    assertEquals("Doctor", gr.get("last"));

    AvroGenericRecordWritable garw = new AvroGenericRecordWritable(gr);
    garw.setRecordReaderID(new UID());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream daos = new DataOutputStream(baos);
    garw.write(daos);

    AvroGenericRecordWritable garw2 = new AvroGenericRecordWritable(gr);
    garw2.setRecordReaderID(new UID());

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dais = new DataInputStream(bais);

    garw2.readFields(dais);

    GenericRecord gr2 = garw2.getRecord();

    assertEquals("The", gr2.get("first").toString());
    assertEquals("Doctor", gr2.get("last").toString());
  }
}
