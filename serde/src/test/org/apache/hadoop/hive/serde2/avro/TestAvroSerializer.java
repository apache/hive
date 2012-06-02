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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestAvroSerializer {

  private Schema buildSchema(String recordValues) {
    String s = "{\n" +
        "  \"namespace\": \"org.apache.hadoop.hive\",\n" +
        "  \"name\": \"test_serializer\",\n" +
        "  \"type\": \"record\",\n" +
        "  \"fields\": [" +
        recordValues +
        "  ] }";
    return Schema.parse(s);
  }

  /**
   * Verify that we can serialize an avro value by taking one, running it through
   * the deser process and then serialize it again.
   */
  private GenericRecord serializeAndDeserialize(String recordValue,
       String fieldName, Object fieldValue) throws SerDeException, IOException {
    Schema s = buildSchema(recordValue);
    GenericData.Record r = new GenericData.Record(s);
    r.put(fieldName, fieldValue);

    AvroSerializer as = new AvroSerializer();

    AvroDeserializer ad = new AvroDeserializer();
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);
    ObjectInspector oi = aoig.getObjectInspector();
    List<String> columnNames = aoig.getColumnNames();
    List<TypeInfo> columnTypes = aoig.getColumnTypes();

    AvroGenericRecordWritable agrw = Utils.serializeAndDeserializeRecord(r);
    Object obj = ad.deserialize(columnNames, columnTypes, agrw, s);

    Writable result = as.serialize(obj, oi, columnNames, columnTypes, s);
    assertTrue(result instanceof AvroGenericRecordWritable);
    GenericRecord r2 = ((AvroGenericRecordWritable) result).getRecord();
    assertEquals(s, r2.getSchema());
    return r2;
  }

  @Test
  public void canSerializeStrings() throws SerDeException, IOException {
    singleFieldTest("string1", "hello", "string");
  }

  private void singleFieldTest(String fieldName, Object fieldValue, String fieldType)
          throws SerDeException, IOException {
    GenericRecord r2 = serializeAndDeserialize("{ \"name\":\"" + fieldName +
            "\", \"type\":\"" + fieldType + "\" }", fieldName, fieldValue);
    assertEquals(fieldValue, r2.get(fieldName));
  }

  @Test
  public void canSerializeInts() throws SerDeException, IOException {
    singleFieldTest("int1", 42, "int");
  }

  @Test
  public void canSerializeBooleans() throws SerDeException, IOException {
    singleFieldTest("boolean1", true, "boolean");
  }

  @Test
  public void canSerializeFloats() throws SerDeException, IOException {
    singleFieldTest("float1", 42.24342f, "float");
  }

  @Test
  public void canSerializeDoubles() throws SerDeException, IOException {
    singleFieldTest("double1", 24.00000001, "double");
  }

  @Test
  public void canSerializeLists() throws SerDeException, IOException {
    List<Integer> intList = new ArrayList<Integer>();
    Collections.addAll(intList, 1,2, 3);
    String field = "{ \"name\":\"list1\", \"type\":{\"type\":\"array\", \"items\":\"int\"} }";
    GenericRecord r = serializeAndDeserialize(field, "list1", intList);
    assertEquals(intList, r.get("list1"));
  }

  @Test
  public void canSerializeMaps() throws SerDeException, IOException {
    Map<String, Boolean> m = new Hashtable<String, Boolean>();
    m.put("yes", true);
    m.put("no", false);
    String field = "{ \"name\":\"map1\", \"type\":{\"type\":\"map\", \"values\":\"boolean\"} }";
    GenericRecord r = serializeAndDeserialize(field, "map1", m);

    assertEquals(m, r.get("map1"));
  }

  @Test
  public void canSerializeStructs() throws SerDeException {
    String field = "{ \"name\":\"struct1\", \"type\":{\"type\":\"record\", " +
            "\"name\":\"struct1_name\", \"fields\": [\n" +
                   "{ \"name\":\"sInt\", \"type\":\"int\" }, { \"name\"" +
            ":\"sBoolean\", \"type\":\"boolean\" }, { \"name\":\"sString\", \"type\":\"string\" } ] } }";

    Schema s = buildSchema(field);
    GenericData.Record innerRecord = new GenericData.Record(s.getField("struct1").schema());

    innerRecord.put("sInt", 77);
    innerRecord.put("sBoolean", false);
    innerRecord.put("sString", "tedious");

    GenericData.Record r = new GenericData.Record(s);
    r.put("struct1", innerRecord);

    AvroSerializer as = new AvroSerializer();

    AvroDeserializer ad = new AvroDeserializer();
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);
    ObjectInspector oi = aoig.getObjectInspector();
    List<String> columnNames = aoig.getColumnNames();
    List<TypeInfo> columnTypes = aoig.getColumnTypes();
    AvroGenericRecordWritable agrw = new AvroGenericRecordWritable(r);
    Object obj = ad.deserialize(columnNames, columnTypes, agrw, s);

    Writable result = as.serialize(obj, oi, columnNames, columnTypes, s);
    assertTrue(result instanceof AvroGenericRecordWritable);
    GenericRecord r2 = ((AvroGenericRecordWritable) result).getRecord();
    assertEquals(s, r2.getSchema());

    GenericRecord r3 = (GenericRecord)r2.get("struct1");
    assertEquals(77, r3.get("sInt"));
    assertEquals(false, r3.get("sBoolean"));
    assertEquals("tedious", r3.get("sString"));
  }

  @Test
  public void canSerializeUnions() throws SerDeException, IOException {
    String field = "{ \"name\":\"union1\", \"type\":[\"float\", \"boolean\", \"string\"] }";
    GenericRecord r = serializeAndDeserialize(field, "union1", 424.4f);
    assertEquals(424.4f, r.get("union1"));

    r = serializeAndDeserialize(field, "union1", true);
    assertEquals(true, r.get("union1"));

    r = serializeAndDeserialize(field, "union1", "hello");
    assertEquals("hello", r.get("union1"));
  }

  private enum enum1 {BLUE, RED , GREEN};
  @Test
  public void canSerializeEnums() throws SerDeException, IOException {
    for(enum1 e : enum1.values()) {
      String field = "{ \"name\":\"enum1\", \"type\":{\"type\":\"enum\", " +
              "\"name\":\"enum1_values\", \"symbols\":[\"BLUE\",\"RED\", \"GREEN\"]} }";
      GenericRecord r = serializeAndDeserialize(field, "enum1", e);

      assertEquals(e, enum1.valueOf(r.get("enum1").toString()));
    }

  }

  @Test
  public void canSerializeNullableTypes() throws SerDeException, IOException {
    String field = "{ \"name\":\"nullableint\", \"type\":[\"int\", \"null\"] }";
    GenericRecord r = serializeAndDeserialize(field, "nullableint", 42);
    assertEquals(42, r.get("nullableint"));

    r = serializeAndDeserialize(field, "nullableint", null);
    assertNull(r.get("nullableint"));
  }

  @Test
  public void canSerializeBytes() throws SerDeException, IOException {
    String field = "{ \"name\":\"bytes1\", \"type\":\"bytes\" }";
    ByteBuffer bb = ByteBuffer.wrap("easy as one two three".getBytes());
    bb.rewind();
    GenericRecord r = serializeAndDeserialize(field, "bytes1", bb);

    assertEquals(bb, r.get("bytes1"));
  }

  @Test
  public void canSerializeFixed() throws SerDeException, IOException {
    String field = "{ \"name\":\"fixed1\", \"type\":{\"type\":\"fixed\", " +
            "\"name\":\"threebytes\", \"size\":3} }";
    GenericData.Fixed fixed = new GenericData.Fixed(buildSchema(field), "k9@".getBytes());
    GenericRecord r = serializeAndDeserialize(field, "fixed1", fixed);

    assertArrayEquals(fixed.bytes(), ((GenericData.Fixed) r.get("fixed1")).bytes());
  }

}
