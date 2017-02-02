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

import org.apache.avro.generic.GenericArray;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimalV1;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
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
    return AvroSerdeUtils.getSchemaFor(s);
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
    singleFieldTest("string1", "hello", "\"string\"");
  }

  private void singleFieldTest(String fieldName, Object fieldValue, String fieldType)
          throws SerDeException, IOException {
    GenericRecord r2 = serializeAndDeserialize("{ \"name\":\"" + fieldName +
            "\", \"type\":" + fieldType + " }", fieldName, fieldValue);
    assertEquals(fieldValue, r2.get(fieldName));
  }

  @Test
  public void canSerializeInts() throws SerDeException, IOException {
    singleFieldTest("int1", 42, "\"int\"");
  }

  @Test
  public void canSerializeBooleans() throws SerDeException, IOException {
    singleFieldTest("boolean1", true, "\"boolean\"");
  }

  @Test
  public void canSerializeFloats() throws SerDeException, IOException {
    singleFieldTest("float1", 42.24342f, "\"float\"");
  }

  @Test
  public void canSerializeDoubles() throws SerDeException, IOException {
    singleFieldTest("double1", 24.00000001, "\"double\"");
  }

  @Test
  public void canSerializeDecimals() throws SerDeException, IOException {
    ByteBuffer bb = ByteBuffer.wrap(HiveDecimal.create("3.1416").bigIntegerBytes());
    singleFieldTest("dec1", bb.rewind(),
        "{\"type\":\"bytes\", \"logicalType\":\"decimal\", \"precision\":5, \"scale\":4}");
  }

  @Test
  public void canSerializeLists() throws SerDeException, IOException {
    List<Integer> intList = new ArrayList<Integer>();
    Collections.addAll(intList, 1,2, 3);
    String field = "{ \"name\":\"list1\", \"type\":{\"type\":\"array\", \"items\":\"int\"} }";
    GenericRecord r = serializeAndDeserialize(field, "list1", intList);
    final Object list1 = r.get("list1");
    Assert.assertTrue(list1 instanceof GenericArray);
    Assert.assertTrue(list1 instanceof List);
    assertEquals(intList, list1);
  }

  @Test
  public void canSerializeListOfDecimals() throws SerDeException, IOException {
    List<Buffer> bbList = new ArrayList<Buffer>();
    String[] decs = new String[] {"3.1416", "4.7779", "0.2312", "9.1000", "5.5555"};
    for (int i = 0; i < decs.length; i++) {
      bbList.add(AvroSerdeUtils.getBufferFromDecimal(HiveDecimal.create(decs[i]), 4));
    }
    String field = "{ \"name\":\"list1\", \"type\":{\"type\":\"array\"," +
        " \"items\":{\"type\":\"bytes\", \"logicalType\":\"decimal\", \"precision\":5, \"scale\":4}} }";
    GenericRecord r = serializeAndDeserialize(field, "list1", bbList);
    assertEquals(bbList, r.get("list1"));
  }

  @Test
  public void canSerializeMaps() throws SerDeException, IOException {
    Map<String, Boolean> m = new HashMap<String, Boolean>();
    m.put("yes", true);
    m.put("no", false);
    String field = "{ \"name\":\"map1\", \"type\":{\"type\":\"map\", \"values\":\"boolean\"} }";
    GenericRecord r = serializeAndDeserialize(field, "map1", m);

    assertEquals(m, r.get("map1"));
  }

  @Test
  public void canSerializeMapOfDecimals() throws SerDeException, IOException {
    Map<String, Buffer> m = new HashMap<String, Buffer>();
    m.put("yes", AvroSerdeUtils.getBufferFromDecimal(HiveDecimal.create("3.14"), 4));
    m.put("no", AvroSerdeUtils.getBufferFromDecimal(HiveDecimal.create("6.2832732"), 4));
    String field = "{ \"name\":\"map1\", \"type\":{\"type\":\"map\"," +
        " \"values\":{\"type\":\"bytes\", \"logicalType\":\"decimal\", \"precision\":5, \"scale\":4}} }";
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
    agrw.setFileSchema(r.getSchema());
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
    String field = "{ \"name\":\"union1\", \"type\":[\"float\", \"boolean\", \"string\"," +
        " {\"type\":\"bytes\", \"logicalType\":\"decimal\", \"precision\":5, \"scale\":4}] }";
    GenericRecord r = serializeAndDeserialize(field, "union1", 424.4f);
    assertEquals(424.4f, r.get("union1"));

    r = serializeAndDeserialize(field, "union1", true);
    assertEquals(true, r.get("union1"));

    r = serializeAndDeserialize(field, "union1", "hello");
    assertEquals("hello", r.get("union1"));

    HiveDecimal dec = HiveDecimal.create("3.1415926");
    r = serializeAndDeserialize(field, "union1", AvroSerdeUtils.getBufferFromDecimal(dec, 4));
    HiveDecimal dec1 = AvroSerdeUtils.getHiveDecimalFromByteBuffer((ByteBuffer) r.get("union1"), 4);

    // For now, old class.
    HiveDecimalV1 oldDec = HiveDecimalV1.create(dec.bigDecimalValue());
    assertEquals(oldDec.setScale(4).toString(), dec1.toString());
  }

  private enum enum1 {BLUE, RED , GREEN};
  @Test
  public void canSerializeEnums() throws SerDeException, IOException {
    String type = "{\"type\": \"enum\", \"name\": \"enum1_values\", " +
            "\"symbols\":[\"BLUE\",\"RED\",\"GREEN\"]}";
    Schema schema = AvroSerdeUtils.getSchemaFor(type);
    String field = "{ \"name\":\"enum1\", \"type\": " + schema + " }";
    for(enum1 e : enum1.values()) {
      GenericEnumSymbol symbol = new GenericData.EnumSymbol(schema, e.toString());
      GenericRecord r = serializeAndDeserialize(field, "enum1", symbol);

      assertEquals(e, enum1.valueOf(r.get("enum1").toString()));
    }

  }

  @Test
  public void canSerializeNullableEnums() throws SerDeException, IOException {
    String type = "{\"type\": \"enum\", \"name\": \"enum1_values\",\n" +
            "  \"namespace\": \"org.apache.hadoop.hive\",\n" +
            "  \"symbols\":[\"BLUE\",\"RED\",\"GREEN\"]}";
    Schema schema = AvroSerdeUtils.getSchemaFor(type);
    String field = "{ \"name\":\"nullableenum\", \"type\": [\"null\", " + schema + "] }";
    GenericEnumSymbol symbol = new GenericData.EnumSymbol(schema, enum1.BLUE.toString());
    GenericRecord r = serializeAndDeserialize(field, "nullableenum", symbol);
    assertEquals(enum1.BLUE, enum1.valueOf(r.get("nullableenum").toString()));

    r = serializeAndDeserialize(field, "nullableenum", null);
    assertNull(r.get("nullableenum"));
  }

  @Test
  public void canSerializeNullablePrimitiveTypes() throws SerDeException, IOException {
    String field = "{ \"name\":\"nullableint\", \"type\":[\"int\", \"null\"] }";
    GenericRecord r = serializeAndDeserialize(field, "nullableint", 42);
    assertEquals(42, r.get("nullableint"));

    r = serializeAndDeserialize(field, "nullableint", null);
    assertNull(r.get("nullableint"));
  }

  @Test 
  public void canSerializeMapsWithNullablePrimitiveValues() throws SerDeException, IOException {
    String field = "{ \"name\":\"mapWithNulls\", \"type\": " +
            "{\"type\":\"map\", \"values\": [\"null\", \"boolean\"]} }";

    Map<String, Boolean> m = new HashMap<String, Boolean>();
    m.put("yes", true);
    m.put("no", false);
    m.put("maybe", null);
    GenericRecord r = serializeAndDeserialize(field, "mapWithNulls", m);

    Object result = r.get("mapWithNulls");
    assertEquals(m, result);
  }

  @Test
  public void canSerializeNullableRecords() throws SerDeException, IOException {
    String field = "{ \"name\":\"nullableStruct\", \"type\": [\"null\", {\"type\":\"record\", " +
            "\"name\":\"struct1_name\", \"fields\": [\n" +
              "{ \"name\":\"sInt\", \"type\":\"int\" }, " +
              "{ \"name\":\"sBoolean\", \"type\":\"boolean\" }, " +
              "{ \"name\":\"sString\", \"type\":\"string\" } ] }] }";

    Schema s = buildSchema(field);
    Schema nullable = s.getField("nullableStruct").schema();
    assertTrue(AvroSerdeUtils.isNullableType(nullable));
    GenericData.Record innerRecord =
            new GenericData.Record(AvroSerdeUtils.getOtherTypeFromNullableType(nullable));

    innerRecord.put("sInt", 77);
    innerRecord.put("sBoolean", false);
    innerRecord.put("sString", "tedious");

    GenericRecord r = serializeAndDeserialize(field, "nullableStruct", innerRecord);
    Object result = r.get("nullableStruct");
    assertNotSame(innerRecord, result);
    assertEquals(innerRecord, result);

    r = serializeAndDeserialize(field, "nullableStruct", null);
    assertNull(r.get("nullableStruct"));
  }

  @Test
  public void canSerializeNullableLists() throws SerDeException, IOException {
    List<Integer> intList = new ArrayList<Integer>();
    Collections.addAll(intList, 1,2, 3);
    String field = "{ \"name\":\"nullableList\", \"type\": [\"null\", " +
            "{\"type\":\"array\", \"items\":\"int\"}] }";
    GenericRecord r = serializeAndDeserialize(field, "nullableList", intList);
    Object result = r.get("nullableList");
    assertNotSame(intList, result);
    assertEquals(intList, result);

    r = serializeAndDeserialize(field, "nullableList", null);
    assertNull(r.get("nullableList"));
  }

  @Test
  public void canSerializeNullableMaps() throws SerDeException, IOException {
    String field = "{ \"name\":\"nullableMap\", \"type\": [\"null\", " +
            "{\"type\":\"map\", \"values\":\"boolean\"}] }";

    Map<String, Boolean> m = new HashMap<String, Boolean>();
    m.put("yes", true);
    m.put("no", false);
    GenericRecord r = serializeAndDeserialize(field, "nullableMap", m);

    Object result = r.get("nullableMap");
    assertNotSame(m, result);
    assertEquals(m, result);

    r = serializeAndDeserialize(field, "nullableMap", null);
    assertNull(r.get("nullableMap"));
  }

  @Test
  public void canSerializeNullableFixed() throws SerDeException, IOException {
    String field = "{ \"name\":\"nullableFixed\", \"type\": [\"null\", " +
            "{\"type\":\"fixed\", \"name\":\"threebytes\", \"size\":3}] }";
    Schema s = buildSchema(field);
    Schema nullable = s.getField("nullableFixed").schema();
    assertTrue(AvroSerdeUtils.isNullableType(nullable));

    GenericData.Fixed fixed = new GenericData.Fixed(
            AvroSerdeUtils.getOtherTypeFromNullableType(nullable), "k9@".getBytes());
    GenericRecord r = serializeAndDeserialize(field, "nullableFixed", fixed);

    GenericData.Fixed result = (GenericData.Fixed) r.get("nullableFixed");
    assertNotSame(fixed, result);
    assertArrayEquals(fixed.bytes(), result.bytes());

    r = serializeAndDeserialize(field, "nullableFixed", null);
    assertNull(r.get("nullableFixed"));
  }

  @Test
  public void canSerializeNullableBytes() throws SerDeException, IOException {
    String field = "{ \"name\":\"nullableBytes\", \"type\":[\"null\", \"bytes\"] }";
    ByteBuffer bb = ByteBuffer.wrap("easy as one two three".getBytes());
    bb.rewind();
    GenericRecord r = serializeAndDeserialize(field, "nullableBytes", bb);

    Object result = r.get("nullableBytes");
    assertNotSame(bb, result);
    assertEquals(bb, result);

    r = serializeAndDeserialize(field, "nullableBytes", null);
    assertNull(r.get("nullableBytes"));
  }

  @Test
  public void canSerializeNullableDecimals() throws SerDeException, IOException {
    String field = "{ \"name\":\"nullableBytes\", \"type\":[\"null\", " +
        "{\"type\":\"bytes\", \"logicalType\":\"decimal\", \"precision\":5, \"scale\":4}] }";
    Buffer bb = AvroSerdeUtils.getBufferFromDecimal(HiveDecimal.create("3.1416"), 4);
    GenericRecord r = serializeAndDeserialize(field, "nullableBytes", bb);

    Object result = r.get("nullableBytes");
    assertNotSame(bb, result);
    assertEquals(bb, result);

    r = serializeAndDeserialize(field, "nullableBytes", null);
    assertNull(r.get("nullableBytes"));
  }

  @Test
  public void canSerializeArraysWithNullablePrimitiveElements() throws SerDeException, IOException {
    final String field = "{ \"name\":\"listWithNulls\", \"type\": " +
            "{\"type\":\"array\", \"items\": [\"null\", \"int\"]} }";
    List<Integer> intList = new ArrayList<Integer>();
    Collections.addAll(intList, 1,2, null, 3);
    GenericRecord r = serializeAndDeserialize(field, "listWithNulls", intList);
    Object result = r.get("listWithNulls");
    assertNotSame(intList, result);
    assertEquals(intList, result);
  }

  @Test
  public void canSerializeArraysWithNullableComplexElements() throws SerDeException, IOException {
    final String field = "{ \"name\":\"listOfNullableLists\", \"type\": " +
            "{\"type\":\"array\", \"items\": [\"null\", " +
              "{\"type\": \"array\", \"items\": \"int\"}]} }";
    List<List<Integer>> intListList = new ArrayList<List<Integer>>();
    List<Integer>  intList = new ArrayList<Integer>();
    Collections.addAll(intList, 1,2,3);
    Collections.addAll(intListList, intList, null);
    GenericRecord r = serializeAndDeserialize(field, "listOfNullableLists", intListList);
    Object result = r.get("listOfNullableLists");
    assertNotSame(intListList, result);
    assertEquals(intListList, result);
  }

  @Test
  public void canSerializeRecordsWithNullableComplexElements() throws SerDeException, IOException {
    String field = "{ \"name\":\"struct1\", \"type\":{\"type\":\"record\", " +
            "\"name\":\"struct1_name\", \"fields\": [\n" +
                   "{ \"name\":\"sInt\", \"type\":\"int\" }, { \"name\"" +
            ":\"sBoolean\", \"type\":\"boolean\" }, { \"name\":\"nullableList\", \"type\":[\"null\", " +
            "{ \"type\":\"array\", \"items\":\"int\"}] } ] } }";

    Schema s = buildSchema(field);
    GenericData.Record innerRecord = new GenericData.Record(s.getField("struct1").schema());

    innerRecord.put("sInt", 77);
    innerRecord.put("sBoolean", false);
    List<Integer> intList = new ArrayList<Integer>();
    Collections.addAll(intList, 1,2,3);
    innerRecord.put("nullableList", intList);

    GenericRecord r = serializeAndDeserialize(field, "struct1", innerRecord);
    Object result = r.get("struct1");
    assertNotSame(innerRecord, result);
    assertEquals(innerRecord, result);

    innerRecord.put("nullableList", null);
    r = serializeAndDeserialize(field, "struct1", innerRecord);
    result = r.get("struct1");
    assertNotSame(innerRecord, result);
    assertEquals(innerRecord, result);
  }

  @Test
  public void canSerializeMapsWithNullableComplexValues() throws SerDeException, IOException {
    String field = "{ \"name\":\"mapWithNullableLists\", \"type\": " +
            "{\"type\":\"map\", \"values\": [\"null\", " +
              "{\"type\": \"array\", \"items\": \"int\"}]} }";

    Map<String, List<Integer>> m = new HashMap<String, List<Integer>>();
    List<Integer> intList = new ArrayList<Integer>();
    Collections.addAll(intList, 1,2,3);
    m.put("list", intList);
    m.put("null", null);
    GenericRecord r = serializeAndDeserialize(field, "mapWithNullableLists", m);

    Object result = r.get("mapWithNullableLists");
    assertNotSame(m, result);
    assertEquals(m, result);
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
