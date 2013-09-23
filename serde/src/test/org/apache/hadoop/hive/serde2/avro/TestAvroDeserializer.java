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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.junit.Test;

public class TestAvroDeserializer {
  private final GenericData GENERIC_DATA = GenericData.get();

  @Test
  public void canDeserializeVoidType() throws IOException, SerDeException {
    String schemaString = "{\n" +
        "  \"type\": \"record\", \n" +
        "  \"name\": \"nullTest\",\n" +
        "  \"fields\" : [\n" +
        "    {\"name\": \"isANull\", \"type\": \"null\"}\n" +
        "  ]\n" +
        "}";
    Schema s = Schema.parse(schemaString);
    GenericData.Record record = new GenericData.Record(s);

    record.put("isANull", null);
    assertTrue(GENERIC_DATA.validate(s, record));

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();

    ArrayList<Object> row = (ArrayList<Object>)de.deserialize(aoig.getColumnNames(),
            aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    Object theVoidObject = row.get(0);
    assertNull(theVoidObject);

    StandardStructObjectInspector oi = (StandardStructObjectInspector)aoig.getObjectInspector();
    StructField fieldRef = oi.getStructFieldRef("isANull");

    Object shouldBeNull = oi.getStructFieldData(row, fieldRef);
    assertNull(shouldBeNull);
    assertTrue(fieldRef.getFieldObjectInspector() instanceof VoidObjectInspector);
  }

  @Test
  public void canDeserializeMapsWithPrimitiveKeys() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.MAP_WITH_PRIMITIVE_VALUE_TYPE);
    GenericData.Record record = new GenericData.Record(s);

    Map<String, Long> m = new Hashtable<String, Long>();
    m.put("one", 1l);
    m.put("two", 2l);
    m.put("three", 3l);

    record.put("aMap", m);
    assertTrue(GENERIC_DATA.validate(s, record));
    System.out.println("record = " + record);

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();

    ArrayList<Object> row = (ArrayList<Object>)de.deserialize(aoig.getColumnNames(),
            aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    Object theMapObject = row.get(0);
    assertTrue(theMapObject instanceof Map);
    Map theMap = (Map)theMapObject;

    // Verify the raw object that's been created
    assertEquals(1l, theMap.get("one"));
    assertEquals(2l, theMap.get("two"));
    assertEquals(3l, theMap.get("three"));

    // Verify that the provided object inspector can pull out these same values
    StandardStructObjectInspector oi =
            (StandardStructObjectInspector)aoig.getObjectInspector();

    List<Object> z = oi.getStructFieldsDataAsList(row);
    assertEquals(1, z.size());
    StructField fieldRef = oi.getStructFieldRef("amap");

    Map theMap2 = (Map)oi.getStructFieldData(row, fieldRef);
    assertEquals(1l, theMap2.get("one"));
    assertEquals(2l, theMap2.get("two"));
    assertEquals(3l, theMap2.get("three"));
  }

  @Test
  public void canDeserializeArrays() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.ARRAY_WITH_PRIMITIVE_ELEMENT_TYPE);
    GenericData.Record record = new GenericData.Record(s);

    List<String> list = new ArrayList<String>();
    list.add("Eccleston");
    list.add("Tennant");
    list.add("Smith");

    record.put("anArray", list);
    assertTrue(GENERIC_DATA.validate(s, record));
    System.out.println("Array-backed record = " + record);

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();
    ArrayList<Object> row = (ArrayList<Object>)de.deserialize(aoig.getColumnNames(),
            aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    Object theArrayObject = row.get(0);
    assertTrue(theArrayObject instanceof List);
    List theList = (List)theArrayObject;

    // Verify the raw object that's been created
    assertEquals("Eccleston", theList.get(0));
    assertEquals("Tennant", theList.get(1));
    assertEquals("Smith", theList.get(2));

    // Now go the correct way, through objectinspectors
    StandardStructObjectInspector oi =
            (StandardStructObjectInspector)aoig.getObjectInspector();
    StructField fieldRefToArray = oi.getStructFieldRef("anArray");

    Object anArrayData = oi.getStructFieldData(row, fieldRefToArray);
    StandardListObjectInspector anArrayOI =
            (StandardListObjectInspector)fieldRefToArray.getFieldObjectInspector();
    assertEquals(3, anArrayOI.getListLength(anArrayData));

    JavaStringObjectInspector elementOI =
            (JavaStringObjectInspector)anArrayOI.getListElementObjectInspector();

    Object firstElement = anArrayOI.getListElement(anArrayData, 0);
    assertEquals("Eccleston", elementOI.getPrimitiveJavaObject(firstElement));
    assertTrue(firstElement instanceof String);

    Object secondElement = anArrayOI.getListElement(anArrayData, 1);
    assertEquals("Tennant", elementOI.getPrimitiveJavaObject(secondElement));
    assertTrue(secondElement instanceof String);

    Object thirdElement = anArrayOI.getListElement(anArrayData, 2);
    assertEquals("Smith", elementOI.getPrimitiveJavaObject(thirdElement));
    assertTrue(thirdElement instanceof String);

  }

  @Test
  public void canDeserializeRecords() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.RECORD_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);
    GenericData.Record innerRecord = new GenericData.Record(s.getField("aRecord").schema());
    innerRecord.put("int1", 42);
    innerRecord.put("boolean1", true);
    innerRecord.put("long1", 42432234234l);
    record.put("aRecord", innerRecord);
    assertTrue(GENERIC_DATA.validate(s, record));

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();
    ArrayList<Object> row =
            (ArrayList<Object>)de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    Object theRecordObject = row.get(0);
    System.out.println("theRecordObject = " + theRecordObject.getClass().getCanonicalName());

    // The original record was lost in the deserialization, so just go the
    // correct way, through objectinspectors
    StandardStructObjectInspector oi = (StandardStructObjectInspector)aoig.getObjectInspector();
    List<? extends StructField> allStructFieldRefs = oi.getAllStructFieldRefs();
    assertEquals(1, allStructFieldRefs.size());
    StructField fieldRefForaRecord = allStructFieldRefs.get(0);
    assertEquals("arecord", fieldRefForaRecord.getFieldName());
    Object innerRecord2 = oi.getStructFieldData(row, fieldRefForaRecord);

    // Extract innerRecord field refs
    StandardStructObjectInspector innerRecord2OI =
            (StandardStructObjectInspector) fieldRefForaRecord.getFieldObjectInspector();

    List<? extends StructField> allStructFieldRefs1 = innerRecord2OI.getAllStructFieldRefs();
    assertEquals(3, allStructFieldRefs1.size());
    assertEquals("int1", allStructFieldRefs1.get(0).getFieldName());
    assertEquals("boolean1", allStructFieldRefs1.get(1).getFieldName());
    assertEquals("long1", allStructFieldRefs1.get(2).getFieldName());

    innerRecord2OI.getStructFieldsDataAsList(innerRecord2);
    assertEquals(42, innerRecord2OI.getStructFieldData(innerRecord2, allStructFieldRefs1.get(0)));
    assertEquals(true, innerRecord2OI.getStructFieldData(innerRecord2, allStructFieldRefs1.get(1)));
    assertEquals(42432234234l, innerRecord2OI.getStructFieldData(innerRecord2, allStructFieldRefs1.get(2)));
  }

  private class ResultPair { // Because Pairs give Java the vapors.
    public final ObjectInspector oi;
    public final Object value;
    public final Object unionObject;

    private ResultPair(ObjectInspector oi, Object value, Object unionObject) {
      this.oi = oi;
      this.value = value;
      this.unionObject = unionObject;
    }
  }

  @Test
  public void canDeserializeUnions() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.UNION_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);

    record.put("aUnion", "this is a string");

    ResultPair result = unionTester(s, record);
    assertTrue(result.value instanceof String);
    assertEquals("this is a string", result.value);
    UnionObjectInspector uoi = (UnionObjectInspector)result.oi;
    assertEquals(1, uoi.getTag(result.unionObject));

    // Now the other enum possibility
    record = new GenericData.Record(s);
    record.put("aUnion", 99);
    result = unionTester(s, record);
    assertTrue(result.value instanceof Integer);
    assertEquals(99, result.value);
    uoi = (UnionObjectInspector)result.oi;
    assertEquals(0, uoi.getTag(result.unionObject));
  }

  private ResultPair unionTester(Schema s, GenericData.Record record)
          throws SerDeException, IOException {
    assertTrue(GENERIC_DATA.validate(s, record));
    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();
    ArrayList<Object> row =
            (ArrayList<Object>)de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    StandardStructObjectInspector oi = (StandardStructObjectInspector)aoig.getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    assertEquals(1, fieldRefs.size());
    StructField fieldRef = fieldRefs.get(0);
    assertEquals("aunion", fieldRef.getFieldName());
    Object theUnion = oi.getStructFieldData(row, fieldRef);

    assertTrue(fieldRef.getFieldObjectInspector() instanceof UnionObjectInspector);
    UnionObjectInspector fieldObjectInspector =
            (UnionObjectInspector)fieldRef.getFieldObjectInspector();
    Object value = fieldObjectInspector.getField(theUnion);

    return new ResultPair(fieldObjectInspector, value, theUnion);
  }

  @Test // Enums are one of two types we fudge for Hive. Enums go in, Strings come out.
  public void canDeserializeEnums() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.ENUM_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);

    record.put("baddies", new GenericData.EnumSymbol(s.getField("baddies").schema(),"DALEKS"));
    assertTrue(GENERIC_DATA.validate(s, record));

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();
    ArrayList<Object> row =
            (ArrayList<Object>)de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    StandardStructObjectInspector oi = (StandardStructObjectInspector)aoig.getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    assertEquals(1, fieldRefs.size());
    StructField fieldRef = fieldRefs.get(0);

    assertEquals("baddies", fieldRef.getFieldName());

    Object theStringObject = oi.getStructFieldData(row, fieldRef);
    assertTrue(fieldRef.getFieldObjectInspector() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)fieldRef.getFieldObjectInspector();

    String finalValue = soi.getPrimitiveJavaObject(theStringObject);
    assertEquals("DALEKS", finalValue);
  }

  @Test // Fixed doesn't exist in Hive. Fixeds go in, lists of bytes go out.
  public void canDeserializeFixed() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.FIXED_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);

    byte [] bytes = "ANANCIENTBLUEBOX".getBytes();
    record.put("hash", new GenericData.Fixed(s, bytes));
    assertTrue(GENERIC_DATA.validate(s, record));

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();
    ArrayList<Object> row =
            (ArrayList<Object>)de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    Object byteObject = row.get(0);
    assertTrue(byteObject instanceof byte[]);
    byte[] outBytes = (byte[]) byteObject;
    // Verify the raw object that's been created
    for(int i = 0; i < bytes.length; i++) {
      assertEquals(bytes[i], outBytes[i]);
    }

    // Now go the correct way, through objectinspectors
    StandardStructObjectInspector oi = (StandardStructObjectInspector)aoig.getObjectInspector();
    List<Object> fieldsDataAsList = oi.getStructFieldsDataAsList(row);
    assertEquals(1, fieldsDataAsList.size());
    StructField fieldRef = oi.getStructFieldRef("hash");

    outBytes = (byte[]) oi.getStructFieldData(row, fieldRef);
    for(int i = 0; i < outBytes.length; i++) {
      assertEquals(bytes[i], outBytes[i]);
    }
  }

  @Test
  public void canDeserializeBytes() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.BYTES_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);

    byte [] bytes = "ANANCIENTBLUEBOX".getBytes();

    ByteBuffer bb = ByteBuffer.wrap(bytes);
    bb.rewind();
    record.put("bytesField", bb);
    assertTrue(GENERIC_DATA.validate(s, record));

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();
    ArrayList<Object> row =
            (ArrayList<Object>)de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    Object byteObject = row.get(0);
    assertTrue(byteObject instanceof byte[]);
    byte[] outBytes = (byte[]) byteObject;
    // Verify the raw object that's been created
    for(int i = 0; i < bytes.length; i++) {
      assertEquals(bytes[i], outBytes[i]);
    }

    // Now go the correct way, through objectinspectors
    StandardStructObjectInspector oi = (StandardStructObjectInspector)aoig.getObjectInspector();
    List<Object> fieldsDataAsList = oi.getStructFieldsDataAsList(row);
    assertEquals(1, fieldsDataAsList.size());
    StructField fieldRef = oi.getStructFieldRef("bytesField");

    outBytes = (byte[]) oi.getStructFieldData(row, fieldRef);
    for(int i = 0; i < outBytes.length; i++) {
      assertEquals(bytes[i], outBytes[i]);
    }
  }

  @Test
  public void canDeserializeNullableTypes() throws IOException, SerDeException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.NULLABLE_STRING_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);
    record.put("nullableString", "this is a string");

    verifyNullableType(record, s, "nullableString", "this is a string");

    record = new GenericData.Record(s);
    record.put("nullableString", null);
    verifyNullableType(record, s, "nullableString", null);
  }

   @Test
   public void canDeserializeNullableEnums() throws IOException, SerDeException {
     Schema s = Schema.parse(TestAvroObjectInspectorGenerator.NULLABLE_ENUM_SCHEMA);
     GenericData.Record record = new GenericData.Record(s);
     record.put("nullableEnum", new GenericData.EnumSymbol(AvroSerdeUtils.getOtherTypeFromNullableType(s.getField("nullableEnum").schema()), "CYBERMEN"));

     verifyNullableType(record, s, "nullableEnum", "CYBERMEN");

     record = new GenericData.Record(s);
     record.put("nullableEnum", null);
     verifyNullableType(record, s, "nullableEnum", null);
   }

  @Test
  public void canDeserializeMapWithNullablePrimitiveValues() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.MAP_WITH_NULLABLE_PRIMITIVE_VALUE_TYPE_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);

    Map<String, Long> m = new HashMap<String, Long>();
    m.put("one", 1l);
    m.put("two", 2l);
    m.put("three", 3l);
    m.put("mu", null);

    record.put("aMap", m);
    assertTrue(GENERIC_DATA.validate(s, record));
    System.out.println("record = " + record);

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();

    ArrayList<Object> row = (ArrayList<Object>)de.deserialize(aoig.getColumnNames(),
            aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    Object theMapObject = row.get(0);
    assertTrue(theMapObject instanceof Map);
    Map theMap = (Map)theMapObject;

    // Verify the raw object that's been created
    assertEquals(1l, theMap.get("one"));
    assertEquals(2l, theMap.get("two"));
    assertEquals(3l, theMap.get("three"));
    assertTrue(theMap.containsKey("mu"));
    assertEquals(null, theMap.get("mu"));

    // Verify that the provided object inspector can pull out these same values
    StandardStructObjectInspector oi =
            (StandardStructObjectInspector)aoig.getObjectInspector();

    List<Object> z = oi.getStructFieldsDataAsList(row);
    assertEquals(1, z.size());
    StructField fieldRef = oi.getStructFieldRef("amap");

    Map theMap2 = (Map)oi.getStructFieldData(row, fieldRef);
    assertEquals(1l, theMap2.get("one"));
    assertEquals(2l, theMap2.get("two"));
    assertEquals(3l, theMap2.get("three"));
    assertTrue(theMap2.containsKey("mu"));
    assertEquals(null, theMap2.get("mu"));
  }

  private void verifyNullableType(GenericData.Record record, Schema s, String fieldName,
                                  String expected) throws SerDeException, IOException {
    assertTrue(GENERIC_DATA.validate(s, record));

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();
    ArrayList<Object> row =
            (ArrayList<Object>)de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);
    assertEquals(1, row.size());
    Object rowElement = row.get(0);

    StandardStructObjectInspector oi = (StandardStructObjectInspector)aoig.getObjectInspector();
    List<Object> fieldsDataAsList = oi.getStructFieldsDataAsList(row);
    assertEquals(1, fieldsDataAsList.size());
    StructField fieldRef = oi.getStructFieldRef(fieldName);
    ObjectInspector fieldObjectInspector = fieldRef.getFieldObjectInspector();
    StringObjectInspector soi = (StringObjectInspector)fieldObjectInspector;

    if(expected == null) {
      assertNull(soi.getPrimitiveJavaObject(rowElement));
    } else {
      assertEquals(expected, soi.getPrimitiveJavaObject(rowElement));
    }
  }

  @Test
  public void verifyCaching() throws SerDeException, IOException {
    Schema s = Schema.parse(TestAvroObjectInspectorGenerator.RECORD_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);
    GenericData.Record innerRecord = new GenericData.Record(s.getField("aRecord").schema());
    innerRecord.put("int1", 42);
    innerRecord.put("boolean1", true);
    innerRecord.put("long1", 42432234234l);
    record.put("aRecord", innerRecord);
    assertTrue(GENERIC_DATA.validate(s, record));

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);
    UID recordReaderID = new UID();
    garw.setRecordReaderID(recordReaderID);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroDeserializer de = new AvroDeserializer();
    ArrayList<Object> row =
        (ArrayList<Object>) de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);

    assertEquals(1, de.getNoEncodingNeeded().size());
    assertEquals(0, de.getReEncoderCache().size());

    // Read the record with the same record reader ID
    row = (ArrayList<Object>) de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);

    //Expecting not to change the size of internal structures
    assertEquals(1, de.getNoEncodingNeeded().size());
    assertEquals(0, de.getReEncoderCache().size());

    //Read the record with **different** record reader ID
    garw.setRecordReaderID(new UID()); //New record reader ID
    row = (ArrayList<Object>) de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, s);

    //Expecting to change the size of internal structures
    assertEquals(2, de.getNoEncodingNeeded().size());
    assertEquals(0, de.getReEncoderCache().size());

  //Read the record with **different** record reader ID and **evolved** schema
    Schema evolvedSchema = Schema.parse(s.toString());
    evolvedSchema.getField("aRecord").schema().addProp("Testing", "meaningless");
    garw.setRecordReaderID(recordReaderID = new UID()); //New record reader ID
    row =
            (ArrayList<Object>)de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, evolvedSchema);

    //Expecting to change the size of internal structures
    assertEquals(2, de.getNoEncodingNeeded().size());
    assertEquals(1, de.getReEncoderCache().size());

  //Read the record with existing record reader ID and same **evolved** schema
    garw.setRecordReaderID(recordReaderID); //Reuse record reader ID
    row =
            (ArrayList<Object>)de.deserialize(aoig.getColumnNames(), aoig.getColumnTypes(), garw, evolvedSchema);

    //Expecting NOT to change the size of internal structures
    assertEquals(2, de.getNoEncodingNeeded().size());
    assertEquals(1, de.getReEncoderCache().size());

  }
}
