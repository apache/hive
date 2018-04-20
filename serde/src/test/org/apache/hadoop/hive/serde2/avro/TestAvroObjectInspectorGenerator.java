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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.junit.Test;

public class TestAvroObjectInspectorGenerator {
  private static final TypeInfo STRING = TypeInfoFactory.getPrimitiveTypeInfo("string");
  private static final TypeInfo INT = TypeInfoFactory.getPrimitiveTypeInfo("int");
  private static final TypeInfo BOOLEAN = TypeInfoFactory.getPrimitiveTypeInfo("boolean");
  private static final TypeInfo LONG = TypeInfoFactory.getPrimitiveTypeInfo("bigint");
  private static final TypeInfo FLOAT = TypeInfoFactory.getPrimitiveTypeInfo("float");
  private static final TypeInfo DOUBLE = TypeInfoFactory.getPrimitiveTypeInfo("double");
  private static final TypeInfo VOID = TypeInfoFactory.getPrimitiveTypeInfo("void");

  // These schemata are used in other tests
  static public final String MAP_WITH_PRIMITIVE_VALUE_TYPE = "{\n" +
      "  \"namespace\": \"testing\",\n" +
      "  \"name\": \"oneMap\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aMap\",\n" +
      "      \"type\":{\"type\":\"map\",\n" +
      "      \"values\":\"long\"}\n" +
      "\t}\n" +
      "  ]\n" +
      "}";
  static public final String ARRAY_WITH_PRIMITIVE_ELEMENT_TYPE = "{\n" +
      "  \"namespace\": \"testing\",\n" +
      "  \"name\": \"oneArray\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"anArray\",\n" +
      "      \"type\":{\"type\":\"array\",\n" +
      "      \"items\":\"string\"}\n" +
      "\t}\n" +
      "  ]\n" +
      "}";
  public static final String RECORD_SCHEMA = "{\n" +
      "  \"namespace\": \"testing.test.mctesty\",\n" +
      "  \"name\": \"oneRecord\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aRecord\",\n" +
      "      \"type\":{\"type\":\"record\",\n" +
      "              \"name\":\"recordWithinARecord\",\n" +
      "              \"fields\": [\n" +
      "                 {\n" +
      "                  \"name\":\"int1\",\n" +
      "                  \"type\":\"int\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"boolean1\",\n" +
      "                  \"type\":\"boolean\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"long1\",\n" +
      "                  \"type\":\"long\"\n" +
      "                }\n" +
      "      ]}\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String NULLABLE_RECORD_SCHEMA = "[\"null\", " + RECORD_SCHEMA + "]";
  public static final String SINGLE_ITEM_UNION_SCHEMA = "{\n" +
      "  \"namespace\": \"test.a.rossa\",\n" +
      "  \"name\": \"oneUnion\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aUnion\",\n" +
      "      \"type\":[\"string\"]\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String UNION_SCHEMA = "{\n" +
      "  \"namespace\": \"test.a.rossa\",\n" +
      "  \"name\": \"oneUnion\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aUnion\",\n" +
      "      \"type\":[\"int\", \"string\"]\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String UNION_SCHEMA_2 = "{\n" +
          "  \"namespace\": \"test.a.rossa\",\n" +
          "  \"name\": \"oneUnion\",\n" +
          "  \"type\": \"record\",\n" +
          "  \"fields\": [\n" +
          "    {\n" +
          "      \"name\":\"aUnion\",\n" +
          "      \"type\":[\"null\", \"int\", \"string\"]\n" +
          "    }\n" +
          "  ]\n" +
          "}";
  public static final String UNION_SCHEMA_3 = "{\n" +
          "  \"namespace\": \"test.a.rossa\",\n" +
          "  \"name\": \"oneUnion\",\n" +
          "  \"type\": \"record\",\n" +
          "  \"fields\": [\n" +
          "    {\n" +
          "      \"name\":\"aUnion\",\n" +
          "      \"type\":[\"float\",\"int\"]\n" +
          "    }\n" +
          "  ]\n" +
          "}";
  public static final String UNION_SCHEMA_4 = "{\n" +
          "  \"namespace\": \"test.a.rossa\",\n" +
          "  \"name\": \"oneUnion\",\n" +
          "  \"type\": \"record\",\n" +
          "  \"fields\": [\n" +
          "    {\n" +
          "      \"name\":\"aUnion\",\n" +
          "      \"type\":[\"int\",\"float\",\"long\"]\n" +
          "    }\n" +
          "  ]\n" +
          "}";
  public static final String ENUM_SCHEMA = "{\n" +
      "  \"namespace\": \"clever.namespace.name.in.space\",\n" +
      "  \"name\": \"oneEnum\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "   {\n" +
      "      \"name\":\"baddies\",\n" +
      "      \"type\":{\"type\":\"enum\",\"name\":\"villians\", \"symbols\": " +
          "[\"DALEKS\", \"CYBERMEN\", \"SLITHEEN\", \"JAGRAFESS\"]}\n" +
      "      \n" +
      "      \n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String FIXED_SCHEMA = "{\n" +
      "  \"namespace\": \"ecapseman\",\n" +
      "  \"name\": \"oneFixed\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "   {\n" +
      "      \"name\":\"hash\",\n" +
      "      \"type\":{\"type\": \"fixed\", \"name\": \"MD5\", \"size\": 16}\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String NULLABLE_STRING_SCHEMA = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"nullableUnionTest\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"nullableString\", \"type\":[\"null\", \"string\"]}\n" +
      "  ]\n" +
      "}";
  public static final String MAP_WITH_NULLABLE_PRIMITIVE_VALUE_TYPE_SCHEMA = "{\n" +
      "  \"namespace\": \"testing\",\n" +
      "  \"name\": \"mapWithNullableUnionTest\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aMap\",\n" +
      "      \"type\":{\"type\":\"map\",\n" +
      "      \"values\":[\"null\",\"long\"]}\n" +
      "\t}\n" +
      "  ]\n" +
      "}";
  public static final String NULLABLE_ENUM_SCHEMA = "{\n" +
      "  \"namespace\": \"clever.namespace.name.in.space\",\n" +
      "  \"name\": \"nullableUnionTest\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "   {\n" +
      "      \"name\":\"nullableEnum\",\n" +
      "      \"type\": [\"null\", {\"type\":\"enum\",\"name\":\"villians\", \"symbols\": " +
          "[\"DALEKS\", \"CYBERMEN\", \"SLITHEEN\", \"JAGRAFESS\"]}]\n" +
      "      \n" +
      "      \n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String BYTES_SCHEMA = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"bytesTest\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"bytesField\", \"type\":\"bytes\"}\n" +
      "  ]\n" +
      "}";

  public static final String KITCHEN_SINK_SCHEMA = "{\n" +
      "  \"namespace\": \"org.apache.hadoop.hive\",\n" +
      "  \"name\": \"kitchsink\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"string1\",\n" +
      "      \"type\":\"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"string2\",\n" +
      "      \"type\":\"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"int1\",\n" +
      "      \"type\":\"int\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"boolean1\",\n" +
      "      \"type\":\"boolean\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"long1\",\n" +
      "      \"type\":\"long\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"float1\",\n" +
      "      \"type\":\"float\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"double1\",\n" +
      "      \"type\":\"double\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"inner_record1\",\n" +
      "      \"type\":{ \"type\":\"record\",\n" +
      "               \"name\":\"inner_record1_impl\",\n" +
      "               \"fields\": [\n" +
      "                          {\"name\":\"int_in_inner_record1\",\n" +
      "                           \"type\":\"int\"},\n" +
      "                          {\"name\":\"string_in_inner_record1\",\n" +
      "                           \"type\":\"string\"}\n" +
      "                         ]\n" +
      "       }\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"enum1\",\n" +
      "      \"type\":{\"type\":\"enum\", \"name\":\"enum1_values\", " +
          "\"symbols\":[\"ENUM1_VALUES_VALUE1\",\"ENUM1_VALUES_VALUE2\", \"ENUM1_VALUES_VALUE3\"]}\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"array1\",\n" +
      "      \"type\":{\"type\":\"array\", \"items\":\"string\"}\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"map1\",\n" +
      "      \"type\":{\"type\":\"map\", \"values\":\"string\"}\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"union1\",\n" +
      "      \"type\":[\"float\", \"boolean\", \"string\"]\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"fixed1\",\n" +
      "      \"type\":{\"type\":\"fixed\", \"name\":\"fourbytes\", \"size\":4}\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"null1\",\n" +
      "      \"type\":\"null\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"UnionNullInt\",\n" +
      "      \"type\":[\"int\", \"null\"]\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"bytes1\",\n" +
      "      \"type\":\"bytes\"\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  @Test // that we can only process records
  public void failOnNonRecords() throws Exception {
    String nonRecordSchema = "{ \"type\": \"enum\",\n" +
        "  \"name\": \"Suit\",\n" +
        "  \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\n" +
        "}";

    Schema s = AvroSerdeUtils.getSchemaFor(nonRecordSchema);
    try {
      new AvroObjectInspectorGenerator(s);
      fail("Should not be able to handle non-record Avro types");
    } catch(SerDeException sde) {
      assertTrue(sde.getMessage().startsWith("Schema for table must be of type RECORD"));
    }
  }

  @Test
  public void primitiveTypesWorkCorrectly() throws SerDeException {
    final String bunchOfPrimitives = "{\n" +
        "  \"namespace\": \"testing\",\n" +
        "  \"name\": \"PrimitiveTypes\",\n" +
        "  \"type\": \"record\",\n" +
        "  \"fields\": [\n" +
        "    {\n" +
        "      \"name\":\"aString\",\n" +
        "      \"type\":\"string\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\":\"anInt\",\n" +
        "      \"type\":\"int\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\":\"aBoolean\",\n" +
        "      \"type\":\"boolean\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\":\"aLong\",\n" +
        "      \"type\":\"long\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\":\"aFloat\",\n" +
        "      \"type\":\"float\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\":\"aDouble\",\n" +
        "      \"type\":\"double\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\":\"aNull\",\n" +
        "      \"type\":\"null\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(AvroSerdeUtils.getSchemaFor(bunchOfPrimitives));

    String [] expectedColumnNames = {"aString", "anInt", "aBoolean", "aLong", "aFloat", "aDouble", "aNull"};
    verifyColumnNames(expectedColumnNames, aoig.getColumnNames());

    TypeInfo [] expectedColumnTypes = {STRING, INT, BOOLEAN, LONG, FLOAT, DOUBLE, VOID};
    verifyColumnTypes(expectedColumnTypes, aoig.getColumnTypes());

    // Rip apart the object inspector, making sure we got what we expect.
    final ObjectInspector oi = aoig.getObjectInspector();
    assertTrue(oi instanceof StandardStructObjectInspector);
    final StandardStructObjectInspector ssoi = (StandardStructObjectInspector)oi;
    List<? extends StructField> structFields = ssoi.getAllStructFieldRefs();
    assertEquals(expectedColumnNames.length, structFields.size());

    for(int i = 0; i < expectedColumnNames.length;i++) {
      assertEquals("Column names don't match",
              expectedColumnNames[i].toLowerCase(), structFields.get(i).getFieldName());
      assertEquals("Column types don't match",
              expectedColumnTypes[i].getTypeName(),
              structFields.get(i).getFieldObjectInspector().getTypeName());
    }
  }

  private void verifyColumnTypes(TypeInfo[] expectedColumnTypes, List<TypeInfo> columnTypes) {
    for(int i = 0; i < expectedColumnTypes.length; i++) {
      assertEquals(expectedColumnTypes[i], columnTypes.get(i));

    }
  }

  private void verifyColumnNames(String[] expectedColumnNames, List<String> columnNames) {
    for(int i = 0; i < expectedColumnNames.length; i++) {
      assertEquals(expectedColumnNames[i], columnNames.get(i));
    }
  }

  @Test
  public void canHandleMapsWithPrimitiveValueTypes() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(MAP_WITH_PRIMITIVE_VALUE_TYPE);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);
    verifyMap(aoig, "aMap");
  }

  /**
   * Check a given AvroObjectInspectorGenerator to verify that it matches our test
   * schema's expected map.
   * @param aoig should already have been intitialized, may not be null
   * @param fieldName name of the contianed column, will always fail if null.
   */
  private void verifyMap(final AvroObjectInspectorGenerator aoig, final String fieldName) {
    // Column names
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals(fieldName, aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    TypeInfo typeInfo = aoig.getColumnTypes().get(0);
    assertEquals(ObjectInspector.Category.MAP, typeInfo.getCategory());
    assertTrue(typeInfo instanceof MapTypeInfo);
    MapTypeInfo mapTypeInfo = (MapTypeInfo)typeInfo;

    assertEquals("bigint" /* == long in Avro */, mapTypeInfo.getMapValueTypeInfo().getTypeName());
    assertEquals("string", mapTypeInfo.getMapKeyTypeInfo().getTypeName());
  }

  @Test
  public void canHandleArrays() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(ARRAY_WITH_PRIMITIVE_ELEMENT_TYPE);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    // Column names
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals("anArray", aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    TypeInfo typeInfo = aoig.getColumnTypes().get(0);
    assertEquals(ObjectInspector.Category.LIST, typeInfo.getCategory());
    assertTrue(typeInfo instanceof ListTypeInfo);
    ListTypeInfo listTypeInfo = (ListTypeInfo)typeInfo;

    assertEquals("string", listTypeInfo.getListElementTypeInfo().getTypeName());
  }

  @Test
  public void canHandleRecords() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(RECORD_SCHEMA);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    // Column names
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals("aRecord", aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    TypeInfo typeInfo = aoig.getColumnTypes().get(0);
    assertEquals(ObjectInspector.Category.STRUCT, typeInfo.getCategory());
    assertTrue(typeInfo instanceof StructTypeInfo);
    StructTypeInfo structTypeInfo = (StructTypeInfo)typeInfo;

    // Check individual elements of subrecord
    ArrayList<String> allStructFieldNames = structTypeInfo.getAllStructFieldNames();
    ArrayList<TypeInfo> allStructFieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
    assertEquals(allStructFieldNames.size(), 3);
    String[] names = new String[]{"int1", "boolean1", "long1"};
    String [] typeInfoStrings = new String [] {"int", "boolean", "bigint"};
    for(int i = 0; i < allStructFieldNames.size(); i++) {
      assertEquals("Fieldname " + allStructFieldNames.get(i) +
              " doesn't match expected " + names[i], names[i], allStructFieldNames.get(i));
      assertEquals("Typeinfo " + allStructFieldTypeInfos.get(i) +
              " doesn't match expected " + typeInfoStrings[i], typeInfoStrings[i],
              allStructFieldTypeInfos.get(i).getTypeName());
    }
  }

  @Test
  public void canHandleUnions() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(UNION_SCHEMA);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    // Column names
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals("aUnion", aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    TypeInfo typeInfo = aoig.getColumnTypes().get(0);
    assertTrue(typeInfo instanceof UnionTypeInfo);
    UnionTypeInfo uti = (UnionTypeInfo)typeInfo;

    // Check that the union has come out unscathed. No scathing of unions allowed.
    List<TypeInfo> typeInfos = uti.getAllUnionObjectTypeInfos();
    assertEquals(2, typeInfos.size());
    assertEquals(INT, typeInfos.get(0));
    assertEquals(STRING, typeInfos.get(1));
    assertEquals("uniontype<int,string>", uti.getTypeName());
  }

  @Test // Enums are one of two Avro types that Hive doesn't have any native support for.
  public void canHandleEnums() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(ENUM_SCHEMA);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    // Column names - we lose the enumness of this schema
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals("baddies", aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    assertEquals(STRING, aoig.getColumnTypes().get(0));
  }

  @Test // Hive has no concept of Avro's fixed type.  Fixed -> arrays of bytes
  public void canHandleFixed() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(FIXED_SCHEMA);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    // Column names
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals("hash", aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    TypeInfo typeInfo = aoig.getColumnTypes().get(0);
    assertTrue(typeInfo instanceof PrimitiveTypeInfo);
    assertEquals(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory(), PrimitiveCategory.BINARY);
  }

  @Test // Avro considers bytes primitive, Hive doesn't. Make them list of tinyint.
  public void canHandleBytes() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(BYTES_SCHEMA);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    // Column names
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals("bytesField", aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    TypeInfo typeInfo = aoig.getColumnTypes().get(0);
    assertTrue(typeInfo instanceof PrimitiveTypeInfo);
    assertEquals(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory(), PrimitiveCategory.BINARY);
  }

  @Test // That Union[T, NULL] is converted to just T.
  public void convertsNullableTypes() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(NULLABLE_STRING_SCHEMA);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals("nullableString", aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    TypeInfo typeInfo = aoig.getColumnTypes().get(0);
    assertTrue(typeInfo instanceof PrimitiveTypeInfo);
    PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
    // Verify the union has been hidden and just the main type has been returned.
    assertEquals(PrimitiveObjectInspector.PrimitiveCategory.STRING, pti.getPrimitiveCategory());
  }

  @Test // That Union[T, NULL] is converted to just T, within a Map
  public void convertsMapsWithNullablePrimitiveTypes() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(MAP_WITH_NULLABLE_PRIMITIVE_VALUE_TYPE_SCHEMA);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);
    verifyMap(aoig, "aMap");
  }

  @Test // That Union[T, NULL] is converted to just T.
  public void convertsNullableEnum() throws SerDeException {
    Schema s = AvroSerdeUtils.getSchemaFor(NULLABLE_ENUM_SCHEMA);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);
    assertEquals(1, aoig.getColumnNames().size());
    assertEquals("nullableEnum", aoig.getColumnNames().get(0));

    // Column types
    assertEquals(1, aoig.getColumnTypes().size());
    TypeInfo typeInfo = aoig.getColumnTypes().get(0);
    assertTrue(typeInfo instanceof PrimitiveTypeInfo);
    PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
    // Verify the union has been hidden and just the main type has been returned.
    assertEquals(PrimitiveObjectInspector.PrimitiveCategory.STRING, pti.getPrimitiveCategory());
  }

  @Test
  public void objectInspectorsAreCached() throws SerDeException {
    // Verify that Hive is caching the object inspectors for us.
    Schema s = AvroSerdeUtils.getSchemaFor(KITCHEN_SINK_SCHEMA);
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    Schema s2 = AvroSerdeUtils.getSchemaFor(KITCHEN_SINK_SCHEMA);
    AvroObjectInspectorGenerator aoig2 = new AvroObjectInspectorGenerator(s2);


    assertEquals(aoig.getObjectInspector(), aoig2.getObjectInspector());
    // For once we actually want reference equality in Java.
    assertTrue(aoig.getObjectInspector() == aoig2.getObjectInspector());
  }
}
