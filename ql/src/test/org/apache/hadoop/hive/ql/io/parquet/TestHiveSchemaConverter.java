/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import static org.apache.hadoop.hive.ql.io.parquet.HiveParquetSchemaTestUtils.createHiveColumnsFrom;
import static org.apache.hadoop.hive.ql.io.parquet.HiveParquetSchemaTestUtils.createHiveTypeInfoFrom;
import static org.apache.hadoop.hive.ql.io.parquet.HiveParquetSchemaTestUtils.testConversion;
import static org.junit.Assert.assertEquals;

import static org.apache.hadoop.hive.ql.io.parquet.HiveParquetSchemaTestUtils.testLogicalTypeAnnotation;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;


public class TestHiveSchemaConverter {

  Configuration conf = new Configuration();

  @Test
  public void testSimpleType() throws Exception {
    testConversion(
            "a,b,c,d,e,f,g",
            "int,bigint,double,boolean,string,float,binary",
            "message hive_schema {\n"
            + "  optional int32 a;\n"
            + "  optional int64 b;\n"
            + "  optional double c;\n"
            + "  optional boolean d;\n"
            + "  optional binary e (UTF8);\n"
            + "  optional float f;\n"
            + "  optional binary g;\n"
            + "}\n");
  }

  @Test
  public void testSpecialIntType() throws Exception {
    testConversion(
            "a,b",
            "tinyint,smallint",
            "message hive_schema {\n"
            + "  optional int32 a (INT_8);\n"
            + "  optional int32 b (INT_16);\n"
            + "}\n");
  }

  @Test
  public void testSpecialIntTypeWithLogicatlTypeAnnotations() throws Exception {
    testConversion(
            "a,b",
            "tinyint,smallint",
            "message hive_schema {\n"
            + "  optional int32 a (INTEGER(8,true));\n"
            + "  optional int32 b (INTEGER(16,true));\n"
            + "}\n");
  }

  @Test
  public void testDecimalType() throws Exception {
    testConversion(
            "a",
            "decimal(5,2)",
            "message hive_schema {\n"
            + "  optional fixed_len_byte_array(3) a (DECIMAL(5,2));\n"
            + "}\n");
  }

  @Test
  public void testCharType() throws Exception {
    testConversion(
        "a",
        "char(5)",
        "message hive_schema {\n"
            + "  optional binary a (UTF8);\n"
            + "}\n");
  }

  @Test
  public void testVarcharType() throws Exception {
    testConversion(
        "a",
        "varchar(10)",
        "message hive_schema {\n"
            + "  optional binary a (UTF8);\n"
            + "}\n");
  }

  @Test
  public void testDateType() throws Exception {
    testConversion(
        "a",
        "date",
        "message hive_schema {\n"
            + "  optional int32 a (DATE);\n"
            + "}\n");
  }

  @Test
  public void testTimestampType() throws Exception {
    conf.setBoolean(HiveConf.ConfVars.HIVE_PARQUET_WRITE_INT64_TIMESTAMP.varname, false);
    testConversion(
        "a",
        "timestamp",
        "message hive_schema {\n"
            + "  optional int96 a;\n"
            + "}\n",
            conf);
  }

  @Test
  public void testArray() throws Exception {
    testConversion("arrayCol",
            "array<int>",
            "message hive_schema {\n"
            + "  optional group arrayCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional int32 array_element;\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testArrayDecimal() throws Exception {
    testConversion("arrayCol",
            "array<decimal(5,2)>",
            "message hive_schema {\n"
            + "  optional group arrayCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional fixed_len_byte_array(3) array_element (DECIMAL(5,2));\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testArrayTinyInt() throws Exception {
    testConversion("arrayCol",
            "array<tinyint>",
            "message hive_schema {\n"
            + "  optional group arrayCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional int32 array_element (INT_8);\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testArraySmallInt() throws Exception {
    testConversion("arrayCol",
            "array<smallint>",
            "message hive_schema {\n"
            + "  optional group arrayCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional int32 array_element (INT_16);\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testArrayString() throws Exception {
    testConversion("arrayCol",
            "array<string>",
            "message hive_schema {\n"
            + "  optional group arrayCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional binary array_element (UTF8);\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testArrayTimestamp() throws Exception {
    conf.setBoolean(HiveConf.ConfVars.HIVE_PARQUET_WRITE_INT64_TIMESTAMP.varname, false);
    testConversion("arrayCol",
            "array<timestamp>",
            "message hive_schema {\n"
            + "  optional group arrayCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional int96 array_element;\n"
            + "    }\n"
            + "  }\n"
            + "}\n",
            conf);
  }

  @Test
  public void testArrayStruct() throws Exception {
    testConversion("structCol",
            "array<struct<a:string,b:int>>",
            "message hive_schema {\n"
            + "  optional group structCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional group array_element {\n"
            + "        optional binary a (UTF8);\n"
            + "        optional int32 b;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testArrayInArray() throws Exception {
    final List<String> columnNames = createHiveColumnsFrom("arrayCol");
    ListTypeInfo listTypeInfo = new ListTypeInfo();
    listTypeInfo.setListElementTypeInfo(TypeInfoUtils.getTypeInfosFromTypeString("int").get(0));
    List<TypeInfo> typeInfos = new ArrayList<>();
    ListTypeInfo listTypeInfo2 = new ListTypeInfo();
    listTypeInfo2.setListElementTypeInfo(listTypeInfo);
    typeInfos.add(listTypeInfo2);
    final MessageType messageTypeFound = HiveSchemaConverter.convert(columnNames, typeInfos);
    final MessageType expectedMT = MessageTypeParser.parseMessageType(
        "message hive_schema {\n"
            + "  optional group arrayCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional group array_element (LIST) {\n"
            + "        repeated group bag {\n"
            + "          optional int32 array_element;\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
    assertEquals(expectedMT, messageTypeFound);
  }

  @Test
  public void testStruct() throws Exception {
    testConversion("structCol",
            "struct<a:int,b:double,c:boolean,d:decimal(5,2)>",
            "message hive_schema {\n"
            + "  optional group structCol {\n"
            + "    optional int32 a;\n"
            + "    optional double b;\n"
            + "    optional boolean c;\n"
            + "    optional fixed_len_byte_array(3) d (DECIMAL(5,2));\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testStructInts() throws Exception {
    testConversion("structCol",
            "struct<a:tinyint,b:smallint,c:int,d:bigint>",
            "message hive_schema {\n"
            + "  optional group structCol {\n"
            + "    optional int32 a (INT_8);\n"
            + "    optional int32 b (INT_16);\n"
            + "    optional int32 c;\n"
            + "    optional int64 d;\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testStructStrings() throws Exception {
    testConversion("structCol",
            "struct<a:char(5),b:varchar(25),c:string>",
            "message hive_schema {\n"
            + "  optional group structCol {\n"
            + "    optional binary a (UTF8);\n"
            + "    optional binary b (UTF8);\n"
            + "    optional binary c (UTF8);\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testStructTimestamp() throws Exception {
    conf.setBoolean(HiveConf.ConfVars.HIVE_PARQUET_WRITE_INT64_TIMESTAMP.varname, false);
    testConversion("structCol",
            "struct<a:timestamp>",
            "message hive_schema {\n"
            + "  optional group structCol {\n"
            + "    optional int96 a;\n"
            + "  }\n"
            + "}\n",
            conf);
  }

  @Test
  public void testStructList() throws Exception {
    testConversion("structCol",
            "struct<a:array<string>,b:int,c:string>",
            "message hive_schema {\n"
            + "  optional group structCol {\n"
            + "    optional group a (LIST) {\n"
            + "      repeated group bag {\n"
            + "        optional binary array_element (UTF8);\n"
            + "      }\n"
            + "    }\n"
            + "    optional int32 b;\n"
            + "    optional binary c (UTF8);"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testMap() throws Exception {
    testConversion("mapCol",
            "map<string,string>",
            "message hive_schema {\n"
            + "  optional group mapCol (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      optional binary value (UTF8);\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testMapDecimal() throws Exception {
    testConversion("mapCol",
            "map<string,decimal(5,2)>",
            "message hive_schema {\n"
            + "  optional group mapCol (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      optional fixed_len_byte_array(3) value (DECIMAL(5,2));\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testMapInts() throws Exception {
    testConversion("mapCol",
            "map<smallint,tinyint>",
            "message hive_schema {\n"
            + "  optional group mapCol (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required int32 key (INT_16);\n"
            + "      optional int32 value (INT_8);\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testMapStruct() throws Exception {
    testConversion("mapCol",
            "map<string,struct<a:smallint,b:int>>",
            "message hive_schema {\n"
            + "  optional group mapCol (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      optional group value {\n"
            + "        optional int32 a (INT_16);\n"
            + "        optional int32 b;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testMapList() throws Exception {
    testConversion("mapCol",
            "map<string,array<string>>",
            "message hive_schema {\n"
            + "  optional group mapCol (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      optional group value (LIST) {\n"
            + "        repeated group bag {\n"
            + "          optional binary array_element (UTF8);\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testLogicalTypes() throws Exception {
    conf.setBoolean(HiveConf.ConfVars.HIVE_PARQUET_WRITE_INT64_TIMESTAMP.varname, true);
    testLogicalTypeAnnotation("string", "a", LogicalTypeAnnotation.stringType(), conf);
    testLogicalTypeAnnotation("int", "a", null, conf);
    testLogicalTypeAnnotation("smallint", "a", LogicalTypeAnnotation.intType(16, true), conf);
    testLogicalTypeAnnotation("tinyint", "a", LogicalTypeAnnotation.intType(8, true), conf);
    testLogicalTypeAnnotation("bigint", "a", null, conf);
    testLogicalTypeAnnotation("double", "a", null, conf);
    testLogicalTypeAnnotation("float", "a", null, conf);
    testLogicalTypeAnnotation("boolean", "a", null, conf);
    testLogicalTypeAnnotation("binary", "a", null, conf);
    testLogicalTypeAnnotation("timestamp", "a",
        LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS), conf);
    testLogicalTypeAnnotation("char(3)", "a", LogicalTypeAnnotation.stringType(), conf);
    testLogicalTypeAnnotation("varchar(30)", "a", LogicalTypeAnnotation.stringType(), conf);
    testLogicalTypeAnnotation("decimal(7,2)", "a", LogicalTypeAnnotation.decimalType(2, 7), conf);
  }

  @Test
  public void testMapOriginalType() throws Exception {
    final MessageType messageTypeFound = createSchema("map<string,string>", "mapCol");
    // this messageType only has one optional field, whose name is mapCol, original Type is MAP
    assertEquals(1, messageTypeFound.getFieldCount());
    Type topLevel = messageTypeFound.getFields().get(0);
    checkField(topLevel, "mapCol", Repetition.OPTIONAL, LogicalTypeAnnotation.mapType());

    assertEquals(1, topLevel.asGroupType().getFieldCount());
    Type secondLevel = topLevel.asGroupType().getFields().get(0);
    // there is one repeated field for mapCol, the field name is "map" and its original Type is
    // MAP_KEY_VALUE;
    checkField(secondLevel, "key_value", Repetition.REPEATED,
        LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance());
  }

  @Test
  public void testListOriginalType() throws Exception {

    final MessageType messageTypeFound = createSchema("array<tinyint>", "arrayCol");

    assertEquals(1, messageTypeFound.getFieldCount());
    Type topLevel = messageTypeFound.getFields().get(0);
    checkField(topLevel, "arrayCol", Repetition.OPTIONAL, LogicalTypeAnnotation.listType());

    assertEquals(1, topLevel.asGroupType().getFieldCount());
    Type secondLevel = topLevel.asGroupType().getFields().get(0);
    checkField(secondLevel, "bag", Repetition.REPEATED, null);

    assertEquals(1, secondLevel.asGroupType().getFieldCount());
    Type thirdLevel = secondLevel.asGroupType().getFields().get(0);
    checkField(thirdLevel, "array_element", Repetition.OPTIONAL, LogicalTypeAnnotation.intType(8, true));
  }

  @Test
  public void testStructOriginalType() throws Exception {

    final MessageType messageTypeFound = createSchema("struct<a:smallint,b:string>", "structCol");

    assertEquals(1, messageTypeFound.getFieldCount());
    Type topLevel = messageTypeFound.getFields().get(0);
    checkField(topLevel, "structCol", Repetition.OPTIONAL, null);

    assertEquals(2, topLevel.asGroupType().getFieldCount());
    Type a = topLevel.asGroupType().getFields().get(0);
    checkField(a, "a", Repetition.OPTIONAL, LogicalTypeAnnotation.intType(16, true));
    Type b = topLevel.asGroupType().getFields().get(1);
    checkField(b, "b", Repetition.OPTIONAL, LogicalTypeAnnotation.stringType());
  }

  private MessageType createSchema(String hiveColumnTypes, String hiveColumnNames) {
    List<String> columnNames = createHiveColumnsFrom(hiveColumnNames);
    List<TypeInfo> columnTypes = createHiveTypeInfoFrom(hiveColumnTypes);
    return HiveSchemaConverter.convert(columnNames, columnTypes);
  }

  private void checkField(Type field, String expectedName, Repetition expectedRepetition,
      LogicalTypeAnnotation expectedLogicalType) {
    assertEquals(expectedName, field.getName());
    assertEquals(expectedLogicalType, field.getLogicalTypeAnnotation());
    assertEquals(expectedRepetition, field.getRepetition());
  }
}
