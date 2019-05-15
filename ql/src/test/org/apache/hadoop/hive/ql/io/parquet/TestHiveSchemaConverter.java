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

import java.util.List;

import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;


public class TestHiveSchemaConverter {

  @Test
  public void testSimpleType() throws Exception {
    testConversion(
            "a,b,c,d",
            "int,bigint,double,boolean",
            "message hive_schema {\n"
            + "  optional int32 a;\n"
            + "  optional int64 b;\n"
            + "  optional double c;\n"
            + "  optional boolean d;\n"
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
  public void testMap() throws Exception {
    testConversion("mapCol",
            "map<string,string>",
            "message hive_schema {\n"
            + "  optional group mapCol (MAP) {\n"
            + "    repeated group map (MAP_KEY_VALUE) {\n"
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
            + "    repeated group map (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      optional fixed_len_byte_array(3) value (DECIMAL(5,2));\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testMapOriginalType() throws Exception {
    final String hiveColumnTypes = "map<string,string>";
    final String hiveColumnNames = "mapCol";
    final List<String> columnNames = createHiveColumnsFrom(hiveColumnNames);
    final List<TypeInfo> columnTypes = createHiveTypeInfoFrom(hiveColumnTypes);
    final MessageType messageTypeFound = HiveSchemaConverter.convert(columnNames, columnTypes);
    // this messageType only has one optional field, whose name is mapCol, original Type is MAP
    assertEquals(1, messageTypeFound.getFieldCount());
    org.apache.parquet.schema.Type topLevel = messageTypeFound.getFields().get(0);
    assertEquals("mapCol",topLevel.getName());
    assertEquals(OriginalType.MAP, topLevel.getOriginalType());
    assertEquals(Repetition.OPTIONAL, topLevel.getRepetition());

    assertEquals(1, topLevel.asGroupType().getFieldCount());
    org.apache.parquet.schema.Type secondLevel = topLevel.asGroupType().getFields().get(0);
    //there is one repeated field for mapCol, the field name is "map" and its original Type is MAP_KEY_VALUE;
    assertEquals("map", secondLevel.getName());
    assertEquals(OriginalType.MAP_KEY_VALUE, secondLevel.getOriginalType());
    assertEquals(Repetition.REPEATED, secondLevel.getRepetition());
  }
}
