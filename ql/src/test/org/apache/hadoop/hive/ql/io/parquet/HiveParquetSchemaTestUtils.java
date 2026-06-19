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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class HiveParquetSchemaTestUtils {

  public static List<String> createHiveColumnsFrom(final String columnNamesStr) {
    List<String> columnNames;
    if (columnNamesStr.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNamesStr.split(","));
    }

    return columnNames;
  }

  public static List<TypeInfo> createHiveTypeInfoFrom(final String columnsTypeStr) {
    List<TypeInfo> columnTypes;

    if (columnsTypeStr.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnsTypeStr);
    }

    return columnTypes;
  }

  /**
   * Only use if Configuration/HiveConf not needed for converting schema.
   */
  public static void testConversion(
      final String columnNamesStr,
      final String columnsTypeStr,
      final String actualSchema) throws Exception {
    testConversion(columnNamesStr, columnsTypeStr, actualSchema, null);
  }

  public static void testConversion(
      final String columnNamesStr,
      final String columnsTypeStr,
      final String actualSchema,
      final Configuration conf) throws Exception {
    final List<String> columnNames = createHiveColumnsFrom(columnNamesStr);
    final List<TypeInfo> columnTypes = createHiveTypeInfoFrom(columnsTypeStr);
    final MessageType messageTypeFound = HiveSchemaConverter.convert(columnNames, columnTypes,
        conf);
    final MessageType expectedMT = MessageTypeParser.parseMessageType(actualSchema);
    assertEquals("converting " + columnNamesStr + ": " + columnsTypeStr + " to " + actualSchema,
      expectedMT, messageTypeFound);

    // Required to check the original types manually as PrimitiveType.equals does not care about it
    List<Type> expectedFields = expectedMT.getFields();
    List<Type> actualFields = messageTypeFound.getFields();
    for (int i = 0, n = expectedFields.size(); i < n; ++i) {

      LogicalTypeAnnotation expectedLogicalType = expectedFields.get(i).getLogicalTypeAnnotation();
      LogicalTypeAnnotation actualLogicalType = actualFields.get(i).getLogicalTypeAnnotation();
      assertEquals("Logical type annotations of the field do not match", expectedLogicalType, actualLogicalType);
    }
  }

  public static void testLogicalTypeAnnotation(String hiveColumnType, String hiveColumnName,
      LogicalTypeAnnotation expectedLogicalType, Configuration conf) throws Exception {
    Map<String, LogicalTypeAnnotation> expectedLogicalTypeForColumn = new HashMap<>();
    expectedLogicalTypeForColumn.put(hiveColumnName, expectedLogicalType);
    testLogicalTypeAnnotations(hiveColumnName, hiveColumnType, expectedLogicalTypeForColumn, conf);
  }

  public static void testLogicalTypeAnnotations(final String hiveColumnNames,
      final String hiveColumnTypes, final Map<String, LogicalTypeAnnotation> expectedLogicalTypes,
      Configuration conf) throws Exception {
    final List<String> columnNames = createHiveColumnsFrom(hiveColumnNames);
    final List<TypeInfo> columnTypes = createHiveTypeInfoFrom(hiveColumnTypes);
    final MessageType messageTypeFound = HiveSchemaConverter.convert(columnNames, columnTypes,
        conf);
    List<Type> actualFields = messageTypeFound.getFields();
    for (Type actualField : actualFields) {
      LogicalTypeAnnotation expectedLogicalType = expectedLogicalTypes.get(actualField.getName());
      LogicalTypeAnnotation actualLogicalType = actualField.getLogicalTypeAnnotation();
      if (expectedLogicalType != null) {
        assertNotNull("The logical type annotation cannot be null.", actualLogicalType);
        assertEquals("Logical type annotations of the field do not match", expectedLogicalType,
            actualLogicalType);
      } else {
        assertNull("The logical type annotation must be null.", actualLogicalType);
      }
    }
  }
}
