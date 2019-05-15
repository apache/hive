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

import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

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

  public static void testConversion(
    final String columnNamesStr,
    final String columnsTypeStr,
    final String actualSchema) throws Exception {
    final List<String> columnNames = createHiveColumnsFrom(columnNamesStr);
    final List<TypeInfo> columnTypes = createHiveTypeInfoFrom(columnsTypeStr);
    final MessageType messageTypeFound = HiveSchemaConverter.convert(columnNames, columnTypes);
    final MessageType expectedMT = MessageTypeParser.parseMessageType(actualSchema);
    assertEquals("converting " + columnNamesStr + ": " + columnsTypeStr + " to " + actualSchema,
      expectedMT, messageTypeFound);

    // Required to check the original types manually as PrimitiveType.equals does not care about it
    List<Type> expectedFields = expectedMT.getFields();
    List<Type> actualFields = messageTypeFound.getFields();
    for (int i = 0, n = expectedFields.size(); i < n; ++i) {
      OriginalType exp = expectedFields.get(i).getOriginalType();
      OriginalType act = actualFields.get(i).getOriginalType();
      assertEquals("Original types of the field do not match", exp, act);
    }
  }
}
