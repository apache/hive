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
package org.apache.hadoop.hive.ql.io.parquet.read;

import com.google.common.collect.Sets;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.apache.hadoop.hive.ql.io.parquet.HiveParquetSchemaTestUtils.testConversion;

public class TestDataWritableReadSupport {
  @Test
  public void testGetProjectedSchema1() throws Exception {
    MessageType originalMsg = MessageTypeParser.parseMessageType(
        "message hive_schema {\n"
      + "  optional group structCol {\n"
      + "    optional int32 a;\n"
      + "    optional double b;\n"
      + "    optional boolean c;\n"
      + "    optional fixed_len_byte_array(3) d (DECIMAL(5,2));\n"
      + "  }\n"
      + "}\n");

    testConversion("structCol", "struct<a:int>", DataWritableReadSupport
      .getProjectedSchema(originalMsg, Arrays.asList("structCol"), Arrays.asList(0),
        Sets.newHashSet("structCol.a")).toString());
  }

  @Test
  public void testGetProjectedSchema2() throws Exception {
    MessageType originalMsg = MessageTypeParser.parseMessageType(
      "message hive_schema {\n"
        + "  optional group structCol {\n"
        + "    optional int32 a;\n"
        + "    optional double b;\n"
        + "  }\n"
        + "}\n");

    testConversion("structCol", "struct<a:int,b:double>", DataWritableReadSupport
      .getProjectedSchema(originalMsg, Arrays.asList("structCol"), Arrays.asList(0),
        Sets.newHashSet("structCol.a", "structCol.b")).toString());
  }

  @Test
  public void testGetProjectedSchema3() throws Exception {
    MessageType originalMsg = MessageTypeParser.parseMessageType(
      "message hive_schema {\n"
        + "  optional group structCol {\n"
        + "    optional int32 a;\n"
        + "    optional double b;\n"
        + "  }\n"
        + "  optional boolean c;\n"
        + "}\n");

    testConversion("structCol,c", "struct<b:double>,boolean", DataWritableReadSupport
      .getProjectedSchema(originalMsg, Arrays.asList("structCol", "c"), Arrays.asList(0, 1),
        Sets.newHashSet("structCol.b", "c")).toString());
  }

  @Test
  public void testGetProjectedSchema4() throws Exception {
    MessageType originalMsg = MessageTypeParser.parseMessageType(
      "message hive_schema {\n"
        + "  optional group structCol {\n"
        + "    optional int32 a;\n"
        + "    optional group subStructCol {\n"
        + "      optional int64 b;\n"
        + "      optional boolean c;\n"
        + "    }\n"
        + "  }\n"
        + "  optional boolean d;\n"
        + "}\n");

    testConversion("structCol", "struct<subStructCol:struct<b:bigint>>", DataWritableReadSupport
      .getProjectedSchema(originalMsg, Arrays.asList("structCol"), Arrays.asList(0),
        Sets.newHashSet("structCol.subStructCol.b")).toString());
  }

  @Test
  public void testGetProjectedSchema5() throws Exception {
    MessageType originalMsg = MessageTypeParser.parseMessageType(
      "message hive_schema {\n"
        + "  optional group structCol {\n"
        + "    optional int32 a;\n"
        + "    optional group subStructCol {\n"
        + "      optional int64 b;\n"
        + "      optional boolean c;\n"
        + "    }\n"
        + "  }\n"
        + "  optional boolean d;\n"
        + "}\n");

    testConversion("structCol", "struct<subStructCol:struct<b:bigint,c:boolean>>",
      DataWritableReadSupport
        .getProjectedSchema(originalMsg, Arrays.asList("structCol"), Arrays.asList(0),
          Sets.newHashSet("structCol.subStructCol", "structCol.subStructCol.b",
            "structCol.subStructCol.c")).toString());
  }
}
