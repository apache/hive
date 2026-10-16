/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.read;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.hadoop.hive.ql.io.parquet.HiveParquetSchemaTestUtils.testConversion;
import static org.junit.Assert.assertEquals;

public class TestDataWritableReadSupport {
  private static final String ENCODING = VariantEncoding.TYPE_INFO.getTypeName();

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

  @Test
  public void testVariantProjectsMetadataAndValueWhenRebuilt() {
    MessageType fileSchema = Types.buildMessage()
        .addField(shreddedVariant("v", false))
        .named("hive_schema");

    MessageType projected = rebuiltProjection(fileSchema, "v", "variant");
    MessageType asStruct = DataWritableReadSupport.getSchemaByName(
        fileSchema, List.of("v"), TypeInfoUtils.getTypeInfosFromTypeString(ENCODING));

    // the caller reads typed_value itself
    assertEquals(asStruct, projected);
    assertEquals(2, projected.getType("v").asGroupType().getFieldCount());

    // Hive's own readers get the group as in the file, and fail on its typed_value
    assertEquals(fileSchema, DataWritableReadSupport.getRequestedSchema(false, List.of("v"),
        List.of(TypeInfoFactory.getVariantTypeInfo()), fileSchema, new Configuration()));
  }

  @Test
  public void testVariantInsideStructProjectsMetadataAndValueWhenRebuilt() {
    MessageType fileSchema = Types.buildMessage()
        .addField(Types.optionalGroup().addField(shreddedVariant("v", false)).named("s"))
        .named("hive_schema");

    MessageType projected = rebuiltProjection(fileSchema, "s", "struct<v:variant>");
    MessageType asStruct = DataWritableReadSupport.getSchemaByName(fileSchema, List.of("s"),
        TypeInfoUtils.getTypeInfosFromTypeString("struct<v:" + ENCODING + ">"));

    assertEquals(asStruct, projected);
  }

  @Test
  public void testVariantListElementProjectsMetadataAndValueWhenRebuilt() {
    for (String[] names : new String[][] {{"list", "element"}, {"bag", "array_element"}}) {
      MessageType fileSchema = Types.buildMessage()
          .addField(Types.optionalGroup().as(LogicalTypeAnnotation.listType())
              .addField(Types.repeatedGroup().addField(shreddedVariant(names[1], true)).named(names[0]))
              .named("l"))
          .named("hive_schema");

      MessageType projected = rebuiltProjection(fileSchema, "l", "array<variant>");
      MessageType asStruct = DataWritableReadSupport.getSchemaByName(fileSchema, List.of("l"),
          TypeInfoUtils.getTypeInfosFromTypeString("array<" + ENCODING + ">"));

      // the element is projected to metadata and value, in this order
      assertEquals(asStruct, projected);
      GroupType element = projected.getType("l").asGroupType().getType(0).asGroupType().getType(0).asGroupType();
      assertEquals(List.of("metadata", "value"), element.getFields().stream().map(Type::getName).toList());
    }
  }

  /** A shredded variant group: its metadata and value, value first if asked, then a typed_value. */
  private static GroupType shreddedVariant(String name, boolean valueFirst) {
    Types.GroupBuilder<GroupType> group = Types.optionalGroup().as(LogicalTypeAnnotation.variantType((byte) 1));
    if (valueFirst) {
      group.optional(PrimitiveTypeName.BINARY).named("value").required(PrimitiveTypeName.BINARY).named("metadata");
    } else {
      group.required(PrimitiveTypeName.BINARY).named("metadata").optional(PrimitiveTypeName.BINARY).named("value");
    }
    return group.optional(PrimitiveTypeName.INT64).named("typed_value").named(name);
  }

  /** The requested schema of the vectorized reader for a caller that rebuilds shredded variant values. */
  private static MessageType rebuiltProjection(MessageType fileSchema, String column, String type) {
    Configuration conf = new Configuration();
    conf.setBoolean(DataWritableReadSupport.SHREDDED_VARIANT_REBUILT, true);
    List<TypeInfo> types = TypeInfoUtils.getTypeInfosFromTypeString(type);
    return DataWritableReadSupport.getRequestedSchema(false, List.of(column), types, fileSchema, conf);
  }
}
