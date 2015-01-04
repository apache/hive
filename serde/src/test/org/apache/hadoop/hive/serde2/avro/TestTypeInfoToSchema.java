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

import com.google.common.io.Resources;

import org.junit.Assert;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestTypeInfoToSchema {

  private static Logger LOGGER = Logger.getLogger(TestTypeInfoToSchema.class);
  private static final List<String> COLUMN_NAMES = Arrays.asList("testCol");
  private static final TypeInfo STRING = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.STRING_TYPE_NAME);
  private static final TypeInfo INT = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.INT_TYPE_NAME);
  private static final TypeInfo BOOLEAN = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.BOOLEAN_TYPE_NAME);
  private static final TypeInfo LONG = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.BIGINT_TYPE_NAME);
  private static final TypeInfo FLOAT = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.FLOAT_TYPE_NAME);
  private static final TypeInfo DOUBLE = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.DOUBLE_TYPE_NAME);
  private static final TypeInfo BINARY = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.BINARY_TYPE_NAME);
  private static final TypeInfo BYTE = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.TINYINT_TYPE_NAME);
  private static final TypeInfo SHORT = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.SMALLINT_TYPE_NAME);
  private static final TypeInfo VOID = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.VOID_TYPE_NAME);
  private static final TypeInfo DATE = TypeInfoFactory.getPrimitiveTypeInfo(
      serdeConstants.DATE_TYPE_NAME);
  private static final TypeInfo TIMESTAMP =
    TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.TIMESTAMP_TYPE_NAME);
  private static final int PRECISION = 4;
  private static final int SCALE = 2;
  private static final TypeInfo DECIMAL = TypeInfoFactory.getPrimitiveTypeInfo(
      new DecimalTypeInfo(PRECISION, SCALE).getQualifiedName());
  private static final int CHAR_LEN = 5;
  private static final TypeInfo CHAR = TypeInfoFactory.getPrimitiveTypeInfo(
      new CharTypeInfo(CHAR_LEN).getQualifiedName());
  private static final TypeInfo VARCHAR = TypeInfoFactory.getPrimitiveTypeInfo(
      new VarcharTypeInfo(CHAR_LEN).getQualifiedName());

  private TypeInfoToSchema typeInfoToSchema;

  private final String lineSeparator = System.getProperty("line.separator");

  private String getAvroSchemaString(TypeInfo columnType) {
    return typeInfoToSchema.convert(
        COLUMN_NAMES,
        Arrays.asList(columnType),
        Arrays.asList(""),
        "org.apache.hive.avro.testing",
        "avrotest",
        "This is to test hive-avro").toString();
  }

  private String genSchemaWithoutNull(String specificSchema) {
    return "{" +
        "\"type\":\"record\"," +
        "\"name\":\"avrotest\"," +
        "\"namespace\":\"org.apache.hive.avro.testing\"," +
        "\"doc\":\"This is to test hive-avro\"," +
        "\"fields\":[" +
        "{\"name\":\"testCol\"," +
        "\"type\":" + specificSchema + "," +
        "\"doc\":\"\"," +
        "\"default\":null}" +
        "]}";
  }

  private String genSchema(String specificSchema) {
    specificSchema = "[\"null\"," + specificSchema + "]";
    return genSchemaWithoutNull(specificSchema);
  }

  @Before
  public void setUp() {
    typeInfoToSchema = new TypeInfoToSchema();
  }

  @Test
  public void createAvroStringSchema() {
    final String specificSchema = "\"string\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for string's avro schema failed",
        expectedSchema, getAvroSchemaString(STRING));
  }

  @Test
  public void createAvroBinarySchema() {
    final String specificSchema = "\"bytes\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for binary's avro schema failed",
        expectedSchema, getAvroSchemaString(BINARY));
  }

  @Test
  public void createAvroBytesSchema() {
    final String specificSchema = "\"int\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for bytes's avro schema failed",
        expectedSchema, getAvroSchemaString(BYTE));
  }

  @Test
  public void createAvroShortSchema() {
    final String specificSchema = "\"int\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for short's avro schema failed",
        expectedSchema, getAvroSchemaString(SHORT));
  }

  @Test
  public void createAvroIntSchema() {
    final String specificSchema = "\"int\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for int's avro schema failed",
        expectedSchema, getAvroSchemaString(INT));
  }

  @Test
  public void createAvroLongSchema() {
    final String specificSchema = "\"long\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for long's avro schema failed",
        expectedSchema, getAvroSchemaString(LONG));
  }

  @Test
  public void createAvroFloatSchema() {
    final String specificSchema = "\"float\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for float's avro schema failed",
        expectedSchema, getAvroSchemaString(FLOAT));
  }

  @Test
  public void createAvroDoubleSchema() {
    final String specificSchema = "\"double\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for double's avro schema failed",
        expectedSchema, getAvroSchemaString(DOUBLE));
  }

  @Test
  public void createAvroBooleanSchema() {
    final String specificSchema = "\"boolean\"";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for boolean's avro schema failed",
        expectedSchema, getAvroSchemaString(BOOLEAN));
  }

  @Test
  public void createAvroVoidSchema() {
    final String specificSchema = "\"null\"";
    String expectedSchema = genSchemaWithoutNull(specificSchema);

    Assert.assertEquals("Test for void's avro schema failed",
        expectedSchema, getAvroSchemaString(VOID));
  }

  @Test
  public void createAvroDecimalSchema() {
    final String specificSchema = "{" +
        "\"type\":\"bytes\"," +
        "\"logicalType\":\"decimal\"," +
        "\"precision\":" + PRECISION + "," +
        "\"scale\":" + SCALE + "}";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for decimal's avro schema failed",
        expectedSchema, getAvroSchemaString(DECIMAL));
  }

  @Test
  public void createAvroCharSchema() {
    final String specificSchema = "{" +
        "\"type\":\"string\"," +
        "\"logicalType\":\"char\"," +
        "\"maxLength\":" + CHAR_LEN + "}";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for char's avro schema failed",
        expectedSchema, getAvroSchemaString(CHAR));
  }

  @Test
  public void createAvroVarcharSchema() {
    final String specificSchema = "{" +
        "\"type\":\"string\"," +
        "\"logicalType\":\"varchar\"," +
        "\"maxLength\":" + CHAR_LEN + "}";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for varchar's avro schema failed",
        expectedSchema, getAvroSchemaString(VARCHAR));
  }

  @Test
  public void createAvroDateSchema() {
    final String specificSchema = "{" +
        "\"type\":\"int\"," +
        "\"logicalType\":\"date\"}";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for date in avro schema failed",
        expectedSchema, getAvroSchemaString(DATE));
  }

  @Test
  public void createAvroTimestampSchema() {
    final String specificSchema = "{" +
      "\"type\":\"long\"," +
      "\"logicalType\":\"timestamp-millis\"}";
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for timestamp in avro schema failed",
      expectedSchema, getAvroSchemaString(TIMESTAMP));
  }

  @Test
  public void createAvroListSchema() {
    ListTypeInfo listTypeInfo = new ListTypeInfo();
    listTypeInfo.setListElementTypeInfo(STRING);

    final String specificSchema = Schema.createArray(Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING)))).toString();
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for list's avro schema failed",
        expectedSchema, getAvroSchemaString(listTypeInfo));
  }

  @Test
  public void createAvroMapSchema() {
    MapTypeInfo mapTypeInfo = new MapTypeInfo();
    mapTypeInfo.setMapKeyTypeInfo(STRING);
    mapTypeInfo.setMapValueTypeInfo(INT);

    final String specificSchema = Schema.createMap(Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.INT)))).toString();
    String expectedSchema = genSchema(specificSchema);

    Assert.assertEquals("Test for map's avro schema failed",
        expectedSchema, getAvroSchemaString(mapTypeInfo));
  }

  @Test
  public void createAvroUnionSchema() {
    UnionTypeInfo unionTypeInfo = new UnionTypeInfo();
    unionTypeInfo.setAllUnionObjectTypeInfos(Arrays.asList(INT, FLOAT, STRING));

    final String specificSchema = Schema.createUnion(
        Arrays.asList(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.FLOAT),
            Schema.create(Schema.Type.STRING))).toString();
    String expectedSchema = genSchemaWithoutNull(specificSchema);

    Assert.assertEquals("Test for union's avro schema failed",
        expectedSchema, getAvroSchemaString(unionTypeInfo));
  }

  @Test
  public void createAvroUnionSchemaOfNull() {
    UnionTypeInfo unionTypeInfo = new UnionTypeInfo();
    unionTypeInfo.setAllUnionObjectTypeInfos(Arrays.asList(VOID));

    final String specificSchema = Schema.createUnion(
        Arrays.asList(
            Schema.create(Schema.Type.NULL))).toString();
    String expectedSchema = genSchemaWithoutNull(specificSchema);

    Assert.assertEquals("Test for union's avro schema failed",
        expectedSchema, getAvroSchemaString(unionTypeInfo));
  }

  @Test
  public void createAvroUnionSchemaOfOne() {
    UnionTypeInfo unionTypeInfo = new UnionTypeInfo();
    unionTypeInfo.setAllUnionObjectTypeInfos(Arrays.asList(STRING));

    final String specificSchema = Schema.createUnion(
        Arrays.asList(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.STRING))).toString();
    String expectedSchema = genSchemaWithoutNull(specificSchema);

    Assert.assertEquals("Test for union's avro schema failed",
        expectedSchema, getAvroSchemaString(unionTypeInfo));
  }

  @Test
  public void createAvroUnionSchemaWithNull() {
    UnionTypeInfo unionTypeInfo = new UnionTypeInfo();
    unionTypeInfo.setAllUnionObjectTypeInfos(Arrays.asList(INT, FLOAT, STRING, VOID));

    final String specificSchema = Schema.createUnion(
        Arrays.asList(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.FLOAT),
            Schema.create(Schema.Type.STRING))).toString();
    String expectedSchema = genSchemaWithoutNull(specificSchema);

    Assert.assertEquals("Test for union's avro schema failed",
        expectedSchema, getAvroSchemaString(unionTypeInfo));
  }

  @Test
  public void createAvroStructSchema() throws IOException {
    StructTypeInfo structTypeInfo = new StructTypeInfo();
    ArrayList<String> names = new ArrayList<String>();
    names.add("field1");
    names.add("field2");
    names.add("field3");
    names.add("field4");
    names.add("field5");
    names.add("field6");
    names.add("field7");
    names.add("field8");
    names.add("field9");
    names.add("field10");
    names.add("field11");
    names.add("field12");
    names.add("field13");
    names.add("field14");
    structTypeInfo.setAllStructFieldNames(names);
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(STRING);
    typeInfos.add(CHAR);
    typeInfos.add(VARCHAR);
    typeInfos.add(BINARY);
    typeInfos.add(BYTE);
    typeInfos.add(SHORT);
    typeInfos.add(INT);
    typeInfos.add(LONG);
    typeInfos.add(FLOAT);
    typeInfos.add(DOUBLE);
    typeInfos.add(BOOLEAN);
    typeInfos.add(DECIMAL);
    typeInfos.add(DATE);
    typeInfos.add(VOID);
    structTypeInfo.setAllStructFieldTypeInfos(typeInfos);
    LOGGER.info("structTypeInfo is " + structTypeInfo);

    final String specificSchema = IOUtils.toString(Resources.getResource("avro-struct.avsc")
        .openStream()).replace(lineSeparator, "");
    String expectedSchema = genSchema(
        specificSchema);

    Assert.assertEquals("Test for struct's avro schema failed",
        expectedSchema, getAvroSchemaString(structTypeInfo));
  }

  @Test
  public void createAvroNestedStructSchema() throws IOException {
    StructTypeInfo structTypeInfo = new StructTypeInfo();
    ArrayList<String> names = new ArrayList<String>();
    names.add("field1");
    names.add("field2");
    structTypeInfo.setAllStructFieldNames(names);
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(STRING);
    typeInfos.add(INT);
    structTypeInfo.setAllStructFieldTypeInfos(typeInfos);

    StructTypeInfo superStructTypeInfo = new StructTypeInfo();
    ArrayList<String> superNames = new ArrayList<String>();
    superNames.add("superfield1");
    superNames.add("superfield2");
    superStructTypeInfo.setAllStructFieldNames(superNames);
    ArrayList<TypeInfo> superTypeInfos = new ArrayList<TypeInfo>();
    superTypeInfos.add(STRING);
    superTypeInfos.add(structTypeInfo);
    superStructTypeInfo.setAllStructFieldTypeInfos(superTypeInfos);

    final String specificSchema = IOUtils.toString(Resources.getResource("avro-nested-struct.avsc")
        .openStream()).replace(lineSeparator, "");
    String expectedSchema = genSchema(
        specificSchema);
    Assert.assertEquals("Test for nested struct's avro schema failed",
        expectedSchema, getAvroSchemaString(superStructTypeInfo));
  }
}