/**
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

import org.apache.hadoop.hive.ql.io.parquet.convert.ParquetToHiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.schema.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TestParquetToHiveSchemaConverter {

  static ParquetToHiveSchemaConverter parquetToHiveSchemaConverter;

  @BeforeClass
  public static void setUp() {
    parquetToHiveSchemaConverter = new ParquetToHiveSchemaConverter();
  }

  @Test
  public void testUtf8() {
    GroupType groupType = Types.repeatedGroup().
                                  required(PrimitiveType.PrimitiveTypeName.BINARY).
                                    as(OriginalType.UTF8).
                                  named("utf8").
                                named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("utf8");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.stringTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testEnum() {
    GroupType groupType = Types.repeatedGroup().
                                  required(PrimitiveType.PrimitiveTypeName.BINARY).
                                    as(OriginalType.ENUM).
                                  named("enum").
                                named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("enum");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.stringTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testBinary() {
    GroupType groupType = Types.requiredGroup().
                                 required(PrimitiveType.PrimitiveTypeName.BINARY).
                                 named("binary").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("binary");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.binaryTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testBoolean() {
    GroupType groupType = Types.requiredGroup().
                                 required(PrimitiveType.PrimitiveTypeName.BOOLEAN).
                                 named("boolean").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("boolean");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.booleanTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testDouble() {
    GroupType groupType = Types.repeatedGroup().
                                 required(PrimitiveType.PrimitiveTypeName.DOUBLE).
                                 named("double").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("double");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.doubleTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testFixedLenByteArray() {
    GroupType groupType = Types.requiredGroup().
                                 required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).
                                   length(8).
                                 named("fixedLenByteArray").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("fixedLenByteArray");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.binaryTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testFloat() {
    GroupType groupType = Types.requiredGroup().
                                 required(PrimitiveType.PrimitiveTypeName.FLOAT).
                                 named("float").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("float");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.floatTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testInt32() {
    GroupType groupType = Types.repeatedGroup().
                                 required(PrimitiveType.PrimitiveTypeName.INT32).
                                 named("int32").
                               named("top");


    ArrayList<String> names = new ArrayList<String>();
    names.add("int32");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.intTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testInt64() {
    GroupType groupType = Types.repeatedGroup().
                                 required(PrimitiveType.PrimitiveTypeName.INT64).
                                 named("int64").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("int64");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.longTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void testInt96() {
    GroupType groupType = Types.requiredGroup().
                                 required(PrimitiveType.PrimitiveTypeName.INT96).
                                 named("int96").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("int96");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.timestampTypeInfo);

    Properties props = new Properties();
    props.setProperty("parquet.int96.is.timestamp", "");

    test(groupType, names, typeInfos, new ParquetToHiveSchemaConverter(props));
  }

  @Test
  public void testInt96Negative() {
    GroupType groupType = Types.requiredGroup().
                                 required(PrimitiveType.PrimitiveTypeName.INT96).
                                 named("int96").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("int96");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.timestampTypeInfo);

    try {
      test(groupType, names, typeInfos);
      Assert.fail("Failed to throw UnsupportedOperationException for INT96");
    } catch (UnsupportedOperationException use) {
      // It's good!
    }
  }

  @Test
  public void primitiveOptional() {
    GroupType groupType = Types.optionalGroup().
                                 optional(PrimitiveType.PrimitiveTypeName.INT32).
                                 named("int32").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("int32");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.intTypeInfo);

    test(groupType, names, typeInfos);
  }

  @Test
  public void listOfPrimitives() {
    GroupType groupType = Types.requiredGroup().
                                 repeated(PrimitiveType.PrimitiveTypeName.INT32).
                                 named("intlist").
                               named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("intlist");
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.intTypeInfo));

    test(groupType, names, typeInfos);
  }

  @Test
  public void listOfStruct() {
    GroupType groupType = Types.requiredGroup().
                                  repeatedGroup().
                                    required(PrimitiveType.PrimitiveTypeName.BINARY).
                                      as(OriginalType.UTF8).
                                    named("string").
                                    required(PrimitiveType.PrimitiveTypeName.FLOAT).
                                    named("float").
                                  named("structlist").
                                named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("structlist");
    final List<TypeInfo> structTypeInfos = new ArrayList<TypeInfo>();
    structTypeInfos.add(TypeInfoFactory.stringTypeInfo);
    structTypeInfos.add(TypeInfoFactory.floatTypeInfo);
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.getStructTypeInfo(Arrays.asList
        ("string", "float"), structTypeInfos)));

    test(groupType, names, typeInfos);
  }

  @Test
  public void structOfPrimitives() {
    GroupType groupType = Types.requiredGroup().
                                  requiredGroup().
                                    required(PrimitiveType.PrimitiveTypeName.BINARY).
                                      as(OriginalType.UTF8).
                                    named("utf8").
                                    required(PrimitiveType.PrimitiveTypeName.BINARY).
                                      as(OriginalType.ENUM).
                                    named("enum").
                                    required(PrimitiveType.PrimitiveTypeName.BINARY).
                                    named("binary").
                                    required(PrimitiveType.PrimitiveTypeName.BOOLEAN).
                                    named("boolean").
                                    required(PrimitiveType.PrimitiveTypeName.DOUBLE).
                                    named("double").
                                    required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).
                                      length(8).
                                    named("fixedLenByteArray").
                                    required(PrimitiveType.PrimitiveTypeName.FLOAT).
                                    named("float").
                                    required(PrimitiveType.PrimitiveTypeName.INT32).
                                    named("int32").
                                    required(PrimitiveType.PrimitiveTypeName.INT64).
                                    named("int64").
                                  named("struct").
                                named("top");

    ArrayList<String> names = new ArrayList<String>();
    names.add("struct");
    final List<TypeInfo> structTypeInfos = new ArrayList<TypeInfo>();
    structTypeInfos.add(TypeInfoFactory.stringTypeInfo);
    structTypeInfos.add(TypeInfoFactory.stringTypeInfo);
    structTypeInfos.add(TypeInfoFactory.binaryTypeInfo);
    structTypeInfos.add(TypeInfoFactory.booleanTypeInfo);
    structTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
    structTypeInfos.add(TypeInfoFactory.binaryTypeInfo);
    structTypeInfos.add(TypeInfoFactory.floatTypeInfo);
    structTypeInfos.add(TypeInfoFactory.intTypeInfo);
    structTypeInfos.add(TypeInfoFactory.longTypeInfo);
    ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    typeInfos.add(TypeInfoFactory.getStructTypeInfo(Arrays.asList
            ("utf8", "enum", "binary", "boolean", "double", "fixedLenByteArray", "float",
                "int32", "int64"),
        structTypeInfos));

    test(groupType, names, typeInfos);
  }

  @Test
  public void nestedStruct() {
    GroupType groupType = Types.requiredGroup().
                                  requiredGroup().
                                    requiredGroup().
                                      required(PrimitiveType.PrimitiveTypeName.INT32).
                                      named("int").
                                      required(PrimitiveType.PrimitiveTypeName.BINARY).
                                        as(OriginalType.UTF8).
                                      named("string").
                                    named("innerstruct").
                                  named("outerstruct").
                                named("top");

    final ArrayList<TypeInfo> typeInfos = new ArrayList(Arrays.asList(
        TypeInfoFactory.getStructTypeInfo(Arrays.asList("innerstruct"), Arrays.asList(
            TypeInfoFactory.getStructTypeInfo(new ArrayList(Arrays.asList("int", "string")),
                new ArrayList<TypeInfo>(Arrays.asList(
                    TypeInfoFactory.intTypeInfo,
                    TypeInfoFactory.stringTypeInfo)))
        ))
    ));

    test(groupType, new ArrayList<String>(Arrays.asList("outerstruct")), typeInfos);
  }

  @Test
  public void map() {
    GroupType groupType = Types.requiredGroup().
        addField(
          ConversionPatterns.mapType(
            Type.Repetition.REQUIRED,
            "map",
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("key"),
            Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("value"))
        ).named("top");


    final ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>(Arrays.asList(
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.doubleTypeInfo)
    ));

    test(groupType, new ArrayList<String>(Arrays.asList("map")), typeInfos);
  }

  @Test
  public void nestedMap() {

    GroupType groupType = Types.requiredGroup().
      addField(
        ConversionPatterns.mapType(
          Type.Repetition.REQUIRED,
          "map",
          Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("key"),
          ConversionPatterns.mapType(
            Type.Repetition.REQUIRED,
            "value",
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named
              ("key"),
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("value")
          )
        )
      ).named("top");

    final ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>(Arrays.asList(
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo,
                TypeInfoFactory.longTypeInfo))
    ));

    test(groupType, new ArrayList<String>(Arrays.asList("map")), typeInfos);
  }

  private void test(GroupType groupType, ArrayList<String> names, ArrayList<TypeInfo> typeInfos) {
    test(groupType, names, typeInfos, parquetToHiveSchemaConverter);
  }

  private void test(GroupType groupType, ArrayList<String> names, ArrayList<TypeInfo> typeInfos,
                    ParquetToHiveSchemaConverter converter) {
    StructTypeInfo structTypeInfo = new StructTypeInfo();
    structTypeInfo.setAllStructFieldNames(names);
    structTypeInfo.setAllStructFieldTypeInfos(typeInfos);

    final StructTypeInfo actualTypeInfo = converter.convert(groupType);
    Assert.assertEquals(structTypeInfo, actualTypeInfo);
  }
}