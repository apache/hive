/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.iceberg.types.Types.NestedField.required;


public class TestIcebergObjectInspector {

  private final Schema schema = new Schema(
          required(1, "binary_field", Types.BinaryType.get(), "binary comment"),
          required(2, "boolean_field", Types.BooleanType.get(), "boolean comment"),
          required(3, "date_field", Types.DateType.get(), "date comment"),
          required(4, "decimal_field", Types.DecimalType.of(38, 18), "decimal comment"),
          required(5, "double_field", Types.DoubleType.get(), "double comment"),
          required(6, "fixed_field", Types.FixedType.ofLength(3), "fixed comment"),
          required(7, "float_field", Types.FloatType.get(), "float comment"),
          required(8, "integer_field", Types.IntegerType.get(), "integer comment"),
          required(9, "long_field", Types.LongType.get(), "long comment"),
          required(10, "string_field", Types.StringType.get(), "string comment"),
          required(11, "timestamp_field", Types.TimestampType.withoutZone(), "timestamp comment"),
          required(12, "timestamptz_field", Types.TimestampType.withZone(), "timestamptz comment"),
          required(13, "uuid_field", Types.UUIDType.get(), "uuid comment"),
          required(14, "list_field",
                  Types.ListType.ofRequired(15, Types.StringType.get()), "list comment"),
          required(16, "map_field",
                  Types.MapType.ofRequired(17, 18, Types.StringType.get(), Types.IntegerType.get()),
                  "map comment"),
          required(19, "struct_field", Types.StructType.of(
                  Types.NestedField.required(20, "nested_field", Types.StringType.get(), "nested field comment")),
                  "struct comment"
          ),
          required(21, "time_field", Types.TimeType.get(), "time comment")
  );

  @Test
  public void testIcebergObjectInspector() {
    ObjectInspector oi = IcebergObjectInspector.create(schema);
    Assertions.assertNotNull(oi);
    Assertions.assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());

    StructObjectInspector soi = (StructObjectInspector) oi;

    // binary
    StructField binaryField = soi.getStructFieldRef("binary_field");
    Assertions.assertEquals(1, binaryField.getFieldID());
    Assertions.assertEquals("binary_field", binaryField.getFieldName());
    Assertions.assertEquals("binary comment", binaryField.getFieldComment());
    Assertions.assertEquals(IcebergBinaryObjectInspector.get(),
        binaryField.getFieldObjectInspector());

    // boolean
    StructField booleanField = soi.getStructFieldRef("boolean_field");
    Assertions.assertEquals(2, booleanField.getFieldID());
    Assertions.assertEquals("boolean_field", booleanField.getFieldName());
    Assertions.assertEquals("boolean comment", booleanField.getFieldComment());
    Assertions.assertEquals(getPrimitiveObjectInspector(boolean.class),
        booleanField.getFieldObjectInspector());

    // date
    StructField dateField = soi.getStructFieldRef("date_field");
    Assertions.assertEquals(3, dateField.getFieldID());
    Assertions.assertEquals("date_field", dateField.getFieldName());
    Assertions.assertEquals("date comment", dateField.getFieldComment());
    if (HiveVersion.min(HiveVersion.HIVE_3)) {
      Assertions.assertEquals("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergDateObjectInspectorHive3",
          dateField.getFieldObjectInspector().getClass().getName());
    } else {
      Assertions.assertEquals("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergDateObjectInspector",
          dateField.getFieldObjectInspector().getClass().getName());
    }

    // decimal
    StructField decimalField = soi.getStructFieldRef("decimal_field");
    Assertions.assertEquals(4, decimalField.getFieldID());
    Assertions.assertEquals("decimal_field", decimalField.getFieldName());
    Assertions.assertEquals("decimal comment", decimalField.getFieldComment());
    Assertions.assertEquals(IcebergDecimalObjectInspector.get(38, 18),
        decimalField.getFieldObjectInspector());

    // double
    StructField doubleField = soi.getStructFieldRef("double_field");
    Assertions.assertEquals(5, doubleField.getFieldID());
    Assertions.assertEquals("double_field", doubleField.getFieldName());
    Assertions.assertEquals("double comment", doubleField.getFieldComment());
    Assertions.assertEquals(getPrimitiveObjectInspector(double.class),
        doubleField.getFieldObjectInspector());

    // fixed
    StructField fixedField = soi.getStructFieldRef("fixed_field");
    Assertions.assertEquals(6, fixedField.getFieldID());
    Assertions.assertEquals("fixed_field", fixedField.getFieldName());
    Assertions.assertEquals("fixed comment", fixedField.getFieldComment());
    Assertions.assertEquals(IcebergFixedObjectInspector.get(), fixedField.getFieldObjectInspector());

    // float
    StructField floatField = soi.getStructFieldRef("float_field");
    Assertions.assertEquals(7, floatField.getFieldID());
    Assertions.assertEquals("float_field", floatField.getFieldName());
    Assertions.assertEquals("float comment", floatField.getFieldComment());
    Assertions.assertEquals(getPrimitiveObjectInspector(float.class),
        floatField.getFieldObjectInspector());

    // integer
    StructField integerField = soi.getStructFieldRef("integer_field");
    Assertions.assertEquals(8, integerField.getFieldID());
    Assertions.assertEquals("integer_field", integerField.getFieldName());
    Assertions.assertEquals("integer comment", integerField.getFieldComment());
    Assertions.assertEquals(getPrimitiveObjectInspector(int.class),
        integerField.getFieldObjectInspector());

    // long
    StructField longField = soi.getStructFieldRef("long_field");
    Assertions.assertEquals(9, longField.getFieldID());
    Assertions.assertEquals("long_field", longField.getFieldName());
    Assertions.assertEquals("long comment", longField.getFieldComment());
    Assertions.assertEquals(getPrimitiveObjectInspector(long.class),
        longField.getFieldObjectInspector());

    // string
    StructField stringField = soi.getStructFieldRef("string_field");
    Assertions.assertEquals(10, stringField.getFieldID());
    Assertions.assertEquals("string_field", stringField.getFieldName());
    Assertions.assertEquals("string comment", stringField.getFieldComment());
    Assertions.assertEquals(getPrimitiveObjectInspector(String.class),
        stringField.getFieldObjectInspector());

    // timestamp without tz
    StructField timestampField = soi.getStructFieldRef("timestamp_field");
    Assertions.assertEquals(11, timestampField.getFieldID());
    Assertions.assertEquals("timestamp_field", timestampField.getFieldName());
    Assertions.assertEquals("timestamp comment", timestampField.getFieldComment());
    Assertions.assertEquals(IcebergTimestampObjectInspectorHive3.get(),
        timestampField.getFieldObjectInspector());

    // timestamp with tz
    StructField timestampTzField = soi.getStructFieldRef("timestamptz_field");
    Assertions.assertEquals(12, timestampTzField.getFieldID());
    Assertions.assertEquals("timestamptz_field", timestampTzField.getFieldName());
    Assertions.assertEquals("timestamptz comment", timestampTzField.getFieldComment());
    Assertions.assertEquals(IcebergTimestampWithZoneObjectInspectorHive3.get(),
        timestampTzField.getFieldObjectInspector());

    // UUID
    StructField uuidField = soi.getStructFieldRef("uuid_field");
    Assertions.assertEquals(13, uuidField.getFieldID());
    Assertions.assertEquals("uuid_field", uuidField.getFieldName());
    Assertions.assertEquals("uuid comment", uuidField.getFieldComment());
    Assertions.assertEquals(IcebergUUIDObjectInspector.get(), uuidField.getFieldObjectInspector());

    // list
    StructField listField = soi.getStructFieldRef("list_field");
    Assertions.assertEquals(14, listField.getFieldID());
    Assertions.assertEquals("list_field", listField.getFieldName());
    Assertions.assertEquals("list comment", listField.getFieldComment());
    Assertions.assertEquals(getListObjectInspector(String.class),
        listField.getFieldObjectInspector());

    // map
    StructField mapField = soi.getStructFieldRef("map_field");
    Assertions.assertEquals(16, mapField.getFieldID());
    Assertions.assertEquals("map_field", mapField.getFieldName());
    Assertions.assertEquals("map comment", mapField.getFieldComment());
    Assertions.assertEquals(getMapObjectInspector(String.class, int.class),
        mapField.getFieldObjectInspector());

    // struct
    StructField structField = soi.getStructFieldRef("struct_field");
    Assertions.assertEquals(19, structField.getFieldID());
    Assertions.assertEquals("struct_field", structField.getFieldName());
    Assertions.assertEquals("struct comment", structField.getFieldComment());

    ObjectInspector expectedObjectInspector = new IcebergRecordObjectInspector(
            (Types.StructType) schema.findType(19), ImmutableList.of(getPrimitiveObjectInspector(String.class)));
    Assertions.assertEquals(expectedObjectInspector, structField.getFieldObjectInspector());

    // time
    StructField timeField = soi.getStructFieldRef("time_field");
    Assertions.assertEquals(21, timeField.getFieldID());
    Assertions.assertEquals("time_field", timeField.getFieldName());
    Assertions.assertEquals("time comment", timeField.getFieldComment());
    Assertions.assertEquals(IcebergTimeObjectInspector.get(), timeField.getFieldObjectInspector());
  }

  private static ObjectInspector getPrimitiveObjectInspector(Class<?> clazz) {
    PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(clazz);
    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo);
  }

  private static ObjectInspector getListObjectInspector(Class<?> clazz) {
    return ObjectInspectorFactory.getStandardListObjectInspector(getPrimitiveObjectInspector(clazz));
  }

  private static ObjectInspector getMapObjectInspector(Class<?> keyClazz, Class<?> valueClazz) {
    return ObjectInspectorFactory.getStandardMapObjectInspector(
            getPrimitiveObjectInspector(keyClazz), getPrimitiveObjectInspector(valueClazz));
  }

}
