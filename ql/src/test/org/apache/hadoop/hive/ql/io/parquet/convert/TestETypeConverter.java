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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet.convert;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.convert.ETypeConverter.BinaryConverter;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.Test;

/**
 * Tests for class ETypeConverter.
 */
public class TestETypeConverter {

  @Test
  public void testGetDecimalConverter() throws Exception {
    TypeInfo hiveTypeInfo = new DecimalTypeInfo(7, 2);
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("value");
    Writable writable = getWritableFromBinaryConverter(hiveTypeInfo, primitiveType, Binary.fromString("155"));
    HiveDecimalWritable decimalWritable = (HiveDecimalWritable) writable;
    assertEquals(2, decimalWritable.getScale());
  }

  @Test
  public void testGetDecimalConverterIntHiveType() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("value");
    Writable writable =
        getWritableFromPrimitiveConverter(createHiveTypeInfo("int"), primitiveType, 2200);
    IntWritable intWritable = (IntWritable) writable;
    assertEquals(22, intWritable.get());
  }

  @Test
  public void testGetDecimalConverterBigIntHiveType() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("value");
    Writable writable =
        getWritableFromPrimitiveConverter(createHiveTypeInfo("bigint"), primitiveType, 2200);
    LongWritable longWritable = (LongWritable) writable;
    assertEquals(22, longWritable.get());
  }

  @Test
  public void testGetDecimalConverterFloatHiveType() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("value");
    Writable writable =
        getWritableFromPrimitiveConverter(createHiveTypeInfo("float"), primitiveType, 2200);
    FloatWritable floatWritable = (FloatWritable) writable;
    assertEquals(22, (int)floatWritable.get());
  }

  @Test
  public void testGetDecimalConverterDoubleHiveType() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.decimalType(2, 7)).named("value");
    Writable writable =
        getWritableFromPrimitiveConverter(createHiveTypeInfo("double"), primitiveType, 2200);
    DoubleWritable doubleWritable = (DoubleWritable) writable;
    assertEquals(22, (int) doubleWritable.get());
  }

  @Test
  public void testGetInt64TimestampConverterTinyIntHiveType() {
    testGetInt64TimestampConverterNumericHiveType("1970-01-01 00:00:00.005", "tinyint", 5);
  }

  @Test
  public void testGetInt64TimestampConverterSmallIntHiveType() {
    testGetInt64TimestampConverterNumericHiveType("1970-01-01 00:00:00.005", "smallint", 5);
  }

  @Test
  public void testGetInt64TimestampConverterIntHiveType() {
    testGetInt64TimestampConverterNumericHiveType("1970-01-01 00:00:00.005", "int", 5);
  }

  @Test
  public void testGetInt64TimestampConverterBigIntHiveType() {
    testGetInt64TimestampConverterNumericHiveType("1998-10-03 09:58:31.231", "bigint", 907408711231L);
  }

  @Test
  public void testGetInt64TimestampConverterFloatHiveType() {
    testGetInt64TimestampConverterNumericHiveType("1970-01-01 00:00:00.005", "float", 5.0f);
  }

  @Test
  public void testGetInt64TimestampConverterDoubleHiveType() {
    testGetInt64TimestampConverterNumericHiveType("1970-01-01 00:00:00.005", "double", 5.0d);
  }

  @Test
  public void testGetInt64TimestampConverterDecimalHiveType() {
    testGetInt64TimestampConverterNumericHiveType("1970-01-01 00:00:00.005", "decimal(1,0)", HiveDecimal.create(5));
  }

  @Test
  public void testGetInt64TimestampConverterNoHiveType() {
    Timestamp ts = Timestamp.valueOf("2022-10-24 11:35:00.005");
    PrimitiveType primitiveType = createInt64TimestampType(false, TimeUnit.MILLIS);
    Writable writable = getWritableFromPrimitiveConverter(null, primitiveType, ts.toEpochMilli());
    assertEquals("2022-10-24 11:35:00.005", ((TimestampWritableV2) writable).getTimestamp().toString());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetInt64NoLogicalAnnotationTimestampHiveType() {
    Timestamp ts = Timestamp.valueOf("2022-10-24 11:43:00.005");
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT64).named("int64");
    getWritableFromPrimitiveConverter(TypeInfoFactory.timestampTypeInfo, primitiveType, ts.toEpochMilli());
  }
  
  private void testGetInt64TimestampConverterNumericHiveType(String timestamp, String type, Object expected) {
    Timestamp ts = Timestamp.valueOf(timestamp);
    PrimitiveType primitiveType = createInt64TimestampType(false, TimeUnit.MILLIS);
    PrimitiveTypeInfo info = getPrimitiveTypeInfo(type);
    Writable writable = getWritableFromPrimitiveConverter(info, primitiveType, ts.toEpochMilli());
    final Object actual;
    switch (info.getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
      actual = ((IntWritable) writable).get();
      break;
    case LONG:
      actual = ((LongWritable) writable).get();
      break;
    case FLOAT:
      actual = ((FloatWritable) writable).get();
      break;
    case DOUBLE:
      actual = ((DoubleWritable) writable).get();
      break;
    case DECIMAL:
      actual = ((HiveDecimalWritable) writable).getHiveDecimal();
      break;
    default:
      throw new IllegalStateException(info.toString());
    }
    assertEquals(expected, actual);
  }

  @Test
  public void testGetInt96TimestampConverterBigIntHiveType() {
    Timestamp timestamp = Timestamp.valueOf("1998-10-03 09:58:31.231");
    NanoTime nanoTime = NanoTimeUtils.getNanoTime(timestamp, ZoneOffset.UTC, false);
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT96).named("value");
    Writable writable = getWritableFromBinaryConverter(createHiveTypeInfo("bigint"), primitiveType, nanoTime.toBinary());
    // Retrieve as BigInt
    LongWritable longWritable = (LongWritable) writable;
    assertEquals(nanoTime.getTimeOfDayNanos(), longWritable.get());
  }

  @Test
  public void testGetTimestampConverter() throws Exception {
    Timestamp timestamp = Timestamp.valueOf("2018-06-15 15:12:20.0");
    NanoTime nanoTime = NanoTimeUtils.getNanoTime(timestamp, ZoneOffset.UTC, false);
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT96).named("value");
    Writable writable = getWritableFromBinaryConverter(null, primitiveType, nanoTime.toBinary());
    TimestampWritableV2 timestampWritable = (TimestampWritableV2) writable;
    assertEquals(timestamp.getNanos(), timestampWritable.getNanos());
  }

  @Test
  public void testGetTimestampProlepticConverter() throws Exception {
    Timestamp timestamp = Timestamp.valueOf("1572-06-15 15:12:20.0");
    NanoTime nanoTime = NanoTimeUtils.getNanoTime(timestamp, ZoneOffset.UTC, false);
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT96).named("value");
    Writable writable = getWritableFromBinaryConverter(null, primitiveType, nanoTime.toBinary());
    TimestampWritableV2 timestampWritable = (TimestampWritableV2) writable;
    assertEquals(timestamp.getNanos(), timestampWritable.getNanos());
  }

  @Test
  public void testGetInt64MillisTimestampConverter() throws Exception {
    Timestamp timestamp = Timestamp.valueOf("2018-07-15 15:12:20.112");
    PrimitiveType primitiveType = createInt64TimestampType(false, TimeUnit.MILLIS);
    Writable writable = getWritableFromPrimitiveConverter(null, primitiveType, timestamp.toEpochMilli());
    TimestampWritableV2 timestampWritable = (TimestampWritableV2) writable;
    assertEquals(timestamp.toEpochMilli(), timestampWritable.getTimestamp().toEpochMilli());
  }

  @Test
  public void testGetInt64MillisTimestampProlepticConverter() throws Exception {
    Timestamp timestamp = Timestamp.valueOf("1572-07-15 15:12:20.112");
    PrimitiveType primitiveType = createInt64TimestampType(false, TimeUnit.MILLIS);
    Writable writable = getWritableFromPrimitiveConverter(null, primitiveType, timestamp.toEpochMilli());
    TimestampWritableV2 timestampWritable = (TimestampWritableV2) writable;
    assertEquals(timestamp.toEpochMilli(), timestampWritable.getTimestamp().toEpochMilli());
  }

  @Test
  public void testGetInt64MicrosTimestampConverter() throws Exception {
    Timestamp timestamp = Timestamp.valueOf("2018-07-15 15:12:20.112233");
    PrimitiveType primitiveType = createInt64TimestampType(false, TimeUnit.MICROS);
    long time = timestamp.toEpochSecond() * 1000000 + timestamp.getNanos() / 1000;
    Writable writable = getWritableFromPrimitiveConverter(null, primitiveType, time);
    TimestampWritableV2 timestampWritable = (TimestampWritableV2) writable;
    assertEquals(timestamp.toEpochMilli(), timestampWritable.getTimestamp().toEpochMilli());
    assertEquals(timestamp.getNanos(), timestampWritable.getNanos());
  }

  @Test
  public void testGetInt64NanosTimestampConverter() throws Exception {
    Timestamp timestamp = Timestamp.valueOf("2018-07-15 15:12:20.11223344");
    PrimitiveType primitiveType = createInt64TimestampType(false, TimeUnit.NANOS);
    long time = timestamp.toEpochSecond() * 1000000000 + timestamp.getNanos();
    Writable writable = getWritableFromPrimitiveConverter(null, primitiveType, time);
    TimestampWritableV2 timestampWritable = (TimestampWritableV2) writable;
    assertEquals(timestamp.toEpochMilli(), timestampWritable.getTimestamp().toEpochMilli());
    assertEquals(timestamp.getNanos(), timestampWritable.getNanos());
  }

  @Test
  public void testGetInt64NanosAdjustedToUTCTimestampConverter() throws Exception {
    ZoneId zone = ZoneId.systemDefault();
    Timestamp timestamp = Timestamp.valueOf("2018-07-15 15:12:20.11223344");
    PrimitiveType primitiveType = createInt64TimestampType(true, TimeUnit.NANOS);
    long time = timestamp.toEpochSecond() * 1000000000 + timestamp.getNanos();
    Writable writable = getWritableFromPrimitiveConverter(null, primitiveType, time);
    TimestampWritableV2 timestampWritable = (TimestampWritableV2) writable;
    timestamp = Timestamp.ofEpochSecond(timestamp.toEpochSecond(), timestamp.getNanos(), zone);
    assertEquals(timestamp.toEpochMilli(), timestampWritable.getTimestamp().toEpochMilli());
    assertEquals(timestamp.getNanos(), timestampWritable.getNanos());
  }

  @Test
  public void testGetTextConverterForString() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("value");
    String value = "this_is_a_value";
    Text textWritable = (Text) getWritableFromBinaryConverter(stringTypeInfo, primitiveType,
            Binary.fromString(value));
    assertEquals(value, textWritable.toString());
  }

  @Test
  public void testGetTextConverterForCharPadsValueWithSpacesTillLen() {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("value");
    String value = "this_is_a_value";
    HiveCharWritable textWritable = (HiveCharWritable) getWritableFromBinaryConverter(
            new CharTypeInfo(value.length() + 2), primitiveType, Binary.fromString(value));
    assertEquals(value + "  ", textWritable.toString());
  }

  @Test
  public void testGetTextConverterForCharTruncatesValueExceedingLen() {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("value");
    String value = "this_is_a_value";
    HiveCharWritable textWritable = (HiveCharWritable) getWritableFromBinaryConverter(
            new CharTypeInfo(6), primitiveType, Binary.fromString(value));
    assertEquals(value.substring(0, 6), textWritable.toString());
  }

  @Test
  public void testGetTextConverterForVarcharTruncatesValueExceedingLen() {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("value");
    String value = "this_is_a_value";
    HiveVarcharWritable textWritable = (HiveVarcharWritable) getWritableFromBinaryConverter(
            new VarcharTypeInfo(6), primitiveType, Binary.fromString(value));
    assertEquals(value.substring(0, 6), textWritable.toString());
  }

  @Test
  public void testGetTextConverterForVarchar() {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType()).named("value");
    String value = "this_is_a_value";
    HiveVarcharWritable textWritable = (HiveVarcharWritable) getWritableFromBinaryConverter(
            new VarcharTypeInfo(34), primitiveType, Binary.fromString(value));
    assertEquals(value, textWritable.toString());
  }

  @Test
  public void testGetTextConverterNoHiveTypeInfo() {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType()).named("value");
    String value = "this_is_a_value";
    Text textWritable = (Text) getWritableFromBinaryConverter(null, primitiveType, Binary.fromString(value));
    assertEquals(value, textWritable.toString());
  }

  @Test
  public void testGetIntConverterForTinyInt() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT32)
        .as(LogicalTypeAnnotation.intType(8, false)).named("value");
    Writable writable =
        getWritableFromPrimitiveConverter(createHiveTypeInfo("tinyint"), primitiveType, 125);
    IntWritable intWritable = (IntWritable) writable;
    assertEquals(125, intWritable.get());
  }

  @Test
  public void testGetIntConverterForFloat() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT32).named("value");
    Writable writable = getWritableFromPrimitiveConverter(createHiveTypeInfo("float"), primitiveType, 22225);
    FloatWritable floatWritable = (FloatWritable) writable;
    assertEquals((float) 22225, (float) floatWritable.get(), 0);
  }

  @Test
  public void testGetIntConverterForBigint() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT32).named("value");
    Writable writable = getWritableFromPrimitiveConverter(createHiveTypeInfo("bigint"), primitiveType, 22225);
    LongWritable longWritable = (LongWritable) writable;
    assertEquals(22225, longWritable.get());
  }

  @Test
  public void testGetIntConverterForDouble() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT32).named("value");
    Writable writable = getWritableFromPrimitiveConverter(createHiveTypeInfo("double"), primitiveType, 22225);
    DoubleWritable doubleWritable = (DoubleWritable) writable;
    assertEquals((double) 22225, (double) doubleWritable.get(), 0);
  }

  @Test
  public void testGetIntConverterForSmallint() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT32)
        .as(LogicalTypeAnnotation.intType(16, false)).named("value");
    Writable writable =
        getWritableFromPrimitiveConverter(createHiveTypeInfo("smallint"), primitiveType, 32766);
    IntWritable intWritable = (IntWritable) writable;
    assertEquals(32766, intWritable.get());
  }

  @Test
  public void testGetIntConverterNoHiveTypeInfo() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT32).named("value");
    Writable writable = getWritableFromPrimitiveConverter(null, primitiveType, 12225);
    IntWritable intWritable = (IntWritable) writable;
    assertEquals(12225, intWritable.get());
  }

  @Test
  public void testGetDoubleConverter() throws Exception {
    MyConverterParent converterParent = new MyConverterParent();
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.DOUBLE).named("value");
    PrimitiveConverter converter = ETypeConverter.getNewConverter(primitiveType, 1, converterParent, null);
    ((PrimitiveConverter) converter).addDouble(3276);
    Writable writable = converterParent.getValue();
    DoubleWritable doubleWritable = (DoubleWritable) writable;
    assertEquals(3276, doubleWritable.get(), 0);
  }

  @Test
  public void testGetBooleanConverter() throws Exception {
    MyConverterParent converterParent = new MyConverterParent();
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BOOLEAN).named("value");
    PrimitiveConverter converter = ETypeConverter.getNewConverter(primitiveType, 1, converterParent, null);
    ((PrimitiveConverter) converter).addBoolean(true);
    Writable writable = converterParent.getValue();
    BooleanWritable booleanWritable = (BooleanWritable) writable;
    assertEquals(true, booleanWritable.get());
  }

  @Test
  public void testGetFloatConverter() throws Exception {
    MyConverterParent converterParent = new MyConverterParent();
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.FLOAT).named("value");
    PrimitiveConverter converter = ETypeConverter.getNewConverter(primitiveType, 1, converterParent, null);
    ((PrimitiveConverter) converter).addFloat(3276f);
    Writable writable = converterParent.getValue();
    FloatWritable floatWritable = (FloatWritable) writable;
    assertEquals(3276f, floatWritable.get(), 0);
  }

  @Test
  public void testGetFloatConverterForDouble() throws Exception {
    MyConverterParent converterParent = new MyConverterParent();
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.FLOAT).named("value");
    PrimitiveConverter converter =
        ETypeConverter.getNewConverter(primitiveType, 1, converterParent, createHiveTypeInfo("double"));
    ((PrimitiveConverter) converter).addFloat(3276f);
    Writable writable = converterParent.getValue();
    DoubleWritable doubleWritable = (DoubleWritable) writable;
    assertEquals(3276d, doubleWritable.get(), 0);
  }

  @Test
  public void testGetBinaryConverter() throws Exception {
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.BINARY).named("value");
    Writable writable = getWritableFromBinaryConverter(null, primitiveType, Binary.fromString("this_is_a_value"));
    BytesWritable byteWritable = (BytesWritable) writable;
    assertEquals("this_is_a_value", new String(byteWritable.getBytes()));
  }

  @Test
  public void testGetLongConverter() throws Exception {
    MyConverterParent converterParent = new MyConverterParent();
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT64).named("value");
    PrimitiveConverter converter = ETypeConverter.getNewConverter(primitiveType, 1, converterParent, null);
    ((PrimitiveConverter) converter).addLong(12225);
    Writable writable = converterParent.getValue();
    LongWritable longWritable = (LongWritable) writable;
    assertEquals(12225L, longWritable.get());
  }

  @Test
  public void testGetConverterForList() {
    MyConverterParent converterParent = new MyConverterParent();
    GroupType type =
        Types.optionalList().element(Types.optional(PrimitiveTypeName.INT64).named("value")).named("array");
    HiveGroupConverter f = HiveGroupConverter.getConverterFromDescription(type, 1, converterParent, null);
    assertTrue(f instanceof HiveCollectionConverter);
  }

  @Test
  public void testGetConverterForMap() {
    MyConverterParent converterParent = new MyConverterParent();
    GroupType type = Types.optionalMap().key(Types.optional(PrimitiveTypeName.INT64).named("key"))
        .value(Types.optional(PrimitiveTypeName.INT64).named("value")).named("map");
    HiveGroupConverter f = HiveGroupConverter.getConverterFromDescription(type, 1, converterParent, null);
    assertTrue(f instanceof HiveCollectionConverter);
  }

  @Test
  public void testGetConverterForStruct() {
    MyConverterParent converterParent = new MyConverterParent();
    GroupType type = Types.buildGroup(Repetition.OPTIONAL).named("struct");
    HiveGroupConverter f = HiveGroupConverter.getConverterFromDescription(type, 1, converterParent, null);
    assertTrue(f instanceof HiveStructConverter);
  }

  private Writable getWritableFromBinaryConverter(TypeInfo hiveTypeInfo, PrimitiveType primitiveType,
      Binary valueToAdd) {
    MyConverterParent converterParent = new MyConverterParent();
    PrimitiveConverter converter = ETypeConverter.getNewConverter(primitiveType, 1, converterParent, hiveTypeInfo);
    ((BinaryConverter) converter).addBinary(valueToAdd);
    return converterParent.getValue();
  }

  private Writable getWritableFromPrimitiveConverter(TypeInfo hiveTypeInfo, PrimitiveType primitiveType,
      Integer valueToAdd) {
    MyConverterParent converterParent = new MyConverterParent();
    PrimitiveConverter converter = ETypeConverter.getNewConverter(primitiveType, 1, converterParent, hiveTypeInfo);
    ((PrimitiveConverter) converter).addInt(valueToAdd);
    return converterParent.getValue();
  }

  private Writable getWritableFromPrimitiveConverter(TypeInfo hiveTypeInfo, PrimitiveType primitiveType,
      Long valueToAdd) {
    MyConverterParent converterParent = new MyConverterParent();
    PrimitiveConverter converter = ETypeConverter.getNewConverter(primitiveType, 1, converterParent, hiveTypeInfo);
    ((PrimitiveConverter) converter).addLong(valueToAdd);
    return converterParent.getValue();
  }

  private PrimitiveTypeInfo createHiveTypeInfo(String typeName) {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName(typeName);
    return hiveTypeInfo;
  }

  private PrimitiveType createInt64TimestampType(boolean isAdjustedToUTC, TimeUnit unit) {
    TimestampLogicalTypeAnnotation logicalType = TimestampLogicalTypeAnnotation.timestampType(isAdjustedToUTC, unit);
    PrimitiveType primitiveType = Types.optional(PrimitiveTypeName.INT64).as(logicalType).named("value");
    return primitiveType;
  }
}
