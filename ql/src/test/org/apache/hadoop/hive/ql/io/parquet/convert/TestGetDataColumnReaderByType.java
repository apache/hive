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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.DefaultParquetDataColumnReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromBooleanPageReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromDecimalPageReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromDoublePageReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromFloatPageReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromInt32PageReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromInt64PageReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromInt96PageReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromStringPageReader;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

/**
 * Tests for ParquetDataColumnReaderFactory#getDataColumnReaderByType.
 */
public class TestGetDataColumnReaderByType {

  @Test
  public void testGetDecimalReader() throws Exception {
    TypeInfo hiveTypeInfo = new DecimalTypeInfo(7, 2);
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(
            Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(20)
                .as(LogicalTypeAnnotation.decimalType(2, 5)).named("value"),
            hiveTypeInfo, null, true, null, false);
    assertTrue(reader instanceof TypesFromDecimalPageReader);
  }

  @Test
  public void testGetStringReader() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("string");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory.getDataColumnReaderByType(Types
        .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value"),
        hiveTypeInfo, null, true, null, false);
    assertTrue(reader instanceof TypesFromStringPageReader);
  }

  @Test
  public void testGetDecimalReaderFromBinaryPrimitive() throws Exception {
    TypeInfo hiveTypeInfo = new DecimalTypeInfo(7, 2);
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.decimalType(2, 5)).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof TypesFromDecimalPageReader);
  }

  @Test
  public void testGetBinaryReaderNoOriginalType() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("string");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.BINARY).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof DefaultParquetDataColumnReader);
  }

  @Test
  public void testGetBinaryReaderJsonOriginalType() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("binary");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory.getDataColumnReaderByType(Types
        .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.jsonType()).named("value"),
        hiveTypeInfo, null, true, null, false);
    assertTrue(reader instanceof DefaultParquetDataColumnReader);
  }

  @Test
  public void testGetIntReader() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("int");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false)).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof TypesFromInt32PageReader);
  }

  @Test
  public void testGetIntReaderNoOriginalType() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("int");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.INT32).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof TypesFromInt32PageReader);
  }

  @Test
  public void testGetInt64ReaderNoOriginalType() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("bigint");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory.getDataColumnReaderByType(
        Types.optional(PrimitiveTypeName.INT64).named("value"), hiveTypeInfo, null, true, null, false);
    assertTrue(reader instanceof TypesFromInt64PageReader);
  }

  @Test
  public void testGetInt64Reader() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("bigint");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.intType(64, false)).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof TypesFromInt64PageReader);
  }

  @Test
  public void testGetFloatReader() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("float");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.FLOAT).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof TypesFromFloatPageReader);
  }

  @Test
  public void testGetDoubleReader() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("double");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.DOUBLE).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof TypesFromDoublePageReader);
  }

  @Test
  public void testGetInt96Reader() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("timestamp");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.INT96).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof TypesFromInt96PageReader);
  }

  @Test
  public void testGetBooleanReader() throws Exception {
    PrimitiveTypeInfo hiveTypeInfo = new PrimitiveTypeInfo();
    hiveTypeInfo.setTypeName("boolean");
    ParquetDataColumnReader reader = ParquetDataColumnReaderFactory
        .getDataColumnReaderByType(Types.optional(PrimitiveTypeName.BOOLEAN).named("value"), hiveTypeInfo, null, true,
            null, false);
    assertTrue(reader instanceof TypesFromBooleanPageReader);
  }
}
