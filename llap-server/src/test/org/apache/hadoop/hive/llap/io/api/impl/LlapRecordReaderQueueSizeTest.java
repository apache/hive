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

package org.apache.hadoop.hive.llap.io.api.impl;

import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Supplier;
import java.util.stream.IntStream;

public class LlapRecordReaderQueueSizeTest {

  private static final int END_EXCLUSIVE = 300;
  private static final int MAX_BUFFERED_SIZE = 1 << 30; //1GB

  @Test public void testMaxEqMin() {
    int expected = LlapRecordReader.determineQueueLimit(0, 100, 100, null, true);
    Assert.assertEquals(100, expected);
  }

  @Test public void testMaxIsEnforced() {
    TypeInfo[] cols = { new DecimalTypeInfo() };
    int actual = LlapRecordReader.determineQueueLimit(Long.MAX_VALUE, 10, 1, cols, true);
    Assert.assertEquals(10, actual);
  }

  @Test public void testMinIsEnforced() {
    TypeInfo[] cols = { new DecimalTypeInfo() };
    int actual = LlapRecordReader.determineQueueLimit(0, 10, 5, cols, true);
    Assert.assertEquals(5, actual);
  }

  @Test public void testOrderDecimal64VsFatDecimals() {
    TypeInfo[] cols = IntStream.range(0, 300).mapToObj(i -> new DecimalTypeInfo()).toArray(TypeInfo[]::new);
    int actual = LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, cols, true);
    Assert.assertEquals(75, actual);
    // the idea it to see an order of 10 when using fat Decimals
    actual = LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, cols, false);
    Assert.assertEquals(7, actual);
  }

  @Test public void testOrderDecimal64VsLong() {
    TypeInfo[] decimalCols = ArrayOf(() -> new DecimalTypeInfo(TypeDescription.MAX_DECIMAL64_PRECISION, 0));
    TypeInfo[] longCols = ArrayOf(() -> TypeInfoFactory.longTypeInfo);
    Assert.assertEquals(LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, longCols, true),
        LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, decimalCols, true));
  }

  @Test public void testStringsColumns() {
    TypeInfo[] charsCols = ArrayOf(() -> TypeInfoFactory.charTypeInfo);
    TypeInfo[] stringCols = ArrayOf(() -> TypeInfoFactory.stringTypeInfo);
    TypeInfo[] binaryCols = ArrayOf(() -> TypeInfoFactory.binaryTypeInfo);
    Assert.assertEquals(LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, stringCols, true), 9);
    Assert.assertEquals(9, LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, charsCols, true));
    Assert.assertEquals(9, LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, binaryCols, true));
  }

  @Test public void testLongColumns() {
    TypeInfo[] longsCols = ArrayOf(() -> TypeInfoFactory.longTypeInfo);
    TypeInfo[] intCols = ArrayOf(() -> TypeInfoFactory.intTypeInfo);
    TypeInfo[] byteCols = ArrayOf(() -> TypeInfoFactory.byteTypeInfo);
    Assert.assertEquals(75, LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, longsCols, true));
    Assert.assertEquals(75, LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, intCols, true));
    Assert.assertEquals(75, LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, byteCols, true));
  }

  @Test public void testTimestampsColumns() {
    TypeInfo[] tsCols = ArrayOf(() -> TypeInfoFactory.timestampTypeInfo);
    TypeInfo[] intervalCols = ArrayOf(() -> TypeInfoFactory.intervalDayTimeTypeInfo);
    Assert.assertEquals(38, LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, tsCols, true));
    Assert.assertEquals(38, LlapRecordReader.determineQueueLimit(MAX_BUFFERED_SIZE, 10000, 5, intervalCols, true));
  }

  private static TypeInfo[] ArrayOf(Supplier<TypeInfo> supplier) {
    return IntStream.range(0, END_EXCLUSIVE).mapToObj(i -> supplier.get()).toArray(TypeInfo[]::new);
  }
}
