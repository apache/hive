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

package org.apache.hadoop.hive.ql.io.parquet;

import org.apache.parquet.column.ParquetProperties.WriterVersion;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestVectorizedDictionaryEncodingColumnReader extends VectorizedColumnReaderTestBase {
  static boolean isDictionaryEncoding = true;

  @BeforeClass
  public static void setup() throws IOException {
    removeFile();
    writeData(initWriterFromFile(), isDictionaryEncoding);
    writeFlatFile(WriterVersion.PARQUET_1_0, isDictionaryEncoding);
    writeFlatFile(WriterVersion.PARQUET_2_0, isDictionaryEncoding);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    removeFile();
    deleteFlatFiles(isDictionaryEncoding);
  }

  @Test
  public void testIntRead() throws Exception {
    intRead(isDictionaryEncoding);
    longReadInt(isDictionaryEncoding);
    floatReadInt(isDictionaryEncoding);
    doubleReadInt(isDictionaryEncoding);
  }

  @Test
  public void testLongRead() throws Exception {
    longRead(isDictionaryEncoding);
    floatReadLong(isDictionaryEncoding);
    doubleReadLong(isDictionaryEncoding);
  }

  @Test
  public void testTimestamp() throws Exception {
    timestampRead(isDictionaryEncoding);
    stringReadTimestamp(isDictionaryEncoding);
  }

  @Test
  public void testDoubleRead() throws Exception {
    doubleRead(isDictionaryEncoding);
    stringReadDouble(isDictionaryEncoding);
  }

  @Test
  public void testFloatRead() throws Exception {
    floatRead(isDictionaryEncoding);
    doubleReadFloat(isDictionaryEncoding);
  }

  @Test
  public void testBinaryRead() throws Exception {
    binaryRead(isDictionaryEncoding);
  }

  @Test
  public void testStructRead() throws Exception {
    structRead(isDictionaryEncoding);
  }

  @Test
  public void testNestedStructRead() throws Exception {
    structRead(isDictionaryEncoding);
  }

  @Test
  public void structReadSomeNull() throws Exception {
    structReadSomeNull(isDictionaryEncoding);
  }

  @Test
  public void testFlatColumnsPageV1() throws Exception {
    flatColumnsRead(WriterVersion.PARQUET_1_0, isDictionaryEncoding);
  }

  @Test
  public void testFlatColumnsPageV2() throws Exception {
    flatColumnsRead(WriterVersion.PARQUET_2_0, isDictionaryEncoding);
  }

  @Test
  public void testFlatDecimalPrecisionNarrowing() throws Exception {
    flatDecimalNarrowingRead(WriterVersion.PARQUET_1_0, isDictionaryEncoding);
    flatDecimalNarrowingRead(WriterVersion.PARQUET_2_0, isDictionaryEncoding);
  }

  @Test
  public void testDateRead() throws Exception {
    flatDateRead(WriterVersion.PARQUET_1_0, isDictionaryEncoding);
    flatDateRead(WriterVersion.PARQUET_2_0, isDictionaryEncoding);
  }

  @Test
  public void testStringAndBinaryRead() throws Exception {
    flatStringRead(WriterVersion.PARQUET_1_0, isDictionaryEncoding);
    flatStringRead(WriterVersion.PARQUET_2_0, isDictionaryEncoding);
  }

  @Test
  public void testFlatBooleanRead() throws Exception {
    flatBooleanRead(WriterVersion.PARQUET_1_0, isDictionaryEncoding);
    flatBooleanRead(WriterVersion.PARQUET_2_0, isDictionaryEncoding);
  }

  @Test
  public void testEmptyBatch() throws Exception {
    emptyBatchRead(WriterVersion.PARQUET_1_0, isDictionaryEncoding);
    emptyBatchRead(WriterVersion.PARQUET_2_0, isDictionaryEncoding);
  }

  @Test
  public void testRangeNulls() throws Exception {
    rangeNullsRead(isDictionaryEncoding);
  }

  @Test
  public void testInt96WriterZone() throws Exception {
    int96WriterZoneRead(isDictionaryEncoding);
  }

  @Test
  public void testRejectedTimestamp() throws Exception {
    rejectedTimestampRead(isDictionaryEncoding);
  }

  @Test
  public void testStructFieldNotRepeating() throws Exception {
    structFieldNotRepeating(isDictionaryEncoding);
  }

  @Test
  public void testDictionaryFallbackToPlain() throws Exception {
    dictionaryFallbackToPlainRead(WriterVersion.PARQUET_1_0);
    dictionaryFallbackToPlainRead(WriterVersion.PARQUET_2_0);
  }

  @Test
  public void testStructFieldNullsAtEveryLevel() throws Exception {
    structFieldNullsAtEveryLevel(isDictionaryEncoding);
  }

  @Test
  public void decimalRead() throws Exception {
    decimalRead(isDictionaryEncoding);
    stringReadDecimal(isDictionaryEncoding);
  }

  @Test
  public void testDecimal64Read() throws Exception {
    decimal64Read(isDictionaryEncoding);
  }

  @Test
  public void testDecimal64ReadInt32() throws Exception {
    decimal64ReadInt32();
  }

  @Test
  public void testDecimal64ReadInt64() throws Exception {
    decimal64ReadInt64();
  }
}
