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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedPrimitiveColumnReader;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesWriter;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;

public class TestVectorizedColumnReader extends VectorizedColumnReaderTestBase {
  static boolean isDictionaryEncoding = false;

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
  public void testBooleanRead() throws Exception {
    booleanRead();
    stringReadBoolean();
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
    nestedStructRead0(isDictionaryEncoding);
    nestedStructRead1(isDictionaryEncoding);
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

  @Test
  public void testDecimal64ReadScaleEvolution() throws Exception {
    decimal64ReadScaleEvolution();
  }

  @Test
  public void testDecimal64ReadPrecisionNarrowing() throws Exception {
    decimal64ReadPrecisionNarrowing();
  }

  @Test
  public void testDecimal64ReadFixedLenByteArray() throws Exception {
    decimal64ReadFixedLenByteArray();
  }

  @Test
  public void verifyBatchOffsets() throws Exception {
    super.verifyBatchOffsets();
  }

  /**
   * A v1 page whose definition levels are BIT_PACKED, as writers before parquet-mr 1.0 wrote them: the levels decode
   * one at a time, for a top-level column and for a struct field, which also keeps them.
   */
  @Test
  public void testBitPackedDefinitionLevels() throws Exception {
    MessageType schema = parseMessageType("message m { optional int32 x; optional group s { optional int32 y; } }");
    int rows = 3000;
    for (String[] path : new String[][] { { "x" }, { "s", "y" } }) {
      ColumnDescriptor descriptor = schema.getColumnDescription(path);
      int maxLevel = descriptor.getMaxDefinitionLevel();
      ByteBitPackingValuesWriter levels = new ByteBitPackingValuesWriter(maxLevel, Packer.BIG_ENDIAN);
      ByteBuffer values = ByteBuffer.allocate(rows * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      for (int r = 0; r < rows; r++) {
        levels.writeInteger(level(r, maxLevel));
        if (level(r, maxLevel) == maxLevel) {
          values.putInt(r);
        }
      }
      BytesInput bytes = BytesInput.concat(levels.getBytes(), BytesInput.from(values.array(), 0, values.position()));
      DataPage page = new DataPageV1(bytes, rows, (int) bytes.size(),
          Statistics.createStats(descriptor.getPrimitiveType()), Encoding.BIT_PACKED, Encoding.BIT_PACKED,
          Encoding.PLAIN);
      VectorizedPrimitiveColumnReader reader = new VectorizedPrimitiveColumnReader(descriptor, new PageReader() {
        private DataPage next = page;

        @Override
        public DictionaryPage readDictionaryPage() {
          return null;
        }

        @Override
        public long getTotalValueCount() {
          return rows;
        }

        @Override
        public DataPage readPage() {
          DataPage p = next;
          next = null;
          return p;
        }
      }, false, null, false, true, schema.getType(path), TypeInfoFactory.intTypeInfo);
      for (int row = 0; row < rows; ) {
        LongColumnVector vector = new LongColumnVector(1000);
        reader.readBatch(1000, vector, TypeInfoFactory.intTypeInfo);
        for (int i = 0; i < 1000; i++, row++) {
          String at = String.join(".", path) + " row " + row;
          assertEquals(at, level(row, maxLevel) < maxLevel, vector.isNull[i]);
          if (!vector.isNull[i]) {
            assertEquals(at, row, vector.vector[i]);
          }
          if (path.length > 1) {
            assertEquals(at, level(row, maxLevel), reader.getDefinitionLevels()[i]);
          }
        }
      }
    }
  }

  /** Every fifth row is NULL, at each level below the maximum in turn. */
  private static int level(int row, int maxLevel) {
    return row % 5 == 0 ? (row / 5) % maxLevel : maxLevel;
  }

  private class TestVectorizedParquetRecordReader extends VectorizedParquetRecordReader {
    public TestVectorizedParquetRecordReader(
        org.apache.hadoop.mapred.InputSplit oldInputSplit, JobConf conf) throws IOException {
      super(oldInputSplit, conf);
    }

    @Override
    protected ParquetInputSplit getSplit(JobConf conf) throws IOException {
      return null;
    }
  }

  @Test
  public void testNullSplitForParquetReader() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS,"int32_field");
    conf.set(IOConstants.COLUMNS_TYPES,"int");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    conf.set(PARQUET_READ_SCHEMA, "message test { required int32 int32_field;}");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    Job vectorJob = new Job(conf, "read vector");
    ParquetInputFormat.setInputPaths(vectorJob, file);
    initialVectorizedRowBatchCtx(conf);
    FileSplit fsplit = getFileSplit(vectorJob);
    JobConf jobConf = new JobConf(conf);
    TestVectorizedParquetRecordReader testReader = new TestVectorizedParquetRecordReader(fsplit, jobConf);
    Assert.assertNull("Test should return null split from getSplit() method", testReader.getSplit(null));
  }
}
