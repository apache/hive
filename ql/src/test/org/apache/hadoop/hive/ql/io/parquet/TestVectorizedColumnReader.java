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
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;

public class TestVectorizedColumnReader extends VectorizedColumnReaderTestBase {
  static boolean isDictionaryEncoding = false;

  @BeforeClass
  public static void setup() throws IOException {
    removeFile();
    writeData(initWriterFromFile(), isDictionaryEncoding);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    removeFile();
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

  /**
   * A caller that has already ruled row groups out names the ones left, and the reader reads those and no
   * others. Nothing else in the suite would notice if the picks were ignored: the rows returned and their
   * positions are the same either way, so only the blocks the reader settled on can show it.
   */
  @Test
  public void testReaderReadsOnlyThePickedRowGroups() throws Exception {
    Configuration conf = newSingleColumnConf();
    Job vectorJob = new Job(conf, "read vector");
    ParquetInputFormat.setInputPaths(vectorJob, file);
    initialVectorizedRowBatchCtx(conf);
    FileSplit fsplit = getFileSplit(vectorJob);
    JobConf jobConf = new JobConf(conf);
    ParquetMetadata footer = ParquetFileReader.readFooter(jobConf, file, ParquetMetadataConverter.NO_FILTER);
    int rowGroups = footer.getBlocks().size();

    boolean[] everyRowGroup = new boolean[rowGroups];
    Arrays.fill(everyRowGroup, true);
    Assert.assertEquals("Picking every row group should read the whole split",
        rowGroups, readerOver(footer, everyRowGroup, fsplit, jobConf).getFilteredBlocks().size());

    Assert.assertNull("Picking no row group should leave the reader nothing to read",
        readerOver(footer, new boolean[rowGroups], fsplit, jobConf).getFilteredBlocks());

    VectorizedParquetInputFormat inputFormat = new VectorizedParquetInputFormat();
    Assert.assertThrows("Picks belonging to another footer should be refused, not applied by index",
        IOException.class, () -> inputFormat.setMetadata(footer, new boolean[rowGroups + 1]));
  }

  private static VectorizedParquetRecordReader readerOver(ParquetMetadata footer, boolean[] includedRowGroups,
      FileSplit fsplit, JobConf jobConf) throws IOException {
    VectorizedParquetInputFormat inputFormat = new VectorizedParquetInputFormat();
    inputFormat.setMetadata(footer, includedRowGroups);
    return (VectorizedParquetRecordReader) inputFormat.getRecordReader(fsplit, jobConf, Reporter.NULL);
  }

  private static Configuration newSingleColumnConf() {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "int32_field");
    conf.set(IOConstants.COLUMNS_TYPES, "int");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    conf.set(PARQUET_READ_SCHEMA, "message test { required int32 int32_field;}");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    return conf;
  }
}
