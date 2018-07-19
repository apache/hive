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

package org.apache.hadoop.hive.ql.io.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

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

  private class TestVectorizedParquetRecordReader extends VectorizedParquetRecordReader {
    public TestVectorizedParquetRecordReader(
        org.apache.hadoop.mapred.InputSplit oldInputSplit, JobConf conf) {
      super(oldInputSplit, conf);
    }
    @Override
    protected ParquetInputSplit getSplit(
        org.apache.hadoop.mapred.InputSplit oldInputSplit, JobConf conf) {
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
    Assert.assertNull("Test should return null split from getSplit() method", testReader.getSplit(fsplit, jobConf));
  }
}
