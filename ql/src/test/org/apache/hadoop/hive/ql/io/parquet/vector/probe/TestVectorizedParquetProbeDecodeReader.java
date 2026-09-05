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

package org.apache.hadoop.hive.ql.io.parquet.vector.probe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.parquet.VectorizedColumnReaderTestBase;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedColumnReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end test that the ProbeDecode filter path in {@code VectorizedPrimitiveColumnReader}
 * (a) leaves filtered rows as null-marked slots in the column vector and (b) still decodes
 * surviving rows correctly.
 *
 * <p>The test bypasses {@code VectorizedParquetRecordReader.nextBatch}'s ProbeDecodeState
 * resolution (which requires a live MapJoin operator + hash table) and instead reflects into
 * the reader to reach the primitive column readers directly. It then calls the filter-aware
 * {@code readBatch(total, column, type, ParquetProbeFilter)} signature that
 * {@code VectorizedPrimitiveColumnReader} exposes to the outer reader -- the same call
 * {@code nextBatch} would make on the probe path.
 *
 * <p>Both dictionary-encoded and PLAIN-encoded pages are exercised so both the coalesced-skip
 * path ({@code readDictionaryIDs} + {@code pendingSkip} + {@code skipInts}) and the per-row
 * skip path ({@code readIntegers}/{@code readLongs}/{@code readDoubles}/{@code readBinaries})
 * are hit.
 */
public class TestVectorizedParquetProbeDecodeReader extends VectorizedColumnReaderTestBase {

  private static final int N_ROWS = 256;
  private static final MessageType WRITE_SCHEMA = MessageTypeParser.parseMessageType(
      "message test { "
          + "required int32  int_col; "
          + "required int64  long_col; "
          + "required double dbl_col; "
          + "required binary str_col (UTF8); "
          + "}");

  private java.io.File tempDir;
  private Path tempFile;

  @Before
  public void setUpFile() throws Exception {
    tempDir = java.nio.file.Files.createTempDirectory("probe-decode-").toFile();
    tempFile = new Path(new java.io.File(tempDir, "data.parquet").toURI());
  }

  @After
  public void tearDownFile() {
    if (tempDir != null && tempDir.exists()) {
      for (java.io.File f : tempDir.listFiles()) {
        f.delete();
      }
      tempDir.delete();
    }
  }

  /** Filter accepts rows whose index is even (0, 2, 4, ...). */
  private static ParquetProbeFilter halfFilter(int size) {
    boolean[] bits = new boolean[size];
    for (int i = 0; i < size; i++) {
      bits[i] = (i % 2 == 0);
    }
    return ParquetProbeFilter.newBitmap(bits);
  }

  /**
   * When {@code dictionary=true}, values cycle through a small distinct set so Parquet
   * dictionary-encodes each page; otherwise every value is unique so the writer falls back to
   * PLAIN encoding.
   */
  private void writeFile(boolean dictionary) throws IOException {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(WRITE_SCHEMA, conf);
    SimpleGroupFactory gf = new SimpleGroupFactory(WRITE_SCHEMA);

    try (ParquetWriter<Group> writer = new ParquetWriter<>(tempFile, new GroupWriteSupport(),
        CompressionCodecName.UNCOMPRESSED, 1024 * 1024, 1024 * 1024, 512, dictionary, false,
        ParquetWriter.DEFAULT_WRITER_VERSION, conf)) {
      for (int i = 0; i < N_ROWS; i++) {
        int intVal = dictionary ? (i % 4) : i;
        long longVal = dictionary ? (i % 4) : (long) i;
        double dblVal = dictionary ? (i % 4) : (double) i;
        String strVal = dictionary ? ("v" + (i % 4)) : ("v" + i);
        Group g = gf.newGroup()
            .append("int_col", intVal)
            .append("long_col", longVal)
            .append("dbl_col", dblVal)
            .append("str_col", Binary.fromString(strVal));
        writer.write(g);
      }
    }
  }

  private VectorizedParquetRecordReader openReader() throws Exception {
    Configuration readerConf = new Configuration();
    readerConf.set(org.apache.hadoop.hive.ql.io.IOConstants.COLUMNS,
        "int_col,long_col,dbl_col,str_col");
    readerConf.set(org.apache.hadoop.hive.ql.io.IOConstants.COLUMNS_TYPES,
        "int,bigint,double,string");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,1,2,3");
    HiveConf.setBoolVar(readerConf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(readerConf, HiveConf.ConfVars.PLAN, "//tmp");
    org.apache.hadoop.mapreduce.Job vectorJob = new org.apache.hadoop.mapreduce.Job(readerConf, "read");
    org.apache.parquet.hadoop.ParquetInputFormat.setInputPaths(vectorJob, tempFile);
    initialVectorizedRowBatchCtx(readerConf, null);
    return new VectorizedParquetRecordReader(getFileSplit(vectorJob, tempFile), new JobConf(readerConf));
  }

  /**
   * Force {@code checkEndOfRowGroup} to run on the reader without consuming any rows through
   * {@code nextBatch}. This leaves {@code columnReaders} populated and page state at row 0.
   */
  @SuppressWarnings("unchecked")
  private static VectorizedColumnReader[] primeColumnReaders(VectorizedParquetRecordReader r)
      throws ReflectiveOperationException {
    Method m = VectorizedParquetRecordReader.class.getDeclaredMethod("checkEndOfRowGroup");
    m.setAccessible(true);
    m.invoke(r);
    Field f = VectorizedParquetRecordReader.class.getDeclaredField("columnReaders");
    f.setAccessible(true);
    return (VectorizedColumnReader[]) f.get(r);
  }

  /**
   * Expected value at row {@code i} for the given column and encoding, matching what
   * {@link #writeFile} wrote.
   */
  private static long expectedInt(int i, boolean dict)  { return dict ? (i % 4) : i; }
  private static long expectedLong(int i, boolean dict) { return dict ? (i % 4) : (long) i; }
  private static double expectedDbl(int i, boolean dict) { return dict ? (i % 4) : (double) i; }
  private static String expectedStr(int i, boolean dict) { return dict ? ("v" + (i % 4)) : ("v" + i); }

  private void runFilterHonoringTest(boolean dictionary) throws Exception {
    writeFile(dictionary);

    VectorizedParquetRecordReader reader = openReader();
    try {
      VectorizedColumnReader[] readers = primeColumnReaders(reader);
      assertNotNull("columnReaders must be populated after checkEndOfRowGroup", readers);
      assertEquals(4, readers.length);

      int batchSize = Math.min(VectorizedRowBatch.DEFAULT_SIZE, N_ROWS);
      ParquetProbeFilter filter = halfFilter(batchSize);

      LongColumnVector intVec = new LongColumnVector(batchSize);
      LongColumnVector longVec = new LongColumnVector(batchSize);
      DoubleColumnVector dblVec = new DoubleColumnVector(batchSize);
      BytesColumnVector strVec = new BytesColumnVector(batchSize);
      for (ColumnVector v : new ColumnVector[] { intVec, longVec, dblVec, strVec }) {
        v.init();
      }
      // Vectors start with noNulls=true; every column reader clears that when it hits a
      // filtered slot (see setNullValue in VectorizedPrimitiveColumnReader).

      // Types must line up with the columns projection.
      TypeInfo intType = TypeInfoFactory.getPrimitiveTypeInfo("int");
      TypeInfo longType = TypeInfoFactory.getPrimitiveTypeInfo("bigint");
      TypeInfo dblType = TypeInfoFactory.getPrimitiveTypeInfo("double");
      TypeInfo strType = TypeInfoFactory.getPrimitiveTypeInfo("string");

      // Drive the filter-aware read path on each column reader. This is the same call that
      // VectorizedParquetRecordReader.nextBatch issues on non-key columns when the ProbeDecode
      // path is active.
      readers[0].readBatch(batchSize, intVec, intType, filter);
      readers[1].readBatch(batchSize, longVec, longType, filter);
      readers[2].readBatch(batchSize, dblVec, dblType, filter);
      readers[3].readBatch(batchSize, strVec, strType, filter);

      // Each vector must now carry the surviving rows' values in the accepted slots and a
      // null-marked slot everywhere else. noNulls must be false since we produced nulls.
      for (int i = 0; i < batchSize; i++) {
        boolean selected = (i % 2 == 0);
        if (selected) {
          assertFalse("row " + i + " must not be null in int_col", intVec.isNull[i]);
          assertFalse("row " + i + " must not be null in long_col", longVec.isNull[i]);
          assertFalse("row " + i + " must not be null in dbl_col", dblVec.isNull[i]);
          assertFalse("row " + i + " must not be null in str_col", strVec.isNull[i]);

          assertEquals("int_col value at row " + i, expectedInt(i, dictionary), intVec.vector[i]);
          assertEquals("long_col value at row " + i, expectedLong(i, dictionary), longVec.vector[i]);
          assertEquals("dbl_col value at row " + i, expectedDbl(i, dictionary), dblVec.vector[i], 0.0);
          String actual = new String(strVec.vector[i], strVec.start[i], strVec.length[i],
              StandardCharsets.UTF_8);
          assertEquals("str_col value at row " + i, expectedStr(i, dictionary), actual);
        } else {
          assertTrue("row " + i + " must be null in int_col", intVec.isNull[i]);
          assertTrue("row " + i + " must be null in long_col", longVec.isNull[i]);
          assertTrue("row " + i + " must be null in dbl_col", dblVec.isNull[i]);
          assertTrue("row " + i + " must be null in str_col", strVec.isNull[i]);
        }
      }
      assertFalse("noNulls must be cleared once a filtered row is emitted (int_col)",
          intVec.noNulls);
      assertFalse("noNulls must be cleared once a filtered row is emitted (long_col)",
          longVec.noNulls);
      assertFalse("noNulls must be cleared once a filtered row is emitted (dbl_col)",
          dblVec.noNulls);
      assertFalse("noNulls must be cleared once a filtered row is emitted (str_col)",
          strVec.noNulls);
    } finally {
      reader.close();
    }
  }

  /**
   * Filter should be a no-op when it accepts every row: the resulting vectors are identical to
   * a baseline unfiltered read. This is the "no regression" case for a filter that matches all.
   */
  private void runAllPassNoOpTest(boolean dictionary) throws Exception {
    writeFile(dictionary);

    VectorizedParquetRecordReader baselineReader = openReader();
    VectorizedParquetRecordReader filteredReader = openReader();
    try {
      VectorizedColumnReader[] baseline = primeColumnReaders(baselineReader);
      VectorizedColumnReader[] filtered = primeColumnReaders(filteredReader);
      int batchSize = Math.min(VectorizedRowBatch.DEFAULT_SIZE, N_ROWS);

      boolean[] allTrue = new boolean[batchSize];
      java.util.Arrays.fill(allTrue, true);
      ParquetProbeFilter allPass = ParquetProbeFilter.newBitmap(allTrue);

      TypeInfo intType = TypeInfoFactory.getPrimitiveTypeInfo("int");
      TypeInfo longType = TypeInfoFactory.getPrimitiveTypeInfo("bigint");
      TypeInfo dblType = TypeInfoFactory.getPrimitiveTypeInfo("double");
      TypeInfo strType = TypeInfoFactory.getPrimitiveTypeInfo("string");

      LongColumnVector baseInt = new LongColumnVector(batchSize);
      baseInt.init();
      LongColumnVector filtInt = new LongColumnVector(batchSize);
      filtInt.init();
      baseline[0].readBatch(batchSize, baseInt, intType);
      filtered[0].readBatch(batchSize, filtInt, intType, allPass);
      for (int i = 0; i < batchSize; i++) {
        assertEquals("int_col allPass row " + i, baseInt.vector[i], filtInt.vector[i]);
        assertEquals("int_col allPass isNull row " + i, baseInt.isNull[i], filtInt.isNull[i]);
      }

      BytesColumnVector baseStr = new BytesColumnVector(batchSize);
      baseStr.init();
      BytesColumnVector filtStr = new BytesColumnVector(batchSize);
      filtStr.init();
      baseline[3].readBatch(batchSize, baseStr, strType);
      filtered[3].readBatch(batchSize, filtStr, strType, allPass);
      for (int i = 0; i < batchSize; i++) {
        assertEquals("str_col allPass isNull row " + i, baseStr.isNull[i], filtStr.isNull[i]);
        String b = new String(baseStr.vector[i], baseStr.start[i], baseStr.length[i], StandardCharsets.UTF_8);
        String f = new String(filtStr.vector[i], filtStr.start[i], filtStr.length[i], StandardCharsets.UTF_8);
        assertEquals("str_col allPass row " + i, b, f);
      }
    } finally {
      baselineReader.close();
      filteredReader.close();
    }
  }

  @Test
  public void filterHonoredOnDictionaryEncodedPages() throws Exception {
    // Exercises readDictionaryIDs -> pendingSkip -> DictionaryValuesReader.skip(int)
    // -> RunLengthBitPackingHybridDecoder.skipInts (the O(runs) fast-path).
    runFilterHonoringTest(true);
  }

  @Test
  public void filterHonoredOnPlainEncodedPages() throws Exception {
    // Exercises the per-row skip path in readIntegers / readLongs / readDoubles / readBinaries.
    runFilterHonoringTest(false);
  }

  @Test
  public void allPassFilterIsNoOpDict() throws Exception {
    runAllPassNoOpTest(true);
  }

  @Test
  public void allPassFilterIsNoOpPlain() throws Exception {
    runAllPassNoOpTest(false);
  }
}
