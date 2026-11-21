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
package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * End-to-end test for the LLAP encode path's source-reader contract when the source
 * InputFormat is {@link MapredParquetInputFormat}: with the JobConf shaped by
 * {@link SerDeEncodedDataReader#buildSourceReaderJobConf(JobConf)}, the reader emits
 * one row per {@code next(...)} and {@link PassThruOffsetReader} sees every row of a
 * multi-batch Parquet file.
 *
 * <p>Writes a real Parquet file with {@value #ROW_COUNT} rows (i.e. more than two
 * vectorized batches at {@link VectorizedRowBatch#DEFAULT_SIZE} = 1024) and drives it
 * through the exact call the encode path performs:
 * <pre>
 *   sourceInputFormat.getRecordReader(split, buildSourceReaderJobConf(jobConf), reporter);
 *   ... wrap in PassThruOffsetReader(reader, ...);
 *   ... count next() -&gt; true iterations
 * </pre>
 *
 * <p>The encode loop feeds each value produced by {@code sourceReader.next(key, value)}
 * to {@code DeserializerOrcWriter.writeOneRow(Writable)}, so the row-per-{@code next()}
 * contract is what keeps every row reaching the ORC-shaped LLAP cache. A second test
 * pins down why {@link SerDeEncodedDataReader#buildSourceReaderJobConf(JobConf)} exists:
 * if you hand the caller's vectorized JobConf straight to
 * {@link MapredParquetInputFormat#getRecordReader}, its vectorized branch fills a whole
 * {@link VectorizedRowBatch} per {@code next()} and only one row per batch would make
 * it through the encode loop.
 */
public class TestSerDeEncodedDataReaderParquetE2E {

  /** > 2 * VectorizedRowBatch.DEFAULT_SIZE so the file contains at least three batches. */
  private static final int ROW_COUNT = 2500;

  private static Path parquetFile;

  @BeforeClass
  public static void writeTestParquetFile() throws IOException {
    parquetFile = new Path(
        Files.createTempDirectory("llap-serde-encode-parquet-e2e").toString(), "data.parquet");

    Configuration writerConf = new Configuration();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message hive_schema { required int32 id; }");
    GroupWriteSupport.setSchema(schema, writerConf);
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(parquetFile)
        .withType(schema)
        .withConf(writerConf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriterVersion(ParquetWriter.DEFAULT_WRITER_VERSION)
        .build()) {
      for (int i = 0; i < ROW_COUNT; i++) {
        writer.write(groupFactory.newGroup().append("id", i));
      }
    }
  }

  @AfterClass
  public static void deleteTestParquetFile() throws IOException {
    if (parquetFile != null) {
      FileSystem fs = parquetFile.getFileSystem(new Configuration());
      if (fs.exists(parquetFile)) {
        fs.delete(parquetFile.getParent(), true);
      }
    }
  }

  /**
   * The primary invariant: with the source-reader JobConf produced by
   * {@link SerDeEncodedDataReader#buildSourceReaderJobConf(JobConf)},
   * {@link MapredParquetInputFormat} returns a row-shaped reader and
   * {@link PassThruOffsetReader} emits every row of the file -- even when the caller's
   * JobConf was configured for vectorization (as it is in LLAP query execution).
   */
  @Test
  public void sourceReaderEmitsEveryRowOfMultiBatchParquetFile() throws Exception {
    JobConf callerJobConf = buildVectorizedJobConf();
    JobConf sourceReaderConf = SerDeEncodedDataReader.buildSourceReaderJobConf(callerJobConf);

    int iterations = countPassThruIterations(sourceReaderConf);

    assertEquals("PassThruOffsetReader must observe one row per next() call and cover "
        + "every row of the underlying Parquet split.",
        ROW_COUNT, iterations);
  }

  /**
   * Documents why {@link SerDeEncodedDataReader#buildSourceReaderJobConf(JobConf)} is
   * necessary: {@link MapredParquetInputFormat}'s vectorized branch fills a whole
   * {@link VectorizedRowBatch} per {@code next(...)}, which does not match the encode
   * loop's row-per-{@code next()} contract. Handing a vectorized JobConf straight to
   * {@code getRecordReader} would make {@link PassThruOffsetReader} observe only
   * {@code ceil(ROW_COUNT / DEFAULT_SIZE)} iterations for a file with {@link #ROW_COUNT}
   * rows -- which is why the encode path must clone the JobConf and disable
   * vectorization before asking for a source reader.
   */
  @Test
  public void mapredParquetVectorizedBranchIsBatchShapedNotRowShaped() throws Exception {
    JobConf vectorizedJobConf = buildVectorizedJobConf();

    int iterations = countPassThruIterations(vectorizedJobConf);

    int expectedBatchCount =
        (ROW_COUNT + VectorizedRowBatch.DEFAULT_SIZE - 1) / VectorizedRowBatch.DEFAULT_SIZE;
    assertEquals("MapredParquetInputFormat's vectorized reader fills a whole batch per "
        + "next(), so PassThruOffsetReader iterations collapse to ceil(rows / DEFAULT_SIZE) "
        + "-- this is precisely the shape mismatch buildSourceReaderJobConf works around.",
        expectedBatchCount, iterations);
  }

  /**
   * Drives {@link MapredParquetInputFormat#getRecordReader} through the same
   * {@link PassThruOffsetReader} wrapper the LLAP encode path uses, and counts how
   * many rows the wrapper reports before EOF.
   */
  private static int countPassThruIterations(JobConf jobConf) throws IOException {
    long fileLen = parquetFile.getFileSystem(jobConf).getFileStatus(parquetFile).getLen();
    FileSplit split = new FileSplit(parquetFile, 0L, fileLen, (String[]) null);

    MapredParquetInputFormat inputFormat = new MapredParquetInputFormat();
    @SuppressWarnings("rawtypes")
    RecordReader sourceReader = inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
    assertNotNull("MapredParquetInputFormat must return a RecordReader", sourceReader);

    PassThruOffsetReader wrapper = new PassThruOffsetReader(sourceReader, jobConf, 0, 0);
    try {
      int count = 0;
      while (wrapper.next()) {
        count++;
      }
      return count;
    } finally {
      wrapper.close();
    }
  }

  /**
   * Builds a JobConf that puts {@link MapredParquetInputFormat} on its vectorized
   * branch, matching the shape of the JobConf the LLAP encode path receives from
   * query execution.
   */
  private static JobConf buildVectorizedJobConf() {
    JobConf job = new JobConf();
    HiveConf.setBoolVar(job, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(job, ConfVars.PLAN, "//tmp");
    job.setBoolean(Utilities.VECTOR_MODE, true);

    job.set(IOConstants.COLUMNS, "id");
    job.set(IOConstants.COLUMNS_TYPES, "int");
    job.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    job.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    job.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "id");

    MapWork mapWork = new MapWork();
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(new VectorizedRowBatchCtx(
        new String[] {"id"},
        new TypeInfo[] {TypeInfoFactory.intTypeInfo},
        null, null, 0, 0, new VirtualColumn[0], new String[0], null));
    Utilities.setMapWork(job, mapWork);
    return job;
  }
}
