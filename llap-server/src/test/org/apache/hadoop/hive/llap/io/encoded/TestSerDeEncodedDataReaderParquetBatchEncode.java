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
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.EncodingWriter;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.VectorSourceOrcWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.hive.ql.io.orc.Writer;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * End-to-end test for the batch-shaped LLAP encode path added on top of the row-loss
 * fix. When {@code hive.llap.io.encode.vector.parquet.enabled} is on and the source
 * is {@link MapredParquetInputFormat}, {@link SerDeEncodedDataReader} keeps the
 * vectorized JobConf, wraps the resulting {@code VectorizedParquetRecordReader} in
 * {@link PassThruBatchReader}, and feeds each {@link VectorizedRowBatch} straight
 * into a {@link VectorSourceOrcWriter} — skipping the per-row
 * {@code ParquetHiveSerDe.deserialize} / {@code orcWriter.addRow(Object)} round-trip.
 *
 * <p>Writes a real Parquet file with {@value #ROW_COUNT} rows (i.e. more than two
 * vectorized batches at {@link VectorizedRowBatch#DEFAULT_SIZE} = 1024) and drives
 * the batch path directly through its two components ({@code PassThruBatchReader}
 * + {@code VectorSourceOrcWriter}) with a mocked ORC writer standing in for the
 * cache-backed one, asserting:
 * <ol>
 *   <li>The reader observes exactly {@code ceil(ROW_COUNT / DEFAULT_SIZE)} batches
 *       (one call to {@code writeBatch(...)} per batch — the row-shape default
 *       would have silently collapsed to that many rows).</li>
 *   <li>The batch sizes captured by the ORC writer sum to {@code ROW_COUNT} — no
 *       rows are dropped.</li>
 * </ol>
 */
public class TestSerDeEncodedDataReaderParquetBatchEncode {

  /** > 2 * VectorizedRowBatch.DEFAULT_SIZE so the file contains at least three batches. */
  private static final int ROW_COUNT = 2500;

  private static Path parquetFile;

  @BeforeClass
  public static void writeTestParquetFile() throws IOException {
    parquetFile = new Path(
        Files.createTempDirectory("llap-serde-encode-parquet-batch").toString(), "data.parquet");

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
   * The batch path preserves every row across a multi-batch Parquet file.
   *
   * <p>{@link PassThruBatchReader} declares {@code isBatchShaped() == true}, so the
   * encode loop routes each {@link VectorizedRowBatch} to
   * {@link VectorSourceOrcWriter#writeBatch(VectorizedRowBatch)} rather than the
   * per-row {@code writeOneRow(Writable)}. The batch sizes captured on the
   * underlying ORC writer must add up to the full row count.
   */
  @Test
  public void batchShapedEncodeCoversEveryRow() throws Exception {
    JobConf callerJobConf = buildVectorizedJobConf();
    long fileLen = parquetFile.getFileSystem(callerJobConf).getFileStatus(parquetFile).getLen();
    FileSplit split = new FileSplit(parquetFile, 0L, fileLen, (String[]) null);

    RecordReader<?, ?> sourceReader =
        new MapredParquetInputFormat().getRecordReader(split, callerJobConf, Reporter.NULL);
    assertNotNull("MapredParquetInputFormat must return a RecordReader on a vectorized JobConf",
        sourceReader);

    PassThruBatchReader reader = new PassThruBatchReader(sourceReader, callerJobConf, 0, 0);
    assertTrue("PassThruBatchReader must report isBatchShaped()", reader.isBatchShaped());

    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(IdRow.class, ObjectInspectorOptions.JAVA);
    // sourceIncludes = [0] means includes.size() == colCount, so the writer runs in
    // "no projection" mode and forwards the source VRB unchanged. That matches this
    // test's JobConf, which reads the only column in the file.
    VectorSourceOrcWriter writer =
        new VectorSourceOrcWriter(oi, Collections.singletonList(0), 4096);
    Writer mockOrcWriter = mock(Writer.class);
    // The Parquet vectorized reader reuses the same VRB across next() calls and mutates
    // its size, so capture batch.size at addRowBatch(...) time rather than post-hoc.
    int[] orcWriterRowCount = new int[]{0};
    int[] orcWriterCalls = new int[]{0};
    doAnswer(inv -> {
      VectorizedRowBatch b = inv.getArgument(0);
      orcWriterRowCount[0] += b.size;
      orcWriterCalls[0]++;
      return null;
    }).when(mockOrcWriter).addRowBatch(org.mockito.ArgumentMatchers.any(VectorizedRowBatch.class));
    injectOrcWriter(writer, mockOrcWriter);

    try {
      int batches = 0;
      int totalRows = 0;
      while (reader.next()) {
        Writable value = reader.getCurrentRow();
        assertTrue("Batch-shaped reader must return VectorizedRowBatch, got "
            + value.getClass().getName(), value instanceof VectorizedRowBatch);
        VectorizedRowBatch batch = (VectorizedRowBatch) value;
        writer.writeBatch(batch);
        batches++;
        totalRows += batch.size;
      }
      int expectedBatchCount =
          (ROW_COUNT + VectorizedRowBatch.DEFAULT_SIZE - 1) / VectorizedRowBatch.DEFAULT_SIZE;
      assertEquals("Expected one writeBatch(...) per source VRB",
          expectedBatchCount, batches);
      assertEquals("Sum of VRB sizes across writeBatch(...) calls must equal the row count "
          + "-- no rows dropped by the batch encode path", ROW_COUNT, totalRows);
    } finally {
      reader.close();
    }

    assertEquals("Every VRB passed to writeBatch(...) must reach orcWriter.addRowBatch(...)",
        ROW_COUNT, orcWriterRowCount[0]);
    assertEquals("One addRowBatch(...) call per writeBatch(...) call",
        (ROW_COUNT + VectorizedRowBatch.DEFAULT_SIZE - 1) / VectorizedRowBatch.DEFAULT_SIZE,
        orcWriterCalls[0]);
  }

  /** Reflection hook so the test doesn't need a fully-initialized cache writer. */
  private static void injectOrcWriter(EncodingWriter target, Writer w) throws Exception {
    java.lang.reflect.Field f = EncodingWriter.class.getDeclaredField("orcWriter");
    f.setAccessible(true);
    f.set(target, w);
  }

  /** POJO backing the reflection-based StructObjectInspector used above. */
  public static class IdRow { public int id; }

  /**
   * Builds a JobConf that puts {@link MapredParquetInputFormat} on its vectorized
   * branch — matching the shape of the JobConf the LLAP encode path receives from
   * query execution when the opt-in batch path is on and no
   * {@link SerDeEncodedDataReader#buildSourceReaderJobConf(JobConf)} clone is
   * applied.
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
