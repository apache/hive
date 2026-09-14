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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.EncodingWriter;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.ReaderWithOffsets;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.VectorSourceOrcWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit test for {@link SerDeEncodedDataReader#buildSourceReaderJobConf(JobConf)}.
 *
 * <p>The LLAP encode path constructs its source {@link org.apache.hadoop.mapred.RecordReader}
 * via {@code sourceInputFormat.getRecordReader(split, buildSourceReaderJobConf(jobConf),
 * reporter)}. The clone must have vectorization disabled so that vectorized input formats
 * (e.g. {@link org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat}) pick their
 * row-per-{@code next()} branch, which is what {@link
 * SerDeEncodedDataReader.DeserializerOrcWriter#writeOneRow} expects.
 *
 * <p>These tests pin that contract without spinning up any LLAP infrastructure.
 */
public class TestSerDeEncodedDataReader {

  @Test
  public void clonedJobConfHasVectorizationDisabled() {
    JobConf original = new JobConf();
    HiveConf.setBoolVar(original, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    original.setBoolean(Utilities.VECTOR_MODE, true);

    JobConf clone = SerDeEncodedDataReader.buildSourceReaderJobConf(original);

    assertNotSame("Helper must return a clone, not the original JobConf", original, clone);
    assertFalse("HIVE_VECTORIZATION_ENABLED must be false on the clone",
        HiveConf.getBoolVar(clone, ConfVars.HIVE_VECTORIZATION_ENABLED));
    assertFalse("Utilities.VECTOR_MODE must be false on the clone so that "
            + "Utilities.getIsVectorized(clone) short-circuits to false",
        clone.getBoolean(Utilities.VECTOR_MODE, true));
    assertFalse("Utilities.getIsVectorized must report false for the clone",
        Utilities.getIsVectorized(clone));
  }

  @Test
  public void originalJobConfIsNotMutated() {
    JobConf original = new JobConf();
    HiveConf.setBoolVar(original, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    original.setBoolean(Utilities.VECTOR_MODE, true);

    SerDeEncodedDataReader.buildSourceReaderJobConf(original);

    assertTrue("Helper must not mutate the caller's JobConf: HIVE_VECTORIZATION_ENABLED",
        HiveConf.getBoolVar(original, ConfVars.HIVE_VECTORIZATION_ENABLED));
    assertTrue("Helper must not mutate the caller's JobConf: Utilities.VECTOR_MODE",
        original.getBoolean(Utilities.VECTOR_MODE, false));
  }

  @Test
  public void unrelatedConfigsArePropagated() {
    JobConf original = new JobConf();
    // A random unrelated key + a Hive config, to make sure the clone carries over context
    // that the downstream InputFormat may still need (e.g. hive.io.file.readcolumn.ids).
    original.set("test.unrelated.key", "kept");
    HiveConf.setVar(original, ConfVars.LLAP_IO_ENCODE_FORMATS,
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");

    JobConf clone = SerDeEncodedDataReader.buildSourceReaderJobConf(original);

    assertEquals("Unrelated JobConf entries must survive the clone",
        "kept", clone.get("test.unrelated.key"));
    assertEquals("Unrelated Hive configs must survive the clone",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        HiveConf.getVar(clone, ConfVars.LLAP_IO_ENCODE_FORMATS));
  }

  /**
   * Guards the {@link Utilities#VECTOR_MODE} short-circuit inside
   * {@link Utilities#getIsVectorized(org.apache.hadoop.conf.Configuration)}: even when
   * {@code HIVE_VECTORIZATION_ENABLED} is left {@code true} on the original conf, setting
   * {@code VECTOR_MODE=false} on the clone must be enough to make
   * {@link org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat#getRecordReader}
   * pick the row-mode branch.
   */
  @Test
  public void vectorModeShortCircuitWinsOverHiveVectorizationEnabled() {
    JobConf original = new JobConf();
    HiveConf.setBoolVar(original, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    // Deliberately do NOT set VECTOR_MODE on the original.

    JobConf clone = SerDeEncodedDataReader.buildSourceReaderJobConf(original);

    assertFalse("Even with only VECTOR_MODE flipped, getIsVectorized(clone) must be false",
        Utilities.getIsVectorized(clone));
  }

  /**
   * Pins the SPI defaults introduced with the batch-shaped encode path.
   * {@link ReaderWithOffsets#isBatchShaped()} defaults to {@code false} so every existing
   * row-shaped reader keeps its old behaviour, and {@link EncodingWriter#writeBatch}
   * defaults to throwing so an accidental call on a row-shaped writer surfaces immediately
   * instead of silently dropping rows.
   */
  @Test
  public void encodingWriterAndReaderDefaultsAreRowShaped() throws Exception {
    ReaderWithOffsets rowShapedReader = new ReaderWithOffsets() {
      @Override public boolean next() { return false; }
      @Override public Writable getCurrentRow() { return null; }
      @Override public void close() {}
      @Override public boolean hasOffsets() { return false; }
      @Override public long getCurrentRowStartOffset() { return -1; }
      @Override public long getCurrentRowEndOffset() { return -1; }
    };
    assertFalse("Default isBatchShaped() must be false so existing row-shaped readers "
        + "don't accidentally opt into writeBatch(VectorizedRowBatch) routing",
        rowShapedReader.isBatchShaped());

    class RowOnlyEncodingWriter extends EncodingWriter {
      RowOnlyEncodingWriter() { super(null, 0); }
      @Override public boolean isOnlyWritingIncludedColumns() { return false; }
      @Override public void writeOneRow(Writable row) {}
      @Override public void setCurrentStripeOffsets(long a, long b, long c, long d) {}
      @Override public void flushIntermediateData() {}
      @Override public void writeIntermediateFooter() {}
      @Override public List<VectorizedRowBatch> extractCurrentVrbs() { return null; }
    }
    EncodingWriter rowOnly = new RowOnlyEncodingWriter();
    try {
      rowOnly.writeBatch(new VectorizedRowBatch(1));
      fail("Default writeBatch(VectorizedRowBatch) must throw so a batch-shaped reader "
          + "wired to a row-shaped writer fails loudly rather than silently dropping rows");
    } catch (UnsupportedOperationException expected) {
      // ok
    }
  }

  /**
   * Pins the batch-shaped writer's contract: {@link VectorSourceOrcWriter#writeBatch}
   * forwards to the ORC writer, and calling {@link VectorSourceOrcWriter#writeOneRow}
   * on it throws — the row-shaped code path must never be reachable on this writer.
   */
  @Test
  public void vectorSourceOrcWriterIsBatchShapedOnly() throws Exception {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(BatchRow.class, ObjectInspectorOptions.JAVA);
    // Single-column OI + sourceIncludes = [0] means includes.size() == colCount, so the
    // writer is in "no projection" mode -- writeBatch forwards the source VRB unchanged.
    VectorSourceOrcWriter writer =
        new VectorSourceOrcWriter(oi, Collections.singletonList(0), 4096);

    try {
      writer.writeOneRow(null);
      fail("writeOneRow(Writable) must throw on VectorSourceOrcWriter (batch-shaped only)");
    } catch (UnsupportedOperationException expected) {
      // ok
    }

    // Route a fake batch through a mocked ORC writer to prove writeBatch calls addRowBatch.
    Writer orcWriter = mock(Writer.class);
    injectOrcWriter(writer, orcWriter);
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    batch.cols[0] = new LongColumnVector(1024);
    batch.size = 7;
    writer.writeBatch(batch);

    ArgumentCaptor<VectorizedRowBatch> captor = ArgumentCaptor.forClass(VectorizedRowBatch.class);
    verify(orcWriter).addRowBatch(captor.capture());
    assertEquals("writeBatch must forward the exact VRB size to orcWriter.addRowBatch(...)",
        7, captor.getValue().size);
  }

  /**
   * Regression: {@link VectorSourceOrcWriter} must translate the sparse projected VRB
   * (unprojected {@code cols[i]} left {@code null} by
   * {@code VectorizedRowBatchCtx.createVectorizedRowBatch}) into a dense batch whose
   * columns line up with its ORC schema, otherwise the underlying ORC {@code TreeWriter}
   * NPEs at the first missing column vector. Handed a 3-column source OI with
   * sourceIncludes = [0, 2] and a batch that only allocates {@code cols[0]} and
   * {@code cols[2]}, the writer must forward a 2-column dense VRB whose cols alias the
   * source's projected vectors -- and it must report {@code isOnlyWritingIncludedColumns()}
   * so the surrounding CacheWriter indexes against the projected column ids.
   */
  @Test
  public void vectorSourceOrcWriterRepacksSparseVrb() throws Exception {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(ThreeColRow.class, ObjectInspectorOptions.JAVA);
    List<Integer> includes = Arrays.asList(0, 2);
    VectorSourceOrcWriter writer = new VectorSourceOrcWriter(oi, includes, 4096);

    assertTrue("With a strict projection the writer must narrow the cache to the "
        + "projected columns so CacheWriter uses splitColumnIds, not the full columnIds",
        writer.isOnlyWritingIncludedColumns());

    Writer orcWriter = mock(Writer.class);
    injectOrcWriter(writer, orcWriter);

    // Sparse source VRB: 3 slots, only cols[0] and cols[2] have ColumnVectors -- exactly
    // what VectorizedRowBatchCtx builds when dataColumnNums == {0, 2}.
    VectorizedRowBatch sparseBatch = new VectorizedRowBatch(3);
    LongColumnVector c0 = new LongColumnVector(1024);
    LongColumnVector c2 = new LongColumnVector(1024);
    sparseBatch.cols[0] = c0;
    sparseBatch.cols[1] = null;
    sparseBatch.cols[2] = c2;
    sparseBatch.size = 11;

    writer.writeBatch(sparseBatch);

    ArgumentCaptor<VectorizedRowBatch> captor = ArgumentCaptor.forClass(VectorizedRowBatch.class);
    verify(orcWriter).addRowBatch(captor.capture());
    VectorizedRowBatch dense = captor.getValue();
    assertEquals("Dense VRB must carry exactly one slot per projected column", 2, dense.cols.length);
    assertEquals("Dense VRB row count must match the source batch", 11, dense.size);
    assertSame("cols[0] of the dense VRB must alias the source's projected cols[0]",
        c0, dense.cols[0]);
    assertSame("cols[1] of the dense VRB must alias the source's projected cols[2] "
        + "(the second entry in sourceIncludes)", c2, dense.cols[1]);
  }

  /** Reflection hook so we can install a tracking ORC writer without a full init(). */
  private static void injectOrcWriter(EncodingWriter target, Writer w) throws Exception {
    java.lang.reflect.Field f = EncodingWriter.class.getDeclaredField("orcWriter");
    f.setAccessible(true);
    f.set(target, w);
  }

  /** POJO backing the reflection-based StructObjectInspector used above. */
  public static class BatchRow { public long id; }

  /** 3-column POJO used to build a sparse-VRB test case. */
  public static class ThreeColRow {
    public long a;
    public long b;
    public long c;
  }
}
