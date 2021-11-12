/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.CacheWriter;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.DeserializerOrcWriter;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.EncodingWriter;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/** The class that writes rows from a text reader to an ORC writer using VectorDeserializeRow. */
class VectorDeserializeOrcWriter extends EncodingWriter implements Runnable {
  private final VectorizedRowBatchCtx vrbCtx;
  private Writer orcWriter;
  private final LazySimpleDeserializeRead deserializeRead;
  private final VectorDeserializeRow<?> vectorDeserializeRow;
  private final StructObjectInspector destinationOi;
  private final boolean usesSourceIncludes;
  private final List<Integer> sourceIncludes;

  private final boolean isAsync;
  private final ConcurrentLinkedQueue<WriteOperation> queue;
  private AsyncCallback completion;
  private Future<?> future;

  // Stored here only as async operation context.
  private final boolean[] cacheIncludes;

  private VectorizedRowBatch sourceBatch, destinationBatch;
  private List<VectorizedRowBatch> currentBatches;
  private final ExecutorService encodeExecutor;

  // TODO: if more writers are added, separate out an EncodingWriterFactory
  public static EncodingWriter create(InputFormat<?, ?> sourceIf, Deserializer serDe, Map<Path, PartitionDesc> parts,
      Configuration daemonConf, Configuration jobConf, Path splitPath, StructObjectInspector sourceOi,
      List<Integer> sourceIncludes, boolean[] cacheIncludes, int allocSize, ExecutorService encodeExecutor)
      throws IOException {
    // Vector SerDe can be disabled both on client and server side.
    if (!HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_IO_ENCODE_VECTOR_SERDE_ENABLED)
        || !HiveConf.getBoolVar(jobConf, ConfVars.LLAP_IO_ENCODE_VECTOR_SERDE_ENABLED)
        || !(sourceIf instanceof TextInputFormat) || !(serDe instanceof LazySimpleSerDe)) {
      return new DeserializerOrcWriter(serDe, sourceOi, allocSize);
    }
    Path path = splitPath.getFileSystem(daemonConf).makeQualified(splitPath);
    PartitionDesc partDesc = HiveFileFormatUtils.getFromPathRecursively(parts, path, null);
    if (partDesc == null) {
      LlapIoImpl.LOG.info("Not using VertorDeserializeOrcWriter: no partition desc for " + path);
      return new DeserializerOrcWriter(serDe, sourceOi, allocSize);
    }
    Properties tblProps = partDesc.getTableDesc().getProperties();
    if ("true".equalsIgnoreCase(tblProps.getProperty(
        serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST))) {
      LlapIoImpl.LOG.info("Not using VertorDeserializeOrcWriter due to "
        + serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
      return new DeserializerOrcWriter(serDe, sourceOi, allocSize);
    }
    for (StructField sf : sourceOi.getAllStructFieldRefs()) {
      Category c = sf.getFieldObjectInspector().getCategory();
      if (c != Category.PRIMITIVE) {
        LlapIoImpl.LOG.info("Not using VertorDeserializeOrcWriter: " + c + " is not supported");
        return new DeserializerOrcWriter(serDe, sourceOi, allocSize);
      }
    }
    LlapIoImpl.LOG.info("Creating VertorDeserializeOrcWriter for " + path);
    return new VectorDeserializeOrcWriter(
        jobConf, tblProps, sourceOi, sourceIncludes, cacheIncludes, allocSize, encodeExecutor);
  }

  private VectorDeserializeOrcWriter(Configuration conf, Properties tblProps, StructObjectInspector sourceOi,
      List<Integer> sourceIncludes, boolean[] cacheIncludes, int allocSize, ExecutorService encodeExecutor)
      throws IOException {
    super(sourceOi, allocSize);
    // See also: the usage of VectorDeserializeType, for binary. For now, we only want text.
    this.vrbCtx = createVrbCtx(sourceOi, tblProps, conf);
    this.sourceIncludes = sourceIncludes;
    this.cacheIncludes = cacheIncludes;
    this.sourceBatch = vrbCtx.createVectorizedRowBatch();
    deserializeRead = new LazySimpleDeserializeRead(vrbCtx.getRowColumnTypeInfos(),
      vrbCtx.getRowdataTypePhysicalVariations(),/* useExternalBuffer */ true, createSerdeParams(conf, tblProps));
    vectorDeserializeRow = new VectorDeserializeRow<LazySimpleDeserializeRead>(deserializeRead);
    int colCount = vrbCtx.getRowColumnTypeInfos().length;
    boolean[] includes = null;
    this.usesSourceIncludes = sourceIncludes.size() < colCount;
    if (usesSourceIncludes) {
      // VectorDeserializeRow produces "sparse" VRB when includes are used; we need to write the
      // "dense" VRB to ORC. Ideally, we'd use projection columns, but ORC writer doesn't use them.
      // In any case, we would also need to build a new OI for OrcWriter config.
      // This is why OrcWriter is created after this writer, by the way.
      this.destinationBatch = new VectorizedRowBatch(sourceIncludes.size());
      includes = new boolean[colCount];
      int inclBatchIx = 0;
      List<String> childNames = new ArrayList<>(sourceIncludes.size());
      List<ObjectInspector> childOis = new ArrayList<>(sourceIncludes.size());
      List<? extends StructField> sourceFields = sourceOi.getAllStructFieldRefs();
      for (Integer columnId : sourceIncludes) {
        includes[columnId] = true;
        assert inclBatchIx <= columnId;
        // Note that we use the same vectors in both batches. Clever, very clever.
        destinationBatch.cols[inclBatchIx++] = sourceBatch.cols[columnId];
        StructField sourceField = sourceFields.get(columnId);
        childNames.add(sourceField.getFieldName());
        childOis.add(sourceField.getFieldObjectInspector());
      }
      // This is only used by ORC to derive the structure. Most fields are unused.
      destinationOi = new LazySimpleStructObjectInspector(
          childNames, childOis, null, (byte)0, null);
      destinationBatch.setPartitionInfo(sourceIncludes.size(), 0);
      if (LlapIoImpl.LOG.isDebugEnabled()) {
        LlapIoImpl.LOG.debug("Includes for deserializer are " + DebugUtils.toString(includes));
      }
      try {
        vectorDeserializeRow.init(includes);
      } catch (HiveException e) {
        throw new IOException(e);
      }
    } else {
      // No includes - use the standard batch.
      this.destinationBatch = sourceBatch;
      this.destinationOi = sourceOi;
      try {
        vectorDeserializeRow.init();
      } catch (HiveException e) {
        throw new IOException(e);
      }
    }
    this.isAsync = HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ENCODE_VECTOR_SERDE_ASYNC_ENABLED);
    if (isAsync) {
      currentBatches = new LinkedList<>();
      queue = new ConcurrentLinkedQueue<>();
    } else {
      queue = null;
      currentBatches = null;
    }
    this.encodeExecutor = encodeExecutor;
  }

  public void startAsync(AsyncCallback callback) {
    this.completion = callback;
    this.future = encodeExecutor.submit(this);
  }

  private static VectorizedRowBatchCtx createVrbCtx(StructObjectInspector oi, final Properties tblProps,
    final Configuration conf) throws IOException {
    final boolean useDecimal64ColumnVectors = HiveConf.getVar(conf, ConfVars
      .HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED).equalsIgnoreCase("decimal_64");
    final String serde = tblProps.getProperty(serdeConstants.SERIALIZATION_LIB);
    final String inputFormat = tblProps.getProperty(hive_metastoreConstants.FILE_INPUT_FORMAT);
    final boolean isTextFormat = inputFormat != null && inputFormat.equals(TextInputFormat.class.getName()) &&
      serde != null && serde.equals(LazySimpleSerDe.class.getName());
    List<DataTypePhysicalVariation> dataTypePhysicalVariations = new ArrayList<>();
    if (isTextFormat) {
      StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(oi);
      int dataColumnCount = structTypeInfo.getAllStructFieldTypeInfos().size();
      for (int i = 0; i < dataColumnCount; i++) {
        DataTypePhysicalVariation dataTypePhysicalVariation = DataTypePhysicalVariation.NONE;
        if (useDecimal64ColumnVectors) {
          TypeInfo typeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(i);
          if (typeInfo instanceof DecimalTypeInfo) {
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            if (HiveDecimalWritable.isPrecisionDecimal64(decimalTypeInfo.precision())) {
              dataTypePhysicalVariation = DataTypePhysicalVariation.DECIMAL_64;
            }
          }
        }
        dataTypePhysicalVariations.add(dataTypePhysicalVariation);
      }
    }
    VectorizedRowBatchCtx vrbCtx = new VectorizedRowBatchCtx();
    try {
      vrbCtx.init(oi, new String[0]);
    } catch (HiveException e) {
      throw new IOException(e);
    }
    if (!dataTypePhysicalVariations.isEmpty()) {
      vrbCtx.setRowDataTypePhysicalVariations(dataTypePhysicalVariations.toArray(new DataTypePhysicalVariation[0]));
    }
    return vrbCtx;
  }

  private static LazySerDeParameters createSerdeParams(
      Configuration conf, Properties tblProps) throws IOException {
    try {
      return new LazySerDeParameters(conf, tblProps, LazySimpleSerDe.class.getName());
    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void init(CacheWriter cacheWriter, Configuration conf, Path path) throws IOException {
    this.orcWriter = super.createOrcWriter(cacheWriter, conf, path, destinationOi);
    this.cacheWriter = cacheWriter;
  }

  public interface AsyncCallback {
    void onComplete(VectorDeserializeOrcWriter writer);
  }

  @Override
  public void run() {
    while (true) {
      WriteOperation op = null;
      int fallbackMs = 8;
      while (true) {
        // The reason we poll here is that a blocking queue causes the query thread to spend
        // non-trivial amount of time signaling when an element is added; we'd rather that the
        // time was wasted on this background thread.
        op = queue.poll();
        if (op != null) break;
        if (fallbackMs > 262144) { // Arbitrary... we don't expect caller to hang out for 7+ mins.
          LlapIoImpl.LOG.error("ORC encoder timed out waiting for input");
          discardData();
          return;
        }
        try {
          Thread.sleep(fallbackMs);
        } catch (InterruptedException e) {
          LlapIoImpl.LOG.error("ORC encoder interrupted waiting for input");
          discardData();
          return;
        }
        fallbackMs <<= 1;
      }
      try {
        if (op.apply(orcWriter, cacheWriter)) {
          LlapIoImpl.LOG.info("ORC encoder received a exit event");
          completion.onComplete(this);
          return;
        }
      } catch (Exception e) {
        LlapIoImpl.LOG.error("ORC encoder failed", e);
        discardData();
        return;
      }
    }
  }

  private void discardData() {
    try {
      cacheWriter.discardData();
    } catch (Exception ex) {
      LlapIoImpl.LOG.error("Failed to close an async cache writer", ex);
    }
  }

  @Override
  public void writeOneRow(Writable row) throws IOException {
    if (sourceBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
      flushBatch();
    }

    BinaryComparable binComp = (BinaryComparable)row;
    deserializeRead.set(binComp.getBytes(), 0, binComp.getLength());

    // Deserialize and append new row using the current batch size as the index.
    try {
      // Not using ByRef now since it's unsafe for text readers. Might be safe for others.
      vectorDeserializeRow.deserialize(sourceBatch, sourceBatch.size++);
    } catch (Exception e) {
      throw new IOException("DeserializeRead detail: "
          + vectorDeserializeRow.getDetailedReadPositionString(), e);
    }
  }

  private void flushBatch() throws IOException {
    addBatchToWriter();
    if (!isAsync) {
      for (int c = 0; c < sourceBatch.cols.length; ++c) {
        // This resets vectors in both batches.
        ColumnVector colVector = sourceBatch.cols[c];
        if (colVector != null) {
          colVector.reset();
          colVector.init();
        }
      }
      sourceBatch.selectedInUse = false;
      sourceBatch.size = 0;
      sourceBatch.endOfFile = false;
      propagateSourceBatchFieldsToDest();
    } else {
      // In addBatchToWriter, we have passed the batch to both ORC and operator pipeline
      // (neither ever changes the vectors). We'd need a set of vectors batch to write to.
      // TODO: for now, create this from scratch. Ideally we should return the vectors from ops.
      //       We could also have the ORC thread create it for us in its spare time...
      this.sourceBatch = vrbCtx.createVectorizedRowBatch();
      if (usesSourceIncludes) {
        this.destinationBatch = new VectorizedRowBatch(sourceIncludes.size());
        int inclBatchIx = 0;
        for (Integer columnId : sourceIncludes) {
          destinationBatch.cols[inclBatchIx++] = sourceBatch.cols[columnId];
        }
        destinationBatch.setPartitionInfo(sourceIncludes.size(), 0);
      } else {
        this.destinationBatch = sourceBatch;
      }
    }
  }

  private void propagateSourceBatchFieldsToDest() {
    if (destinationBatch == sourceBatch) return;
    destinationBatch.selectedInUse = sourceBatch.selectedInUse;
    destinationBatch.size = sourceBatch.size;
    destinationBatch.endOfFile = sourceBatch.endOfFile;
  }

  void addBatchToWriter() throws IOException {
    propagateSourceBatchFieldsToDest();
    if (!isAsync) {
      orcWriter.addRowBatch(destinationBatch);
    } else {
      //Lock ColumnVectors so we don't accidentally reset them before they're written out
      for (ColumnVector cv : destinationBatch.cols) {
        if (cv != null) {
          cv.incRef();
        }
      }
      currentBatches.add(destinationBatch);
      addWriteOp(new VrbOperation(destinationBatch));
    }
  }

  @Override
  public void flushIntermediateData() throws IOException {
    if (sourceBatch.size > 0) {
      flushBatch();
    }
  }

  @Override
  public void writeIntermediateFooter() throws IOException {
    if (isAsync) {
      addWriteOp(new IntermediateFooterOperation());
    } else {
      orcWriter.writeIntermediateFooter();
    }
  }

  private void addWriteOp(WriteOperation wo) throws AssertionError {
    if (queue.offer(wo)) return;
    throw new AssertionError("Queue full"); // This should never happen with linked list queue.
  }

  @Override
  public void setCurrentStripeOffsets(long currentKnownTornStart,
      long firstStartOffset, long lastStartOffset, long fileOffset) {
    if (isAsync) {
      addWriteOp(new SetStripeDataOperation(
          currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset));
    } else {
      cacheWriter.setCurrentStripeOffsets(
          currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset);
    }
  }

  @Override
  public void close() throws IOException {
    if (sourceBatch.size > 0) {
      addBatchToWriter();
    }
    if (!isAsync) {
      orcWriter.close();
    } else {
      addWriteOp(new CloseOperation());
    }
  }

  public List<VectorizedRowBatch> extractCurrentVrbs() {
    if (!isAsync) return null;
    List<VectorizedRowBatch> result = currentBatches;
    currentBatches = new LinkedList<>();
    return result;
  }

  interface WriteOperation {
    boolean apply(Writer writer, CacheWriter cacheWriter) throws IOException;
  }

  private static class VrbOperation implements WriteOperation {
    private VectorizedRowBatch batch;

    public VrbOperation(VectorizedRowBatch batch) {
      // LlapIoImpl.LOG.debug("Adding batch " + batch);
      this.batch = batch;
    }

    @Override
    public boolean apply(Writer writer, CacheWriter cacheWriter) throws IOException {
      // LlapIoImpl.LOG.debug("Writing batch " + batch);
      writer.addRowBatch(batch);
      for (ColumnVector cv : batch.cols) {
        if (cv != null) {
          assert (cv.decRef() == 0);
        }
      }
      return false;
    }
  }

  private static class IntermediateFooterOperation implements WriteOperation {
    @Override
    public boolean apply(Writer writer, CacheWriter cacheWriter) throws IOException {
      writer.writeIntermediateFooter();
      return false;
    }
  }

  private static class SetStripeDataOperation implements WriteOperation {
    private final long currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset;
    public SetStripeDataOperation(long currentKnownTornStart,
        long firstStartOffset, long lastStartOffset, long fileOffset) {
      this.currentKnownTornStart = currentKnownTornStart;
      this.firstStartOffset = firstStartOffset;
      this.lastStartOffset = lastStartOffset;
      this.fileOffset = fileOffset;
    }

    @Override
    public boolean apply(Writer writer, CacheWriter cacheWriter) throws IOException {
      cacheWriter.setCurrentStripeOffsets(
          currentKnownTornStart, firstStartOffset, lastStartOffset, fileOffset);
      return false;
    }
  }

  private static class CloseOperation implements WriteOperation {
    @Override
    public boolean apply(Writer writer, CacheWriter cacheWriter) throws IOException {
      writer.close();
      return true; // The thread should stop after this. 
    }
  }

  public boolean[] getOriginalCacheIncludes() {
    return cacheIncludes;
  }

  @Override
  public boolean isOnlyWritingIncludedColumns() {
    return usesSourceIncludes;
  }

  public void interrupt() {
    if (future != null) {
      future.cancel(true);
    }
  }
}
