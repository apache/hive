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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.DeserialerOrcWriter;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.EncodingWriter;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/** The class that writes rows from a text reader to an ORC writer using VectorDeserializeRow. */
class VertorDeserializeOrcWriter implements EncodingWriter {
  private Writer orcWriter;
  private final LazySimpleDeserializeRead deserializeRead;
  private final VectorDeserializeRow<?> vectorDeserializeRow;
  private final VectorizedRowBatch sourceBatch, destinationBatch;
  private final boolean hasIncludes;
  private final StructObjectInspector destinationOi;

  // TODO: if more writers are added, separate out an EncodingWriterFactory
  public static EncodingWriter create(InputFormat<?, ?> sourceIf, Deserializer serDe,
      Map<Path, PartitionDesc> parts, Configuration daemonConf,
      Configuration jobConf, Path splitPath, StructObjectInspector sourceOi,
      List<Integer> includes) throws IOException {
    // Vector SerDe can be disabled both on client and server side.
    if (!HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_IO_ENCODE_VECTOR_SERDE_ENABLED)
        || !HiveConf.getBoolVar(jobConf, ConfVars.LLAP_IO_ENCODE_VECTOR_SERDE_ENABLED)
        || !(sourceIf instanceof TextInputFormat) || !(serDe instanceof LazySimpleSerDe)) {
      return new DeserialerOrcWriter(serDe, sourceOi);
    }
    Path path = splitPath.getFileSystem(daemonConf).makeQualified(splitPath);
    PartitionDesc partDesc = HiveFileFormatUtils.getPartitionDescFromPathRecursively(
        parts, path, null);
    if (partDesc == null) {
      LlapIoImpl.LOG.info("Not using VertorDeserializeOrcWriter: no partition desc for " + path);
      return new DeserialerOrcWriter(serDe, sourceOi);
    }
    Properties tblProps = partDesc.getTableDesc().getProperties();
    if ("true".equalsIgnoreCase(tblProps.getProperty(
        serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST))) {
      LlapIoImpl.LOG.info("Not using VertorDeserializeOrcWriter due to "
        + serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
      return new DeserialerOrcWriter(serDe, sourceOi);
    }
    for (StructField sf : sourceOi.getAllStructFieldRefs()) {
      Category c = sf.getFieldObjectInspector().getCategory();
      if (c != Category.PRIMITIVE) {
        LlapIoImpl.LOG.info("Not using VertorDeserializeOrcWriter: " + c + " is not supported");
        return new DeserialerOrcWriter(serDe, sourceOi);
      }
    }
    LlapIoImpl.LOG.info("Creating VertorDeserializeOrcWriter for " + path);
    return new VertorDeserializeOrcWriter(daemonConf, tblProps, sourceOi, includes);
  }

  private VertorDeserializeOrcWriter(Configuration conf, Properties tblProps,
      StructObjectInspector sourceOi, List<Integer> columnIds) throws IOException {
    // See also: the usage of VectorDeserializeType, for binary. For now, we only want text.
    VectorizedRowBatchCtx vrbCtx = createVrbCtx(sourceOi);
    this.sourceBatch = vrbCtx.createVectorizedRowBatch();
    deserializeRead = new LazySimpleDeserializeRead(vrbCtx.getRowColumnTypeInfos(),
        /* useExternalBuffer */ true, createSerdeParams(conf, tblProps));
    vectorDeserializeRow = new VectorDeserializeRow<LazySimpleDeserializeRead>(deserializeRead);
    int colCount = vrbCtx.getRowColumnTypeInfos().length;
    boolean[] includes = null;
    this.hasIncludes = columnIds.size() < colCount;
    if (hasIncludes) {
      // VectorDeserializeRow produces "sparse" VRB when includes are used; we need to write the
      // "dense" VRB to ORC. Ideally, we'd use projection columns, but ORC writer doesn't use them.
      // In any case, we would also need to build a new OI for OrcWriter config.
      // This is why OrcWriter is created after this writer, by the way.
      this.destinationBatch = new VectorizedRowBatch(columnIds.size());
      includes = new boolean[colCount];
      int inclBatchIx = 0;
      List<String> childNames = new ArrayList<>(columnIds.size());
      List<ObjectInspector> childOis = new ArrayList<>(columnIds.size());
      List<? extends StructField> sourceFields = sourceOi.getAllStructFieldRefs();
      for (Integer columnId : columnIds) {
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
      destinationBatch.setPartitionInfo(columnIds.size(), 0);
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
  }

  private static VectorizedRowBatchCtx createVrbCtx(StructObjectInspector oi) throws IOException {
    VectorizedRowBatchCtx vrbCtx = new VectorizedRowBatchCtx();
    try {
      vrbCtx.init(oi, new String[0]);
    } catch (HiveException e) {
      throw new IOException(e);
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
  public boolean hasIncludes() {
    return hasIncludes;
  }

  @Override
  public StructObjectInspector getDestinationOi() {
    return destinationOi;
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
      // TODO: can we use ByRef? Probably not, need to see text record reader.
      vectorDeserializeRow.deserialize(sourceBatch, sourceBatch.size++);
    } catch (Exception e) {
      throw new IOException("DeserializeRead detail: "
          + vectorDeserializeRow.getDetailedReadPositionString(), e);
    }
  }

  private void flushBatch() throws IOException {
    addBatchToWriter();

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
  }

  private void propagateSourceBatchFieldsToDest() {
    if (destinationBatch == sourceBatch) return;
    destinationBatch.selectedInUse = sourceBatch.selectedInUse;
    destinationBatch.size = sourceBatch.size;
    destinationBatch.endOfFile = sourceBatch.endOfFile;
  }

  private void addBatchToWriter() throws IOException {
    propagateSourceBatchFieldsToDest();
    // LlapIoImpl.LOG.info("Writing includeOnlyBatch " + s + "; data "+ includeOnlyBatch);
    orcWriter.addRowBatch(destinationBatch);
  }

  @Override
  public void flushIntermediateData() throws IOException {
    if (sourceBatch.size > 0) {
      flushBatch();
    }
  }

  @Override
  public void writeIntermediateFooter() throws IOException {
    orcWriter.writeIntermediateFooter();
  }

  @Override
  public void close() throws IOException {
    if (sourceBatch.size > 0) {
      addBatchToWriter();
    }
    orcWriter.close();
  }

  @Override
  public void init(Writer orcWriter) {
    this.orcWriter = orcWriter;
  }
}