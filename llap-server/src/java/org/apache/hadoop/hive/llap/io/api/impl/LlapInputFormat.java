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


package org.apache.hadoop.hive.llap.io.api.impl;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.hive.ql.io.BatchToRowInputFormat;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat.AvoidSplitCombination;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.LlapCacheOnlyInputFormatInterface;
import org.apache.hadoop.hive.ql.io.LlapAwareSplit;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat.NullRowsRecordReader;
import org.apache.hadoop.hive.ql.io.SelfDescribingInputFormatInterface;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.common.util.HiveStringUtils;

public class LlapInputFormat implements InputFormat<NullWritable, VectorizedRowBatch>,
    VectorizedInputFormatInterface, SelfDescribingInputFormatInterface,
    AvoidSplitCombination {
  private static final String NONVECTOR_SETTING_MESSAGE = "disable "
      + ConfVars.LLAP_IO_NONVECTOR_WRAPPER_ENABLED.varname + " to work around this error";

  private static final Map<String, VirtualColumn> ALLOWED_VIRTUAL_COLUMNS = Collections.unmodifiableMap(
          new HashMap<String, VirtualColumn>() {{
    put(VirtualColumn.ROWID.getName(), VirtualColumn.ROWID);
    put(VirtualColumn.ROWISDELETED.getName(), VirtualColumn.ROWISDELETED);
  }});

  private final InputFormat<NullWritable, VectorizedRowBatch> sourceInputFormat;
  private final AvoidSplitCombination sourceASC;
  private final Deserializer sourceSerDe;
  final ColumnVectorProducer cvp;
  final ExecutorService executor;
  private static final String hostName = HiveStringUtils.getHostname();

  private final Configuration daemonConf;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  LlapInputFormat(InputFormat sourceInputFormat, Deserializer sourceSerDe,
      ColumnVectorProducer cvp, ExecutorService executor, Configuration daemonConf) {
    this.executor = executor;
    this.cvp = cvp;
    this.daemonConf = daemonConf;
    this.sourceInputFormat = sourceInputFormat;
    this.sourceASC = (sourceInputFormat instanceof AvoidSplitCombination)
        ? (AvoidSplitCombination)sourceInputFormat : null;
    this.sourceSerDe = sourceSerDe;
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    // Check LLAP-aware split (e.g. OrcSplit) to make sure it's compatible.
    RecordReader<NullWritable, VectorizedRowBatch> noLlap = checkLlapSplit(
        split, job, reporter);
    if (noLlap != null) return noLlap;

    FileSplit fileSplit = (FileSplit) split;
    reporter.setStatus(fileSplit.toString());

    try {
      boolean vectorized = Utilities.getIsVectorized(job);
      if (!vectorized && !canWrapToRows(job)) {
        // The pipeline wants rows and no batch-to-row wrapper exists for this format; decide here,
        // before a reader (and its read pipeline, footer fetch, counters) is built for nothing.
        return fallback(split, job, reporter);
      }
      // At this entry point, we are going to assume that these are logical table columns.
      // Perhaps we should go thru the code and clean this up to be more explicit; for now, we
      // will start with this single assumption and maintain clear semantics from here.
      List<Integer> tableIncludedCols = ColumnProjectionUtils.isReadAllColumns(job)
          ? null : ColumnProjectionUtils.getReadColumnIDs(job);
      LlapRecordReader rr = LlapRecordReader.create(job, fileSplit, tableIncludedCols, hostName,
          cvp, executor, sourceInputFormat, sourceSerDe, reporter, daemonConf);
      if (rr == null) {
        // Reader-specific incompatibility: SMB, schema evolution, or the producer declined the split.
        return fallback(split, job, reporter);
      }
      RecordReader<NullWritable, VectorizedRowBatch> result =
          vectorized ? rr : wrapToRows(rr, tableIncludedCols, split);
      // This starts the reader in the background.
      rr.start();
      return result;
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception ex) {
      Throwable rootCause = JavaUtils.findRootCause(ex);
      if (checkLimitReached(job)
          && (rootCause instanceof InterruptedException || rootCause instanceof ClosedByInterruptException)) {
        LlapIoImpl.LOG.info("Ignoring exception while getting record reader as limit is reached", rootCause);
        return new NullRowsRecordReader(job, split);
      } else {
        throw new IOException(ex);
      }
    }
  }

  /** The non-vectorized pipeline can take LLAP batches only through the source format's row wrapper. */
  private boolean canWrapToRows(JobConf job) {
    return HiveConf.getBoolVar(job, ConfVars.LLAP_IO_ROW_WRAPPER_ENABLED)
        && sourceInputFormat instanceof BatchToRowInputFormat;
  }

  /** Only called when {@link #canWrapToRows} held, so the cast is safe. */
  private RecordReader<NullWritable, VectorizedRowBatch> wrapToRows(
      LlapRecordReader rr, List<Integer> includedCols, InputSplit split) throws IOException {
    LlapIoImpl.LOG.info("Using batch-to-row converter for split: " + split);
    return bogusCast(((BatchToRowInputFormat) sourceInputFormat).getWrapper(
        rr, rr.getVectorizedRowBatchCtx(), includedCols));
  }

  /**
   * Serves the split with the source format. A cache-only format still gets the LLAP caches
   * injected, so a declined split reads through the same cache entries as the native path.
   */
  private RecordReader<NullWritable, VectorizedRowBatch> fallback(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    if (sourceInputFormat instanceof LlapCacheOnlyInputFormatInterface) {
      // injectLlapCaches rather than initCacheOnlyInputFormat: it also pins
      // parquet.read.allocation.size, so the reader asks for each column chunk in one read.
      HiveInputFormat.injectLlapCaches(bogusCast(sourceInputFormat), LlapProxy.getIo(), job);
    }
    return sourceInputFormat.getRecordReader(split, job, reporter);
  }

  private boolean checkLimitReached(JobConf job) {
    /*
     * 2 assumptions here when using "tez.mapreduce.vertex.name"
     *
     * 1. The execution engine is tez, which is valid in case of LLAP.
     * 2. The property "tez.mapreduce.vertex.name" is present: it is handled in MRInputBase.initialize.
     *    On Input codepaths we cannot use properties from TezProcessor.initTezAttributes.
     */
    return LimitOperator.checkLimitReachedForVertex(job, job.get("tez.mapreduce.vertex.name"));
  }

  public RecordReader<NullWritable, VectorizedRowBatch> checkLlapSplit(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    boolean useLlapIo = true;
    if (split instanceof LlapAwareSplit) {
      useLlapIo = ((LlapAwareSplit) split).canUseLlapIo(job);
    }
    if (useLlapIo) return null;

    LlapIoImpl.LOG.warn("Not using LLAP IO for an unsupported split: " + split);
    return sourceInputFormat.getRecordReader(split, job, reporter);
  }

  // Returning either a vectorized or non-vectorized reader from the same call requires breaking
  // generics... this is how vectorization currently works.
  @SuppressWarnings("unchecked")
  private static <A, B, C, D> RecordReader<A, B> bogusCast(RecordReader<C, D> rr) {
    return (RecordReader<A, B>)rr;
  }

  // Same break, for the format itself: injectLlapCaches is written against the row-shaped
  // signature HiveInputFormat deals in, and this class holds the vectorized one.
  @SuppressWarnings("unchecked")
  private static <A, B, C, D> InputFormat<A, B> bogusCast(InputFormat<C, D> inputFormat) {
    return (InputFormat<A, B>)inputFormat;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return sourceInputFormat.getSplits(job, numSplits);
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) throws IOException {
    return sourceASC == null ? false : sourceASC.shouldSkipCombine(path, conf);
  }

  static VectorizedRowBatchCtx createFakeVrbCtx(MapWork mapWork) throws HiveException {
    // This is based on Vectorizer code, minus the validation.

    // Add all non-virtual columns from the TableScan operator.
    RowSchema rowSchema = findTsOp(mapWork).getSchema();
    final List<String> colNames = new ArrayList<String>(rowSchema.getSignature().size());
    final List<TypeInfo> colTypes = new ArrayList<TypeInfo>(rowSchema.getSignature().size());
    ArrayList<VirtualColumn> virtualColumnList = new ArrayList<>(2);
    for (ColumnInfo c : rowSchema.getSignature()) {
      String columnName = c.getInternalName();
      if (ALLOWED_VIRTUAL_COLUMNS.containsKey(columnName)) {
        virtualColumnList.add(ALLOWED_VIRTUAL_COLUMNS.get(columnName));
      } else if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(columnName)) {
        continue;
      }
      colNames.add(columnName);
      colTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(c.getTypeName()));
    }

    // Determine the partition columns using the first partition descriptor.
    // Note - like vectorizer, this assumes partition columns go after data columns.
    int partitionColumnCount = 0;
    Iterator<Path> paths = mapWork.getPathToAliases().keySet().iterator();
    if (paths.hasNext()) {
      PartitionDesc partDesc = mapWork.getPathToPartitionInfo().get(paths.next());
      if (partDesc != null) {
        Map<String, String> partSpec = partDesc.getPartSpec();
        if (partSpec != null && !partSpec.isEmpty()) {
          partitionColumnCount = partSpec.size();
        }
      }
    }
    final VirtualColumn[] virtualColumns = virtualColumnList.toArray(new VirtualColumn[0]);
    return new VectorizedRowBatchCtx(colNames.toArray(new String[colNames.size()]),
        colTypes.toArray(new TypeInfo[colTypes.size()]), null, null, partitionColumnCount,
        virtualColumns.length, virtualColumns, new String[0], null);
  }

  static TableScanOperator findTsOp(MapWork mapWork) throws HiveException {
    if (mapWork.getAliasToWork() == null) {
      throw new HiveException("Unexpected - aliasToWork is missing; " + NONVECTOR_SETTING_MESSAGE);
    }
    Iterator<Operator<?>> ops = mapWork.getAliasToWork().values().iterator();
    TableScanOperator tableScanOperator = null;
    while (ops.hasNext()) {
      Operator<?> op = ops.next();
      if (op instanceof TableScanOperator) {
        if (tableScanOperator != null) {
          throw new HiveException("Unexpected - more than one TSOP; " + NONVECTOR_SETTING_MESSAGE);
        }
        tableScanOperator = (TableScanOperator)op;
      }
    }
    return tableScanOperator;
  }

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures() {
    return new VectorizedSupport.Support[] {VectorizedSupport.Support.DECIMAL_64};
  }
}
