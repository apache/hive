/**
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


package org.apache.hadoop.hive.llap.io.api.impl;

import org.apache.hadoop.hive.ql.io.BatchToRowInputFormat;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.ReadPipeline;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat.AvoidSplitCombination;
import org.apache.hadoop.hive.ql.io.LlapAwareSplit;
import org.apache.hadoop.hive.ql.io.SelfDescribingInputFormatInterface;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
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
import org.apache.orc.OrcUtils;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class LlapInputFormat implements InputFormat<NullWritable, VectorizedRowBatch>,
    VectorizedInputFormatInterface, SelfDescribingInputFormatInterface,
    AvoidSplitCombination {
  private static final String NONVECTOR_SETTING_MESSAGE = "disable "
      + ConfVars.LLAP_IO_NONVECTOR_WRAPPER_ENABLED.varname + " to work around this error";

  @SuppressWarnings("rawtypes")
  private final InputFormat sourceInputFormat;
  private final AvoidSplitCombination sourceASC;
  @SuppressWarnings("deprecation")
  private final Deserializer sourceSerDe;
  final ColumnVectorProducer cvp;
  final ExecutorService executor;
  private final String hostName;

  @SuppressWarnings("rawtypes")
  LlapInputFormat(InputFormat sourceInputFormat, Deserializer sourceSerDe,
      ColumnVectorProducer cvp, ExecutorService executor) {
    this.executor = executor;
    this.cvp = cvp;
    this.sourceInputFormat = sourceInputFormat;
    this.sourceASC = (sourceInputFormat instanceof AvoidSplitCombination)
        ? (AvoidSplitCombination)sourceInputFormat : null;
    this.sourceSerDe = sourceSerDe;
    this.hostName = HiveStringUtils.getHostname();
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    RecordReader<NullWritable, VectorizedRowBatch> noLlap = checkLlapSplit(split, job, reporter);
    if (noLlap != null) return noLlap;

    boolean isVectorized = Utilities.getUseVectorizedInputFileFormat(job);

    FileSplit fileSplit = (FileSplit) split;
    reporter.setStatus(fileSplit.toString());
    try {
      List<Integer> includedCols = ColumnProjectionUtils.isReadAllColumns(job)
          ? null : ColumnProjectionUtils.getReadColumnIDs(job);
      LlapRecordReader rr = new LlapRecordReader(job, fileSplit, includedCols, hostName, cvp,
          executor, sourceInputFormat, sourceSerDe, reporter);
      if (!rr.init()) {
        return sourceInputFormat.getRecordReader(split, job, reporter);
      }

      return wrapLlapReader(isVectorized, includedCols, rr, split, job, reporter);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  public RecordReader<NullWritable, VectorizedRowBatch> wrapLlapReader(
      boolean isVectorized, List<Integer> includedCols, LlapRecordReader rr,
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    // vectorized row batch reader
    if (isVectorized) {
      return rr;
    } else if (sourceInputFormat instanceof BatchToRowInputFormat) {
      LlapIoImpl.LOG.info("Using batch-to-row converter for split: " + split);
      return bogusCast(((BatchToRowInputFormat) sourceInputFormat).getWrapper(
          rr, rr.getVectorizedRowBatchCtx(), includedCols));
    } else {
      LlapIoImpl.LOG.warn("Not using LLAP IO for an unsupported split: " + split);
      return sourceInputFormat.getRecordReader(split, job, reporter);
    }
  }

  public RecordReader<NullWritable, VectorizedRowBatch> checkLlapSplit(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    boolean useLlapIo = true;
    if (split instanceof LlapAwareSplit) {
      useLlapIo = ((LlapAwareSplit) split).canUseLlapIo();
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
    for (ColumnInfo c : rowSchema.getSignature()) {
      String columnName = c.getInternalName();
      if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(columnName)) continue;
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
        LinkedHashMap<String, String> partSpec = partDesc.getPartSpec();
        if (partSpec != null && partSpec.isEmpty()) {
          partitionColumnCount = partSpec.size();
        }
      }
    }
    return new VectorizedRowBatchCtx(colNames.toArray(new String[colNames.size()]),
        colTypes.toArray(new TypeInfo[colTypes.size()]), null, partitionColumnCount, new String[0]);
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
}
