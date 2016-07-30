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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.counters.FragmentCountersMap;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.ReadPipeline;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat.AvoidSplitCombination;
import org.apache.hadoop.hive.ql.io.LlapAwareSplit;
import org.apache.hadoop.hive.ql.io.SelfDescribingInputFormatInterface;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class LlapInputFormat implements InputFormat<NullWritable, VectorizedRowBatch>,
    VectorizedInputFormatInterface, SelfDescribingInputFormatInterface,
    AvoidSplitCombination {
  @SuppressWarnings("rawtypes")
  private final InputFormat sourceInputFormat;
  private final AvoidSplitCombination sourceASC;
  private final ColumnVectorProducer cvp;
  private final ListeningExecutorService executor;
  private final String hostName;

  @SuppressWarnings("rawtypes")
  LlapInputFormat(InputFormat sourceInputFormat, ColumnVectorProducer cvp,
      ListeningExecutorService executor) {
    // TODO: right now, we do nothing with source input format, ORC-only in the first cut.
    //       We'd need to plumb it thru and use it to get data to cache/etc.
    assert sourceInputFormat instanceof OrcInputFormat;
    this.executor = executor;
    this.cvp = cvp;
    this.sourceInputFormat = sourceInputFormat;
    this.sourceASC = (sourceInputFormat instanceof AvoidSplitCombination)
        ? (AvoidSplitCombination)sourceInputFormat : null;
    this.hostName = HiveStringUtils.getHostname();
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    boolean useLlapIo = true;
    if (split instanceof LlapAwareSplit) {
      useLlapIo = ((LlapAwareSplit) split).canUseLlapIo();
    }

    // validate for supported types. Until we fix HIVE-14089 we need this check.
    if (useLlapIo) {
      useLlapIo = Utilities.checkLlapIOSupportedTypes(job);
    }

    if (!useLlapIo) {
      LlapIoImpl.LOG.warn("Not using LLAP IO for an unsupported split: " + split);
      return sourceInputFormat.getRecordReader(split, job, reporter);
    }
    boolean isVectorMode = Utilities.getUseVectorizedInputFileFormat(job);
    if (!isVectorMode) {
      LlapIoImpl.LOG.error("No LLAP IO in non-vectorized mode");
      throw new UnsupportedOperationException("No LLAP IO in non-vectorized mode");
    }
    FileSplit fileSplit = (FileSplit)split;
    reporter.setStatus(fileSplit.toString());
    try {
      List<Integer> includedCols = ColumnProjectionUtils.isReadAllColumns(job)
          ? null : ColumnProjectionUtils.getReadColumnIDs(job);
      LlapRecordReader rr = new LlapRecordReader(job, fileSplit, includedCols, hostName);

      if (!rr.init()) {
        return sourceInputFormat.getRecordReader(split, job, reporter);
      }

      return rr;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return sourceInputFormat.getSplits(job, numSplits);
  }

  private class LlapRecordReader
      implements RecordReader<NullWritable, VectorizedRowBatch>, Consumer<ColumnVectorBatch> {
    private final Logger LOG = LoggerFactory.getLogger(LlapRecordReader.class);
    private final FileSplit split;
    private final List<Integer> columnIds;
    private final SearchArgument sarg;
    private final String[] columnNames;
    private final VectorizedRowBatchCtx rbCtx;
    private final boolean[] columnsToIncludeTruncated;
    private final Object[] partitionValues;

    private final LinkedList<ColumnVectorBatch> pendingData = new LinkedList<ColumnVectorBatch>();
    private ColumnVectorBatch lastCvb = null;
    private boolean isFirst = true;

    private Throwable pendingError = null;
    /** Vector that is currently being processed by our user. */
    private boolean isDone = false;
    private final boolean isClosed = false;
    private final ConsumerFeedback<ColumnVectorBatch> feedback;
    private final QueryFragmentCounters counters;
    private long firstReturnTime;

    private final JobConf jobConf;
    private final TypeDescription fileSchema;
    private final boolean[] includedColumns;
    private final ReadPipeline rp;

    public LlapRecordReader(JobConf job, FileSplit split, List<Integer> includedCols,
        String hostName) throws IOException, HiveException {
      this.jobConf = job;
      this.split = split;
      this.columnIds = includedCols;
      this.sarg = ConvertAstToSearchArg.createFromConf(job);
      this.columnNames = ColumnProjectionUtils.getReadColumnNames(job);
      String dagId = job.get("tez.mapreduce.dag.index");
      String vertexId = job.get("tez.mapreduce.vertex.index");
      String taskId = job.get("tez.mapreduce.task.index");
      String taskAttemptId = job.get("tez.mapreduce.task.attempt.index");
      TezCounters taskCounters = null;
      if (dagId != null && vertexId != null && taskId != null && taskAttemptId != null) {
        String fullId = Joiner.on('_').join(dagId, vertexId, taskId, taskAttemptId);
        taskCounters = FragmentCountersMap.getCountersForFragment(fullId);
        LOG.info("Received dagid_vertexid_taskid_attempid: {}", fullId);
      } else {
        LOG.warn("Not using tez counters as some identifier is null." +
            " dagId: {} vertexId: {} taskId: {} taskAttempId: {}",
            dagId, vertexId, taskId, taskAttemptId);
      }
      this.counters = new QueryFragmentCounters(job, taskCounters);
      this.counters.setDesc(QueryFragmentCounters.Desc.MACHINE, hostName);

      MapWork mapWork = Utilities.getMapWork(job);
      rbCtx = mapWork.getVectorizedRowBatchCtx();

      columnsToIncludeTruncated = rbCtx.getColumnsToIncludeTruncated(job);

      int partitionColumnCount = rbCtx.getPartitionColumnCount();
      if (partitionColumnCount > 0) {
        partitionValues = new Object[partitionColumnCount];
        VectorizedRowBatchCtx.getPartitionValues(rbCtx, job, split, partitionValues);
      } else {
        partitionValues = null;
      }

      // Create the consumer of encoded data; it will coordinate decoding to CVBs.
      rp = cvp.createReadPipeline(this, split, columnIds, sarg, columnNames, counters);
      feedback = rp;
      fileSchema = rp.getFileSchema();
      includedColumns = rp.getIncludedColumns();
    }

    /**
     * Starts the data read pipeline
     */
    public boolean init() {
      boolean isAcidScan = HiveConf.getBoolVar(jobConf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN);
      TypeDescription readerSchema = OrcInputFormat.getDesiredRowTypeDescr(jobConf, isAcidScan,
          Integer.MAX_VALUE);
      SchemaEvolution schemaEvolution = new SchemaEvolution(fileSchema, readerSchema,
          includedColumns);
      for (Integer colId : columnIds) {
        if (!schemaEvolution.isPPDSafeConversion(colId)) {
          LlapIoImpl.LOG.warn("Unsupported schema evolution! Disabling Llap IO for {}", split);
          return false;
        }
      }

      ListenableFuture<Void> future = executor.submit(rp.getReadCallable());
      // TODO: we should NOT do this thing with handler. Reader needs to do cleanup in most cases.
      Futures.addCallback(future, new UncaughtErrorHandler());
      return true;
    }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
      assert value != null;
      if (isClosed) {
        throw new AssertionError("next called after close");
      }
      // Add partition cols if necessary (see VectorizedOrcInputFormat for details).
      boolean wasFirst = isFirst;
      if (isFirst) {
        if (partitionValues != null) {
          rbCtx.addPartitionColsToBatch(value, partitionValues);
        }
        isFirst = false;
      }
      ColumnVectorBatch cvb = null;
      try {
        cvb = nextCvb();
      } catch (InterruptedException e) {
        // Query might have been canceled. Stop the background processing.
        feedback.stop();
        throw new IOException(e);
      }
      if (cvb == null) {
        if (wasFirst) {
          firstReturnTime = counters.startTimeCounter();
        }
        counters.incrTimeCounter(LlapIOCounters.CONSUMER_TIME_NS, firstReturnTime);
        return false;
      }
      if (columnIds.size() != cvb.cols.length) {
        throw new RuntimeException("Unexpected number of columns, VRB has " + columnIds.size()
            + " included, but the reader returned " + cvb.cols.length);
      }
      // VRB was created from VrbCtx, so we already have pre-allocated column vectors
      for (int i = 0; i < cvb.cols.length; ++i) {
        // Return old CVs (if any) to caller. We assume these things all have the same schema.
        cvb.swapColumnVector(i, value.cols, columnIds.get(i));
      }
      value.selectedInUse = false;
      value.size = cvb.size;
      if (wasFirst) {
        firstReturnTime = counters.startTimeCounter();
      }
      return true;
    }


    private final class UncaughtErrorHandler implements FutureCallback<Void> {
      @Override
      public void onSuccess(Void result) {
        // Successful execution of reader is supposed to call setDone.
      }

      @Override
      public void onFailure(Throwable t) {
        // Reader is not supposed to throw AFTER calling setError.
        LlapIoImpl.LOG.error("Unhandled error from reader thread " + t.getMessage());
        setError(t);
      }
    }

    ColumnVectorBatch nextCvb() throws InterruptedException, IOException {
      boolean isFirst = (lastCvb == null);
      if (!isFirst) {
        feedback.returnData(lastCvb);
      }
      synchronized (pendingData) {
        // We are waiting for next block. Either we will get it, or be told we are done.
        boolean doLogBlocking = LlapIoImpl.LOG.isTraceEnabled() && isNothingToReport();
        if (doLogBlocking) {
          LlapIoImpl.LOG.trace("next will block");
        }
        while (isNothingToReport()) {
          pendingData.wait(100);
        }
        if (doLogBlocking) {
          LlapIoImpl.LOG.trace("next is unblocked");
        }
        rethrowErrorIfAny();
        lastCvb = pendingData.poll();
      }
      if (LlapIoImpl.LOG.isTraceEnabled() && lastCvb != null) {
        LlapIoImpl.LOG.trace("Processing will receive vector {}", lastCvb);
      }
      return lastCvb;
    }

    private boolean isNothingToReport() {
      return !isDone && pendingData.isEmpty() && pendingError == null;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public VectorizedRowBatch createValue() {
      return rbCtx.createVectorizedRowBatch(columnsToIncludeTruncated);
    }

    @Override
    public long getPos() throws IOException {
      return -1; // Position doesn't make sense for async reader, chunk order is arbitrary.
    }

    @Override
    public void close() throws IOException {
      if (LlapIoImpl.LOG.isTraceEnabled()) {
        LlapIoImpl.LOG.trace("close called; closed {}, done {}, err {}, pending {}",
            isClosed, isDone, pendingError, pendingData.size());
      }
      LlapIoImpl.LOG.info("Llap counters: {}" ,counters); // This is where counters are logged!
      feedback.stop();
      rethrowErrorIfAny();
    }

    private void rethrowErrorIfAny() throws IOException {
      if (pendingError == null) return;
      if (pendingError instanceof IOException) {
        throw (IOException)pendingError;
      }
      throw new IOException(pendingError);
    }

    @Override
    public void setDone() {
      if (LlapIoImpl.LOG.isTraceEnabled()) {
        LlapIoImpl.LOG.trace("setDone called; closed {}, done {}, err {}, pending {}",
            isClosed, isDone, pendingError, pendingData.size());
      }
      synchronized (pendingData) {
        isDone = true;
        pendingData.notifyAll();
      }
    }

    @Override
    public void consumeData(ColumnVectorBatch data) {
      if (LlapIoImpl.LOG.isTraceEnabled()) {
        LlapIoImpl.LOG.trace("consume called; closed {}, done {}, err {}, pending {}",
            isClosed, isDone, pendingError, pendingData.size());
      }
      synchronized (pendingData) {
        if (isClosed) {
          return;
        }
        pendingData.add(data);
        pendingData.notifyAll();
      }
    }

    @Override
    public void setError(Throwable t) {
      counters.incrCounter(LlapIOCounters.NUM_ERRORS);
      LlapIoImpl.LOG.info("setError called; closed {}, done {}, err {}, pending {}",
          isClosed, isDone, pendingError, pendingData.size());
      assert t != null;
      synchronized (pendingData) {
        pendingError = t;
        pendingData.notifyAll();
      }
    }

    @Override
    public float getProgress() throws IOException {
      // TODO: plumb progress info thru the reader if we can get metadata from loader first.
      return 0.0f;
    }
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) throws IOException {
    return sourceASC == null ? false : sourceASC.shouldSkipCombine(path, conf);
  }
}
