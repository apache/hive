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

import java.util.ArrayList;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.counters.FragmentCountersMap;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.daemon.impl.StatsRecordingThreadPool;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.ReadPipeline;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

class LlapRecordReader
    implements RecordReader<NullWritable, VectorizedRowBatch>, Consumer<ColumnVectorBatch> {
  private static final Logger LOG = LoggerFactory.getLogger(LlapRecordReader.class);

  private final FileSplit split;
  private final List<Integer> columnIds;
  private final SearchArgument sarg;
  private final String[] columnNames;
  private final VectorizedRowBatchCtx rbCtx;
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
  private final boolean[] includedColumns;
  private final ReadPipeline rp;
  private final ExecutorService executor;
  private final int columnCount;

  private SchemaEvolution evolution;

  private final boolean isAcidScan;

  public LlapRecordReader(JobConf job, FileSplit split, List<Integer> includedCols,
      String hostName, ColumnVectorProducer cvp, ExecutorService executor,
      InputFormat<?, ?> sourceInputFormat, Deserializer sourceSerDe, Reporter reporter)
          throws IOException, HiveException {
    this.executor = executor;
    this.jobConf = job;
    this.split = split;
    this.sarg = ConvertAstToSearchArg.createFromConf(job);
    this.columnNames = ColumnProjectionUtils.getReadColumnNames(job);
    final String fragmentId = LlapTezUtils.getFragmentId(job);
    final String dagId = LlapTezUtils.getDagId(job);
    final String queryId = HiveConf.getVar(job, HiveConf.ConfVars.HIVEQUERYID);
    MDC.put("dagId", dagId);
    MDC.put("queryId", queryId);
    TezCounters taskCounters = null;
    if (fragmentId != null) {
      MDC.put("fragmentId", fragmentId);
      taskCounters = FragmentCountersMap.getCountersForFragment(fragmentId);
      LOG.info("Received fragment id: {}", fragmentId);
    } else {
      LOG.warn("Not using tez counters as fragment id string is null");
    }
    this.counters = new QueryFragmentCounters(job, taskCounters);
    this.counters.setDesc(QueryFragmentCounters.Desc.MACHINE, hostName);

    MapWork mapWork = Utilities.getMapWork(job);
    VectorizedRowBatchCtx ctx = mapWork.getVectorizedRowBatchCtx();
    rbCtx = ctx != null ? ctx : LlapInputFormat.createFakeVrbCtx(mapWork);
    if (includedCols == null) {
      // Assume including everything means the VRB will have everything.
      includedCols = new ArrayList<>(rbCtx.getRowColumnTypeInfos().length);
      for (int i = 0; i < rbCtx.getRowColumnTypeInfos().length; ++i) {
        includedCols.add(i);
      }
    }
    this.columnIds = includedCols;
    this.columnCount = columnIds.size();

    int partitionColumnCount = rbCtx.getPartitionColumnCount();
    if (partitionColumnCount > 0) {
      partitionValues = new Object[partitionColumnCount];
      VectorizedRowBatchCtx.getPartitionValues(rbCtx, job, split, partitionValues);
    } else {
      partitionValues = null;
    }

    isAcidScan = HiveConf.getBoolVar(jobConf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN);
    TypeDescription schema = OrcInputFormat.getDesiredRowTypeDescr(
        job, isAcidScan, Integer.MAX_VALUE);

    // Create the consumer of encoded data; it will coordinate decoding to CVBs.
    feedback = rp = cvp.createReadPipeline(this, split, columnIds, sarg, columnNames,
        counters, schema, sourceInputFormat, sourceSerDe, reporter, job,
        mapWork.getPathToPartitionInfo());
    evolution = rp.getSchemaEvolution();
    includedColumns = rp.getIncludedColumns();
  }

  /**
   * Starts the data read pipeline
   */
  public boolean init() {
    if (!checkOrcSchemaEvolution()) return false;

    // perform the data read asynchronously
    if (executor instanceof StatsRecordingThreadPool) {
      // Every thread created by this thread pool will use the same handler
      ((StatsRecordingThreadPool) executor).setUncaughtExceptionHandler(
          new IOUncaughtExceptionHandler());
    }
    executor.submit(rp.getReadCallable());
    return true;
  }

  private boolean checkOrcSchemaEvolution() {
    for (int i = 0; i < columnCount; ++i) {
      int projectedColId = columnIds == null ? i : columnIds.get(i);
      // Adjust file column index for ORC struct.
      // LLAP IO does not support ACID. When it supports, this would be auto adjusted.
      int fileColId =  OrcInputFormat.getRootColumn(!isAcidScan) + projectedColId + 1;
      if (!evolution.isPPDSafeConversion(fileColId)) {
        LlapIoImpl.LOG.warn("Unsupported schema evolution! Disabling Llap IO for {}", split);
        return false;
      }
    }
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
    if (columnCount != cvb.cols.length) {
      throw new RuntimeException("Unexpected number of columns, VRB has " + columnCount
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

  public VectorizedRowBatchCtx getVectorizedRowBatchCtx() {
    return rbCtx;
  }

  private final class IOUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
      LlapIoImpl.LOG.error("Unhandled error from reader thread. threadName: {} threadId: {}" +
          " Message: {}", t.getName(), t.getId(), e.getMessage());
      setError(e);
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
    return rbCtx.createVectorizedRowBatch();
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
    MDC.clear();
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
    if (LlapIoImpl.LOG.isDebugEnabled()) {
      LlapIoImpl.LOG.debug("setDone called; closed {}, done {}, err {}, pending {}",
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
