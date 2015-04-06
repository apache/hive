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
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.ReadPipeline;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class LlapInputFormat
  implements InputFormat<NullWritable, VectorizedRowBatch>, VectorizedInputFormatInterface {
  @SuppressWarnings("rawtypes")
  private final InputFormat sourceInputFormat;
  private final ColumnVectorProducer cvp;
  private final ListeningExecutorService executor;

  @SuppressWarnings("rawtypes")
  LlapInputFormat(InputFormat sourceInputFormat, ColumnVectorProducer cvp,
      ListeningExecutorService executor) {
    // TODO: right now, we do nothing with source input format, ORC-only in the first cut.
    //       We'd need to plumb it thru and use it to get data to cache/etc.
    assert sourceInputFormat instanceof OrcInputFormat;
    this.executor = executor;
    this.cvp = cvp;
    this.sourceInputFormat = sourceInputFormat;
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    boolean isVectorMode = Utilities.isVectorMode(job);
    if (!isVectorMode) {
      LlapIoImpl.LOG.error("No llap in non-vectorized mode");
      throw new UnsupportedOperationException("No llap in non-vectorized mode");
    }
    FileSplit fileSplit = (FileSplit)split;
    reporter.setStatus(fileSplit.toString());
    try {
      List<Integer> includedCols = ColumnProjectionUtils.isReadAllColumns(job)
          ? null : ColumnProjectionUtils.getReadColumnIDs(job);
      return new LlapRecordReader(job, fileSplit, includedCols);
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
    private final InputSplit split;
    private final List<Integer> columnIds;
    private final SearchArgument sarg;
    private final String[] columnNames;
    private final VectorizedRowBatchCtx rbCtx;

    private final LinkedList<ColumnVectorBatch> pendingData = new LinkedList<ColumnVectorBatch>();
    private boolean isFirst = true;

    private Throwable pendingError = null;
    /** Vector that is currently being processed by our user. */
    private boolean isDone = false, isClosed = false;
    private ConsumerFeedback<ColumnVectorBatch> feedback;
    private final QueryFragmentCounters counters;

    public LlapRecordReader(JobConf job, FileSplit split, List<Integer> includedCols) {
      this.split = split;
      this.columnIds = includedCols;
      this.sarg = SearchArgumentFactory.createFromConf(job);
      this.columnNames = ColumnProjectionUtils.getReadColumnNames(job);
      this.counters = new QueryFragmentCounters();
      try {
        rbCtx = new VectorizedRowBatchCtx();
        rbCtx.init(job, split);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      startRead();
    }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
      assert value != null;
      if (isClosed) {
        throw new AssertionError("next called after close");
      }
      // Add partition cols if necessary (see VectorizedOrcInputFormat for details).
      if (isFirst) {
        try {
          rbCtx.addPartitionColsToBatch(value);
        } catch (HiveException e) {
          throw new IOException(e);
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
      if (cvb == null) return false;
      if (columnIds.size() != cvb.cols.length) {
        throw new RuntimeException("Unexpected number of columns, VRB has " + columnIds.size()
            + " included, but the reader returned " + cvb.cols.length);
      }
      // VRB was created from VrbCtx, so we already have pre-allocated column vectors
      for (int i = 0; i < cvb.cols.length; ++i) {
        int columnId = columnIds.get(i);
        value.cols[columnId] = cvb.cols[i]; // TODO: reuse CV objects that are replaced
      }
      value.selectedInUse = false;
      value.size = cvb.size;
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

    private void startRead() {
      // Create the consumer of encoded data; it will coordinate decoding to CVBs.
      ReadPipeline rp = cvp.createReadPipeline(
          this, split, columnIds, sarg, columnNames, counters);
      feedback = rp;
      ListenableFuture<Void> future = executor.submit(rp.getReadCallable());
      // TODO: we should NOT do this thing with handler. Reader needs to do cleanup in most cases.
      Futures.addCallback(future, new UncaughtErrorHandler());
    }

    ColumnVectorBatch nextCvb() throws InterruptedException, IOException {
      // TODO: if some collection is needed, return previous ColumnVectorBatch here
      ColumnVectorBatch current = null;
      synchronized (pendingData) {
        // We are waiting for next block. Either we will get it, or be told we are done.
        boolean doLogBlocking = DebugUtils.isTraceMttEnabled() && isNothingToReport();
        if (doLogBlocking) {
          LlapIoImpl.LOG.info("next will block");
        }
        while (isNothingToReport()) {
          pendingData.wait(100);
        }
        if (doLogBlocking) {
          LlapIoImpl.LOG.info("next is unblocked");
        }
        rethrowErrorIfAny();
        current = pendingData.poll();
      }
      if (DebugUtils.isTraceMttEnabled() && current != null) {
        LlapIoImpl.LOG.info("Processing will receive vector " + current);
      }
      return current;
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
      try {
        return rbCtx.createVectorizedRowBatch();
      } catch (HiveException e) {
        throw new RuntimeException("Error creating a batch", e);
      }
    }

    @Override
    public long getPos() throws IOException {
      return -1; // Position doesn't make sense for async reader, chunk order is arbitrary.
    }

    @Override
    public void close() throws IOException {
      if (DebugUtils.isTraceMttEnabled()) {
        LlapIoImpl.LOG.info("close called; closed " + isClosed + ", done " + isDone
            + ", err " + pendingError + ", pending " + pendingData.size());
      }
      LlapIoImpl.LOG.info("QueryFragmentCounters: " + counters);
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
      if (DebugUtils.isTraceMttEnabled()) {
        LlapIoImpl.LOG.info("setDone called; closed " + isClosed
          + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
      }
      LlapIoImpl.LOG.info("DONE: QueryFragmentCounters: " + counters);
      synchronized (pendingData) {
        isDone = true;
        pendingData.notifyAll();
      }
    }

    @Override
    public void consumeData(ColumnVectorBatch data) {
      if (DebugUtils.isTraceMttEnabled()) {
        LlapIoImpl.LOG.info("consume called; closed " + isClosed + ", done " + isDone
            + ", err " + pendingError + ", pending " + pendingData.size());
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
      counters.incrCounter(QueryFragmentCounters.Counter.NUM_ERRORS);
      LlapIoImpl.LOG.info("setError called; closed " + isClosed
        + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
      assert t != null;
      LlapIoImpl.LOG.info("ERROR: QueryFragmentCounters: " + counters);
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
}
