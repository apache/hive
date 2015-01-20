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
import org.apache.hadoop.hive.llap.io.api.VectorReader.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.VectorReader;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.InputSplit;

public class VectorReaderImpl implements VectorReader, Consumer<ColumnVectorBatch> {
  private final InputSplit split;
  private final List<Integer> columnIds;
  private final SearchArgument sarg;
  private final ColumnVectorProducer<?> cvp;

  private final LinkedList<ColumnVectorBatch> pendingData = new LinkedList<ColumnVectorBatch>();
  private Throwable pendingError = null;
  /** Vector that is currently being processed by our user. */
  private boolean isDone = false, isClosed = false;
  private ConsumerFeedback<ColumnVectorBatch> feedback;

  public VectorReaderImpl(InputSplit split, List<Integer> columnIds, SearchArgument sarg,
      ColumnVectorProducer<?> cvp) {
    this.split = split;
    this.columnIds = columnIds;
    this.sarg = sarg;
    this.cvp = cvp;
  }

  @Override
  public ColumnVectorBatch next() throws InterruptedException, IOException {
    // TODO: if some collection is needed, return previous ColumnVectorBatch here
    ColumnVectorBatch current = null;
    if (feedback == null) {
      feedback = cvp.read(split, columnIds, sarg, this);
    }
    if (isClosed) {
      throw new AssertionError("next called after close");
    }
    synchronized (pendingData) {
      // We are waiting for next block. Either we will get it, or be told we are done.
      boolean willBlock = false;
      if (DebugUtils.isTraceMttEnabled()) {
        willBlock = !isDone && pendingData.isEmpty() && pendingError == null;
        if (willBlock) {
          // TODO: separate log objects
          LlapIoImpl.LOG.info("next will block");
        }
      }
      while (!isDone && pendingData.isEmpty() && pendingError == null) {
        pendingData.wait(100);
      }
      if (willBlock) {
        LlapIoImpl.LOG.info("next is unblocked");
      }
      if (pendingError != null) {
        rethrowErrorIfAny();
      }
      current = pendingData.poll();
    }
    if (DebugUtils.isTraceMttEnabled() && current != null) {
      LlapIoImpl.LOG.info("Processing will receive vector " + current);
    }
    return current;
  }

  private void rethrowErrorIfAny() throws IOException {
    if (pendingError == null) return;
    if (pendingError instanceof IOException) {
      throw (IOException)pendingError;
    }
    throw new IOException(pendingError);
  }

  @Override
  public void close() throws IOException {
    if (DebugUtils.isTraceEnabled()) {
      LlapIoImpl.LOG.info("close called; closed " + isClosed + ", done " + isDone
          + ", err " + pendingError + ", pending " + pendingData.size());
    }
    feedback.stop();
    rethrowErrorIfAny();
  }

  @Override
  public void setDone() {
    if (DebugUtils.isTraceEnabled()) {
      LlapIoImpl.LOG.info("setDone called; cclosed " + isClosed
        + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
    }
    synchronized (pendingData) {
      isDone = true;
      pendingData.notifyAll();
    }
  }

  @Override
  public void consumeData(ColumnVectorBatch data) {
    if (DebugUtils.isTraceEnabled()) {
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
    if (DebugUtils.isTraceEnabled()) {
      LlapIoImpl.LOG.info("setError called; closed " + isClosed
        + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
    }
    assert t != null;
    synchronized (pendingData) {
      pendingError = t;
      pendingData.notifyAll();
    }
  }
}
