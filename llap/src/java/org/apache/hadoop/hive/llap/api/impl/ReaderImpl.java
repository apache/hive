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


package org.apache.hadoop.hive.llap.api.impl;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.api.Llap;
import org.apache.hadoop.hive.llap.api.Reader;
import org.apache.hadoop.hive.llap.api.Vector;
import org.apache.hadoop.hive.llap.cache.BufferPool.WeakBuffer;
import org.apache.hadoop.hive.llap.processor.ChunkConsumer;
import org.apache.hadoop.hive.llap.processor.ChunkProducerFeedback;

/**
 * Reader implementation is not thread safe.
 */
public class ReaderImpl implements Reader, ChunkConsumer {
  private ChunkProducerFeedback feedback;
  /**
   * Data that has been passed to use by the pipeline but has not been read.
   * We may add some throttling feedback later if needed when this grows too large.
   */
  private final LinkedList<Vector> pendingData = new LinkedList<Vector>();
  private Throwable pendingError = null;
  /** Vector that is currently being processed by our user. */
  private Vector current = null;
  private boolean isDone = false, isClosed = false;

  @Override
  public void init(ChunkProducerFeedback feedback) {
    this.feedback = feedback;
  }

  @Override
  public Vector next() throws InterruptedException, IOException {
    if (DebugUtils.isTraceEnabled()) {
      Llap.LOG.info("next called; current=" + current + ", closed " + isClosed
        + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
    }
    if (current != null) {
      // Indicate that we are done with this vector.
      if (DebugUtils.isTraceMttEnabled()) {
        Llap.LOG.info("Processing is done with vector " + getVectorDebugDesc(current));
      }
      feedback.returnCompleteVector(current);
      current = null;
    }
    synchronized (pendingData) {
      if (isClosed) {
        throw new AssertionError("next called after close");
      }
      // We are waiting for next block. Either we will get it, or be told we are done.
      boolean willBlock = false;
      if (DebugUtils.isTraceMttEnabled()) {
        willBlock = !isDone && pendingData.isEmpty() && pendingError == null;
        if (willBlock) {
          Llap.LOG.info("next will block");
        }
      }
      while (!isDone && pendingData.isEmpty() && pendingError == null) {
        pendingData.wait(100);
      }
      if (willBlock) {
        Llap.LOG.info("next is unblocked");
      }
      if (pendingError != null) {
        returnAllVectors();
        rethrowErrorIfAny();
      }
      current = pendingData.poll();
    }
    if (DebugUtils.isTraceMttEnabled() && current != null) {
      Llap.LOG.info("Processing will receive vector " + getVectorDebugDesc(current));
    }
    return current;
  }

  private static String getVectorDebugDesc(Vector vector) {
    String vectorDesc = "0x"
        + Integer.toHexString(System.identityHashCode(vector)) + " with buffers [";
    VectorImpl currentImpl = (VectorImpl)vector;
    boolean isFirst = true;
    for (WeakBuffer wb : currentImpl.getCacheBuffers()) {
      if (isFirst) {
        vectorDesc += ", ";
        isFirst = false;
      }
      vectorDesc += wb.toString();
    }
    return vectorDesc + "]";
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
      Llap.LOG.info("close called; current=" + current + ", closed " + isClosed
        + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
    }
    feedback.stop();
    returnAllVectors();
    rethrowErrorIfAny();
  }

  private void returnAllVectors() {
    // 1) We assume next is never called in parallel with close, so we can remove current.
    // 2) After stop, producer shouldn't call consumeChunk (but it may). We set isClosed and
    //    remove pending data under lock - either consume will discard the block, or we will.
    LinkedList<Vector> unused = new LinkedList<Vector>();
    if (current != null) {
      unused.add(current);
      current = null;
    }
    synchronized (pendingData) {
      isClosed = true;
      unused.addAll(pendingData);
      pendingData.clear();
    }
    for (Vector v : unused) {
      feedback.returnCompleteVector(v);
    }
  }

  @Override
  public void setDone() {
    if (DebugUtils.isTraceEnabled()) {
      Llap.LOG.info("setDone called; current=" + current + ", closed " + isClosed
        + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
    }
    synchronized (pendingData) {
      isDone = true;
      pendingData.notifyAll();
    }
  }

  @Override
  public void setError(Throwable t) {
    if (DebugUtils.isTraceEnabled()) {
      Llap.LOG.info("setError called; current=" + current + ", closed " + isClosed
        + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
    }
    assert t != null;
    synchronized (pendingData) {
      pendingError = t;
      pendingData.notifyAll();
    }
  }

  @Override
  public void consumeVector(Vector vector) {
    if (DebugUtils.isTraceEnabled()) {
      Llap.LOG.info("consumeVector called; current=" + current + ", closed " + isClosed
        + ", done " + isDone + ", err " + pendingError + ", pending " + pendingData.size());
    }
    synchronized (pendingData) {
      if (isClosed) {
        feedback.returnCompleteVector(vector);
        return;
      }
      pendingData.add(vector);
      pendingData.notifyAll();
    }
  }
}
