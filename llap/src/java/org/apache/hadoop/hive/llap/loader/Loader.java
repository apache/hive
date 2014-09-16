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


package org.apache.hadoop.hive.llap.loader;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.api.Llap;
import org.apache.hadoop.hive.llap.api.Vector;
import org.apache.hadoop.hive.llap.api.impl.RequestImpl;
import org.apache.hadoop.hive.llap.api.impl.VectorImpl;
import org.apache.hadoop.hive.llap.cache.BufferPool;
import org.apache.hadoop.hive.llap.cache.BufferPool.WeakBuffer;
import org.apache.hadoop.hive.llap.chunk.ChunkWriterImpl;
import org.apache.hadoop.hive.llap.loader.ChunkPool.Chunk;
import org.apache.hadoop.hive.llap.processor.ChunkConsumer;
import org.apache.hadoop.hive.llap.processor.ChunkProducerFeedback;

// TODO: write unit tests if this class becomes less primitive.
public abstract class Loader {
  // For now, we have one buffer pool. Add bufferpool per load when needed.
  private final BufferPool bufferPool;
  private final ConcurrentLinkedQueue<BufferInProgress> reusableBuffers =
      new ConcurrentLinkedQueue<BufferInProgress>();
  protected final ChunkWriterImpl writer;


  public Loader(BufferPool bufferPool) {
    this.bufferPool = bufferPool;
    this.writer = new ChunkWriterImpl();
  }

  protected class LoadContext implements ChunkProducerFeedback {
    public volatile boolean isStopped = false;

    @Override
    public void returnCompleteVector(Vector vector) {
      Loader.this.returnCompleteVector(vector);
    }

    @Override
    public void stop() {
      isStopped = true;
    }
  }

  public final void load(RequestImpl request, ChunkConsumer consumer)
      throws IOException, InterruptedException {
    // TODO: this API is subject to change, just a placeholder. Ideally we want to refactor
    //       so that working with cache and buffer allocation/locking would be here, but right
    //       now it depends on OrcLoader (esp. locking is hard to pull out).
    LoadContext context = new LoadContext();
    consumer.init(context); // passed as ChunkProducerFeedback
    loadInternal(request, consumer, context);
  }

  private void returnCompleteVector(Vector vector) {
    VectorImpl vectorImpl = (VectorImpl)vector;
    for (BufferPool.WeakBuffer buffer : vectorImpl.getCacheBuffers()) {
      if (DebugUtils.isTraceLockingEnabled()) {
        Llap.LOG.info("Unlocking " + buffer + " because reader is done");
      }
      buffer.unlock();
    }
  }

  // TODO: this API is subject to change, just a placeholder.
  protected abstract void loadInternal(RequestImpl request, ChunkConsumer consumer,
      LoadContext context) throws IOException, InterruptedException;

  protected final BufferInProgress prepareReusableBuffer(
      HashSet<WeakBuffer> resultBuffers) throws InterruptedException {
    while (true) {
      BufferInProgress buf = reusableBuffers.poll();
      if (buf == null) {
        WeakBuffer newWb = bufferPool.allocateBuffer();
        if (!resultBuffers.add(newWb)) {
          throw new AssertionError("Cannot add new buffer to resultBuffers");
        }
        return new BufferInProgress(newWb);
      }
      if (resultBuffers.add(buf.buffer)) {
        if (!buf.buffer.lock(true)) {
          resultBuffers.remove(buf.buffer);
          continue;  // Buffer was evicted.
        }
        if (DebugUtils.isTraceLockingEnabled()) {
          Llap.LOG.info("Locked " + buf.buffer + " due to reuse");
        }
      } else if (!buf.buffer.isLocked()) {
        throw new AssertionError(buf.buffer + " is in resultBuffers, but is not locked");
      }
    }
  }

  protected final void returnReusableBuffer(BufferInProgress colBuffer) {
    // Check space - 16 is chunk header plus one segment header, minimum required space.
    // This should be extremely rare.
    // TODO: use different value that makes some sense
    // TODO: with large enough stripes it might be better not to split every stripe into two
    //       buffers but instead not reuse the buffer if e.g. 1Mb/15Mb is left.
    if (colBuffer.getSpaceLeft() < 16) return;
    reusableBuffers.add(colBuffer);
  }

  protected Chunk mergeResultChunks(BufferInProgress colBuffer,
      Chunk existingResult, boolean finalCheck) throws IOException {
    // Both should be extracted in one method, but it's too painful to do in Java.
    int rowCount = colBuffer.getChunkInProgressRows();
    Chunk chunk = colBuffer.extractChunk();
    if (rowCount <= 0) {
      if (finalCheck && existingResult == null) {
        throw new IOException("No rows written for column");
      }
      return existingResult;
    }
    writer.finishChunk(chunk, rowCount);
    return (existingResult == null) ? chunk : existingResult.addChunk(chunk);
  }
}
