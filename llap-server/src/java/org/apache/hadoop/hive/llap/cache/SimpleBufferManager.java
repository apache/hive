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
package org.apache.hadoop.hive.llap.cache;

import java.util.List;

import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;

public class SimpleBufferManager implements BufferUsageManager {
  private final Allocator allocator;
  private final LlapDaemonCacheMetrics metrics;

  public SimpleBufferManager(Allocator allocator, LlapDaemonCacheMetrics metrics) {
    if (LlapIoImpl.LOGL.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Simple buffer manager");
    }
    this.allocator = allocator;
    this.metrics = metrics;
  }

  private boolean lockBuffer(LlapDataBuffer buffer) {
    int rc = buffer.incRef();
    if (rc <= 0) return false;
    metrics.incrCacheNumLockedBuffers();
    return true;
  }

  private void unlockBuffer(LlapDataBuffer buffer) {
    if (buffer.decRef() == 0) {
      if (DebugUtils.isTraceCachingEnabled()) {
        LlapIoImpl.LOG.info("Deallocating " + buffer + " that was not cached");
      }
      allocator.deallocate(buffer);
    }
    metrics.decrCacheNumLockedBuffers();
  }

  @Override
  public void decRefBuffer(MemoryBuffer buffer) {
    unlockBuffer((LlapDataBuffer)buffer);
  }

  @Override
  public void decRefBuffers(List<MemoryBuffer> cacheBuffers) {
    for (MemoryBuffer b : cacheBuffers) {
      unlockBuffer((LlapDataBuffer)b);
    }
  }

  @Override
  public boolean incRefBuffer(MemoryBuffer buffer) {
    return lockBuffer((LlapDataBuffer)buffer);
  }

  @Override
  public Allocator getAllocator() {
    return allocator;
  }
}
