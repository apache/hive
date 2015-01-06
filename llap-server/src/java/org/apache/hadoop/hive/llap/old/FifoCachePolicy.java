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

package org.apache.hadoop.hive.llap.old;

import java.util.Iterator;
import java.util.LinkedHashSet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hive.llap.old.BufferPool.WeakBuffer;

public class FifoCachePolicy implements CachePolicy {
  private final Lock lock = new ReentrantLock();
  private final LinkedHashSet<WeakBuffer> buffers;
  private final int maxBuffers;

  public FifoCachePolicy(int bufferSize, long maxCacheSize) {
    maxBuffers = (int)Math.ceil((maxCacheSize * 1.0) / bufferSize);
    buffers = new LinkedHashSet<BufferPool.WeakBuffer>((int)(maxBuffers / 0.75f));
  }

  @Override
  public WeakBuffer cache(WeakBuffer buffer) {
    WeakBuffer result = null;
    lock.lock();
    try {
      if (buffers.size() == maxBuffers) {
        Iterator<WeakBuffer> iter = buffers.iterator();
        while (iter.hasNext()) {
          WeakBuffer candidate = iter.next();
          if (candidate.invalidate()) {
            iter.remove();
            break;
          }
          result = candidate;
        }
        if (result == null) return CANNOT_EVICT;
      }
      buffers.add(buffer);
    } finally {
      lock.unlock();
    }
    return result;
  }

  @Override
  public void notifyLock(WeakBuffer buffer) {
    // FIFO policy doesn't care.
  }

  @Override
  public void notifyUnlock(WeakBuffer buffer) {
    // FIFO policy doesn't care.
  }
}
