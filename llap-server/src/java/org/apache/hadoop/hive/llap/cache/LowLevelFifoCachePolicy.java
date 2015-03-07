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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;

public class LowLevelFifoCachePolicy implements LowLevelCachePolicy {
  private final Lock lock = new ReentrantLock();
  private final LinkedList<LlapCacheableBuffer> buffers;
  private EvictionListener evictionListener;

  public LowLevelFifoCachePolicy(Configuration conf) {
    if (LlapIoImpl.LOGL.isInfoEnabled()) {
      LlapIoImpl.LOG.info("FIFO cache policy");
    }
    buffers = new LinkedList<LlapCacheableBuffer>();
  }

  @Override
  public void cache(LlapCacheableBuffer buffer, Priority pri) {
    // Ignore priority.
    lock.lock();
    try {
      buffers.add(buffer);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void notifyLock(LlapCacheableBuffer buffer) {
    // FIFO policy doesn't care.
  }

  @Override
  public void notifyUnlock(LlapCacheableBuffer buffer) {
    // FIFO policy doesn't care.
  }

  @Override
  public void setEvictionListener(EvictionListener listener) {
    this.evictionListener = listener;
  }

  @Override
  public long evictSomeBlocks(long memoryToReserve) {
    long evicted = 0;
    lock.lock();
    try {
      Iterator<LlapCacheableBuffer> iter = buffers.iterator();
      while (evicted < memoryToReserve && iter.hasNext()) {
        LlapCacheableBuffer candidate = iter.next();
        if (candidate.invalidate()) {
          iter.remove();
          evicted += candidate.getMemoryUsage();
          evictionListener.notifyEvicted(candidate);
        }
      }
    } finally {
      lock.unlock();
    }
    return evicted;
  }
}
