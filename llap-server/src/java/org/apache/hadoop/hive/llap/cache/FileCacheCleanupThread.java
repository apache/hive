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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hive.common.util.Ref;

/** Class used to slowly clean up a map of FileCache-s. */
abstract class FileCacheCleanupThread<T> extends Thread {
  private final long approxCleanupIntervalSec;
  private final AtomicInteger newEvictions;
  private final ConcurrentHashMap<Object, FileCache<T>> fileMap;

  public FileCacheCleanupThread(String name, ConcurrentHashMap<Object, FileCache<T>> fileMap,
      AtomicInteger newEvictions, long cleanupInterval) {
    super(name);
    this.fileMap = fileMap;
    this.newEvictions = newEvictions;
    this.approxCleanupIntervalSec = cleanupInterval;
    setDaemon(true);
    setPriority(1);
  }

  @Override
  public void run() {
    while (true) {
      try {
        doOneCleanupRound();
      } catch (InterruptedException ex) {
        LlapIoImpl.LOG.warn("Cleanup thread has been interrupted");
        Thread.currentThread().interrupt();
        break;
      } catch (Throwable t) {
        LlapIoImpl.LOG.error("Cleanup has failed; the thread will now exit", t);
        break;
      }
    }
  }

  private void doOneCleanupRound() throws InterruptedException {
    while (true) {
      int evictionsSinceLast = newEvictions.getAndSet(0);
      if (evictionsSinceLast > 0) break;
      synchronized (newEvictions) {
        newEvictions.wait(10000);
      }
    }
    // Duration is an estimate; if the size of the map changes, it can be very different.
    long endTime = System.nanoTime() + approxCleanupIntervalSec * 1000000000L;
    int leftToCheck = 0; // approximate
    for (FileCache<T> fc : fileMap.values()) {
      leftToCheck += getCacheSize(fc);
    }
    // Iterate thru all the filecaches. This is best-effort.
    // If these super-long-lived iterators affect the map in some bad way,
    // we'd need to sleep once per round instead.
    Iterator<Map.Entry<Object, FileCache<T>>> iter = fileMap.entrySet().iterator();
    Ref<Boolean> isPastEndTime = Ref.from(false);
    while (iter.hasNext()) {
      FileCache<T> fc = iter.next().getValue();
      if (!fc.incRef()) {
        throw new AssertionError("Something other than cleanup is removing elements from map");
      }
      leftToCheck = cleanUpOneFileCache(fc, leftToCheck, endTime, isPastEndTime);
      if (getCacheSize(fc) > 0) {
        fc.decRef();
        continue;
      }
      // FileCache might be empty; see if we can remove it. "tryWriteLock"
      if (!fc.startEvicting()) continue;
      if (getCacheSize(fc) == 0) {
        fc.commitEvicting();
        iter.remove();
      } else {
        fc.abortEvicting();
      }
    }
  }

  protected abstract int getCacheSize(FileCache<T> fc);

  protected abstract int cleanUpOneFileCache(FileCache<T> fc, int leftToCheck, long endTime,
      Ref<Boolean> isPastEndTime) throws InterruptedException;
}