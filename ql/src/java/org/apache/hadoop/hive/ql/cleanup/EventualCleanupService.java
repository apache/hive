/*
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

package org.apache.hadoop.hive.ql.cleanup;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventualCleanupService implements CleanupService {
  private final int threadCount;
  private final int queueSize;
  private final ThreadFactory factory;
  private final Logger LOG = LoggerFactory.getLogger(EventualCleanupService.class.getName());
  private final AtomicBoolean isRunning = new AtomicBoolean(true);
  private final BlockingQueue<AsyncDeleteAction> deleteActions;
  private ExecutorService cleanerExecutorService;

  public EventualCleanupService(int threadCount, int queueSize) {
    if (queueSize < threadCount) {
      throw new IllegalArgumentException("Queue size should be greater or equal to thread count. Queue size: "
          + queueSize + ", thread count: " + threadCount);
    }
    this.factory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("EventualCleanupService thread %d").build();
    this.threadCount = threadCount;
    this.queueSize = queueSize;
    this.deleteActions = new LinkedBlockingQueue<>(queueSize);
  }

  @Override
  public synchronized void start() {
    if (cleanerExecutorService != null) {
      LOG.debug("EventualCleanupService is already running.");
      return;
    }
    cleanerExecutorService = Executors.newFixedThreadPool(threadCount, factory);
    for (int i = 0; i < threadCount; i++) {
      cleanerExecutorService.submit(new CleanupRunnable());
    }
    LOG.info("EventualCleanupService started with {} threads and queue of size {}", threadCount, queueSize);
  }

  @Override
  public boolean deleteRecursive(Path path, FileSystem fileSystem) {
    if (isRunning.get()) {
      if (deleteActions.offer(new AsyncDeleteAction(path, fileSystem))) {
        LOG.info("Delete {} operation was queued", path);
      } else {
        try {
          fileSystem.cancelDeleteOnExit(path);
          fileSystem.delete(path, true);
          LOG.info("Deleted {} synchronously as the async queue was full", path);
        } catch (IOException e) {
          LOG.warn("Error removing path {}: {}", path, e);
        }
      }

      return true;
    } else {
      LOG.warn("Delete request {} was ignored as cleanup service is shutting down", path);
      return false;
    }
  }

  @Override
  public void shutdown() {
    isRunning.set(false);
    cleanerExecutorService.shutdown();
  }

  @Override
  public void shutdownNow() {
    isRunning.set(false);
    cleanerExecutorService.shutdownNow();
  }

  @VisibleForTesting
  public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {
    return cleanerExecutorService.awaitTermination(timeout, timeUnit);
  }


  private static class AsyncDeleteAction {
    Path path;
    FileSystem fileSystem;

    public AsyncDeleteAction(Path path, FileSystem fileSystem) {
      this.path = path;
      this.fileSystem = fileSystem;
    }
  }

  private class CleanupRunnable implements Runnable {
    @Override
    public void run() {
      while (isRunning.get() || deleteActions.size() > 0) {
        try {
          AsyncDeleteAction deleteAction = deleteActions.poll(1, TimeUnit.MINUTES);
          if (deleteAction != null) {
            Path path = null;
            try {
              FileSystem fs = deleteAction.fileSystem;
              path = deleteAction.path;
              fs.delete(path, true);
              fs.cancelDeleteOnExit(path);
              LOG.info("Deleted {}", path);
            } catch (IOException e) {
              LOG.warn("Error removing path {}: {}", path, e);
            }
          }
        } catch (InterruptedException e) {
          LOG.debug("PathCleaner was interrupted");
        }
      }
      LOG.info("Cleanup thread shutdown shutdown");
    }
  }
}
