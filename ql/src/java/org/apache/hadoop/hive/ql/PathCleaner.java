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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is used to asynchronously remove directories after query execution
 */
public class PathCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(PathCleaner.class.getName());
  private static final AsyncDeleteAction END_OF_PROCESS = new AsyncDeleteAction(null, null);

  private final BlockingDeque<AsyncDeleteAction> deleteActions = new LinkedBlockingDeque<>();
  private final AtomicBoolean isRunning = new AtomicBoolean(true);
  private final Thread cleanupThread;

  public PathCleaner(String name) {
    cleanupThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (isRunning.get() || deleteActions.size() > 0) {
          try {
            AsyncDeleteAction deleteAction = deleteActions.take();
            if (deleteAction != END_OF_PROCESS) {
              FileSystem fs;
              Path path = null;
              try {
                fs = deleteAction.fileSystem;
                path = deleteAction.path;
                fs.delete(path, true);
                fs.cancelDeleteOnExit(path);
                LOG.debug("Deleted {}", path);
              } catch (IOException e) {
                LOG.warn("Error removing path {}: {}", path, e);
              }
            }
          } catch (InterruptedException e) {
            LOG.warn("PathCleaner was interrupted. Shutting down");
            break;
          }
        }

        LOG.info("PathCleaner shutdown");
      }
    });
    cleanupThread.setName(name + "_PathCleaner");
  }

  /**
   * Adds given path to the queue of delete operations. These delete
   * operations are executed eventually.
   * @param path
   * @param fileSystem
   */
  public void deleteAsync(Path path, FileSystem fileSystem) {
    if (isRunning.get()) {
      deleteActions.add(new AsyncDeleteAction(path, fileSystem));
      LOG.debug("Delete {} operation was queued", path);
    } else {
      LOG.warn("Delete request for {} was ignored as PathCleaner is shutting down", path);
    }
  }

  public void shutdown() {
    isRunning.set(false);
    // signalling the thread so it will finish
    deleteActions.add(END_OF_PROCESS);
  }

  public void start() {
    cleanupThread.start();
  }

  // used only in test
  void awaitTermination(long timeoutMillis) throws InterruptedException {
    cleanupThread.join(timeoutMillis);
  }

  private static class AsyncDeleteAction {
    Path path;
    FileSystem fileSystem;

    public AsyncDeleteAction(Path path, FileSystem fileSystem) {
      this.path = path;
      this.fileSystem = fileSystem;
    }
  }
}
