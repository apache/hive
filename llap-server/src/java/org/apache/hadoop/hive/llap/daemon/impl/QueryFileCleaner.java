/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.CallableWithNdc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryFileCleaner extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(QueryFileCleaner.class);

  private final ListeningScheduledExecutorService executorService;
  private final FileSystem localFs;


  public QueryFileCleaner(Configuration conf, FileSystem localFs) {
    super(QueryFileCleaner.class.getName());
    int numCleanerThreads = conf.getInt(LlapConfiguration.LLAP_DAEMON_NUM_FILE_CLEANER_THREADS,
        LlapConfiguration.LLAP_DAEMON_NUM_FILE_CLEANER_THREADS_DEFAULT);
    ScheduledExecutorService rawExecutor = Executors.newScheduledThreadPool(numCleanerThreads,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("QueryFileCleaner %d").build());
    this.executorService = MoreExecutors.listeningDecorator(rawExecutor);
    this.localFs = localFs;
  }

  public void serviceStart() {
    LOG.info(getName() + " started");
  }

  @Override
  public void serviceStop() {
    executorService.shutdownNow();
    LOG.info(getName() + " stopped");
  }

  public void cleanupDir(String dir, long deleteDelay) {
    LOG.info("Scheduling deletion of {} after {} seconds", dir, deleteDelay);
    executorService.schedule(new FileCleanerCallable(dir), deleteDelay, TimeUnit.SECONDS);
  }

  private class FileCleanerCallable extends CallableWithNdc<Void> {

    private final String dirToDelete;

    private FileCleanerCallable(String dirToDelete) {
      this.dirToDelete = dirToDelete;
    }

    @Override
    protected Void callInternal() {
      Path pathToDelete = new Path(dirToDelete);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting path: " + pathToDelete);
      }
      try {
        localFs.delete(new Path(dirToDelete), true);
      } catch (IOException e) {
        LOG.warn("Ignoring exception while cleaning up path: " + pathToDelete, e);
      }
      return null;
    }
  }
}
