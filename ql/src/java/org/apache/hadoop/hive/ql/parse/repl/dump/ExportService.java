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

package org.apache.hadoop.hive.ql.parse.repl.dump;

import com.cronutils.utils.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A service to dump/export table and partition in parallel manner using asynchronous threads
 */
public class ExportService {

  /**
   * Executor service to execute runnable export jobs
   */
  private ExecutorService execService;
  private static final Logger LOG = LoggerFactory.getLogger(ExportService.class);
  private final List<Future<?>> futures = new LinkedList<>();
  private final AtomicBoolean isServiceRunning =  new AtomicBoolean(false);;
  private final int threadPoolSizeLimit = 100;

  /**
   * Create and configure the ExportService. Create a fixed thread pool of size
   * provided by config parameter REPL_TABLE_DUMP_PARALLELISM.
   * @param hiveConf
   */
  public ExportService(HiveConf hiveConf) {
    int nDumpThreads = hiveConf.getIntVar(HiveConf.ConfVars.REPL_TABLE_DUMP_PARALLELISM);
    if (nDumpThreads == 0) {
      LOG.warn("ExportService is disabled since thread pool size (REPL_TABLE_DUMP_PARALLELISM) is specified as 0");
      isServiceRunning.set(false);
      return;
    }
    if (nDumpThreads > threadPoolSizeLimit) {
      LOG.warn("Thread pool size for ExportService (REPL_TABLE_DUMP_PARALLELISM) is specified higher than limit. " +
              "Choosing thread pool size as 100");
      nDumpThreads = threadPoolSizeLimit;
    }
    ThreadFactory namingThreadFactory = new ThreadFactoryBuilder().setNameFormat("TableAndPartition-dump-thread-%d").build();
    execService = Executors.newFixedThreadPool(nDumpThreads, namingThreadFactory);
    LOG.info("ExportService started with thread pool size {} ", nDumpThreads);
    isServiceRunning.set(true);
  }

  /**
   * Tells whether ExportService is started and configured.
   * If it is shutdown return false else return true
   * @return false/true
   */
  public boolean isExportServiceRunning() {
    return isServiceRunning.get();
  }

  /**
   * Executes the submitted ExportJob
   * @param job
   */
  public void submit(ExportJob job) {
    assert (execService != null);
    futures.add(execService.submit(job));
  }

  /**
   * Wait for termination of all submitted ExportJob to the ExportService.
   * @param timeout indicates time to wait until all tasks complete execution
   * @param unit time unit for wait timeout
   * @return true if executor terminated and false if timeout elapsed
   * @throws InterruptedException
   */
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    assert (execService != null);
    LOG.debug("ExportService : awaiting termination of submitted jobs");
    return execService.awaitTermination(timeout, unit);
  }

  /**
   * Wait for all tasks submitted to ExecutorService threads to complete execution.
   * @throws HiveException
   */
  public void waitForTasksToFinishAndShutdown() throws HiveException {
   for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("ExportService thread failed to perform task ", e.getCause());
        // If not removed task remains as stale task and further table dump operation fails
        // with similar exception
        futures.remove(future);
        throw new HiveException(e.getCause().getMessage(), e.getCause());
      }
   }
   assert (execService != null);
   LOG.debug("ExportService got shutdown");
   execService.shutdown();
   isServiceRunning.set(false);
  }

  /**
   * Only for testing purpose. It returns how many number of tasks were executed
   * in parallel manner at an instant by ExportService.
   * @return Returns total number of executed tasks in parallel at an instant by ExportService.
   */
  @VisibleForTesting
  public long getTotalTaskEverExecuted() {
    assert (execService != null);
    ThreadPoolExecutor executor = (ThreadPoolExecutor) execService;
    return executor.getTaskCount();
  }
}
