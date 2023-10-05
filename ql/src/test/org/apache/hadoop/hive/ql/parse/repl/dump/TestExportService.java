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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.After;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestExportService {
  protected static final Logger LOG = LoggerFactory.getLogger(TestExportService.class);
  @Mock
  HiveConf conf;
  private final int nThreads = 50;
  private static int taskNumber = 0;
  private final int totalTask = 50;
  private Semaphore sem;
  private ExportService exportService;

  @After
  public void finalize()
  {
    for (int i = 0; i < totalTask; i++) {
      sem.release();
    }
  }

  private ExportJob runParallelTask() {
    return new ExportJob() {
      @Override
      public void run() {
        Assert.assertTrue(sem.tryAcquire());
        ++taskNumber;
        LOG.debug("Current task number is: {} and thread is: {} ", taskNumber, Thread.currentThread().getName());
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private void configureAndSubmitTasks() throws HiveException {
    when(conf.getIntVar(HiveConf.ConfVars.REPL_TABLE_DUMP_PARALLELISM)).thenReturn(nThreads);
    exportService = new ExportService(conf);
    taskNumber = 0;
    sem = new Semaphore(totalTask);
    for (int i = 0; i < totalTask; i++) {
      exportService.submit(runParallelTask());
    }
    exportService.waitForTasksToFinishAndShutdown();
  }

  @Test
  public void testExportServiceWithParallelism() throws Exception {
    configureAndSubmitTasks();
    final long actualNumTasksExecuted = exportService.getTotalTaskEverExecuted();
    Assert.assertEquals(totalTask, actualNumTasksExecuted);
    Assert.assertTrue(exportService.await(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
  }

}
