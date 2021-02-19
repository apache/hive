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

package org.apache.hadoop.hive.ql.exec.repl.util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tests the File List implementation.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class})
public class TestFileList {

  @Mock
  private FSDataOutputStream outputStream;

  @Test
  public void testConcurrentAdd() throws Exception {
    FileList fileList = setupFileList(false);
    int numOfEntries = 1000;
    int numOfThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);

    for (int i=1; i<=numOfEntries; i++) {
      executorService.submit(() -> {
        try {
          fileList.add("someEntry");
        } catch (SemanticException e) {
          throw new RuntimeException("Unbale to add to file list.");
        }
      });
    }
    executorService.awaitTermination(1, TimeUnit.MINUTES);
    fileList.close();
    ArgumentCaptor<String> entryArgs = ArgumentCaptor.forClass(String.class);
    Mockito.verify(outputStream, Mockito.times(numOfEntries)).writeBytes(entryArgs.capture());
  }

  private FileList setupFileList(boolean lazyDataCopy) throws Exception {
    HiveConf hiveConf = Mockito.mock(HiveConf.class);
    Mockito.when(hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET)).thenReturn(lazyDataCopy);
    Path backingFile = new Path("/tmp/backingFile");
    FileList fileList = Mockito.spy(new FileList(backingFile, hiveConf));
    Mockito.doReturn(outputStream).when(fileList).initWriter();
    return fileList;
  }
}
