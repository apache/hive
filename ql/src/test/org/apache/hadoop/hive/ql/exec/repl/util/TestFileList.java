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

import java.io.BufferedWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class})
public class TestFileList {

  @Mock
  private HiveConf hiveConf;

  @Mock
  BufferedWriter bufferedWriter;

  @Test
  public void testNoStreaming() throws Exception {
    Mockito.when(hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DATA_COPY_LAZY)).thenReturn(false);
    Path backingFile  = new Path("/tmp/backingFile");
    FileList fileList = new FileList(backingFile, 100, hiveConf);
    fileList.add("Entry1");
    fileList.add("Entry2");
    assertFalse(fileList.isStreamingToFile());
  }

  @Test
  public void testAlwaysStreaming() throws Exception {
    Mockito.when(hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DATA_COPY_LAZY)).thenReturn(true);
    FileListStreamer.setBackingFileWriterInTest(bufferedWriter);
    Path backingFile  = new Path("/tmp/backingFile");
    FileList fileList = new FileList(backingFile, 100, hiveConf);
    assertFalse(fileList.isStreamingInitialized());
    fileList.add("Entry1");
    waitForStreamingInitialization(fileList);
    assertTrue(fileList.isStreamingToFile());
    fileList.close();
    waitForStreamingClosure(fileList);
  }

  @Test
  public void testStreaminOnCacheHit() throws Exception {
    Mockito.when(hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DATA_COPY_LAZY)).thenReturn(false);
    FileListStreamer.setBackingFileWriterInTest(bufferedWriter);
    Path backingFile  = new Path("/tmp/backingFile");
    FileList fileList = new FileList(backingFile, 5, hiveConf);
    fileList.add("Entry1");
    fileList.add("Entry2");
    fileList.add("Entry3");
    Thread.sleep(5000L);
    assertFalse(fileList.isStreamingInitialized());
    fileList.add("Entry4");
    fileList.add("Entry5");
    waitForStreamingInitialization(fileList);
    fileList.close();
    waitForStreamingClosure(fileList);
  }

  @Test
  public void testConcurrentAdd() throws Exception {
    Mockito.when(hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DATA_COPY_LAZY)).thenReturn(false);
    FileListStreamer.setBackingFileWriterInTest(bufferedWriter);
    Path backingFile  = new Path("/tmp/backingFile");
    FileList fileList = new FileList(backingFile, 100, hiveConf);
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
    waitForStreamingInitialization(fileList);
    fileList.close();
    waitForStreamingClosure(fileList);
    ArgumentCaptor<String> entryArgs = ArgumentCaptor.forClass(String.class);
    Mockito.verify(bufferedWriter, Mockito.times(numOfEntries)).write(entryArgs.capture());
  }

  private void waitForStreamingInitialization(FileList fileList) throws InterruptedException {
    long sleepTime = 1000L;
    int iter = 0;
    while (!fileList.isStreamingInitialized()) {
      Thread.sleep(sleepTime);
      iter++;
      if (iter == 5) {
        throw new IllegalStateException("File Streamer not initialized till 5s.");
      }
    }
  }

  private void waitForStreamingClosure(FileList fileList) throws InterruptedException {
    long sleepTime = 1000L;
    int iter = 0;
    while (!fileList.isStreamingClosedProperly()) {
      Thread.sleep(sleepTime);
      iter++;
      if (iter == 5) {
        throw new IllegalStateException("File Streamer not getting closed till 5s.");
      }
    }
  }
}
