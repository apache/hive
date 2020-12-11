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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Tests the File List implementation.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class})
public class TestFileList {

  @Mock
  private BufferedWriter bufferedWriter;


  @Test
  public void testNoStreaming() throws Exception {
    Object tuple[] =  setupAndGetTuple(100, false);
    FileList fileList = (FileList) tuple[0];
    FileListStreamer fileListStreamer = (FileListStreamer) tuple[1];
    fileList.add("Entry1");
    fileList.add("Entry2");
    assertFalse(isStreamingToFile(fileListStreamer));
  }

  @Test
  public void testAlwaysStreaming() throws Exception {
    Object tuple[] =  setupAndGetTuple(100, true);
    FileList fileList = (FileList) tuple[0];
    FileListStreamer fileListStreamer = (FileListStreamer) tuple[1];
    assertFalse(fileListStreamer.isInitialized());
    fileList.add("Entry1");
    waitForStreamingInitialization(fileListStreamer);
    assertTrue(isStreamingToFile(fileListStreamer));
    fileList.close();
    waitForStreamingClosure(fileListStreamer);
  }

  @Test
  public void testStreaminOnCacheHit() throws Exception {
    Object tuple[] =  setupAndGetTuple(5, false);
    FileList fileList = (FileList) tuple[0];
    FileListStreamer fileListStreamer = (FileListStreamer) tuple[1];
    fileList.add("Entry1");
    fileList.add("Entry2");
    fileList.add("Entry3");
    Thread.sleep(5000L);
    assertFalse(fileListStreamer.isInitialized());
    fileList.add("Entry4");
    fileList.add("Entry5");
    waitForStreamingInitialization(fileListStreamer);
    fileList.close();
    waitForStreamingClosure(fileListStreamer);
  }

  @Test
  public void testConcurrentAdd() throws Exception {
    Object tuple[] =  setupAndGetTuple(100, false);
    FileList fileList = (FileList) tuple[0];
    FileListStreamer fileListStreamer = (FileListStreamer) tuple[1];
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
    waitForStreamingInitialization(fileListStreamer);
    fileList.close();
    waitForStreamingClosure(fileListStreamer);
    ArgumentCaptor<String> entryArgs = ArgumentCaptor.forClass(String.class);
    Mockito.verify(bufferedWriter, Mockito.times(numOfEntries)).write(entryArgs.capture());
  }

  private void waitForStreamingInitialization(FileListStreamer fileListStreamer) throws InterruptedException {
    long sleepTime = 1000L;
    int iter = 0;
    while (!fileListStreamer.isInitialized()) {
      Thread.sleep(sleepTime);
      iter++;
      if (iter == 5) {
        throw new IllegalStateException("File Streamer not initialized till 5s.");
      }
    }
  }

  private void waitForStreamingClosure(FileListStreamer fileListStreamer) throws InterruptedException {
    long sleepTime = 1000L;
    int iter = 0;
    while (!isStreamingClosedProperly(fileListStreamer)) {
      Thread.sleep(sleepTime);
      iter++;
      if (iter == 5) {
        throw new IllegalStateException("File Streamer not getting closed till 5s.");
      }
    }
  }

  private Object[] setupAndGetTuple(int cacheSize, boolean lazyDataCopy) throws Exception {
    HiveConf hiveConf = Mockito.mock(HiveConf.class);
    Mockito.when(hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET)).thenReturn(lazyDataCopy);
    Path backingFile = new Path("/tmp/backingFile");
    LinkedBlockingQueue<String> cache =  new LinkedBlockingQueue<>(cacheSize);
    FileListStreamer fileListStreamer = Mockito.spy(new FileListStreamer(cache, backingFile, hiveConf));
    FileList fileList = new FileList(backingFile, fileListStreamer, cache, hiveConf);
    Mockito.doReturn(bufferedWriter).when(fileListStreamer).lazyInitWriter();
    Object[] tuple  = new Object[] {fileList, fileListStreamer};
    return tuple;
  }

  private boolean isStreamingToFile(FileListStreamer fileListStreamer) {
    return fileListStreamer.isInitialized() && fileListStreamer.isAlive();
  }

  private boolean isStreamingClosedProperly(FileListStreamer fileListStreamer) {
    return fileListStreamer.isInitialized() && !fileListStreamer.isAlive() && fileListStreamer.isValid();
  }
}
