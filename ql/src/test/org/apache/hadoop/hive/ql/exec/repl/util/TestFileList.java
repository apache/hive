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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tests the File List implementation.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class})
public class TestFileList {

  private FSDataOutputStream outStream;
  private Exception testException = new IOException("test");

  @Test
  public void testConcurrentAdd() throws Exception {
    FileList fileList = setupFileList();
    int numOfEntries = 1000;
    int numOfThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);

    for (int i=1; i<=numOfEntries; i++) {
      executorService.submit(() -> {
        try {
          fileList.add("someEntry");
        } catch (IOException e) {
          throw new RuntimeException("Unbale to add to file list.");
        }
      });
    }
    executorService.awaitTermination(1, TimeUnit.MINUTES);
    fileList.close();
    ArgumentCaptor<String> entryArgs = ArgumentCaptor.forClass(String.class);
    Mockito.verify(outStream, Mockito.times(numOfEntries)).writeBytes(entryArgs.capture());
  }

  @Test
  public void testWriteRetryCreateFailure() throws Exception {
    String testEntry = "someEntry";
    boolean retryOnCreate = true;
    FileList fileList = setupFileList(retryOnCreate);
    final String retryExhaustedMsg = ErrorMsg.REPL_RETRY_EXHAUSTED
            .format(testException.getMessage());

    try {
      fileList.add(testEntry);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(retryExhaustedMsg));
    }

    //the create keeps failing, so create should be called at least twice,
    //writes and appends do not happen
    Mockito.verify(fileList, Mockito.atLeast(2)).getWriterCreateMode();
    Mockito.verify(fileList, Mockito.times(0)).getWriterAppendMode();
  }

  @Test
  public void testWriteIntermediateRetry() throws Exception {
    String testEntry = "someEntry";
    boolean retryOnCreate = false; // create passes, write operation keeps failing
    FileList fileList = setupFileList(retryOnCreate);
    final String retryExhaustedMsg = ErrorMsg.REPL_RETRY_EXHAUSTED
            .format(testException.getMessage());

    try{
      fileList.add(testEntry);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(retryExhaustedMsg));
    }

    //file-creation done once
    Mockito.verify(fileList, Mockito.times(1)).getWriterCreateMode();
    //writes fail after creation, subsequent attempts should only call append, verify 2 such attempts
    Mockito.verify(fileList, Mockito.atLeast(2)).getWriterAppendMode();
    Mockito.verify(outStream, Mockito.atLeast(2)).writeBytes(Mockito.anyString());
  }

  private FileList setupFileList(boolean... testRetryOnCreate) throws Exception {
    HiveConf hiveConf = Mockito.mock(HiveConf.class);
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Path backingFile = Mockito.spy(new Path("/tmp/backingFile"));
    FileList fileList = Mockito.spy(new FileList(backingFile, hiveConf));
    outStream = Mockito.spy(new FSDataOutputStream(null, null));
    Mockito.doReturn(true).when(hiveConf).getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);

    if(testRetryOnCreate.length == 0) {
      //setup for normal flow, without failures
      Mockito.doReturn(outStream).when(fileList).initWriter();
    }
    else {
      //setup for retries
      Retryable retryable = Retryable.builder()
              .withTotalDuration(30)
              .withInitialDelay(1)
              .withBackoff(1.0)
              .withRetryOnException(IOException.class).build();
      Mockito.doReturn(retryable)
              .when(fileList).buildRetryable();
      Mockito.doReturn(mockFs)
              .when(backingFile).getFileSystem(hiveConf);

      if(testRetryOnCreate[0]) {
        //setup for retry because of create-failure
        Mockito.doReturn(false)
                .when(mockFs).exists(backingFile);
        Mockito.doThrow(testException)
                .when(fileList).getWriterCreateMode();
      }
      else {
        //setup for retry because of failure during writes
        Mockito.when(mockFs.exists(backingFile))
                .thenReturn(false)
                .thenReturn(true);
        Mockito.doReturn(outStream)
                .when(fileList).getWriterAppendMode();
        Mockito.doReturn(outStream)
                .when(fileList).getWriterCreateMode();
        Mockito.doThrow(testException)
                .when(outStream).writeBytes(Mockito.anyString());
      }
    }
    return fileList;
  }
}
