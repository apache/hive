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
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tests the File List implementation.
 */

@RunWith(MockitoJUnitRunner.class)
public class TestFileList {

  HiveConf conf = new HiveConf();
  private FSDataOutputStream outStream;
  private FSDataOutputStream testFileStream;
  final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
          File.separator + TestFileList.class.getCanonicalName() + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
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
    Mockito.verify(testFileStream, Mockito.times(numOfEntries)).writeBytes(entryArgs.capture());
  }

  @Test
  public void testConcurrentAddWithAbort() throws Exception {
    FileList fileList = setupFileList(false, false, false);
    int numOfEntries = 1000;
    int numOfThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
    final String retryExhaustedMsg = ErrorMsg.REPL_RETRY_EXHAUSTED
            .format(testException.getMessage());

    for (int i=1; i<=numOfEntries; i++) {
      executorService.submit(() -> {
        try {
          fileList.add("someEntry");
        } catch (IOException e) {
          Assert.assertTrue(e.getMessage().contains(retryExhaustedMsg));
        }
      });
    }
    executorService.awaitTermination(1, TimeUnit.MINUTES);
    fileList.close();
    ArgumentCaptor<String> entryArgs = ArgumentCaptor.forClass(String.class);
    //retry exhausted should be encountered by the first thread, so the other threads do not write.
    Mockito.verify(outStream, Mockito.times(1)).writeBytes(entryArgs.capture());
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
  public void testWriteNoRetry() throws Exception {
    String testEntry = "someEntry";
    boolean retryOnCreate = false, retryOnWrite = false;
    FileList fileList = setupFileList(retryOnCreate, retryOnWrite);
    final String retryExhaustedMsg = ErrorMsg.REPL_RETRY_EXHAUSTED
            .format(testException.getMessage());

    try {
      fileList.add(testEntry);
    } catch (IOException e) {
      Assert.assertFalse(e.getMessage().contains(retryExhaustedMsg));
      Assert.assertTrue(e.getMessage().contains("test"));
    }

    //the first write fails and no retries are made
    Mockito.verify(fileList, Mockito.times(1)).getWriterCreateMode();
    Mockito.verify(outStream, Mockito.times(1)).writeBytes(Mockito.anyString());
    Mockito.verify(fileList, Mockito.times(0)).getWriterAppendMode();
  }

  @Test
  public void testReadWithDuplicateEntries() throws Exception {
    conf = new HiveConf();
    String testEntry = "someEntry";
    int numUniqueEntries = 100;
    Path testFilePath =  new Path(new Path(TEST_DATA_DIR), "testFile");
    FileList fileList = new FileList(testFilePath, conf);

    for (int i = 1; i <= numUniqueEntries; i++) {
      String currentUniqueEntry = testEntry + i;
      for (int duplicateFactor = 0; duplicateFactor < i; duplicateFactor++) {
        fileList.add(currentUniqueEntry);
      }
    }
    fileList.close();

    int currentCount = 1;
    while (fileList.hasNext()) {
      String entry = fileList.next();
      Assert.assertEquals(testEntry + currentCount, entry);
      currentCount++;
    }
    Assert.assertEquals(currentCount - 1, numUniqueEntries);
  }

  @Test
  public void testReadWithAllDistinctEntries() throws Exception {
    conf = new HiveConf();
    String testEntry = "someEntry";
    int numUniqueEntries = 100;
    Path testFilePath =  new Path(new Path(TEST_DATA_DIR), "testFile");
    FileList fileList = new FileList(testFilePath, conf);

    for (int i = 1; i <= numUniqueEntries; i++) {
      String currentUniqueEntry = testEntry + i;
      fileList.add(currentUniqueEntry);
    }
    fileList.close();

    int currentCount = 1;
    while (fileList.hasNext()) {
      String entry = fileList.next();
      Assert.assertEquals(testEntry + currentCount, entry);
      currentCount++;
    }
    Assert.assertEquals(currentCount - 1, numUniqueEntries);
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

  private FileList setupFileList(boolean... retryParams) throws Exception {
    HiveConf hiveConf = Mockito.mock(HiveConf.class);
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Path backingFile = Mockito.spy(new Path("/tmp/backingFile"));
    FileList fileList = Mockito.spy(new FileList(backingFile, hiveConf));
    outStream = Mockito.spy(new FSDataOutputStream(null, null));
    Retryable retryable = Retryable.builder()
            .withTotalDuration(60)
            .withInitialDelay(1)
            .withBackoff(1.0)
            .withRetryOnException(IOException.class).build();

    if(retryParams.length == 0) {
      //setup for normal flow, without failures
      Path noRetryPath = new Path(new Path(TEST_DATA_DIR), "noRetry");
      testFileStream = Mockito.spy(noRetryPath.getFileSystem(conf).create(noRetryPath));
      Mockito.doReturn(retryable)
              .when(fileList).buildRetryable();
      Mockito.doReturn(true)
              .when(hiveConf).getBoolVar(HiveConf.ConfVars.REPL_COPY_FILE_LIST_ITERATOR_RETRY);
      Mockito.doReturn(testFileStream).when(fileList).initWriter();
    }
    else if (retryParams.length == 1) {
      //setup for retries
      Mockito.doReturn(true)
              .when(hiveConf).getBoolVar(HiveConf.ConfVars.REPL_COPY_FILE_LIST_ITERATOR_RETRY);
      Mockito.doReturn(retryable)
              .when(fileList).buildRetryable();
      Mockito.doReturn(mockFs)
              .when(backingFile).getFileSystem(hiveConf);

      if(retryParams[0]) {
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
    } else if (retryParams.length == 2) {
      //setup for failure but no retry
      Mockito.doReturn(false)
              .when(hiveConf).getBoolVar(HiveConf.ConfVars.REPL_COPY_FILE_LIST_ITERATOR_RETRY);
      Mockito.doReturn(outStream)
              .when(fileList).getWriterCreateMode();
      Mockito.doThrow(testException)
              .when(outStream).writeBytes(Mockito.anyString());
    } else if (retryParams.length == 3) {
      //setup for abort case
      Mockito.doReturn(true)
              .when(hiveConf)
              .getBoolVar(HiveConf.ConfVars.REPL_COPY_FILE_LIST_ITERATOR_RETRY);
      Mockito.doReturn(outStream)
              .when(fileList).initWriter();
    }
    return fileList;
  }
}
