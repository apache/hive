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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

public class FileList implements AutoCloseable, Iterator<String> {
  private static final Logger LOG = LoggerFactory.getLogger(FileList.class);
  private Path backingFile;
  private String nextElement = null;
  private HiveConf conf;
  private BufferedReader backingFileReader;
  private BufferedWriter backingFileWriter;

  public FileList(Path backingFile, HiveConf conf) {
    this.backingFile = backingFile;
    this.conf = conf;
  }

  public void add(String entry) throws SemanticException {
    Retryable retryable = Retryable.builder()
            .withHiveConf(conf)
            .withRetryOnException(IOException.class).build();
    try {
      retryable.executeCallable((Callable<Void>) ()-> {
        synchronized (backingFile ) {
          if (backingFileWriter == null) {
            backingFileWriter = initWriter();
          }
          backingFileWriter.write(entry);
          backingFileWriter.newLine();
        }
        LOG.info("Writing entry {} to file list backed by {}", entry, backingFile);
        return null;
      });
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage(),
              String.valueOf(ErrorMsg.getErrorMsg(e).getErrorCode())));
    }
  }

  BufferedWriter initWriter() throws IOException {
    FileSystem fs = FileSystem.get(backingFile.toUri(), conf);
    return new BufferedWriter(new OutputStreamWriter(fs.create(backingFile)));
  }

  @Override
  public boolean hasNext() {
    if (nextElement != null) {
      return true;
    } else {
      try {
        nextElement = readNextLine();
        return (nextElement != null);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public String next() {
    if (nextElement == null && !hasNext()) {
      throw new NoSuchElementException("No more element in the list backed by " + backingFile);
    }
    String retVal = nextElement;
    nextElement = null;
    return retVal;
  }

  private String readNextLine() throws IOException {
    Retryable retryable = Retryable.builder()
            .withHiveConf(conf)
            .withRetryOnException(IOException.class).build();
    try {
      return retryable.executeCallable(() -> {
        String nextElement;
        if (backingFileReader == null) {
          FileSystem fs = FileSystem.get(backingFile.toUri(), conf);
          if(!fs.exists(backingFile)) {
            return null;
          }
          backingFileReader = new BufferedReader(new InputStreamReader(fs.open(backingFile)));
        }
        nextElement = backingFileReader.readLine();
        return nextElement;
      });
    } catch (Exception e) {
      throw new IOException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage(),
              String.valueOf(ErrorMsg.getErrorMsg(e).getErrorCode())));
    }
  }

  @Override
  public void close() throws IOException {
    if (backingFileReader != null) {
      backingFileReader.close();
    }
    if (backingFileWriter != null) {
      backingFileWriter.close();
    }
    LOG.info("Completed close for File List backed by:{}", backingFile);
  }
}
