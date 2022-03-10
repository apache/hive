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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

public class FileList implements AutoCloseable, Iterator<String> {
  private static final Logger LOG = LoggerFactory.getLogger(FileList.class);
  private final Path backingFile;
  private String nextElement = null;
  private String lastReadElement = null;
  private HiveConf conf;
  private volatile boolean abortOperation = false;
  private volatile boolean retryMode;
  private BufferedReader backingFileReader;
  private volatile FSDataOutputStream backingFileWriter;

  public FileList(Path backingFile, HiveConf conf) {
    this.backingFile = backingFile;
    this.conf = conf;
    this.retryMode = false;
  }

  public void add(String entry) throws IOException {
    if (conf.getBoolVar(HiveConf.ConfVars.REPL_COPY_FILE_LIST_ITERATOR_RETRY)) {
      writeWithRetry(entry);
    } else {
      writeEntry(entry);
    }
  }

  private synchronized void writeEntry(String entry) throws IOException {
    //retry only during creating the file, no retry during writes
    if (backingFileWriter == null) {
      try {
        Retryable retryable = buildRetryable();
        retryable.executeCallable((Callable<Void>) () -> {
          if (this.abortOperation) {
            LOG.debug("Aborting write operation for entry {} to file {}.", entry, backingFile);
            return null;
          }
          backingFileWriter = getWriterCreateMode();
          return null;
        });
      } catch (Exception e) {
        this.abortOperation = true;
        throw new IOException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()));
      }
    }
    if (this.abortOperation) {
      LOG.debug("Aborting write operation for entry {} to file {}.", entry, backingFile);
      return;
    }
    try {
      backingFileWriter.writeBytes(getEntryWithNewline(entry));
      LOG.info("Writing entry {} to file list backed by {}", entry, backingFile);
    } catch (IOException e) {
      this.abortOperation = true;
      LOG.error("Writing entry {} to file list {} failed.", entry, backingFile, e);
      throw e;
    }
  }

  private synchronized void writeWithRetry(String entry) throws IOException {
    Retryable retryable = buildRetryable();
    try {
      retryable.executeCallable((Callable<Void>) () -> {
        if (this.abortOperation) {
          LOG.debug("Aborting write operation for entry {} to file {}.", entry, backingFile);
          return null;
        }
        try {
          if (backingFileWriter == null) {
            backingFileWriter = initWriter();
          }
          backingFileWriter.writeBytes(getEntryWithNewline(entry));
          backingFileWriter.hflush();
          LOG.info("Writing entry {} to file list backed by {}", entry, backingFile);
        } catch (IOException e) {
          LOG.error("Writing entry {} to file list {} failed, attempting retry.", entry, backingFile, e);
          this.retryMode = true;
          close();
          throw e;
        }
        return null;
      });
    } catch (Exception e) {
      this.abortOperation = true;
      throw new IOException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()));
    }
  }

  Retryable buildRetryable() {
    return Retryable.builder()
            .withHiveConf(conf)
            .withRetryOnException(IOException.class).build();
  }

  // Return the entry ensuring it ends with newline.
  private String getEntryWithNewline(String entry) {
    return new StringWriter()
            .append(entry)
            .append(System.lineSeparator())
            .toString();
  }

  FSDataOutputStream initWriter() throws IOException {
    if(shouldAppend()) {
      return getWriterAppendMode(); // append in retry-mode if file has been created already
    }
    else {
      return getWriterCreateMode();
    }
  }

  private boolean shouldAppend() throws IOException {
    return backingFile.getFileSystem(conf).exists(backingFile) && this.retryMode;
  }

  FSDataOutputStream getWriterCreateMode() throws IOException {
    try {
      return backingFile.getFileSystem(conf).create(backingFile);
    } catch (IOException e) {
      LOG.error("Error creating file {}", backingFile, e);
      throw e;
    }
  }

  FSDataOutputStream getWriterAppendMode() throws IOException {
    try {
      return backingFile.getFileSystem(conf).append(backingFile);
    } catch (IOException e) {
      LOG.error("Error opening file {} in append mode", backingFile, e);
      throw e;
    }
  }

  @Override
  public boolean hasNext() {
    /*
    We assume that every add operation either adds an entry completely or doesn't add at all.
    If this assumption changes then in the following check we should check for incomplete entries.
    We remove duplicate entries assuming they are only written consecutively.
    */
    if (nextElement != null && !nextElement.equals(lastReadElement)) {
      return true;
    } else {
      try {
        do {
          nextElement = readNextLine();
          if(nextElement != null && !nextElement.equals(lastReadElement)) {
            return true;
          }
        } while (nextElement != null);
        return false;
      } catch (IOException e) {
        nextElement = null;
        lastReadElement = null;
        backingFileReader = null;
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public String next() {
    if ((nextElement == null || nextElement.equals(lastReadElement)) && !hasNext()) {
      throw new NoSuchElementException("No more element in the list backed by " + backingFile);
    }
    lastReadElement = nextElement;
    nextElement = null;
    return lastReadElement;
  }

  private String readNextLine() throws IOException {
    try{
      String nextElement;
      if (backingFileReader == null) {
        FileSystem fs = backingFile.getFileSystem(conf);
        if (!fs.exists(backingFile)) {
          return null;
        }
        backingFileReader = new BufferedReader(new InputStreamReader(fs.open(backingFile)));
      }
      nextElement = backingFileReader.readLine();
      return nextElement;
    } catch (IOException e) {
      LOG.error("Exception while reading file {}.", backingFile, e);
      close();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (backingFileReader != null) {
        backingFileReader.close();
      }
      if (backingFileWriter != null) {
        backingFileWriter.close();
      }
      LOG.info("Completed close for File List backed by:{}", backingFile);
    } finally {
      backingFileReader = null;
      backingFileWriter = null;
    }
  }
}