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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class FileListStreamer extends Thread implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileListStreamer.class);
  private static final long TIMEOUT_IN_SECS = 5L;
  private volatile boolean stop;
  private final LinkedBlockingQueue<String> cache;
  private Path backingFile;
  private Configuration conf;
  private BufferedWriter backingFileWriter;
  private volatile boolean valid = true;
  private volatile boolean asyncMode = false;
  private final Object COMPLETION_LOCK = new Object();
  private volatile boolean completed = false;



  public FileListStreamer(LinkedBlockingQueue<String> cache, Path backingFile, Configuration conf) throws IOException {
    this.cache = cache;
    this.backingFile = backingFile;
    this.conf = conf;
    init();
  }

  private void init() throws IOException {
    FileSystem fs = FileSystem.get(backingFile.toUri(), conf);
    backingFileWriter = new BufferedWriter(new OutputStreamWriter(fs.create(backingFile, !asyncMode)));
    LOG.info("Initialized a file based store to save a list at: {}, ayncMode:{}", backingFile, asyncMode);
  }

  public boolean isValid() {
    return valid;
  }

  @Override
  public void close() throws IOException {
    if (!asyncMode) {
      closeBackingFile();
      return;
    }
    stop = true;
    synchronized (COMPLETION_LOCK) {
      while (!completed && isValid()) {
        try {
          COMPLETION_LOCK.wait(TimeUnit.SECONDS.toMillis(TIMEOUT_IN_SECS));
        } catch (InterruptedException e) {
        }
      }
    }
    if (!isValid()) {
      throw new IOException("File list is not in a valid state:" + backingFile);
    }
    LOG.info("Completed close for File List backed by ", backingFile);
  }

  public synchronized void writeInThread(String nextEntry) throws SemanticException {
    try {
      backingFileWriter.write(nextEntry);
      backingFileWriter.newLine();
    } catch (IOException e) {
      throw new SemanticException(e);
    }
  }
  @Override
  public void run() {
    asyncMode = true;
    boolean exThrown = false;
    while (!exThrown && (!stop || !cache.isEmpty())) {
      try {
        String nextEntry = cache.poll(TIMEOUT_IN_SECS, TimeUnit.SECONDS);
        if (nextEntry != null) {
          backingFileWriter.write(nextEntry);
          backingFileWriter.newLine();
          LOG.debug("Writing entry {} to file list backed by {}", nextEntry, backingFile);
        }
      } catch (Exception iEx) {
        if (!(iEx instanceof InterruptedException)) {
          // not draining any more. Inform the producer to avoid OOM.
          valid = false;
          LOG.error("Exception while saving the list to file " + backingFile, iEx);
          exThrown = true;
        }
      }
    }
    try{
      closeBackingFile();
      completed = true;
    } finally {
      synchronized (COMPLETION_LOCK) {
        COMPLETION_LOCK.notify();
      }
    }
    LOG.info("Completed the file list streamer backed by: {}", backingFile);
  }

  private void closeBackingFile() {
    try {
      backingFileWriter.close();
      LOG.debug("Closed the file list backing file: {}", backingFile);
    } catch (IOException e) {
      LOG.error("Exception while closing the file list backing file", e);
      valid = false;
    }
  }
}
