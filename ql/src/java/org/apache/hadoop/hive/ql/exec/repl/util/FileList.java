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
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * A file backed list of Strings which is in-memory till the threshold.
 */
public class FileList implements AutoCloseable, Iterator<String> {
  private static final Logger LOG = LoggerFactory.getLogger(FileList.class);
  private static int fileListStreamerID = 0;
  private static final String  FILE_LIST_STREAMER_PREFIX = "file-list-streamer-";

  private LinkedBlockingQueue<String> cache;
  private volatile boolean thresholdHit = false;
  private int thresholdPoint;
  private float thresholdFactor = 0.9f;
  private Path backingFile;
  private FileListStreamer fileListStreamer;
  private String nextElement;
  private boolean noMoreElement;
  private HiveConf conf;
  private BufferedReader backingFileReader;


  public FileList(Path backingFile, int cacheSize, HiveConf conf) {
    this.backingFile = backingFile;
    this.conf = conf;
    if (cacheSize > 0) {
      // Cache size must be > 0 for this list to be used for the write operation.
      this.cache = new LinkedBlockingQueue<>(cacheSize);
      fileListStreamer = new FileListStreamer(cache, backingFile, conf);
      thresholdPoint = getThreshold(cacheSize);
      LOG.debug("File list backed by {} can be used for write operation.", backingFile);
    } else {
      thresholdHit = true;
    }
  }

  @VisibleForTesting
  FileList(Path backingFile, FileListStreamer fileListStreamer, LinkedBlockingQueue<String> cache, HiveConf conf) {
    this.backingFile = backingFile;
    this.fileListStreamer = fileListStreamer;
    this.cache = cache;
    this.conf = conf;
    thresholdPoint = getThreshold(cache.remainingCapacity());
  }

  /**
   * Only add operation is safe for concurrent operations.
   */
  public void add(String entry) throws SemanticException {
    if (thresholdHit && !fileListStreamer.isAlive()) {
      throw new SemanticException("List is not getting saved anymore to file " + backingFile.toString());
    }
    try {
      cache.put(entry);
    } catch (InterruptedException e) {
      throw new SemanticException(e);
    }
    if (!thresholdHit && cache.size() >= thresholdPoint) {
      initStoreToFile(cache.size());
    }
  }

  @Override
  public boolean hasNext() {
    if (!thresholdHit) {
      return (cache != null && !cache.isEmpty());
    }
    if (nextElement != null) {
      return true;
    }
    if (noMoreElement) {
      return false;
    }
    nextElement = readNextLine();
    if (nextElement == null) {
      noMoreElement = true;
    }
    return !noMoreElement;
  }

  @Override
  public String next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more element in the list backed by " + backingFile);
    }
    String retVal = nextElement;
    nextElement = null;
    return thresholdHit ? retVal : cache.poll();
  }

  private synchronized void initStoreToFile(int cacheSize) {
    if (!thresholdHit) {
      fileListStreamer.setName(getNextID());
      fileListStreamer.setDaemon(true);
      fileListStreamer.start();
      thresholdHit = true;
      LOG.info("Started streaming the list elements to file: {}, cache size {}", backingFile, cacheSize);
    }
  }

  private String readNextLine() {
    String nextElement = null;
    try {
      if (backingFileReader == null) {
        FileSystem fs = FileSystem.get(backingFile.toUri(), conf);
        if (fs.exists(backingFile)) {
          backingFileReader = new BufferedReader(new InputStreamReader(fs.open(backingFile)));
        }
      }
      nextElement = (backingFileReader == null) ? null : backingFileReader.readLine();
    } catch (IOException e) {
      LOG.error("Unable to read list from backing file " + backingFile, e);
    }
    return nextElement;
  }

  @Override
  public void close() throws IOException {
    if (thresholdHit && fileListStreamer != null) {
      fileListStreamer.close();
    }
    if (backingFileReader != null) {
      backingFileReader.close();
    }
    LOG.info("Completed close for File List backed by:{}, thresholdHit:{} ", backingFile, thresholdHit);
  }

  private static String getNextID() {
    if (Integer.MAX_VALUE == fileListStreamerID) {
      //reset the counter
      fileListStreamerID = 0;
    }
    fileListStreamerID++;
    return FILE_LIST_STREAMER_PREFIX  + fileListStreamerID;
  }

  public int getThreshold(int cacheSize) {
    boolean copyAtLoad = conf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
    return copyAtLoad ? 0 : (int)(cacheSize * thresholdFactor);
  }
}
