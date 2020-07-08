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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * A file backed list of Strings which is in-memory till the threshold.
 */
public class FileList implements Closeable, Iterator<String> {
  private static final Logger LOG = LoggerFactory.getLogger(FileList.class);
  private static int fileListStreamerID = 0;
  private static final String  FILE_LIST_STREAMER_PREFIX = "file-list-streamer-";

  private LinkedBlockingQueue<String> cache;
  private volatile boolean thresholdHit = false;
  private int thresholdPoint;
  private float thresholdFactor = 0.9f;
  private Path backingFile;
  private FileListStreamer fileListStreamer;
  private FileListOpMode fileListOpMode;
  private String nextElement;
  private boolean noMoreElement;
  private HiveConf conf;
  private BufferedReader backingFileReader;
  private volatile boolean asyncMode;


  /**
   * To be used only for READ mode;
   */
  public FileList(Path backingFile, HiveConf conf) {
    this.backingFile = backingFile;
    thresholdHit = true;
    fileListOpMode = FileListOpMode.READ;
    this.conf = conf;
  }

  /**
   * To be used only for WRITE mode;
   */
  public FileList(Path backingFile, int cacheSize, HiveConf conf, boolean asyncMode) throws IOException {
    this.cache = new LinkedBlockingQueue<>(cacheSize);
    this.backingFile = backingFile;
    fileListStreamer = new FileListStreamer(cache, backingFile, conf);
    fileListOpMode = FileListOpMode.WRITE;
    this.conf = conf;
    thresholdPoint = getThreshold(cacheSize);
    this.asyncMode = asyncMode;
  }

  /**
   * Only add operation is safe for concurrent operation.
   */
  public void add(String entry) throws SemanticException {
    validateMode(FileListOpMode.WRITE);
    if (!asyncMode) {
      fileListStreamer.writeInThread(entry);
      return;
    }
    if (thresholdHit && !fileListStreamer.isValid()) {
      throw new SemanticException("List is not getting saved anymore to file " + backingFile.toString());
    }
    try {
      cache.put(entry);
    } catch (InterruptedException e) {
      throw new SemanticException(e);
    }
    if (!thresholdHit && cache.size() > thresholdPoint) {
      initStoreToFile();
    }
  }

  /**
   * Must be called before the list object can be used for read operation.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (fileListOpMode == FileListOpMode.READ) {
      if (backingFileReader != null) {
        backingFileReader.close();
      }
    } else {
      fileListOpMode = FileListOpMode.CLOSING;
      if (thresholdHit) {
        fileListStreamer.close();
      }
      fileListOpMode = FileListOpMode.READ;
    }
  }

  @Override
  public boolean hasNext() {
    validateMode(FileListOpMode.READ);
    if (!thresholdHit) {
      return !cache.isEmpty();
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
  public String next() {
    validateMode(FileListOpMode.READ);
    if (!hasNext()) {
      throw new NoSuchElementException("No more element in the list backed by " + backingFile);
    }
    String retVal = nextElement;
    nextElement = null;
    return thresholdHit ? retVal : cache.poll();
  }
  private synchronized void initStoreToFile() {
    if (!thresholdHit) {
      fileListStreamer.setName(getNextID());
      fileListStreamer.setDaemon(true);
      fileListStreamer.start();
      thresholdHit = true;
      LOG.info("Started streaming the list elements to file: {}", backingFile);
    }
  }

  private static String getNextID() {
    if (Integer.MAX_VALUE == fileListStreamerID) {
      //reset the counter
      fileListStreamerID = 0;
    }
    fileListStreamerID++;
    return FILE_LIST_STREAMER_PREFIX  + fileListStreamerID;
  }

  private void validateMode(FileListOpMode expectedMode) throws IllegalStateException {
    if (!fileListOpMode.equals(expectedMode)) {
      String logMessage = String.format("Invalid mode for File List, expected:%s, found:%s",
              expectedMode, fileListOpMode);
      throw new IllegalStateException(logMessage);
    }
  }

  public int getThreshold(int cacheSize) {
    boolean copyAtLoad = conf.getBoolVar(HiveConf.ConfVars.REPL_DATA_COPY_LAZY);
    return copyAtLoad ? 0 : (int)(cacheSize * thresholdFactor);
  }

  private enum FileListOpMode {
    READ,
    WRITE,
    CLOSING
  }

}
