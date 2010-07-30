/**
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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public abstract class HiveContextAwareRecordReader<K, V> implements RecordReader<K, V> {
  
  private boolean initDone = false;
  
  /** 
   * Reads the next key/value pair from the input for processing.
   *
   * @param key the key to read data into
   * @param value the value to read data into
   * @return true if a key/value was read, false if at EOF
   */
  public abstract boolean doNext(K key, V value) throws IOException;
  
  /** 
   * Close this {@link InputSplit} to future operations.
   * 
   * @throws IOException
   */ 
  public abstract void doClose() throws IOException;
  
  private IOContext ioCxtRef =  null;
  
  @Override
  public void close() throws IOException {
    doClose();
    initDone = false;
    ioCxtRef = null;
  }
  
  @Override
  public boolean next(K key, V value) throws IOException {
    if(!initDone) {
      throw new IOException("Hive IOContext is not inited.");
    }
    updateIOContext();
    return doNext(key, value);
  }
  
  protected void updateIOContext()
      throws IOException {
    long pointerPos = this.getPos();
    if (!ioCxtRef.isBlockPointer) {
      ioCxtRef.currentBlockStart = pointerPos;
      return;
    }

    if (ioCxtRef.nextBlockStart == -1) {
      ioCxtRef.nextBlockStart = pointerPos;
    }
    if (pointerPos != ioCxtRef.nextBlockStart) {
      // the reader pointer has moved to the end of next block, or the end of
      // current record.

      ioCxtRef.currentBlockStart = ioCxtRef.nextBlockStart;
      ioCxtRef.nextBlockStart = pointerPos;
    }
  }
  
  public IOContext getIOContext() {
    return IOContext.get();
  }
  
  public void initIOContext(long startPos, boolean isBlockPointer, String inputFile) {
    ioCxtRef = this.getIOContext();
    ioCxtRef.currentBlockStart = startPos;
    ioCxtRef.isBlockPointer = isBlockPointer;
    ioCxtRef.inputFile = inputFile;
    initDone = true;
  }

  public void initIOContext(FileSplit split, JobConf job,
      Class inputFormatClass) throws IOException {
    boolean blockPointer = false;
    long blockStart = -1;    
    FileSplit fileSplit = (FileSplit) split;
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(job);
    if (inputFormatClass.getName().contains("SequenceFile")) {
      SequenceFile.Reader in = new SequenceFile.Reader(fs, path, job);
      blockPointer = in.isBlockCompressed();
      in.sync(fileSplit.getStart());
      blockStart = in.getPosition();
      in.close();
    } else if (inputFormatClass.getName().contains("RCFile")) {
      RCFile.Reader in = new RCFile.Reader(fs, path, job);
      blockPointer = true;
      in.sync(fileSplit.getStart());
      blockStart = in.getPosition();
      in.close();
    }
    this.initIOContext(blockStart, blockPointer, split.getPath().toString());
  }
}