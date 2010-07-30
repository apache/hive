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


/**
 * IOContext basically contains the position information of the current
 * key/value. For blockCompressed files, isBlockPointer should return true,
 * and currentBlockStart refers to the RCFile Block or SequenceFile Block. For
 * non compressed files, isBlockPointer should return false, and
 * currentBlockStart refers to the beginning offset of the current row,
 * nextBlockStart refers the end of current row and beginning of next row.
 */
public class IOContext {
  
  private static ThreadLocal<IOContext> threadLocal = new ThreadLocal<IOContext>();
  static {
    if (threadLocal.get() == null) {
      threadLocal.set(new IOContext());      
    }
  }
  
  public static IOContext get() {
    return IOContext.threadLocal.get();
  }

  long currentBlockStart;
  long nextBlockStart;
  boolean isBlockPointer;
  
  String inputFile;
  
  public IOContext() {
    this.currentBlockStart = 0;
    this.nextBlockStart = -1;
    this.isBlockPointer = true;
  }

  public long getCurrentBlockStart() {
    return currentBlockStart;
  }

  public void setCurrentBlockStart(long currentBlockStart) {
    this.currentBlockStart = currentBlockStart;
  }

  public long getNextBlockStart() {
    return nextBlockStart;
  }

  public void setNextBlockStart(long nextBlockStart) {
    this.nextBlockStart = nextBlockStart;
  }

  public boolean isBlockPointer() {
    return isBlockPointer;
  }

  public void setBlockPointer(boolean isBlockPointer) {
    this.isBlockPointer = isBlockPointer;
  }
  
  public String getInputFile() {
    return inputFile;
  }

  public void setInputFile(String inputFile) {
    this.inputFile = inputFile;
  }
}
