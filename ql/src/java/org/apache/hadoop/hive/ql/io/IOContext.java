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

import org.apache.hadoop.fs.Path;

/**
 * IOContext basically contains the position information of the current
 * key/value. For blockCompressed files, isBlockPointer should return true,
 * and currentBlockStart refers to the RCFile Block or SequenceFile Block. For
 * non compressed files, isBlockPointer should return false, and
 * currentBlockStart refers to the beginning offset of the current row,
 * nextBlockStart refers the end of current row and beginning of next row.
 */
public class IOContext {
  private long currentBlockStart;
  private long nextBlockStart;
  private long currentRow;
  private boolean isBlockPointer;
  private boolean ioExceptions;

  // Are we using the fact the input is sorted
  private boolean useSorted = false;
  // Are we currently performing a binary search
  private boolean isBinarySearching = false;
  // Do we want to end the binary search
  private boolean endBinarySearch = false;
  // The result of the comparison of the last row processed
  private Comparison comparison = null;
  // The class name of the generic UDF being used by the filter
  private String genericUDFClassName = null;
  /**
   * supports {@link org.apache.hadoop.hive.ql.metadata.VirtualColumn#ROWID}
   */
  private  RecordIdentifier ri;

  public static enum Comparison {
    GREATER,
    LESS,
    EQUAL,
    UNKNOWN
  }

  private Path inputPath;

  public IOContext() {
    this.currentBlockStart = 0;
    this.nextBlockStart = -1;
    this.currentRow = 0;
    this.isBlockPointer = true;
    this.ioExceptions = false;
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

  public long getCurrentRow() {
    return currentRow;
  }

  public void setCurrentRow(long currentRow) {
    this.currentRow = currentRow;
  }

  public boolean isBlockPointer() {
    return isBlockPointer;
  }

  public void setBlockPointer(boolean isBlockPointer) {
    this.isBlockPointer = isBlockPointer;
  }

  public Path getInputPath() {
    return inputPath;
  }

  public void setInputPath(Path inputPath) {
    this.inputPath = inputPath;
  }

  public void setIOExceptions(boolean ioe) {
    this.ioExceptions = ioe;
  }

  public boolean getIOExceptions() {
    return ioExceptions;
  }

  public boolean useSorted() {
    return useSorted;
  }

  public void setUseSorted(boolean useSorted) {
    this.useSorted = useSorted;
  }

  public boolean isBinarySearching() {
    return isBinarySearching;
  }

  public void setBinarySearching(boolean isBinarySearching) {
    this.isBinarySearching = isBinarySearching;
  }

  public boolean shouldEndBinarySearch() {
    return endBinarySearch;
  }

  public void setEndBinarySearch(boolean endBinarySearch) {
    this.endBinarySearch = endBinarySearch;
  }

  public Comparison getComparison() {
    return comparison;
  }

  public void setComparison(Integer comparison) {
    if (comparison == null && this.isBinarySearching) {
      // Nothing we can do here, so just proceed normally from now on
      endBinarySearch = true;
    } else {
      if (comparison == null) {
        this.comparison = Comparison.UNKNOWN;
      } else if (comparison.intValue() < 0) {
        this.comparison = Comparison.LESS;
      } else if (comparison.intValue() > 0) {
        this.comparison = Comparison.GREATER;
      } else {
        this.comparison = Comparison.EQUAL;
      }
    }
  }

  public String getGenericUDFClassName() {
    return genericUDFClassName;
  }

  public void setGenericUDFClassName(String genericUDFClassName) {
    this.genericUDFClassName = genericUDFClassName;
  }

  public RecordIdentifier getRecordIdentifier() {
    return this.ri;
  }

  public void setRecordIdentifier(RecordIdentifier ri) {
    this.ri = ri;
  }

  /**
   * The thread local IOContext is static, we may need to restart the search if, for instance,
   * multiple files are being searched as part of a CombinedHiveRecordReader
   */
  public void resetSortingValues() {
    this.useSorted = false;
    this.isBinarySearching = false;
    this.endBinarySearch = false;
    this.comparison = null;
    this.genericUDFClassName = null;
  }

}
