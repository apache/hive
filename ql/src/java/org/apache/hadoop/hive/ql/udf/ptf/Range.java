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
package org.apache.hadoop.hive.ql.udf.ptf;

import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;

/**
 * Class to represent a window range in a partition by searching the relative position (ROWS) or
 * relative value (RANGE) of the current row
 */
public class Range {
  /**
   * Start of range, inclusive.
   */
  int start;
  /**
   * End of range, exclusive.
   */
  int end;

  PTFPartition p;
  /**
   * When there are no parameters specified, partition iterator can be made faster;
   * In such cases, it need not materialize the ROW from RowContainer. This saves lots of IO.
   */
  private final boolean optimized;

  public Range(int start, int end, PTFPartition p, boolean optimized) {
    this.start = start;
    this.end = end;
    this.p = p;
    this.optimized = optimized;
  }

  public Range(int start, int end, PTFPartition p) {
    this(start, end, p, false);
  }

  public PTFPartitionIterator<Object> iterator() {
    return p.range(start, end, optimized);
  }

  public int getDiff(Range prevRange) {
    return this.start - prevRange.start + this.end - prevRange.end;
  }

  public int getSize() {
    return end - start;
  }

  public int getStart(){
    return start;
  }

  public int getEnd(){
    return end;
  }

  public String toString() {
    return String.format("Range: %d-%d, size: %d", start, end, getSize());
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash = 31 * hash + end;
    if (p != null){
      hash = 31 * hash + p.hashCode();
    }
    return 31 * hash + start;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Range other = (Range) obj;
    if (end != other.end)
      return false;
    if (start != other.start)
      return false;
    if (p != other.p)
      return false;
    return true;
  }

  public int compareTo(Range other) {
    return getSize() == other.getSize() ? getStart() - other.getStart()
      : getSize() - other.getSize();
  }
}
