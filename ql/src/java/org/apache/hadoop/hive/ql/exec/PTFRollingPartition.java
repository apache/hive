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

package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class PTFRollingPartition extends PTFPartition {

  /*
   * num rows whose output is evaluated.
   */
  int numRowsProcessed;

  /*
   * Relative start position of the windowing. Can be negative.
   */
  int startPos;

  /*
   * Relative end position of the windowing. Can be negative.
   */
  int endPos;

  /*
   * number of rows received.
   */
  int numRowsReceived;
  
  /*
   * State of the Rolling Partition
   * 
   * x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17
   * ^                    ^                                     ^
   * |                    |                                     |
   * |--preceding span--numRowsProcessed---followingSpan --numRowsRecived
   * 
   * a. index x7 : represents the last output row
   * b. so preceding span rows before that are still held on for subsequent rows processing.
   * c. The #of rows beyond numRowsProcessed = followingSpan
   */

  /*
   * cache of rows; guaranteed to contain precedingSpan rows before
   * nextRowToProcess.
   */
  List<Object> currWindow;

  protected PTFRollingPartition(Configuration cfg, AbstractSerDe serDe,
      StructObjectInspector inputOI, StructObjectInspector outputOI,
      int startPos, int endPos) throws HiveException {
    super(cfg, serDe, inputOI, outputOI, false);
    this.startPos = startPos;
    this.endPos = endPos;
    currWindow = new ArrayList<Object>(endPos - startPos + 1);
  }

  public void reset() throws HiveException {
    currWindow.clear();
    numRowsProcessed = 0;
    numRowsReceived = 0;
  }

  public Object getAt(int i) throws HiveException {
    int rangeStart = numRowsReceived - currWindow.size();
    return currWindow.get(i - rangeStart);
  }

  public void append(Object o) throws HiveException {
    @SuppressWarnings("unchecked")
    List<Object> l = (List<Object>) ObjectInspectorUtils.copyToStandardObject(
        o, inputOI, ObjectInspectorCopyOption.WRITABLE);
    currWindow.add(l);
    numRowsReceived++;
  }

  public Object nextOutputRow() throws HiveException {
    Object row = getAt(numRowsProcessed);
    numRowsProcessed++;
    if (numRowsProcessed > -startPos) {
      currWindow.remove(0);
    }
    return row;
  }

  public boolean processedAllRows() {
    return numRowsProcessed >= numRowsReceived;
  }

  /**
   * Gets the next row index that the data within the window are available and can be processed
   * @param wFrameDef
   * @return
   */
  public int rowToProcess(WindowFrameDef wFrameDef) {
    int rowToProcess = numRowsReceived - 1 - Math.max(0, wFrameDef.getEnd().getRelativeOffset());
    return rowToProcess >= 0 ? rowToProcess : -1;
  }

  public int size() {
    return numRowsReceived;
  }

  public PTFPartitionIterator<Object> iterator() throws HiveException {
    return new RollingPItr();
  }

  public void close() {
  }

  class RollingPItr implements PTFPartitionIterator<Object> {

    @Override
    public boolean hasNext() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object next() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getIndex() {
      return PTFRollingPartition.this.numRowsProcessed;
    }

    @Override
    public Object lead(int amt) throws HiveException {
      int i = PTFRollingPartition.this.numRowsProcessed + amt;
      i = i >= PTFRollingPartition.this.numRowsReceived ? PTFRollingPartition.this.numRowsReceived - 1
          : i;
      return PTFRollingPartition.this.getAt(i);
    }

    @Override
    public Object lag(int amt) throws HiveException {
      int i = PTFRollingPartition.this.numRowsProcessed - amt;
      int start = PTFRollingPartition.this.numRowsReceived
          - PTFRollingPartition.this.currWindow.size();

      i = i < start ? start : i;
      return PTFRollingPartition.this.getAt(i);
    }

    @Override
    public Object resetToIndex(int idx) throws HiveException {
      return PTFRollingPartition.this.getAt(idx);
    }

    @Override
    public PTFPartition getPartition() {
      return PTFRollingPartition.this;
    }

    @Override
    public void reset() throws HiveException {
    }

    @Override
    public long count() {
      throw new UnsupportedOperationException();
    }

  }
}
