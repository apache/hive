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

package org.apache.hadoop.hive.llap.io.api.impl;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;

import java.util.Arrays;

/**
 * Unlike VRB, doesn't have some fields, and doesn't have all columns
 * (non-selected, partition cols, cols for downstream ops, etc.)
 * It does, however, hold the FilterContext of the VRB.
 */
public class ColumnVectorBatch {
  public MutableFilterContext filterContext;
  public ColumnVector[] cols;
  public int size;

  public ColumnVectorBatch(int columnCount) {
    this(columnCount, VectorizedRowBatch.DEFAULT_SIZE);
  }

  public ColumnVectorBatch(int columnCount, int batchSize) {
    this.filterContext = new VectorizedRowBatch(0);
    this.cols = new ColumnVector[columnCount];
    this.size = batchSize;
  }

  public void swapColumnVector(int ix, ColumnVector[] other, int otherIx) {
    ColumnVector old = other[otherIx];
    other[otherIx] = cols[ix];
    cols[ix] = old;
  }

  
  @Override
  public String toString() {
    if (size == 0) {
      return "";
    }
    StringBuilder b = new StringBuilder();
    b.append("FilterContext used: ");
    b.append(filterContext.isSelectedInUse());
    b.append(", size: ");
    b.append(filterContext.getSelectedSize());
    b.append('\n');
    b.append("Selected: ");
    b.append(filterContext.isSelectedInUse() ? Arrays.toString(filterContext.getSelected()) : "[]");
    b.append('\n');

    b.append("Column vector types: ");
    for (int k = 0; k < cols.length; k++) {
      ColumnVector cv = cols[k];
      b.append(k);
      b.append(":");
      b.append(cv == null ? "null" : cv.getClass().getSimpleName().replace("ColumnVector", ""));
    }
    b.append('\n');


    for (int i = 0; i < size; i++) {
      b.append('[');
      for (int k = 0; k < cols.length; k++) {
        ColumnVector cv = cols[k];
        if (k > 0) {
          b.append(", ");
        }
        if (cv == null) continue;
        if (cv != null) {
          try {
            cv.stringifyValue(b, i);
          } catch (Exception ex) {
            b.append("invalid");
          }
        }
      }
      b.append(']');
      if (i < size - 1) {
        b.append('\n');
      }
    }

    return b.toString();
  }
}