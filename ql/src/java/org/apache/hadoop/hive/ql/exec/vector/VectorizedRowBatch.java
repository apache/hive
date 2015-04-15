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
package org.apache.hadoop.hive.ql.exec.vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * A VectorizedRowBatch is a set of rows, organized with each column
 * as a vector. It is the unit of query execution, organized to minimize
 * the cost per row and achieve high cycles-per-instruction.
 * The major fields are public by design to allow fast and convenient
 * access by the vectorized query execution code.
 */
public class VectorizedRowBatch implements Writable {
  public int numCols;           // number of columns
  public ColumnVector[] cols;   // a vector for each column
  public int size;              // number of rows that qualify (i.e. haven't been filtered out)
  public int[] selected;        // array of positions of selected values
  public int[] projectedColumns;
  public int projectionSize;

  /*
   * If no filtering has been applied yet, selectedInUse is false,
   * meaning that all rows qualify. If it is true, then the selected[] array
   * records the offsets of qualifying rows.
   */
  public boolean selectedInUse;

  // If this is true, then there is no data in the batch -- we have hit the end of input.
  public boolean endOfFile;

  /*
   * This number is carefully chosen to minimize overhead and typically allows
   * one VectorizedRowBatch to fit in cache.
   */
  public static final int DEFAULT_SIZE = 1024;

  public VectorExpressionWriter[] valueWriters = null;

  /**
   * Return a batch with the specified number of columns.
   * This is the standard constructor -- all batches should be the same size
   *
   * @param numCols the number of columns to include in the batch
   */
  public VectorizedRowBatch(int numCols) {
    this(numCols, DEFAULT_SIZE);
  }

  /**
   * Return a batch with the specified number of columns and rows.
   * Only call this constructor directly for testing purposes.
   * Batch size should normally always be defaultSize.
   *
   * @param numCols the number of columns to include in the batch
   * @param size  the number of rows to include in the batch
   */
  public VectorizedRowBatch(int numCols, int size) {
    this.numCols = numCols;
    this.size = size;
    selected = new int[size];
    selectedInUse = false;
    this.cols = new ColumnVector[numCols];
    projectedColumns = new int[numCols];

    // Initially all columns are projected and in the same order
    projectionSize = numCols;
    for (int i = 0; i < numCols; i++) {
      projectedColumns[i] = i;
    }
  }

  /**
   * Returns the maximum size of the batch (number of rows it can hold)
   */
  public int getMaxSize() {
      return selected.length;
  }

  /**
   * Return count of qualifying rows.
   *
   * @return number of rows that have not been filtered out
   */
  public long count() {
    return size;
  }

  private String toUTF8(Object o) {
    if(o == null || o instanceof NullWritable) {
      return "\\N"; /* as found in LazySimpleSerDe's nullSequence */
    }
    return o.toString();
  }

  @Override
  public String toString() {
    if (size == 0) {
      return "";
    }
    StringBuilder b = new StringBuilder();
    try {
      if (this.selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = selected[j];
          for (int k = 0; k < projectionSize; k++) {
            int projIndex = projectedColumns[k];
            ColumnVector cv = cols[projIndex];
            if (k > 0) {
              b.append('\u0001');
            }
            if (cv.isRepeating) {
              b.append(toUTF8(valueWriters[k].writeValue(cv, 0)));
            } else {
              b.append(toUTF8(valueWriters[k].writeValue(cv, i)));
            }
          }
          if (j < size - 1) {
            b.append('\n');
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          for (int k = 0; k < projectionSize; k++) {
            int projIndex = projectedColumns[k];
            ColumnVector cv = cols[projIndex];
            if (k > 0) {
              b.append('\u0001');
            }
            if (cv.isRepeating) {
              b.append(toUTF8(valueWriters[k].writeValue(cv, 0)));
            } else {
              b.append(toUTF8(valueWriters[k].writeValue(cv, i)));
            }
          }
          if (i < size - 1) {
            b.append('\n');
          }
        }
      }
    } catch (HiveException ex) {
      throw new RuntimeException(ex);
    }
    return b.toString();
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    throw new UnsupportedOperationException("Do you really need me?");
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    throw new UnsupportedOperationException("Don't call me");
  }

  public void setValueWriters(VectorExpressionWriter[] valueWriters) {
    this.valueWriters = valueWriters;
  }

  /**
   * Resets the row batch to default state
   *  - sets selectedInUse to false
   *  - sets size to 0
   *  - sets endOfFile to false
   *  - resets each column
   *  - inits each column
   */
  public void reset() {
    selectedInUse = false;
    size = 0;
    endOfFile = false;
    for (ColumnVector vc : cols) {
      if (vc != null) {
        vc.reset();
        vc.init();
      }
    }
  }
}
