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
package org.apache.hadoop.hive.ql.exec.vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.SuppressFBWarnings;

/**
 * A VectorizedRowBatch is a set of rows, organized with each column
 * as a vector. It is the unit of query execution, organized to minimize
 * the cost per row and achieve high cycles-per-instruction.
 * The major fields are public by design to allow fast and convenient
 * access by the vectorized query execution code.
 */
public class VectorizedRowBatch implements Writable, MutableFilterContext {
  public int numCols;           // number of columns
  public ColumnVector[] cols;   // a vector for each column
  public int size;              // number of rows that qualify (i.e. haven't been filtered out)
  public int[] selected;        // array of positions of selected values
  public int[] projectedColumns;
  public int projectionSize;

  private int dataColumnCount;
  private int partitionColumnCount;


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

  /*
   * This number is a safety limit for 32MB of writables.
   */
  public static final int DEFAULT_BYTES = 32 * 1024 * 1024;

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

    dataColumnCount = -1;
    partitionColumnCount = -1;
  }

  public void setPartitionInfo(int dataColumnCount, int partitionColumnCount) {
    this.dataColumnCount = dataColumnCount;
    this.partitionColumnCount = partitionColumnCount;
  }

  public int getDataColumnCount() {
    return dataColumnCount;
  }

  public int getPartitionColumnCount() {
    return partitionColumnCount;
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

  private static String toUTF8(Object o) {
    if(o == null || o instanceof NullWritable) {
      return "\\N"; /* as found in LazySimpleSerDe's nullSequence */
    }
    return o.toString();
  }

  public String stringifyColumn(int columnNum) {
    if (size == 0) {
      return "";
    }
    StringBuilder b = new StringBuilder();
    b.append("columnNum ");
    b.append(columnNum);
    b.append(", size ");
    b.append(size);
    b.append(", selectedInUse ");
    b.append(selectedInUse);
    ColumnVector colVector = cols[columnNum];
    b.append(", noNulls ");
    b.append(colVector.noNulls);
    b.append(", isRepeating ");
    b.append(colVector.isRepeating);
    b.append('\n');

    final boolean noNulls = colVector.noNulls;
    final boolean[] isNull = colVector.isNull;
    if (colVector.isRepeating) {
      final boolean hasRepeatedValue = (noNulls || !isNull[0]);
      for (int i = 0; i < size; i++) {
        if (hasRepeatedValue) {
          colVector.stringifyValue(b, 0);
        } else {
          b.append("NULL");
        }
        b.append('\n');
      }
    } else {
      for (int i = 0; i < size; i++) {
        final int batchIndex = (selectedInUse ? selected[i] : i);
        if (noNulls || !isNull[batchIndex]) {
          colVector.stringifyValue(b, batchIndex);
        } else {
          b.append("NULL");
        }
        b.append('\n');
      }
    }
    return b.toString();
  }

  private void appendVectorType(StringBuilder b, ColumnVector cv) {
    String colVectorType = null;
    if (cv instanceof LongColumnVector) {
      colVectorType = "LONG";
    } else if (cv instanceof DoubleColumnVector) {
      colVectorType = "DOUBLE";
    } else if (cv instanceof BytesColumnVector) {
      colVectorType = "BYTES";
    } else if (cv instanceof DecimalColumnVector) {
      colVectorType = "DECIMAL";
    } else if (cv instanceof TimestampColumnVector) {
      colVectorType = "TIMESTAMP";
    } else if (cv instanceof IntervalDayTimeColumnVector) {
      colVectorType = "INTERVAL_DAY_TIME";
    } else if (cv instanceof ListColumnVector) {
      colVectorType = "LIST";
    } else if (cv instanceof MapColumnVector) {
      colVectorType = "MAP";
    } else if (cv instanceof StructColumnVector) {
      colVectorType = "STRUCT";
    } else if (cv instanceof UnionColumnVector) {
      colVectorType = "UNION";
    } else {
      colVectorType = "Unknown";
    }
    b.append(colVectorType);

    if (cv instanceof ListColumnVector) {
      ListColumnVector listColumnVector = (ListColumnVector) cv;
      b.append("<");
      appendVectorType(b, listColumnVector.child);
      b.append(">");
    } else if (cv instanceof MapColumnVector) {
      MapColumnVector mapColumnVector = (MapColumnVector) cv;
      b.append("<");
      appendVectorType(b, mapColumnVector.keys);
      b.append(", ");
      appendVectorType(b, mapColumnVector.values);
      b.append(">");
    } else if (cv instanceof StructColumnVector) {
      StructColumnVector structColumnVector = (StructColumnVector) cv;
      b.append("<");
      final int fieldCount = structColumnVector.fields.length;
      for (int i = 0; i < fieldCount; i++) {
        if (i > 0) {
          b.append(", ");
        }
        appendVectorType(b, structColumnVector.fields[i]);
      }
      b.append(">");
    } else if (cv instanceof UnionColumnVector) {
      UnionColumnVector unionColumnVector = (UnionColumnVector) cv;
      b.append("<");
      final int fieldCount = unionColumnVector.fields.length;
      for (int i = 0; i < fieldCount; i++) {
        if (i > 0) {
          b.append(", ");
        }
        appendVectorType(b, unionColumnVector.fields[i]);
      }
      b.append(">");
    }
  }

  public String stringify(String prefix) {
    if (size == 0) {
      return "";
    }
    StringBuilder b = new StringBuilder();
    b.append(prefix);
    b.append("Column vector types: ");
    for (int k = 0; k < projectionSize; k++) {
      int projIndex = projectedColumns[k];
      ColumnVector cv = cols[projIndex];
      if (k > 0) {
        b.append(", ");
      }
      b.append(projIndex);
      b.append(":");
      appendVectorType(b, cv);
    }
    b.append('\n');
    b.append(prefix);

    if (this.selectedInUse) {
      for (int j = 0; j < size; j++) {
        int i = selected[j];
        b.append('[');
        for (int k = 0; k < projectionSize; k++) {
          int projIndex = projectedColumns[k];
          ColumnVector cv = cols[projIndex];
          if (k > 0) {
            b.append(", ");
          }
          if (cv != null) {
            try {
              cv.stringifyValue(b, i);
            } catch (Exception ex) {
              b.append("<invalid>");
            }
          }
        }
        b.append(']');
        if (j < size - 1) {
          b.append('\n');
          b.append(prefix);
        }
      }
    } else {
      for (int i = 0; i < size; i++) {
        b.append('[');
        for (int k = 0; k < projectionSize; k++) {
          int projIndex = projectedColumns[k];
          ColumnVector cv = cols[projIndex];
          if (k > 0) {
            b.append(", ");
          }
          if (cv != null) {
            try {
              cv.stringifyValue(b, i);
            } catch (Exception ex) {
              b.append("<invalid>");
            }
          }
        }
        b.append(']');
        if (i < size - 1) {
          b.append('\n');
          b.append(prefix);
        }
      }
    }
    return b.toString();
  }

  @Override
  public String toString() {
    return stringify("");
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    throw new UnsupportedOperationException("Do you really need me?");
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    throw new UnsupportedOperationException("Don't call me");
  }

  /**
   * Resets the row batch to default state
   *  - sets selectedInUse to false
   *  - sets size to 0
   *  - sets endOfFile to false
   *  - resets each column
   *  - inits each column
   */
  @Override
  public void reset() {
    reset(cols);
  }

  public void reset(Collection<Integer> columnIndices) {
    ColumnVector[] columnsToReset = columnIndices.stream()
            .filter(index -> index < cols.length)
            .map(x -> cols[x])
            .toArray(ColumnVector[]::new);
    reset(columnsToReset);
  }

  private void reset(ColumnVector[] columns) {
    selectedInUse = false;
    size = 0;
    endOfFile = false;
    for (ColumnVector vc : columns) {
      if (vc != null) {
        vc.reset();
        vc.init();
      }
    }
  }

  /**
   * Set the maximum number of rows in the batch.
   * Data is not preserved.
   */
  public void ensureSize(int rows) {
    for (ColumnVector col : cols) {
      col.ensureSize(rows, false);
    }
    updateSelected(rows);
  }

  @Override
  public boolean isSelectedInUse() {
    return selectedInUse;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Expose internal rep for efficiency")
  public int[] getSelected() {
    return selected;
  }

  @Override
  public int getSelectedSize() {
    return size;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Ref external obj for efficiency")
  public void setFilterContext(boolean isSelectedInUse, int[] selected, int selectedSize) {
    this.selectedInUse = isSelectedInUse;
    this.selected = selected;
    this.size = selectedSize;
    // Avoid selected.length < selectedSize since we can borrow a larger array for selected
    // Debug loop for selected array: use without assert when needed (asserts only fail in testing)
    assert validateSelected() : "Selected array may not contain duplicates or unordered values";
  }

  @Override
  public boolean validateSelected() {
    for (int i = 1; i < size; i++) {
      if (selected[i-1] >= selected[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Expose internal rep for efficiency")
  public int[] updateSelected(int minCapacity) {
    if (selected == null || selected.length < minCapacity) {
      selected = new int[minCapacity];
    }
    return selected;
  }

  @Override
  public void setSelectedInUse(boolean selectedInUse) {
    this.selectedInUse = selectedInUse;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Ref external obj for efficiency")
  public void setSelected(int[] selectedArray) {
    selected = selectedArray;
  }

  @Override
  public void setSelectedSize(int selectedSize) {
    size = selectedSize;
  }
}
