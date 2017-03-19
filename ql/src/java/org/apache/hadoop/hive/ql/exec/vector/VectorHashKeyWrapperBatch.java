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

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;

/**
 * Class for handling vectorized hash map key wrappers. It evaluates the key columns in a
 * row batch in a vectorized fashion.
 * This class stores additional information about keys needed to evaluate and output the key values.
 *
 */
public class VectorHashKeyWrapperBatch extends VectorColumnSetInfo {

  public VectorHashKeyWrapperBatch(int keyCount) {
    super(keyCount);
  }

  /**
   * Number of object references in 'this' (for size computation)
   */
  private static final int MODEL_REFERENCES_COUNT = 7;

  /**
   * The key expressions that require evaluation and output the primitive values for each key.
   */
  private VectorExpression[] keyExpressions;

  /**
   * Pre-allocated batch size vector of keys wrappers.
   * N.B. these keys are **mutable** and should never be used in a HashMap.
   * Always clone the key wrapper to obtain an immutable keywrapper suitable
   * to use a key in a HashMap.
   */
  private VectorHashKeyWrapper[] vectorHashKeyWrappers;

  /**
   * The fixed size of the key wrappers.
   */
  private int keysFixedSize;

   /**
   * Returns the compiled fixed size for the key wrappers.
   * @return
   */
  public int getKeysFixedSize() {
    return keysFixedSize;
  }

  /**
   * Accessor for the batch-sized array of key wrappers.
   */
  public VectorHashKeyWrapper[] getVectorHashKeyWrappers() {
    return vectorHashKeyWrappers;
  }

  /**
   * Processes a batch:
   * <ul>
   * <li>Evaluates each key vector expression.</li>
   * <li>Copies out each key's primitive values into the key wrappers</li>
   * <li>computes the hashcode of the key wrappers</li>
   * </ul>
   * @param batch
   * @throws HiveException
   */
  public void evaluateBatch(VectorizedRowBatch batch) throws HiveException {

    for(int i=0;i<batch.size;++i) {
      vectorHashKeyWrappers[i].clearIsNull();
    }

    int keyIndex;
    int columnIndex;
    for(int i = 0; i< longIndices.length; ++i) {
      keyIndex = longIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      LongColumnVector columnVector = (LongColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignLongNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignLongNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignLongNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignLongNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignLongNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignLongNullsNoRepeatingSelection (keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented Long null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<doubleIndices.length; ++i) {
      keyIndex = doubleIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      DoubleColumnVector columnVector = (DoubleColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDoubleNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDoubleNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignDoubleNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDoubleNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignDoubleNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDoubleNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented Double null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<stringIndices.length; ++i) {
      keyIndex = stringIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      BytesColumnVector columnVector = (BytesColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignStringNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignStringNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignStringNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignStringNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignStringNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignStringNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented String null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<decimalIndices.length; ++i) {
      keyIndex = decimalIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      DecimalColumnVector columnVector = (DecimalColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDecimalNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDecimalNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignDecimalNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDecimalNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignDecimalNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDecimalNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented Decimal null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<timestampIndices.length; ++i) {
      keyIndex = timestampIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      TimestampColumnVector columnVector = (TimestampColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignTimestampNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignTimestampNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignTimestampNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignTimestampNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignTimestampNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignTimestampNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented timestamp null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<intervalDayTimeIndices.length; ++i) {
      keyIndex = intervalDayTimeIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      IntervalDayTimeColumnVector columnVector = (IntervalDayTimeColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignIntervalDayTimeNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignIntervalDayTimeNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignIntervalDayTimeNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignIntervalDayTimeNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignIntervalDayTimeNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignIntervalDayTimeNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented intervalDayTime null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<batch.size;++i) {
      vectorHashKeyWrappers[i].setHashKey();
    }
  }

  public void evaluateBatchGroupingSets(VectorizedRowBatch batch,
      boolean[] groupingSetsOverrideIsNulls) throws HiveException {

    for(int i=0;i<batch.size;++i) {
      vectorHashKeyWrappers[i].clearIsNull();
    }
    int keyIndex;
    int columnIndex;
    for(int i = 0; i< longIndices.length; ++i) {
      keyIndex = longIndices[i];
      if (groupingSetsOverrideIsNulls[keyIndex]) {
        final int batchSize = batch.size;
        for(int r = 0; r < batchSize; ++r) {
          vectorHashKeyWrappers[r].assignNullLong(keyIndex, i);
        }
        continue;
      }
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      LongColumnVector columnVector = (LongColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignLongNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignLongNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignLongNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignLongNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignLongNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignLongNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented Long null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<doubleIndices.length; ++i) {
      keyIndex = doubleIndices[i];
      if (groupingSetsOverrideIsNulls[keyIndex]) {
        final int batchSize = batch.size;
        for(int r = 0; r < batchSize; ++r) {
          vectorHashKeyWrappers[r].assignNullDouble(keyIndex, i);
        }
        continue;
      }
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      DoubleColumnVector columnVector = (DoubleColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDoubleNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDoubleNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignDoubleNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDoubleNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignDoubleNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDoubleNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented Double null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<stringIndices.length; ++i) {
      keyIndex = stringIndices[i];
      if (groupingSetsOverrideIsNulls[keyIndex]) {
        final int batchSize = batch.size;
        for(int r = 0; r < batchSize; ++r) {
          vectorHashKeyWrappers[r].assignNullString(keyIndex, i);
        }
        continue;
      }
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      BytesColumnVector columnVector = (BytesColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignStringNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignStringNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignStringNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignStringNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignStringNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignStringNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented String null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<decimalIndices.length; ++i) {
      keyIndex = decimalIndices[i];
      if (groupingSetsOverrideIsNulls[keyIndex]) {
        final int batchSize = batch.size;
        for(int r = 0; r < batchSize; ++r) {
          vectorHashKeyWrappers[r].assignNullDecimal(keyIndex, i);
        }
        continue;
      }
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      DecimalColumnVector columnVector = (DecimalColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDecimalNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDecimalNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignDecimalNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignDecimalNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignDecimalNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignDecimalNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented Decimal null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<timestampIndices.length; ++i) {
      keyIndex = timestampIndices[i];
      if (groupingSetsOverrideIsNulls[keyIndex]) {
        final int batchSize = batch.size;
        for(int r = 0; r < batchSize; ++r) {
          vectorHashKeyWrappers[r].assignNullTimestamp(keyIndex, i);
        }
        continue;
      }
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      TimestampColumnVector columnVector = (TimestampColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignTimestampNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignTimestampNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignTimestampNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignTimestampNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignTimestampNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignTimestampNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented timestamp null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<intervalDayTimeIndices.length; ++i) {
      keyIndex = intervalDayTimeIndices[i];
      if (groupingSetsOverrideIsNulls[keyIndex]) {
        final int batchSize = batch.size;
        for(int r = 0; r < batchSize; ++r) {
          vectorHashKeyWrappers[r].assignNullIntervalDayTime(keyIndex, i);
        }
        continue;
      }
      columnIndex = keyExpressions[keyIndex].getOutputColumn();
      IntervalDayTimeColumnVector columnVector = (IntervalDayTimeColumnVector) batch.cols[columnIndex];
      if (columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignIntervalDayTimeNoNullsNoRepeatingNoSelection(i, batch.size, columnVector);
      } else if (columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignIntervalDayTimeNoNullsNoRepeatingSelection(i, batch.size, columnVector, batch.selected);
      } else if (columnVector.noNulls && columnVector.isRepeating) {
        assignIntervalDayTimeNoNullsRepeating(i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && !batch.selectedInUse) {
        assignIntervalDayTimeNullsNoRepeatingNoSelection(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && columnVector.isRepeating) {
        assignIntervalDayTimeNullsRepeating(keyIndex, i, batch.size, columnVector);
      } else if (!columnVector.noNulls && !columnVector.isRepeating && batch.selectedInUse) {
        assignIntervalDayTimeNullsNoRepeatingSelection(keyIndex, i, batch.size, columnVector, batch.selected);
      } else {
        throw new HiveException (String.format(
            "Unimplemented intervalDayTime null/repeat/selected combination %b/%b/%b",
            columnVector.noNulls, columnVector.isRepeating, batch.selectedInUse));
      }
    }
    for(int i=0;i<batch.size;++i) {
      vectorHashKeyWrappers[i].setHashKey();
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for string type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignStringNullsNoRepeatingSelection(int keyIndex, int index, int size,
      BytesColumnVector columnVector, int[] selected) {
    for(int i=0; i<size; ++i) {
      int row = selected[i];
      if (columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignNullString(keyIndex, index);
      } else {
        vectorHashKeyWrappers[i].assignString(
            index,
            columnVector.vector[row],
            columnVector.start[row],
            columnVector.length[row]);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, possible nulls, repeat values.
   */
  private void assignStringNullsRepeating(int keyIndex, int index, int size, BytesColumnVector columnVector) {
    if (columnVector.isNull[0]) {
      for(int i = 0; i < size; ++i) {
        vectorHashKeyWrappers[i].assignNullString(keyIndex, index);
      }
    } else {
      for(int i = 0; i < size; ++i) {
        vectorHashKeyWrappers[i].assignString(
            index,
            columnVector.vector[0],
            columnVector.start[0],
            columnVector.length[0]);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for string type, possible nulls, no repeat values, no selection vector.
   */
  private void assignStringNullsNoRepeatingNoSelection(int keyIndex, int index, int size,
      BytesColumnVector columnVector) {
    for(int i=0; i<size; ++i) {
      if (columnVector.isNull[i]) {
        vectorHashKeyWrappers[i].assignNullString(keyIndex, index);
      } else {
        vectorHashKeyWrappers[i].assignString(
            index,
            columnVector.vector[i],
            columnVector.start[i],
            columnVector.length[i]);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, repeat values, no selection vector.
   */
  private void assignStringNoNullsRepeating(int index, int size,
      BytesColumnVector columnVector) {
    for(int i = 0; i < size; ++i) {
      vectorHashKeyWrappers[i].assignString(
          index,
          columnVector.vector[0],
          columnVector.start[0],
          columnVector.length[0]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, batch selection vector.
   */
  private void assignStringNoNullsNoRepeatingSelection(int index, int size,
      BytesColumnVector columnVector, int[] selected) {
    for(int i=0; i<size; ++i) {
      int row = selected[i];
      vectorHashKeyWrappers[i].assignString(
          index,
          columnVector.vector[row],
          columnVector.start[row],
          columnVector.length[row]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, no selection vector.
   */
  private void assignStringNoNullsNoRepeatingNoSelection(int index, int size,
      BytesColumnVector columnVector) {
    for(int i=0; i<size; ++i) {
      vectorHashKeyWrappers[i].assignString(
          index,
          columnVector.vector[i],
          columnVector.start[i],
          columnVector.length[i]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignDoubleNullsNoRepeatingSelection(int keyIndex, int index, int size,
      DoubleColumnVector columnVector, int[] selected) {
    for(int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignDouble(index, columnVector.vector[row]);
      } else {
        vectorHashKeyWrappers[i].assignNullDouble(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Double type, repeat null values.
   */
  private void assignDoubleNullsRepeating(int keyIndex, int index, int size,
      DoubleColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignNullDouble(keyIndex, index);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Double type, possible nulls, repeat values.
   */
  private void assignDoubleNullsNoRepeatingNoSelection(int keyIndex, int index, int size,
      DoubleColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        vectorHashKeyWrappers[r].assignDouble(index, columnVector.vector[r]);
      } else {
        vectorHashKeyWrappers[r].assignNullDouble(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, repeat values, no selection vector.
   */
  private void assignDoubleNoNullsRepeating(int index, int size, DoubleColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDouble(index, columnVector.vector[0]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, batch selection vector.
   */
  private void assignDoubleNoNullsNoRepeatingSelection(int index, int size,
      DoubleColumnVector columnVector, int[] selected) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDouble(index, columnVector.vector[selected[r]]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, no selection vector.
   */
  private void assignDoubleNoNullsNoRepeatingNoSelection(int index, int size,
      DoubleColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDouble(index, columnVector.vector[r]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignLongNullsNoRepeatingSelection(int keyIndex, int index, int size,
      LongColumnVector columnVector, int[] selected) {
    for(int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        vectorHashKeyWrappers[i].assignNullLong(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, repeating nulls.
   */
  private void assignLongNullsRepeating(int keyIndex, int index, int size,
      LongColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignNullLong(keyIndex, index);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, possible nulls, no repeat values, no selection vector.
   */
  private void assignLongNullsNoRepeatingNoSelection(int keyIndex, int index, int size,
      LongColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        vectorHashKeyWrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        vectorHashKeyWrappers[r].assignNullLong(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, repeat values, no selection vector.
   */
  private void assignLongNoNullsRepeating(int index, int size, LongColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, batch selection vector.
   */
  private void assignLongNoNullsNoRepeatingSelection(int index, int size,
      LongColumnVector columnVector, int[] selected) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for double type, no nulls, no repeat values, no selection vector.
   */
  private void assignLongNoNullsNoRepeatingNoSelection(int index, int size,
      LongColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Decimal type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignDecimalNullsNoRepeatingSelection(int keyIndex, int index, int size,
      DecimalColumnVector columnVector, int[] selected) {
    for(int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignDecimal(index, columnVector.vector[row]);
      } else {
        vectorHashKeyWrappers[i].assignNullDecimal(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Decimal type, repeat null values.
   */
  private void assignDecimalNullsRepeating(int keyIndex, int index, int size,
      DecimalColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignNullDecimal(keyIndex, index);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Decimal type, possible nulls, repeat values.
   */
  private void assignDecimalNullsNoRepeatingNoSelection(int keyIndex, int index, int size,
      DecimalColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        vectorHashKeyWrappers[r].assignDecimal(index, columnVector.vector[r]);
      } else {
        vectorHashKeyWrappers[r].assignNullDecimal(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Decimal type, no nulls, repeat values, no selection vector.
   */
  private void assignDecimalNoNullsRepeating(int index, int size, DecimalColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDecimal(index, columnVector.vector[0]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Decimal type, no nulls, no repeat values, batch selection vector.
   */
  private void assignDecimalNoNullsNoRepeatingSelection(int index, int size,
      DecimalColumnVector columnVector, int[] selected) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDecimal(index, columnVector.vector[selected[r]]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Decimal type, no nulls, no repeat values, no selection vector.
   */
  private void assignDecimalNoNullsNoRepeatingNoSelection(int index, int size,
      DecimalColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignDecimal(index, columnVector.vector[r]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Timestamp type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignTimestampNullsNoRepeatingSelection(int keyIndex, int index, int size,
      TimestampColumnVector columnVector, int[] selected) {
    for(int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignTimestamp(index, columnVector, row);
      } else {
        vectorHashKeyWrappers[i].assignNullTimestamp(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Timestamp type, repeat null values.
   */
  private void assignTimestampNullsRepeating(int keyIndex, int index, int size,
      TimestampColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignNullTimestamp(keyIndex, index);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Timestamp type, possible nulls, repeat values.
   */
  private void assignTimestampNullsNoRepeatingNoSelection(int keyIndex, int index, int size,
      TimestampColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        vectorHashKeyWrappers[r].assignTimestamp(index, columnVector, r);
      } else {
        vectorHashKeyWrappers[r].assignNullTimestamp(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Timestamp type, no nulls, repeat values, no selection vector.
   */
  private void assignTimestampNoNullsRepeating(int index, int size, TimestampColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignTimestamp(index, columnVector, 0);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Timestamp type, no nulls, no repeat values, batch selection vector.
   */
  private void assignTimestampNoNullsNoRepeatingSelection(int index, int size,
      TimestampColumnVector columnVector, int[] selected) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignTimestamp(index, columnVector, selected[r]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for Timestamp type, no nulls, no repeat values, no selection vector.
   */
  private void assignTimestampNoNullsNoRepeatingNoSelection(int index, int size,
      TimestampColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignTimestamp(index, columnVector, r);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for IntervalDayTime type, possible nulls, no repeat values, batch selection vector.
   */
  private void assignIntervalDayTimeNullsNoRepeatingSelection(int keyIndex, int index, int size,
      IntervalDayTimeColumnVector columnVector, int[] selected) {
    for(int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        vectorHashKeyWrappers[i].assignIntervalDayTime(index, columnVector, row);
      } else {
        vectorHashKeyWrappers[i].assignNullIntervalDayTime(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for IntervalDayTime type, repeat null values.
   */
  private void assignIntervalDayTimeNullsRepeating(int keyIndex, int index, int size,
      IntervalDayTimeColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignNullIntervalDayTime(keyIndex, index);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for IntervalDayTime type, possible nulls, repeat values.
   */
  private void assignIntervalDayTimeNullsNoRepeatingNoSelection(int keyIndex, int index, int size,
      IntervalDayTimeColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        vectorHashKeyWrappers[r].assignIntervalDayTime(index, columnVector, r);
      } else {
        vectorHashKeyWrappers[r].assignNullIntervalDayTime(keyIndex, index);
      }
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for IntervalDayTime type, no nulls, repeat values, no selection vector.
   */
  private void assignIntervalDayTimeNoNullsRepeating(int index, int size, IntervalDayTimeColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignIntervalDayTime(index, columnVector, 0);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for IntervalDayTime type, no nulls, no repeat values, batch selection vector.
   */
  private void assignIntervalDayTimeNoNullsNoRepeatingSelection(int index, int size,
      IntervalDayTimeColumnVector columnVector, int[] selected) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignIntervalDayTime(index, columnVector, selected[r]);
    }
  }

  /**
   * Helper method to assign values from a vector column into the key wrapper.
   * Optimized for IntervalDayTime type, no nulls, no repeat values, no selection vector.
   */
  private void assignIntervalDayTimeNoNullsNoRepeatingNoSelection(int index, int size,
      IntervalDayTimeColumnVector columnVector) {
    for(int r = 0; r < size; ++r) {
      vectorHashKeyWrappers[r].assignIntervalDayTime(index, columnVector, r);
    }
  }

  public static VectorHashKeyWrapperBatch compileKeyWrapperBatch(VectorExpression[] keyExpressions)
      throws HiveException
  {

    final int size = keyExpressions.length;
    ColumnVector.Type[] columnVectorTypes = new ColumnVector.Type[size];
    for (int i = 0; i < size; i++) {
      String typeName = VectorizationContext.mapTypeNameSynonyms(keyExpressions[i].getOutputType());
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      columnVectorTypes[i] = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);
    }
    return compileKeyWrapperBatch(keyExpressions, columnVectorTypes);
  }

  /**
   * Prepares a VectorHashKeyWrapperBatch to work for a specific set of keys.
   * Computes the fast access lookup indices, preallocates all needed internal arrays.
   * This step is done only once per query, not once per batch. The information computed now
   * will be used to generate proper individual VectorKeyHashWrapper objects.
   */
  public static VectorHashKeyWrapperBatch compileKeyWrapperBatch(VectorExpression[] keyExpressions,
      ColumnVector.Type[] columnVectorTypes)
    throws HiveException {
    VectorHashKeyWrapperBatch compiledKeyWrapperBatch = new VectorHashKeyWrapperBatch(keyExpressions.length);
    compiledKeyWrapperBatch.keyExpressions = keyExpressions;

    compiledKeyWrapperBatch.keysFixedSize = 0;

    // Inspect the output type of each key expression.
    for(int i=0; i < columnVectorTypes.length; ++i) {
      compiledKeyWrapperBatch.addKey(columnVectorTypes[i]);
    }
    compiledKeyWrapperBatch.finishAdding();

    compiledKeyWrapperBatch.vectorHashKeyWrappers =
        new VectorHashKeyWrapper[VectorizedRowBatch.DEFAULT_SIZE];
    for(int i=0;i<VectorizedRowBatch.DEFAULT_SIZE; ++i) {
      compiledKeyWrapperBatch.vectorHashKeyWrappers[i] = 
          compiledKeyWrapperBatch.allocateKeyWrapper();
    }

    JavaDataModel model = JavaDataModel.get();

    // Compute the fixed size overhead for the keys
    // start with the keywrapper itself
    compiledKeyWrapperBatch.keysFixedSize += JavaDataModel.alignUp(
        model.object() +
        model.ref() * MODEL_REFERENCES_COUNT +
        model.primitive1(),
        model.memoryAlign());

    // Now add the key wrapper arrays
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForLongArrayOfSize(compiledKeyWrapperBatch.longIndices.length);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForDoubleArrayOfSize(compiledKeyWrapperBatch.doubleIndices.length);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForObjectArrayOfSize(compiledKeyWrapperBatch.stringIndices.length);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForObjectArrayOfSize(compiledKeyWrapperBatch.decimalIndices.length);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForObjectArrayOfSize(compiledKeyWrapperBatch.timestampIndices.length);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForObjectArrayOfSize(compiledKeyWrapperBatch.intervalDayTimeIndices.length);
    compiledKeyWrapperBatch.keysFixedSize += model.lengthForIntArrayOfSize(compiledKeyWrapperBatch.longIndices.length) * 2;
    compiledKeyWrapperBatch.keysFixedSize +=
        model.lengthForBooleanArrayOfSize(keyExpressions.length);

    return compiledKeyWrapperBatch;
  }

  public VectorHashKeyWrapper allocateKeyWrapper() {
    return VectorHashKeyWrapper.allocate(
        longIndices.length,
        doubleIndices.length,
        stringIndices.length,
        decimalIndices.length,
        timestampIndices.length,
        intervalDayTimeIndices.length,
        keyCount);
  }

  /**
   * Get the row-mode writable object value of a key from a key wrapper
   * @param keyOutputWriter
   */
  public Object getWritableKeyValue(VectorHashKeyWrapper kw, int keyIndex,
      VectorExpressionWriter keyOutputWriter)
    throws HiveException {

    if (kw.isNull(keyIndex)) {
      return null;
    }

    ColumnVector.Type columnVectorType = columnVectorTypes[keyIndex];
    int columnTypeSpecificIndex = columnTypeSpecificIndices[keyIndex];

    switch (columnVectorType) {
    case LONG:
      return keyOutputWriter.writeValue(
          kw.getLongValue(columnTypeSpecificIndex));
    case DOUBLE:
      return keyOutputWriter.writeValue(
          kw.getDoubleValue(columnTypeSpecificIndex));
    case BYTES:
      return keyOutputWriter.writeValue(
          kw.getBytes(columnTypeSpecificIndex),
          kw.getByteStart(columnTypeSpecificIndex),
          kw.getByteLength(columnTypeSpecificIndex));
    case DECIMAL:
      return keyOutputWriter.writeValue(
          kw.getDecimal(columnTypeSpecificIndex));
    case TIMESTAMP:
      return keyOutputWriter.writeValue(
          kw.getTimestamp(columnTypeSpecificIndex));
    case INTERVAL_DAY_TIME:
      return keyOutputWriter.writeValue(
          kw.getIntervalDayTime(columnTypeSpecificIndex));
    default:
      throw new HiveException("Unexpected column vector type " + columnVectorType);
    }
  }

  public int getVariableSize(int batchSize) {
    int variableSize = 0;
    if ( 0 < stringIndices.length) {
      for (int k=0; k<batchSize; ++k) {
        VectorHashKeyWrapper hkw = vectorHashKeyWrappers[k];
        variableSize += hkw.getVariableSize();
      }
    }
    return variableSize;
  }
}

