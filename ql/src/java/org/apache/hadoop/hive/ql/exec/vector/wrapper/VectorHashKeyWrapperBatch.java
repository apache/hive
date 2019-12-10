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

package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

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
  private VectorHashKeyWrapperBase[] vectorHashKeyWrappers;

  /**
   * The fixed size of the key wrappers.
   */
  private int keysFixedSize;

  /**
   * Shared hashcontext for all keys in this batch
   */
  private final VectorHashKeyWrapperBase.HashContext hashCtx = new VectorHashKeyWrapperBase.HashContext();

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
  public VectorHashKeyWrapperBase[] getVectorHashKeyWrappers() {
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

    if (keyCount == 0) {
      // all keywrappers must be EmptyVectorHashKeyWrapper
      return;
    }

    for(int i=0;i<batch.size;++i) {
      vectorHashKeyWrappers[i].clearIsNull();
    }

    int keyIndex;
    int columnIndex;
    for(int i = 0; i< longIndices.length; ++i) {
      keyIndex = longIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      LongColumnVector columnVector = (LongColumnVector) batch.cols[columnIndex];

      evaluateLongColumnVector(batch, columnVector, keyIndex, i);
    }

    for(int i=0;i<doubleIndices.length; ++i) {
      keyIndex = doubleIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      DoubleColumnVector columnVector = (DoubleColumnVector) batch.cols[columnIndex];

      evaluateDoubleColumnVector(batch, columnVector, keyIndex, i);
    }

    for(int i=0;i<stringIndices.length; ++i) {
      keyIndex = stringIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      BytesColumnVector columnVector = (BytesColumnVector) batch.cols[columnIndex];

      evaluateStringColumnVector(batch, columnVector, keyIndex, i);
    }

    for(int i=0;i<decimalIndices.length; ++i) {
      keyIndex = decimalIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      DecimalColumnVector columnVector = (DecimalColumnVector) batch.cols[columnIndex];

      evaluateDecimalColumnVector(batch, columnVector, keyIndex, i);
    }

    for(int i=0;i<timestampIndices.length; ++i) {
      keyIndex = timestampIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      TimestampColumnVector columnVector = (TimestampColumnVector) batch.cols[columnIndex];

      evaluateTimestampColumnVector(batch, columnVector, keyIndex, i);
    }

    for(int i=0;i<intervalDayTimeIndices.length; ++i) {
      keyIndex = intervalDayTimeIndices[i];
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      IntervalDayTimeColumnVector columnVector = (IntervalDayTimeColumnVector) batch.cols[columnIndex];

      evaluateIntervalDayTimeColumnVector(batch, columnVector, keyIndex, i);
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
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      LongColumnVector columnVector = (LongColumnVector) batch.cols[columnIndex];

      evaluateLongColumnVector(batch, columnVector, keyIndex, i);
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
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      DoubleColumnVector columnVector = (DoubleColumnVector) batch.cols[columnIndex];

      evaluateDoubleColumnVector(batch, columnVector, keyIndex, i);
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
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      BytesColumnVector columnVector = (BytesColumnVector) batch.cols[columnIndex];

      evaluateStringColumnVector(batch, columnVector, keyIndex, i);
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
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      DecimalColumnVector columnVector = (DecimalColumnVector) batch.cols[columnIndex];

      evaluateDecimalColumnVector(batch, columnVector, keyIndex, i);
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
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      TimestampColumnVector columnVector = (TimestampColumnVector) batch.cols[columnIndex];

      evaluateTimestampColumnVector(batch, columnVector, keyIndex, i);
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
      columnIndex = keyExpressions[keyIndex].getOutputColumnNum();
      IntervalDayTimeColumnVector columnVector = (IntervalDayTimeColumnVector) batch.cols[columnIndex];

      evaluateIntervalDayTimeColumnVector(batch, columnVector, keyIndex, i);
    }

    for(int i=0;i<batch.size;++i) {
      vectorHashKeyWrappers[i].setHashKey();
    }
  }

  private void evaluateLongColumnVector(VectorizedRowBatch batch, LongColumnVector columnVector,
      int keyIndex, int index) {
    if (columnVector.isRepeating) {
      if (columnVector.noNulls || !columnVector.isNull[0]) {
        assignLongNoNullsRepeating(index, batch.size, columnVector);
      } else {
        assignLongNullsRepeating(keyIndex, index, batch.size, columnVector);
      }
    } else if (columnVector.noNulls) {
      if (batch.selectedInUse) {
        assignLongNoNullsNoRepeatingSelection(index, batch.size, columnVector, batch.selected);
      } else {
        assignLongNoNullsNoRepeatingNoSelection(index, batch.size, columnVector);
      }
    } else {
      if (batch.selectedInUse) {
        assignLongNullsNoRepeatingSelection (keyIndex, index, batch.size, columnVector, batch.selected);
      } else {
        assignLongNullsNoRepeatingNoSelection(keyIndex, index, batch.size, columnVector);
      }
    }
  }

  private void evaluateDoubleColumnVector(VectorizedRowBatch batch, DoubleColumnVector columnVector,
      int keyIndex, int index) {
    if (columnVector.isRepeating) {
      if (columnVector.noNulls || !columnVector.isNull[0]) {
        assignDoubleNoNullsRepeating(index, batch.size, columnVector);
      } else {
        assignDoubleNullsRepeating(keyIndex, index, batch.size, columnVector);
      }
    } else if (columnVector.noNulls) {
      if (batch.selectedInUse) {
        assignDoubleNoNullsNoRepeatingSelection(index, batch.size, columnVector, batch.selected);
      } else {
        assignDoubleNoNullsNoRepeatingNoSelection(index, batch.size, columnVector);
      }
    } else {
      if (batch.selectedInUse) {
        assignDoubleNullsNoRepeatingSelection (keyIndex, index, batch.size, columnVector, batch.selected);
      } else {
        assignDoubleNullsNoRepeatingNoSelection(keyIndex, index, batch.size, columnVector);
      }
    }
  }

  private void evaluateStringColumnVector(VectorizedRowBatch batch, BytesColumnVector columnVector,
      int keyIndex, int index) {
    if (columnVector.isRepeating) {
      if (columnVector.noNulls || !columnVector.isNull[0]) {
        assignStringNoNullsRepeating(index, batch.size, columnVector);
      } else {
        assignStringNullsRepeating(keyIndex, index, batch.size, columnVector);
      }
    } else if (columnVector.noNulls) {
      if (batch.selectedInUse) {
        assignStringNoNullsNoRepeatingSelection(index, batch.size, columnVector, batch.selected);
      } else {
        assignStringNoNullsNoRepeatingNoSelection(index, batch.size, columnVector);
      }
    } else {
      if (batch.selectedInUse) {
        assignStringNullsNoRepeatingSelection (keyIndex, index, batch.size, columnVector, batch.selected);
      } else {
        assignStringNullsNoRepeatingNoSelection(keyIndex, index, batch.size, columnVector);
      }
    }
  }

  private void evaluateDecimalColumnVector(VectorizedRowBatch batch, DecimalColumnVector columnVector,
      int keyIndex, int index) {
    if (columnVector.isRepeating) {
      if (columnVector.noNulls || !columnVector.isNull[0]) {
        assignDecimalNoNullsRepeating(index, batch.size, columnVector);
      } else {
        assignDecimalNullsRepeating(keyIndex, index, batch.size, columnVector);
      }
    } else if (columnVector.noNulls) {
      if (batch.selectedInUse) {
        assignDecimalNoNullsNoRepeatingSelection(index, batch.size, columnVector, batch.selected);
      } else {
        assignDecimalNoNullsNoRepeatingNoSelection(index, batch.size, columnVector);
      }
    } else {
      if (batch.selectedInUse) {
        assignDecimalNullsNoRepeatingSelection (keyIndex, index, batch.size, columnVector, batch.selected);
      } else {
        assignDecimalNullsNoRepeatingNoSelection(keyIndex, index, batch.size, columnVector);
      }
    }
  }

  private void evaluateTimestampColumnVector(VectorizedRowBatch batch, TimestampColumnVector columnVector,
      int keyIndex, int index) {
    if (columnVector.isRepeating) {
      if (columnVector.noNulls || !columnVector.isNull[0]) {
        assignTimestampNoNullsRepeating(index, batch.size, columnVector);
      } else {
        assignTimestampNullsRepeating(keyIndex, index, batch.size, columnVector);
      }
    } else if (columnVector.noNulls) {
      if (batch.selectedInUse) {
        assignTimestampNoNullsNoRepeatingSelection(index, batch.size, columnVector, batch.selected);
      } else {
        assignTimestampNoNullsNoRepeatingNoSelection(index, batch.size, columnVector);
      }
    } else {
      if (batch.selectedInUse) {
        assignTimestampNullsNoRepeatingSelection (keyIndex, index, batch.size, columnVector, batch.selected);
      } else {
        assignTimestampNullsNoRepeatingNoSelection(keyIndex, index, batch.size, columnVector);
      }
    }
  }

  private void evaluateIntervalDayTimeColumnVector(VectorizedRowBatch batch, IntervalDayTimeColumnVector columnVector,
      int keyIndex, int index) {
    if (columnVector.isRepeating) {
      if (columnVector.noNulls || !columnVector.isNull[0]) {
        assignIntervalDayTimeNoNullsRepeating(index, batch.size, columnVector);
      } else {
        assignIntervalDayTimeNullsRepeating(keyIndex, index, batch.size, columnVector);
      }
    } else if (columnVector.noNulls) {
      if (batch.selectedInUse) {
        assignIntervalDayTimeNoNullsNoRepeatingSelection(index, batch.size, columnVector, batch.selected);
      } else {
        assignIntervalDayTimeNoNullsNoRepeatingNoSelection(index, batch.size, columnVector);
      }
    } else {
      if (batch.selectedInUse) {
        assignIntervalDayTimeNullsNoRepeatingSelection (keyIndex, index, batch.size, columnVector, batch.selected);
      } else {
        assignIntervalDayTimeNullsNoRepeatingNoSelection(keyIndex, index, batch.size, columnVector);
      }
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
    TypeInfo[] typeInfos = new TypeInfo[size];
    for (int i = 0; i < size; i++) {
      typeInfos[i] = keyExpressions[i].getOutputTypeInfo();
    }
    return compileKeyWrapperBatch(keyExpressions, typeInfos);
  }

  /**
   * Prepares a VectorHashKeyWrapperBatch to work for a specific set of keys.
   * Computes the fast access lookup indices, preallocates all needed internal arrays.
   * This step is done only once per query, not once per batch. The information computed now
   * will be used to generate proper individual VectorKeyHashWrapper objects.
   */
  public static VectorHashKeyWrapperBatch compileKeyWrapperBatch(VectorExpression[] keyExpressions,
      TypeInfo[] typeInfos)
    throws HiveException {
    VectorHashKeyWrapperBatch compiledKeyWrapperBatch = new VectorHashKeyWrapperBatch(keyExpressions.length);
    compiledKeyWrapperBatch.keyExpressions = keyExpressions;

    compiledKeyWrapperBatch.keysFixedSize = 0;

    // Inspect the output type of each key expression.
    for(int i=0; i < typeInfos.length; ++i) {
      compiledKeyWrapperBatch.addKey(typeInfos[i]);
    }
    compiledKeyWrapperBatch.finishAdding();

    compiledKeyWrapperBatch.vectorHashKeyWrappers =
        new VectorHashKeyWrapperBase[VectorizedRowBatch.DEFAULT_SIZE];
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

  public VectorHashKeyWrapperBase allocateKeyWrapper() {
    return VectorHashKeyWrapperFactory.allocate(hashCtx,
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
  public Object getWritableKeyValue(VectorHashKeyWrapperBase kw, int keyIndex,
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
    case DECIMAL_64:
      throw new RuntimeException("Getting writable for DECIMAL_64 not supported");
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

  public void setLongValue(VectorHashKeyWrapperBase kw, int keyIndex, Long value)
    throws HiveException {

    if (columnVectorTypes[keyIndex] != Type.LONG) {
      throw new HiveException("Consistency error: expected LONG type; found: " + columnVectorTypes[keyIndex]);
    }
    int columnTypeSpecificIndex = columnTypeSpecificIndices[keyIndex];

    if (value == null) {
      kw.assignNullLong(keyIndex, columnTypeSpecificIndex);
      return;
    }
    kw.assignLong(keyIndex, columnTypeSpecificIndex, value);
  }

  public void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int keyIndex,
      VectorHashKeyWrapperBase kw)
    throws HiveException {

    ColumnVector colVector = batch.cols[keyIndex];

    if (kw.isNull(keyIndex)) {
      colVector.noNulls = false;
      colVector.isNull[batchIndex] = true;
      return;
    }
    colVector.isNull[batchIndex] = false;

    ColumnVector.Type columnVectorType = columnVectorTypes[keyIndex];
    int columnTypeSpecificIndex = columnTypeSpecificIndices[keyIndex];

    switch (columnVectorType) {
    case LONG:
    case DECIMAL_64:
      ((LongColumnVector) colVector).vector[batchIndex] =
          kw.getLongValue(columnTypeSpecificIndex);
      break;
    case DOUBLE:
      ((DoubleColumnVector) colVector).vector[batchIndex] =
          kw.getDoubleValue(columnTypeSpecificIndex);
      break;
    case BYTES:
      ((BytesColumnVector) colVector).setVal(
          batchIndex,
          kw.getBytes(columnTypeSpecificIndex),
          kw.getByteStart(columnTypeSpecificIndex),
          kw.getByteLength(columnTypeSpecificIndex));
      break;
    case DECIMAL:
      ((DecimalColumnVector) colVector).set(batchIndex,
          kw.getDecimal(columnTypeSpecificIndex));
      break;
    case TIMESTAMP:
      ((TimestampColumnVector) colVector).set(
          batchIndex, kw.getTimestamp(columnTypeSpecificIndex));
      break;
    case INTERVAL_DAY_TIME:
      ((IntervalDayTimeColumnVector) colVector).set(
          batchIndex, kw.getIntervalDayTime(columnTypeSpecificIndex));
      break;
    default:
      throw new HiveException("Unexpected column vector type " + columnVectorType);
    }
  }

  public int getVariableSize(int batchSize) {
    int variableSize = 0;
    if ( 0 < stringIndices.length) {
      for (int k=0; k<batchSize; ++k) {
        VectorHashKeyWrapperBase hkw = vectorHashKeyWrappers[k];
        variableSize += hkw.getVariableSize();
      }
    }
    return variableSize;
  }

  public Comparator<VectorHashKeyWrapperBase> getComparator(String columnSortOrder, String nullOrder) {
    VectorHashKeyWrapperGeneralComparator comparator =
            new VectorHashKeyWrapperGeneralComparator(columnVectorTypes.length);
    for (int i = 0; i < columnVectorTypes.length; ++i) {
      final int columnTypeSpecificIndex = columnTypeSpecificIndices[i];
      ColumnVector.Type columnVectorType = columnVectorTypes[i];
      comparator.addColumnComparator(
              i, columnTypeSpecificIndex, columnVectorType, columnSortOrder.charAt(i), nullOrder.charAt(i));
    }

    return comparator;
  }
}

