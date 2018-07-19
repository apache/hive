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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

/**
 * Utility functions to handle null propagation.
 */
public class NullUtil {

  /**
   * Set the data value for all NULL entries to the designated NULL_VALUE.
   */
  public static void setNullDataEntriesLong(
      LongColumnVector v, boolean selectedInUse, int[] sel, int n) {
    if (v.noNulls) {
      return;
    } else if (v.isRepeating && v.isNull[0]) {
      v.vector[0] = LongColumnVector.NULL_VALUE;
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if(v.isNull[i]) {
          v.vector[i] = LongColumnVector.NULL_VALUE;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if(v.isNull[i]) {
          v.vector[i] = LongColumnVector.NULL_VALUE;
        }
      }
    }
  }

  /**
   * Set the data value for all NULL entries to the designated NULL_VALUE.
   */
  public static void setNullDataEntriesBytes(
      BytesColumnVector v, boolean selectedInUse, int[] sel, int n) {
    if (v.noNulls) {
      return;
    } else if (v.isRepeating && v.isNull[0]) {
      v.vector[0] = null;
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if(v.isNull[i]) {
          v.vector[i] = null;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if(v.isNull[i]) {
          v.vector[i] = null;
        }
      }
    }
  }

  /**
   * Set the data value for all NULL entries to the designated NULL_VALUE.
   */
  public static void setNullDataEntriesTimestamp(
      TimestampColumnVector v, boolean selectedInUse, int[] sel, int n) {
    if (v.noNulls) {
      return;
    } else if (v.isRepeating && v.isNull[0]) {
      v.setNullValue(0);
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if(v.isNull[i]) {
          v.setNullValue(i);
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if(v.isNull[i]) {
          v.setNullValue(i);
        }
      }
    }
  }

  /**
   * Set the data value for all NULL entries to the designated NULL_VALUE.
   */
  public static void setNullDataEntriesIntervalDayTime(
      IntervalDayTimeColumnVector v, boolean selectedInUse, int[] sel, int n) {
    if (v.noNulls) {
      return;
    } else if (v.isRepeating && v.isNull[0]) {
      v.setNullValue(0);
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if(v.isNull[i]) {
          v.setNullValue(i);
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if(v.isNull[i]) {
          v.setNullValue(i);
        }
      }
    }
  }

  // for use by Column-Scalar and Scalar-Column arithmetic for null propagation
  public static void setNullOutputEntriesColScalar(
      ColumnVector v, boolean selectedInUse, int[] sel, int n) {
    if (v instanceof DoubleColumnVector) {

      // No need to set null data entries because the input NaN values
      // will automatically propagate to the output.
      return;
    } else if (v instanceof LongColumnVector) {
      setNullDataEntriesLong((LongColumnVector) v, selectedInUse, sel, n);
    } else if (v instanceof TimestampColumnVector){
      setNullDataEntriesTimestamp((TimestampColumnVector) v, selectedInUse, sel, n);
    }
  }

  /**
   * Set the data value for all NULL entries to NaN
   */
  public static void setNullDataEntriesDouble(
      DoubleColumnVector v, boolean selectedInUse, int[] sel, int n) {
    if (v.noNulls) {
      return;
    } else if (v.isRepeating && v.isNull[0]) {
      v.vector[0] = DoubleColumnVector.NULL_VALUE;
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if (v.isNull[i]) {
          v.vector[i] = DoubleColumnVector.NULL_VALUE;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if (v.isNull[i]) {
          v.vector[i] = DoubleColumnVector.NULL_VALUE;
        }
      }
    }
  }

  /**
   * Set all the entries for which denoms array contains zeroes to NULL; sets all the data
   * values for NULL entries for DoubleColumnVector.NULL_VALUE.
   */
  public static void setNullAndDivBy0DataEntriesDouble(
      DoubleColumnVector v, boolean selectedInUse, int[] sel, int n, LongColumnVector denoms) {
    assert v.isRepeating || !denoms.isRepeating;
    final boolean realNulls = !v.noNulls;
    v.noNulls = false;
    long[] vector = denoms.vector;
    if (v.isRepeating && (v.isNull[0] = ((realNulls && v.isNull[0]) || vector[0] == 0))) {
      v.vector[0] = DoubleColumnVector.NULL_VALUE;
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if (v.isNull[i] = ((realNulls && v.isNull[i]) || vector[i] == 0)) {
          v.vector[i] = DoubleColumnVector.NULL_VALUE;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if (v.isNull[i] = ((realNulls && v.isNull[i]) || vector[i] == 0)) {
          v.vector[i] = DoubleColumnVector.NULL_VALUE;
        }
      }
    }
  }

  /**
   * Set all the entries for which denoms array contains zeroes to NULL; sets all the data
   * values for NULL entries for DoubleColumnVector.NULL_VALUE.
   */
  public static void setNullAndDivBy0DataEntriesDouble(
      DoubleColumnVector v, boolean selectedInUse, int[] sel, int n, DoubleColumnVector denoms) {
    assert v.isRepeating || !denoms.isRepeating;
    final boolean realNulls = !v.noNulls;
    v.noNulls = false;
    double[] vector = denoms.vector;
    if (v.isRepeating && (v.isNull[0] = ((realNulls && v.isNull[0]) || vector[0] == 0))) {
      v.vector[0] = DoubleColumnVector.NULL_VALUE;
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if (v.isNull[i] = ((realNulls && v.isNull[i]) || vector[i] == 0)) {
          v.vector[i] = DoubleColumnVector.NULL_VALUE;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if (v.isNull[i] = ((realNulls && v.isNull[i]) || vector[i] == 0)) {
          v.vector[i] = DoubleColumnVector.NULL_VALUE;
        }
      }
    }
  }

  /**
   * Set all the entries for which denoms array contains zeroes to NULL; sets all the data
   * values for NULL entries for LongColumnVector.NULL_VALUE.
   */
  public static void setNullAndDivBy0DataEntriesLong(
      LongColumnVector v, boolean selectedInUse, int[] sel, int n, LongColumnVector denoms) {
    assert v.isRepeating || !denoms.isRepeating;
    v.noNulls = false;
    long[] vector = denoms.vector;
    if (v.isRepeating && (v.isNull[0] = (v.isNull[0] || vector[0] == 0))) {
      v.vector[0] = LongColumnVector.NULL_VALUE;
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if (v.isNull[i] = (v.isNull[i] || vector[i] == 0)) {
          v.vector[i] = LongColumnVector.NULL_VALUE;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if (v.isNull[i] = (v.isNull[i] || vector[i] == 0)) {
          v.vector[i] = LongColumnVector.NULL_VALUE;
        }
      }
    }
  }

  /**
   * Set all the entries for which denoms array contains zeroes to NULL; sets all the data
   * values for NULL entries for LongColumnVector.NULL_VALUE.
   */
  public static void setNullAndDivBy0DataEntriesLong(
      LongColumnVector v, boolean selectedInUse, int[] sel, int n, DoubleColumnVector denoms) {
    assert v.isRepeating || !denoms.isRepeating;
    v.noNulls = false;
    double[] vector = denoms.vector;
    if (v.isRepeating && (v.isNull[0] = (v.isNull[0] || vector[0] == 0))) {
      v.vector[0] = LongColumnVector.NULL_VALUE;
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if (v.isNull[i] = (v.isNull[i] || vector[i] == 0)) {
          v.vector[i] = LongColumnVector.NULL_VALUE;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if (v.isNull[i] = (v.isNull[i] || vector[i] == 0)) {
          v.vector[i] = LongColumnVector.NULL_VALUE;
        }
      }
    }
  }

  /*
   * Propagate null values for a two-input operator and set isRepeating and noNulls appropriately.
   */
  public static void propagateNullsColCol(ColumnVector inputColVector1,
      ColumnVector inputColVector2, ColumnVector outputColVector, int[] sel,
      int n, boolean selectedInUse) {

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector1.noNulls && inputColVector2.noNulls) {
      if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
        outputColVector.isNull[0] = false;
        outputColVector.isRepeating = true;
      } else {
        if (selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputColVector.isNull[i] = false;
          }
        } else {
          Arrays.fill(outputColVector.isNull, 0, n, false);
        }
      }
    } else if (inputColVector1.noNulls && !inputColVector2.noNulls) {

      if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
        if (!inputColVector2.isNull[0]) {
          outputColVector.isNull[0] = false;
        } else {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
        }
        outputColVector.isRepeating = true;
      } else if (inputColVector2.isRepeating) {
        if (!inputColVector2.isNull[0]) {
          if (selectedInUse) {
            for(int j = 0; j != n; j++) {
              int i = sel[j];
              outputColVector.isNull[i] = false;
            }
          } else {
            Arrays.fill(outputColVector.isNull, 0, n, false);
          }
        } else {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
          outputColVector.isRepeating = true;   // Because every value will be NULL.
        }
      } else {
        outputColVector.noNulls = false;
        if (selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputColVector.isNull[i] = inputColVector2.isNull[i];
          }
        } else {
          System.arraycopy(inputColVector2.isNull, 0, outputColVector.isNull, 0, n);
        }
      }
    } else if (!inputColVector1.noNulls && inputColVector2.noNulls) {

      if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
        if (!inputColVector1.isNull[0]) {
          outputColVector.isNull[0] = false;
        } else {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
        }
        outputColVector.isRepeating = true;
      } else if (inputColVector1.isRepeating) {
        if (!inputColVector1.isNull[0]) {
          if (selectedInUse) {
            for(int j = 0; j != n; j++) {
              int i = sel[j];
              outputColVector.isNull[i] = false;
            }
          } else {
            Arrays.fill(outputColVector.isNull, 0, n, false);
          }
        } else {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
          outputColVector.isRepeating = true;   // Because every value will be NULL.
        }
      } else {
        outputColVector.noNulls = false;
        if (selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputColVector.isNull[i] = inputColVector1.isNull[i];
          }
        } else {
          System.arraycopy(inputColVector1.isNull, 0, outputColVector.isNull, 0, n);
        }
      }
    } else if (!inputColVector1.noNulls && !inputColVector2.noNulls) {

      if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
        if (!inputColVector1.isNull[0] && !inputColVector2.isNull[0]) {
          outputColVector.isNull[0] = false;
        } else {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
        }
        outputColVector.isRepeating = true;
      } else if (inputColVector1.isRepeating && !inputColVector2.isRepeating) {

        if (inputColVector1.isNull[0]) {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
          outputColVector.isRepeating = true;   // Because every value will be NULL.
        } else {
          outputColVector.noNulls = false;
          if (selectedInUse) {
             for(int j = 0; j != n; j++) {
               int i = sel[j];
               outputColVector.isNull[i] = inputColVector2.isNull[i];
             }
          } else {

            // copy nulls from the non-repeating side
            System.arraycopy(inputColVector2.isNull, 0, outputColVector.isNull, 0, n);
          }
        }
      } else if (!inputColVector1.isRepeating && inputColVector2.isRepeating) {
        if (inputColVector2.isNull[0]) {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
          outputColVector.isRepeating = true;   // Because every value will be NULL.
        } else {
          outputColVector.noNulls = false;
          if (selectedInUse) {
             for(int j = 0; j != n; j++) {
               int i = sel[j];
               outputColVector.isNull[i] = inputColVector1.isNull[i];
             }
          } else {
            // copy nulls from the non-repeating side
            System.arraycopy(inputColVector1.isNull, 0, outputColVector.isNull, 0, n);
          }
        }
      } else {                      // neither side is repeating
        outputColVector.noNulls = false;
        if (selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputColVector.isNull[i] = inputColVector1.isNull[i] || inputColVector2.isNull[i];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputColVector.isNull[i] = inputColVector1.isNull[i] || inputColVector2.isNull[i];
          }
        }
      }
    }
  }

  /**
   * Follow the convention that null decimal values are internally set to the smallest
   * positive value available. Prevents accidental zero-divide later in expression
   * evaluation.
   */
  public static void setNullDataEntriesDecimal(
      DecimalColumnVector v, boolean selectedInUse, int[] sel,
      int n) {
    if (v.noNulls) {
      return;
    } else if (v.isRepeating && v.isNull[0]) {
      v.setNullDataValue(0);
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if(v.isNull[i]) {
          v.setNullDataValue(i);
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if(v.isNull[i]) {
          v.setNullDataValue(i);
        }
      }
    }
  }

  // Initialize any entries that could be used in an output vector to have false for null value.
  public static void initOutputNullsToFalse(ColumnVector v, boolean isRepeating, boolean selectedInUse,
      int[] sel, int n) {
    if (v.isRepeating) {
      v.isNull[0] = false;
      return;
    }

    if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        v.isNull[i] = false;
      }
    } else {
      Arrays.fill(v.isNull, 0, n, false);
    }
  }

  /**
   * Filter out rows with null values. Return the number of rows in the batch.
   */
  public static int filterNulls(ColumnVector v, boolean selectedInUse, int[] sel, int n) {
    int newSize = 0;

    if (v.noNulls) {

      // no rows will be filtered
      return n;
    }

    if (v.isRepeating) {

      // all rows are filtered if repeating null, otherwise no rows are filtered
      return v.isNull[0] ? 0 : n;
    }

    if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if (!v.isNull[i]) {
          sel[newSize++] = i;
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if (!v.isNull[i]) {
          sel[newSize++] = i;
        }
      }
    }
    return newSize;
  }
}
