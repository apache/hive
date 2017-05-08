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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Class to keep information on a set of typed vector columns.  Used by
 * other classes to efficiently access the set of columns.
 */
public class VectorColumnSetInfo {

  // For simpler access, we make these members protected instead of
  // providing get methods.

  /**
   * indices of LONG primitive keys.
   */
  protected int[] longIndices;

  /**
   * indices of DOUBLE primitive keys.
   */
  protected int[] doubleIndices;

  /**
   * indices of string (byte[]) primitive keys.
   */
  protected int[] stringIndices;

  /**
   * indices of decimal primitive keys.
   */
  protected int[] decimalIndices;

  /**
   * indices of TIMESTAMP primitive keys.
   */
  protected int[] timestampIndices;

  /**
   * indices of INTERVAL_DAY_TIME primitive keys.
   */
  protected int[] intervalDayTimeIndices;

  final protected int keyCount;
  private int addKeyIndex;

  private int addLongIndex;
  private int addDoubleIndex;
  private int addStringIndex;
  private int addDecimalIndex;
  private int addTimestampIndex;
  private int addIntervalDayTimeIndex;

  // Given the keyIndex these arrays return:
  //   The ColumnVector.Type,
  //   The type specific index into longIndices, doubleIndices, etc...
  protected ColumnVector.Type[] columnVectorTypes;
  protected int[] columnTypeSpecificIndices;

  protected VectorColumnSetInfo(int keyCount) {
    this.keyCount = keyCount;
    this.addKeyIndex = 0;

    // We'll over allocate and then shrink the array for each type
    longIndices = new int[this.keyCount];
    addLongIndex = 0;
    doubleIndices = new int[this.keyCount];
    addDoubleIndex  = 0;
    stringIndices = new int[this.keyCount];
    addStringIndex = 0;
    decimalIndices = new int[this.keyCount];
    addDecimalIndex = 0;
    timestampIndices = new int[this.keyCount];
    addTimestampIndex = 0;
    intervalDayTimeIndices = new int[this.keyCount];
    addIntervalDayTimeIndex = 0;

    columnVectorTypes = new ColumnVector.Type[this.keyCount];
    columnTypeSpecificIndices = new int[this.keyCount];
  }


  protected void addKey(ColumnVector.Type columnVectorType) throws HiveException {

    switch (columnVectorType) {
    case LONG:
      longIndices[addLongIndex] = addKeyIndex;
      columnTypeSpecificIndices[addKeyIndex] = addLongIndex++;
      break;
    case DOUBLE:
      doubleIndices[addDoubleIndex] = addKeyIndex;
      columnTypeSpecificIndices[addKeyIndex] = addDoubleIndex++;
      break;
    case BYTES:
      stringIndices[addStringIndex]= addKeyIndex;
      columnTypeSpecificIndices[addKeyIndex] = addStringIndex++;
      break;
    case DECIMAL:
      decimalIndices[addDecimalIndex]= addKeyIndex;
      columnTypeSpecificIndices[addKeyIndex] = addDecimalIndex++;
        break;
    case TIMESTAMP:
      timestampIndices[addTimestampIndex] = addKeyIndex;
      columnTypeSpecificIndices[addKeyIndex] = addTimestampIndex++;
      break;
    case INTERVAL_DAY_TIME:
      intervalDayTimeIndices[addIntervalDayTimeIndex] = addKeyIndex;
      columnTypeSpecificIndices[addKeyIndex] = addIntervalDayTimeIndex++;
      break;
    default:
      throw new HiveException("Unexpected column vector type " + columnVectorType);
    }

    columnVectorTypes[addKeyIndex] = columnVectorType;
    addKeyIndex++;
  }


  protected void finishAdding() throws HiveException {
    longIndices = Arrays.copyOf(longIndices, addLongIndex);
    doubleIndices = Arrays.copyOf(doubleIndices, addDoubleIndex);
    stringIndices = Arrays.copyOf(stringIndices, addStringIndex);
    decimalIndices = Arrays.copyOf(decimalIndices, addDecimalIndex);
    timestampIndices = Arrays.copyOf(timestampIndices, addTimestampIndex);
    intervalDayTimeIndices = Arrays.copyOf(intervalDayTimeIndices, addIntervalDayTimeIndex);
  }
}