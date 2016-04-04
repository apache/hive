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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

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

  /**
   * Helper class for looking up a key value based on key index.
   */
  public class KeyLookupHelper {
    public int longIndex;
    public int doubleIndex;
    public int stringIndex;
    public int decimalIndex;
    public int timestampIndex;
    public int intervalDayTimeIndex;

    private static final int INDEX_UNUSED = -1;

    private void resetIndices() {
        this.longIndex = this.doubleIndex = this.stringIndex = this.decimalIndex =
            timestampIndex = intervalDayTimeIndex = INDEX_UNUSED;
    }
    public void setLong(int index) {
      resetIndices();
      this.longIndex= index;
    }

    public void setDouble(int index) {
      resetIndices();
      this.doubleIndex = index;
    }

    public void setString(int index) {
      resetIndices();
      this.stringIndex = index;
    }

    public void setDecimal(int index) {
      resetIndices();
      this.decimalIndex = index;
    }

    public void setTimestamp(int index) {
      resetIndices();
      this.timestampIndex= index;
    }

    public void setIntervalDayTime(int index) {
      resetIndices();
      this.intervalDayTimeIndex= index;
    }
  }

  /**
   * Lookup vector to map from key index to primitive type index.
   */
  protected KeyLookupHelper[] indexLookup;

  private int keyCount;
  private int addIndex;

  protected int longIndicesIndex;
  protected int doubleIndicesIndex;
  protected int stringIndicesIndex;
  protected int decimalIndicesIndex;
  protected int timestampIndicesIndex;
  protected int intervalDayTimeIndicesIndex;

  protected VectorColumnSetInfo(int keyCount) {
    this.keyCount = keyCount;
    this.addIndex = 0;

    // We'll over allocate and then shrink the array for each type
    longIndices = new int[this.keyCount];
    longIndicesIndex = 0;
    doubleIndices = new int[this.keyCount];
    doubleIndicesIndex  = 0;
    stringIndices = new int[this.keyCount];
    stringIndicesIndex = 0;
    decimalIndices = new int[this.keyCount];
    decimalIndicesIndex = 0;
    timestampIndices = new int[this.keyCount];
    timestampIndicesIndex = 0;
    intervalDayTimeIndices = new int[this.keyCount];
    intervalDayTimeIndicesIndex = 0;
    indexLookup = new KeyLookupHelper[this.keyCount];
  }

  protected void addKey(String outputType) throws HiveException {
    indexLookup[addIndex] = new KeyLookupHelper();

    String typeName = VectorizationContext.mapTypeNameSynonyms(outputType);

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
    Type columnVectorType = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);

    switch (columnVectorType) {
    case LONG:
      longIndices[longIndicesIndex] = addIndex;
      indexLookup[addIndex].setLong(longIndicesIndex);
      ++longIndicesIndex;
      break;

    case DOUBLE:
      doubleIndices[doubleIndicesIndex] = addIndex;
      indexLookup[addIndex].setDouble(doubleIndicesIndex);
      ++doubleIndicesIndex;
      break;

    case BYTES:
      stringIndices[stringIndicesIndex]= addIndex;
      indexLookup[addIndex].setString(stringIndicesIndex);
      ++stringIndicesIndex;
      break;

    case DECIMAL:
      decimalIndices[decimalIndicesIndex]= addIndex;
      indexLookup[addIndex].setDecimal(decimalIndicesIndex);
      ++decimalIndicesIndex;
      break;

    case TIMESTAMP:
      timestampIndices[timestampIndicesIndex] = addIndex;
      indexLookup[addIndex].setTimestamp(timestampIndicesIndex);
      ++timestampIndicesIndex;
      break;

    case INTERVAL_DAY_TIME:
      intervalDayTimeIndices[intervalDayTimeIndicesIndex] = addIndex;
      indexLookup[addIndex].setIntervalDayTime(intervalDayTimeIndicesIndex);
      ++intervalDayTimeIndicesIndex;
      break;

    default:
      throw new HiveException("Unexpected column vector type " + columnVectorType);
    }

    addIndex++;
  }

  protected void finishAdding() {
    longIndices = Arrays.copyOf(longIndices, longIndicesIndex);
    doubleIndices = Arrays.copyOf(doubleIndices, doubleIndicesIndex);
    stringIndices = Arrays.copyOf(stringIndices, stringIndicesIndex);
    decimalIndices = Arrays.copyOf(decimalIndices, decimalIndicesIndex);
    timestampIndices = Arrays.copyOf(timestampIndices, timestampIndicesIndex);
    intervalDayTimeIndices = Arrays.copyOf(intervalDayTimeIndices, intervalDayTimeIndicesIndex);
  }
}