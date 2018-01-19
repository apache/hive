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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Superclass to support vectorized functions that take a parameter as key of Map
 * and return the value of Map.
 */
public abstract class VectorUDFMapIndexBase extends VectorExpression {

  private static final long serialVersionUID = 1L;

  public VectorUDFMapIndexBase() {
    super();
  }

  public VectorUDFMapIndexBase(int outputColumnNum) {
    super(outputColumnNum);
  }

  /**
   * The index array of MapColumnVector is used to get the value from MapColumnVector based on the
   * index, the following are the steps to get it:
   *   1. Get the current key which is a scalar or from a ColumnVector.
   *   2. Compare the current key and the key from MapColumnVector.
   *   3. Set the index of MapColumnVector to the result array if the keys are same.
   */
  protected int[] getMapValueIndex(MapColumnVector mapV, VectorizedRowBatch batch) {
    int[] indexArray = new int[VectorizedRowBatch.DEFAULT_SIZE];
    for (int i = 0; i < batch.size; i++) {
      boolean findKey = false;
      int offset = (batch.selectedInUse) ? batch.selected[i] : i;
      Object columnKey = getCurrentKey(offset);
      for (int j = 0; j < mapV.lengths[offset]; j++) {
        int index = (int)(mapV.offsets[offset] + j);
        Object tempKey = getKeyByIndex(mapV.keys, index);
        if (compareKey(columnKey, tempKey)) {
          indexArray[offset] = j;
          findKey = true;
          break;
        }
      }
      if (!findKey) {
        indexArray[offset] = -1;
      }
      if (mapV.isRepeating) {
        break;
      }
    }
    return indexArray;
  }

  protected boolean compareKey(Object columnKey, Object otherKey) {
    if (columnKey == null && otherKey == null) {
      return true;
    } else if (columnKey != null && otherKey != null) {
      return compareKeyInternal(columnKey, otherKey);
    } else {
      return false;
    }
  }

  protected boolean compareKeyInternal(Object columnKey, Object otherKey) {
    return columnKey.equals(otherKey);
  }

  abstract Object getKeyByIndex(ColumnVector cv, int index);

  abstract Object getCurrentKey(int index);
}
