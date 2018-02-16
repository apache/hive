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
 * Superclass to support vectorized functions that take a column value as key of Map
 * and return the value of Map.
 */
public abstract class VectorUDFMapIndexBaseCol extends VectorUDFMapIndexBase {

  private static final long serialVersionUID = 1L;

  private int mapColumnNum;
  private int indexColumnNum;
  private ColumnVector indexColumnVector;

  public VectorUDFMapIndexBaseCol() {
    super();
  }

  public VectorUDFMapIndexBaseCol(int mapColumnNum, int indexColumnNum, int outputColumnNum) {
    super(outputColumnNum);
    this.mapColumnNum = mapColumnNum;
    this.indexColumnNum = indexColumnNum;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector outV = batch.cols[outputColumnNum];
    MapColumnVector mapV = (MapColumnVector) batch.cols[mapColumnNum];
    // indexColumnVector includes the keys of Map
    indexColumnVector = batch.cols[indexColumnNum];

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    int[] mapValueIndex;
    if (mapV.isRepeating) {
      if (mapV.isNull[0]) {
        outV.isNull[0] = true;
        outV.noNulls = false;
        outV.isRepeating = true;
      } else {
        mapValueIndex = getMapValueIndex(mapV, batch);
        if (indexColumnVector.isRepeating) {
          // the key is not found in MapColumnVector, set the output as null ColumnVector
          if (mapValueIndex[0] == -1) {
            outV.isNull[0] = true;
            outV.noNulls = false;
          } else {
            // the key is found in MapColumnVector, set the value
            outV.isNull[0] = false;
            outV.setElement(0, (int) (mapV.offsets[0] + mapValueIndex[0]), mapV.values);
          }
          outV.isRepeating = true;
        } else {
          setUnRepeatingOutVector(batch, mapV, outV, mapValueIndex);
        }
      }
    } else {
      mapValueIndex = getMapValueIndex(mapV, batch);
      setUnRepeatingOutVector(batch, mapV, outV, mapValueIndex);
    }
  }

  /**
   * Set the output based on the index array of MapColumnVector.
   */
  private void setUnRepeatingOutVector(VectorizedRowBatch batch, MapColumnVector mapV,
      ColumnVector outV, int[] mapValueIndex) {
    for (int i = 0; i < batch.size; i++) {
      int j = (batch.selectedInUse) ? batch.selected[i] : i;
      if (mapV.isNull[j] || mapValueIndex[j] == -1) {
        outV.isNull[j] = true;
        outV.noNulls = false;
      } else {
        outV.isNull[j] = false;
        outV.setElement(j, (int) (mapV.offsets[j] + mapValueIndex[j]), mapV.values);
      }
    }
    outV.isRepeating = false;
  }

  @Override
  protected Object getCurrentKey(int index) {
    return getKeyByIndex(indexColumnVector, index);
  }

  public int getMapColumnNum() {
    return mapColumnNum;
  }

  public int getIndexColumnNum() {
    return indexColumnNum;
  }
}
