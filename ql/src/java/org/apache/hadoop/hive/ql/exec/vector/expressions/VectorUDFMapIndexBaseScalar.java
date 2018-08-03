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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Superclass to support vectorized functions that take a scalar as key of Map
 * and return the value of Map.
 */
public abstract class VectorUDFMapIndexBaseScalar extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int mapColumnNum;

  public VectorUDFMapIndexBaseScalar() {
    super();
  }

  public VectorUDFMapIndexBaseScalar(int mapColumnNum, int outputColumnNum) {
    super(outputColumnNum);
    this.mapColumnNum = mapColumnNum;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    // return immediately if batch is empty
    final int n = batch.size;
    if (n == 0) {
      return;
    }

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector outV = batch.cols[outputColumnNum];
    MapColumnVector mapV = (MapColumnVector) batch.cols[mapColumnNum];
    ColumnVector valuesV = mapV.values;

    int[] sel = batch.selected;
    boolean[] mapIsNull = mapV.isNull;
    boolean[] outputIsNull = outV.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    outV.isRepeating = false;

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    if (mapV.isRepeating) {
      if (mapV.noNulls || !mapIsNull[0]) {
        final int repeatedMapIndex = findScalarInMap(mapV, 0);
        if (repeatedMapIndex == -1) {
          outV.isNull[0] = true;
          outV.noNulls = false;
        } else {
          outV.isNull[0] = false;
          outV.setElement(0, repeatedMapIndex, valuesV);
        }
      } else {
        outV.isNull[0] = true;
        outV.noNulls = false;
      }
      outV.isRepeating = true;
      return;
    }

    /*
     * Individual row processing for LIST vector with scalar constant INDEX value.
     */
    if (mapV.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outV.noNulls) {
          for (int j = 0; j < n; j++) {
            final int i = sel[j];
            final int mapIndex = findScalarInMap(mapV, i);
            if (mapIndex == -1) {
              outV.isNull[i] = true;
              outV.noNulls = false;
            } else {
              outV.isNull[i] = false;
              outV.setElement(i, mapIndex, valuesV);
            }
          }
        } else {
          for (int j = 0; j < n; j++) {
            final int i = sel[j];
            final int mapIndex = findScalarInMap(mapV, i);
            if (mapIndex == -1) {
              outV.isNull[i] = true;
              outV.noNulls = false;
            } else {
              outV.setElement(i, mapIndex, valuesV);
            }
          }
        }
      } else {
        if (!outV.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outV.isNull, false);
          outV.noNulls = true;
        }
        for (int i = 0; i < n; i++) {
          final long longListLength = mapV.lengths[i];
          final int mapIndex = findScalarInMap(mapV, i);
          if (mapIndex == -1) {
            outV.isNull[i] = true;
            outV.noNulls = false;
          } else {
            outV.setElement(i, mapIndex, valuesV);
          }
        }
      }
    } else /* there are NULLs in the MAP */ {

      if (batch.selectedInUse) {
        for (int j=0; j != n; j++) {
          int i = sel[j];
          if (!mapIsNull[i]) {
            final int mapIndex = findScalarInMap(mapV, i);
            if (mapIndex == -1) {
              outV.isNull[i] = true;
              outV.noNulls = false;
            } else {
              outV.isNull[i] = false;
              outV.setElement(i, mapIndex, valuesV);
            }
          } else {
            outputIsNull[i] = true;
            outV.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (!mapIsNull[i]) {
            final int mapIndex = findScalarInMap(mapV, i);
            if (mapIndex == -1) {
              outV.isNull[i] = true;
              outV.noNulls = false;
            } else {
              outV.isNull[i] = false;
              outV.setElement(i, mapIndex, valuesV);
            }
          } else {
            outputIsNull[i] = true;
            outV.noNulls = false;
          }
        }
      }
    }
  }

  public int findScalarInMap(MapColumnVector mapColumnVector, int mapBatchIndex) {
    throw new RuntimeException("Not implemented");
  }

  public int getMapColumnNum() {
    return mapColumnNum;
  }
}
