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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Returns value of Map.
 * Extends {@link VectorUDFMapIndexBaseScalar}
 */
public class VectorUDFMapIndexDecimalScalar extends VectorUDFMapIndexBaseScalar {

  private static final long serialVersionUID = 1L;

  private HiveDecimal key;
  private double doubleKey;

  public VectorUDFMapIndexDecimalScalar() {
    super();
  }

  public VectorUDFMapIndexDecimalScalar(int mapColumnNum, HiveDecimal key, int outputColumnNum) {
    super(mapColumnNum, outputColumnNum);
    this.key = key;
    doubleKey = key.doubleValue();
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, getMapColumnNum()) + ", key: " + key;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.MAP,
            VectorExpressionDescriptor.ArgumentType.DECIMAL)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }

  @Override
  public int findScalarInMap(MapColumnVector mapColumnVector, int mapBatchIndex) {
    final int offset = (int) mapColumnVector.offsets[mapBatchIndex];
    final int count = (int) mapColumnVector.lengths[mapBatchIndex];

    ColumnVector keys =  mapColumnVector.keys;
    if (keys instanceof DecimalColumnVector) {
      HiveDecimalWritable[] decimalKeyVector = ((DecimalColumnVector) keys).vector;
      for (int i = 0; i < count; i++) {
        if (decimalKeyVector[offset + i].compareTo(key) == 0) {
          return offset + i;
        }
      }
    } else {

      // For some strange reason we receive a double column vector...
      // The way we do VectorExpressionDescriptor may be inadequate in this case...
      double[] doubleKeyVector = ((DoubleColumnVector) keys).vector;
      for (int i = 0; i < count; i++) {
        if (doubleKeyVector[offset + i] == doubleKey) {
          return offset + i;
        }
      }
    }
    return -1;
  }

}
