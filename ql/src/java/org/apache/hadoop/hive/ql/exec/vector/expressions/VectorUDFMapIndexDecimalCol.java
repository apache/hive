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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Returns value of Map.
 * Extends {@link VectorUDFMapIndexBaseCol}
 */
public class VectorUDFMapIndexDecimalCol extends VectorUDFMapIndexBaseCol {

  public VectorUDFMapIndexDecimalCol() {
    super();
  }

  public VectorUDFMapIndexDecimalCol(int mapColumnNum, int indexColumnNum, int outputColumnNum) {
    super(mapColumnNum, indexColumnNum, outputColumnNum);
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, getMapColumnNum()) + ", key: "
        + getColumnParamString(1, getIndexColumnNum());
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
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }

  @Override
  public int findInMap(ColumnVector indexColumnVector, int indexBatchIndex,
      MapColumnVector mapColumnVector, int mapBatchIndex) {
    final int offset = (int) mapColumnVector.offsets[mapBatchIndex];
    final int count = (int) mapColumnVector.lengths[mapBatchIndex];
    HiveDecimalWritable[] keys = ((DecimalColumnVector) mapColumnVector.keys).vector;
    final HiveDecimalWritable index =
        ((DecimalColumnVector) indexColumnVector).vector[indexBatchIndex];
    for (int i = 0; i < count; i++) {
      if (index.compareTo(keys[offset + i]) == 0) {
        return offset + i;
      }
    }
    return -1;
  }
}
