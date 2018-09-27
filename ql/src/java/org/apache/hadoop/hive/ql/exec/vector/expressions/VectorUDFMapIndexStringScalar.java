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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

/**
 * Returns value of Map.
 * Extends {@link VectorUDFMapIndexBaseScalar}
 */
public class VectorUDFMapIndexStringScalar extends VectorUDFMapIndexBaseScalar {

  private byte[] key;

  public VectorUDFMapIndexStringScalar() {
    super();
  }

  public VectorUDFMapIndexStringScalar(int mapColumnNum, byte[] key, int outputColumnNum) {
    super(mapColumnNum, outputColumnNum);
    this.key = key;
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, getMapColumnNum()) + ", key: " + new String(key);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.MAP,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }

  @Override
  public int findScalarInMap(MapColumnVector mapColumnVector, int mapBatchIndex) {
    final int offset = (int) mapColumnVector.offsets[mapBatchIndex];
    final int count = (int) mapColumnVector.lengths[mapBatchIndex];
    BytesColumnVector keyColVector = (BytesColumnVector) mapColumnVector.keys;
    byte[][] keyVector = keyColVector.vector;
    int[] keyStart = keyColVector.start;
    int[] keyLength = keyColVector.length;
    for (int i = 0; i < count; i++) {
      final int keyOffset = offset + i;
      if (StringExpr.equal(key, 0, key.length,
          keyVector[keyOffset], keyStart[keyOffset], keyLength[keyOffset])) {
        return offset + i;
      }
    }
    return -1;
  }
}
