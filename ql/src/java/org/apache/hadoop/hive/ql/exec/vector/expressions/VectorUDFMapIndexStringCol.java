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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

import java.util.Arrays;

/**
 * Returns value of Map.
 * Extends {@link VectorUDFMapIndexBaseCol}
 */
public class VectorUDFMapIndexStringCol extends VectorUDFMapIndexBaseCol {

  public VectorUDFMapIndexStringCol() {
    super();
  }

  public VectorUDFMapIndexStringCol(int mapColumnNum, int indexColumnNum, int outputColumnNum) {
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
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }

  @Override
  protected Object getKeyByIndex(ColumnVector cv, int index) {
    BytesColumnVector bytesCV = (BytesColumnVector) cv;
    return ArrayUtils.subarray(bytesCV.vector[index], bytesCV.start[index],
        bytesCV.start[index] + bytesCV.length[index]);
  }

  @Override
  protected boolean compareKeyInternal(Object columnKey, Object otherKey) {
    return Arrays.equals((byte[])columnKey, (byte[]) otherKey);
  }
}
