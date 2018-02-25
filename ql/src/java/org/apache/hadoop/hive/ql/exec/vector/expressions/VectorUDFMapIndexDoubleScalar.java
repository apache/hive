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
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

/**
 * Returns value of Map.
 * Extends {@link VectorUDFMapIndexBaseScalar}
 */
public class VectorUDFMapIndexDoubleScalar extends VectorUDFMapIndexBaseScalar {

  private HiveDecimal key;

  public VectorUDFMapIndexDoubleScalar() {
    super();
  }

  public VectorUDFMapIndexDoubleScalar(int mapColumnNum, HiveDecimal key, int outputColumnNum) {
    super(mapColumnNum, outputColumnNum);
    this.key = key;
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
  protected Object getKeyByIndex(ColumnVector cv, int index) {
    return ((DoubleColumnVector) cv).vector[index];
  }

  @Override
  public Object getCurrentKey(int index) {
    return key;
  }

  @Override
  protected boolean compareKeyInternal(Object columnKey, Object otherKey) {
    return otherKey.equals(((HiveDecimal) columnKey).doubleValue());
  }
}
