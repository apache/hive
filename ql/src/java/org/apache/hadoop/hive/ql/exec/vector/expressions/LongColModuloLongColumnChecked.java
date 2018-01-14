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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This vector expression implements a Checked variant of LongColModuloLongColumn
 * If the outputTypeInfo is not long it casts the result column vector values to
 * the set outputType so as to have similar result when compared to non-vectorized UDF
 * execution.
 */
public class LongColModuloLongColumnChecked extends LongColModuloLongColumn {
  public LongColModuloLongColumnChecked(int colNum1, int colNum2, int outputColumnNum) {
    super(colNum1, colNum2, outputColumnNum);
  }

  public LongColModuloLongColumnChecked() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    super.evaluate(batch);
    //checked for overflow based on the outputTypeInfo
    OverflowUtils
        .accountForOverflowLong(outputTypeInfo, (LongColumnVector) batch.cols[outputColumnNum], batch.selectedInUse,
            batch.selected, batch.size);
  }

  @Override
  public boolean supportsCheckedExecution() {
    return true;
  }
}
