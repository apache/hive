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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorLimitDesc;

import com.google.common.annotations.VisibleForTesting;

/**
 * Limit operator implementation Limits the number of rows to be passed on.
 **/
public class VectorLimitOperator extends LimitOperator implements VectorizationOperator {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;
  private VectorLimitDesc vectorDesc;

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorLimitOperator() {
    super();
  }

  public VectorLimitOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorLimitOperator(
      CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) {
    this(ctx);
    this.conf = (LimitDesc) conf;
    this.vContext = vContext;
    this.vectorDesc = (VectorLimitDesc) vectorDesc;
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;

    if (currCount + batch.size < offset) {
      currCount += batch.size;
    } else if (currCount >= offset + limit) {
      setDone(true);
    } else {
      int skipSize = 0;
      if (currCount < offset) {
        skipSize = offset - currCount;
      }
      //skip skipSize rows of batch
      batch.size = Math.min(batch.size, offset + limit - currCount);
      if (batch.selectedInUse == false) {
        batch.selectedInUse = true;
        for (int i = 0; i < batch.size - skipSize; i++) {
          batch.selected[i] = skipSize + i;
        }
      } else {
        for (int i = 0; i < batch.size - skipSize; i++) {
          batch.selected[i] = batch.selected[skipSize + i];
        }
      }
      forward(row, inputObjInspectors[tag], true);
      currCount += batch.size;
    }
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }
}
