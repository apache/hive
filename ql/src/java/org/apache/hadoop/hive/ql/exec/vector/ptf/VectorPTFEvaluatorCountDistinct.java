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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

import com.google.common.base.Preconditions;

/**
 * This class evaluates count(column) for a PTF group where a distinct keyword is applied to the
 * partitioning column itself, e.g.:
 *
 * SELECT
 *   txt1,
 *   txt2,
 *   count(distinct txt1) over(partition by txt1) as n,
 *   count(distinct txt2) over(partition by txt2) as m
 * FROM example;
 *
 * In this case, the framework is still supposed to ensure sorting
 * on the key (let's say txt1 for the first Reducer stage), but the original
 * VectorPTFEvaluatorCount is not aware that a distinct keyword was applied
 * to the key column. This case would be simple, because such function should
 * return 1 every time. However, that's just a corner-case, a real scenario is
 * when the partitioning column is not the same. In such cases, a real count
 * distinct implementation is needed:
 *
 * SELECT
 *   txt1,
 *   txt2,
 *   count(distinct txt2) over(partition by txt1) as n,
 *   count(distinct txt1) over(partition by txt2) as m
 * FROM example;
 */
public abstract class VectorPTFEvaluatorCountDistinct extends VectorPTFEvaluatorCount {

  protected Set<Object> uniqueObjects;

  public VectorPTFEvaluatorCountDistinct(WindowFrameDef windowFrameDef,
      VectorExpression inputVecExpr, int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch) throws HiveException {

    evaluateInputExpr(batch);

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    ColumnVector colVector = batch.cols[inputColumnNum];
    if (colVector.isRepeating) {
      if (colVector.noNulls || !colVector.isNull[0]) {
        countValue(colVector, 0);
      }
    } else {
      boolean[] batchIsNull = colVector.isNull;
      for (int i = 0; i < size; i++) {
        if (!batchIsNull[i]) {
          countValue(colVector, i);
        }
      }
    }
  }

  protected void countValue(ColumnVector colVector, int i) {
    Object value = getValue(colVector, i);
    uniqueObjects.add(value);
  }

  protected abstract Object getValue(ColumnVector colVector, int i);

  @Override
  public Object getGroupResult() {
    return Long.valueOf(uniqueObjects.size());
  }

  @Override
  public void resetEvaluator() {
    super.resetEvaluator();
    uniqueObjects = new HashSet<>();
  }
}
