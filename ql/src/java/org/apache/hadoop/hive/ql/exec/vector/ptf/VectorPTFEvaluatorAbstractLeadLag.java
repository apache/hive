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

import java.util.stream.IntStream;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFEvaluatorBase;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorPTFGroupBatches;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.ptf.Range;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

public abstract class VectorPTFEvaluatorAbstractLeadLag extends VectorPTFEvaluatorBase {

  protected int amt;
  protected Object defaultValue;
  protected int defaultValueColumn = -1;
  protected Type type;

  public VectorPTFEvaluatorAbstractLeadLag(WindowFrameDef windowFrameDef,
      VectorExpression inputVecExpr, int outputColumnNum, Type type, int amt,
      VectorExpression defaultValueExpression) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    this.type = type;
    this.amt = amt;
    initDefaultValue(defaultValueExpression);
  }

  private void initDefaultValue(VectorExpression defaultValueExpression) {
    if (defaultValueExpression == null) {
      defaultValue = null;
      return;
    } else if (defaultValueExpression instanceof ConstantVectorExpression) {
      // FIXME: always getLongValue()? check if other type is given in 3rd argument
      long longValue = ((ConstantVectorExpression) defaultValueExpression).getLongValue();
      switch (type) {
      case LONG:
        defaultValue = longValue;
        break;
      case DOUBLE:
        defaultValue = Double.valueOf(longValue);
        break;
      case DECIMAL:
        defaultValue = new HiveDecimalWritable(longValue);
        break;
      default:
        throw new RuntimeException("Unexpected column vector type " + type + " for lag function");
      }
    } else if (defaultValueExpression instanceof IdentityExpression) {
      defaultValueColumn = ((IdentityExpression) defaultValueExpression).getOutputColumnNum();
    } else {
      throw new RuntimeException(
          "Unexpected vector expression type for default value in lag function: "
              + defaultValueExpression.getClass().getName());
    }
  }

  @Override
  public boolean canRunOptimizedCalculation(int rowNum, Range range) {
    return true;
  }

  protected Object getDefaultValue(int rowNum, VectorPTFGroupBatches batches) throws HiveException {
    if (defaultValueColumn > -1) {
      return batches.getValue(rowNum, defaultValueColumn);
    } else {
      return defaultValue;
    }
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch) throws HiveException {
  }

  // TODO: HIVE-25123: Implement vectorized streaming lead/lag
  @Override
  public boolean streamsResult() {
    return false;
  }

  @Override
  public Type getResultColumnVectorType() {
    return type;
  }

  @Override
  public void resetEvaluator() {
  }

  @Override
  public boolean isCacheableForRange() {
    return false;
  }

  @Override
  public void mapCustomColumns(int[] bufferedColumnMap) {
    if (defaultValueColumn > -1) {
      defaultValueColumn = IntStream.range(0, bufferedColumnMap.length)
          .filter(j -> bufferedColumnMap[j] == defaultValueColumn).findFirst()
          .orElseGet(() -> defaultValueColumn);
    }
  }
}
