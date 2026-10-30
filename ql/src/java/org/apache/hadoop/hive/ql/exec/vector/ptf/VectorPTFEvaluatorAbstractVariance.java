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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance.VarianceKind;

import com.google.common.base.Preconditions;

/**
 * Base class of the variance family evaluators: variance/var_pop, var_samp,
 * std/stddev/stddev_pop and stddev_samp.
 *
 * The aggregation state (count, sum and variance) is updated value by value in row order, exactly
 * like GenericUDAFVarianceEvaluator.iterate does in row mode, so the vectorized results are
 * identical to the row mode ones.  LONG, DOUBLE and DECIMAL input values are converted to double
 * the same way row mode does.
 */
public abstract class VectorPTFEvaluatorAbstractVariance extends VectorPTFEvaluatorBase {

  private static final Set<Type> SUPPORTED_INPUT_TYPES =
      EnumSet.of(Type.LONG, Type.DOUBLE, Type.DECIMAL);

  protected final Type inputColumnVectorType;
  protected final VarianceKind varianceKind;

  protected long count;
  protected double sum;
  // sum[(x - avg)^2], this is count times the population variance.
  protected double variance;

  protected VectorPTFEvaluatorAbstractVariance(WindowFrameDef windowFrameDef,
      VectorExpression inputVecExpr, int outputColumnNum, Type inputColumnVectorType,
      VarianceKind varianceKind) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    Preconditions.checkArgument(SUPPORTED_INPUT_TYPES.contains(inputColumnVectorType),
        "Unexpected column vector type %s for %s", inputColumnVectorType, varianceKind);
    this.inputColumnVectorType = inputColumnVectorType;
    this.varianceKind = varianceKind;
  }

  protected final void iterate(double value) {
    count++;
    sum += value;
    if (count > 1) {
      variance = GenericUDAFVariance.calculateIntermediate(count, sum, value, variance);
    }
  }

  protected final double getDouble(ColumnVector colVector, int i) {
    switch (inputColumnVectorType) {
    case LONG:
      return (double) ((LongColumnVector) colVector).vector[i];
    case DOUBLE:
      return ((DoubleColumnVector) colVector).vector[i];
    case DECIMAL:
      return ((DecimalColumnVector) colVector).vector[i].doubleValue();
    default:
      throw new IllegalStateException("Unexpected column vector type " + inputColumnVectorType);
    }
  }

  protected final boolean isResultNull() {
    return GenericUDAFVariance.isVarianceNull(count, varianceKind);
  }

  protected final double calculateResult() {
    return GenericUDAFVariance.calculateVarianceFamilyResult(variance, count, varianceKind);
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DOUBLE;
  }

  @Override
  public void resetEvaluator() {
    count = 0;
    sum = 0.0;
    variance = 0.0;
  }
}
