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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedUDAFs;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Compute the standard deviation by extending GenericUDAFVariance and
 * overriding the terminate() method of the evaluator.
 *
 */
@Description(name = "std,stddev,stddev_pop",
    value = "_FUNC_(x) - Returns the standard deviation of a set of numbers")
public class GenericUDAFStd extends GenericUDAFVariance {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case VARCHAR:
    case CHAR:
    case TIMESTAMP:
    case DECIMAL:
      return new GenericUDAFStdEvaluator();
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * Compute the standard deviation by extending GenericUDAFVarianceEvaluator
   * and overriding the terminate() method of the evaluator.
   *
   */
  @VectorizedUDAFs({
    VectorUDAFVarLong.class, VectorUDAFVarLongComplete.class,
    VectorUDAFVarDouble.class, VectorUDAFVarDoubleComplete.class,
    VectorUDAFVarDecimal.class, VectorUDAFVarDecimalComplete.class,
    VectorUDAFVarTimestamp.class, VectorUDAFVarTimestampComplete.class,
    VectorUDAFVarPartial2.class, VectorUDAFVarFinal.class})
  public static class GenericUDAFStdEvaluator extends
      GenericUDAFVarianceEvaluator {

    /*
     * Calculate the std result when count > 1.  Public so vectorization code can
     * use it, etc.
     */
    public static double calculateStdResult(double variance, long count) {
      return Math.sqrt(variance / count);
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;

      if (myagg.count == 0) { // SQL standard - return null for zero elements
        return null;
      } else {
        if (myagg.count > 1) {
          getResult().set(
              calculateStdResult(myagg.variance, myagg.count));
        } else { // for one element the variance is always 0
          getResult().set(0);
        }
        return getResult();
      }
    }
  }

}
