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
 * Compute the sample variance by extending GenericUDAFVariance and overriding
 * the terminate() method of the evaluator.
 *
 */
@Description(name = "var_samp",
    value = "_FUNC_(x) - Returns the sample variance of a set of numbers.\n"
          + "If applied to an empty set: NULL is returned.\n"
          + "If applied to a set with a single element: NULL is returned.\n"
          + "Otherwise it computes: (S2-S1*S1/N)/(N-1)")
public class GenericUDAFVarianceSample extends GenericUDAFVariance {

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
    case TIMESTAMP:
    case DECIMAL:
      return new GenericUDAFVarianceSampleEvaluator();
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * Compute the sample variance by extending GenericUDAFVarianceEvaluator and
   * overriding the terminate() method of the evaluator.
   */
  @VectorizedUDAFs({
    VectorUDAFVarLong.class, VectorUDAFVarLongComplete.class,
    VectorUDAFVarDouble.class, VectorUDAFVarDoubleComplete.class,
    VectorUDAFVarDecimal.class, VectorUDAFVarDecimalComplete.class,
    VectorUDAFVarTimestamp.class, VectorUDAFVarTimestampComplete.class,
    VectorUDAFVarPartial2.class, VectorUDAFVarFinal.class})
  public static class GenericUDAFVarianceSampleEvaluator extends
      GenericUDAFVarianceEvaluator {

    /*
     * Calculate the variance sample result when count > 1.  Public so vectorization code can
     * use it, etc.
     */
    public static double calculateVarianceSampleResult(double variance, long count) {
      return variance / (count - 1);
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;

      if (myagg.count <= 1) {
        return null;
      } else {
        getResult().set(
            calculateVarianceSampleResult(myagg.variance, myagg.count));
        return getResult();
      }
    }
  }

}
