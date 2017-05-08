/**
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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Compute the sample covariance by extending GenericUDAFCovariance and overriding
 * the terminate() method of the evaluator.
 *
 */
@Description(name = "covar_samp",
    value = "_FUNC_(x,y) - Returns the sample covariance of a set of number pairs",
    extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
        + "Any pair with a NULL is ignored.\n"
        + "If applied to an empty set: NULL is returned.\n"
        + "If applied to a set with a single element: NULL is returned.\n"
        + "Otherwise, it computes the following:\n"
        + "   (SUM(x*y)-SUM(x)*SUM(y)/COUNT(x,y))/(COUNT(x,y)-1)\n"
        + "where neither x nor y is null.")
public class GenericUDAFCovarianceSample extends GenericUDAFCovariance {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly two arguments are expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }

    if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(1,
            "Only primitive type arguments are accepted but "
            + parameters[1].getTypeName() + " is passed.");
    }

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case TIMESTAMP:
    case DECIMAL:
      switch (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DECIMAL:
        return new GenericUDAFCovarianceSampleEvaluator();
      case STRING:
      case BOOLEAN:
      case DATE:
      default:
        throw new UDFArgumentTypeException(1,
            "Only numeric or string type arguments are accepted but "
            + parameters[1].getTypeName() + " is passed.");
      }
    case STRING:
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * Compute the sample covariance by extending GenericUDAFCovarianceEvaluator and
   * overriding the terminate() method of the evaluator.
   */
  public static class GenericUDAFCovarianceSampleEvaluator extends
      GenericUDAFCovarianceEvaluator {

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;

      if (myagg.count <= 1) {
        return null;
      } else {
        DoubleWritable result = getResult();
        result.set(myagg.covar / (myagg.count - 1));
        return result;
      }
    }
  }

}
