/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(name = "$SUM0", value = "_FUNC_(x) - Returns the sum of a set of numbers, zero if empty")
public class GenericUDAFSumEmptyIsZero extends GenericUDAFSum {

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
      return new SumLongZeroIfEmpty();
    case TIMESTAMP:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case VARCHAR:
    case CHAR:
      return new SumDoubleZeroIfEmpty();
    case DECIMAL:
      return new SumHiveDecimalZeroIfEmpty();
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
  }

  public static class SumLongZeroIfEmpty extends GenericUDAFSumLong {

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumLongAgg myagg = (SumLongAgg) agg;
      result.set(myagg.sum);
      return result;
    }
  }

  public static class SumDoubleZeroIfEmpty extends GenericUDAFSumDouble {

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      result.set(myagg.sum);
      return result;
    }
  }

  public static class SumHiveDecimalZeroIfEmpty extends GenericUDAFSumHiveDecimal {

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumHiveDecimalWritableAgg myagg = (SumHiveDecimalWritableAgg) agg;
      result.set(myagg.sum);
      return result;
    }
  }
}
