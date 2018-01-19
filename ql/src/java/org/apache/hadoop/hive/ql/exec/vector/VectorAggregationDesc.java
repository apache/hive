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

import java.util.ArrayList;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.common.util.AnnotationUtils;

import com.google.common.base.Preconditions;

/**
 * VectorAggregationDesc.
 *
 * Mode is GenericUDAFEvaluator.Mode.
 *
 * It is the different modes for an aggregate UDAF (User Defined Aggregation Function).
 *
 *    (Notice the these names are a subset of GroupByDesc.Mode...)
 *
 *        PARTIAL1       Original data            --> Partial aggregation data
 *
 *        PARTIAL2       Partial aggregation data --> Partial aggregation data
 *
 *        FINAL          Partial aggregation data --> Full aggregation data
 *
 *        COMPLETE       Original data            --> Full aggregation data
 *
 *
 * SIMPLEST CASE --> The data type/semantics of original data, partial aggregation
 *     data, and full aggregation data ARE THE SAME.  E.g. MIN, MAX, SUM.  The different
 *     modes can be handled by one aggregation class.
 *
 *     This case has a null for the Mode.
 *
 * FOR OTHERS --> The data type/semantics of partial aggregation data and full aggregation data
 *    ARE THE SAME but different than original data.  This results in 2 aggregation classes:
 *
 *       1) A class that takes original rows and outputs partial/full aggregation
 *          (PARTIAL1/COMPLETE)
 *
 *         and
 *
 *       2) A class that takes partial aggregation and produces full aggregation
 *          (PARTIAL2/FINAL).
 *
 *    E.g. COUNT(*) and COUNT(column)
 *
 * OTHERWISE FULL --> The data type/semantics of partial aggregation data is different than
 *    original data and full aggregation data.
 *
 *    E.g. AVG uses a STRUCT with count and sum for partial aggregation data.  It divides
 *    sum by count to produce the average for final aggregation.
 *
 */
public class VectorAggregationDesc implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  private final AggregationDesc aggrDesc;

  private final TypeInfo inputTypeInfo;
  private final ColumnVector.Type inputColVectorType;
  private final VectorExpression inputExpression;

  private final TypeInfo outputTypeInfo;
  private final ColumnVector.Type outputColVectorType;
  private final DataTypePhysicalVariation outputDataTypePhysicalVariation;

  private final Class<? extends VectorAggregateExpression> vecAggrClass;

  private GenericUDAFEvaluator evaluator;

  public VectorAggregationDesc(AggregationDesc aggrDesc, GenericUDAFEvaluator evaluator,
      TypeInfo inputTypeInfo, ColumnVector.Type inputColVectorType,
      VectorExpression inputExpression, TypeInfo outputTypeInfo,
      ColumnVector.Type outputColVectorType,
      Class<? extends VectorAggregateExpression> vecAggrClass) {

    this.aggrDesc = aggrDesc;
    this.evaluator = evaluator;

    this.inputTypeInfo = inputTypeInfo;
    this.inputColVectorType = inputColVectorType;
    this.inputExpression = inputExpression;

    this.outputTypeInfo = outputTypeInfo;
    this.outputColVectorType = outputColVectorType;
    outputDataTypePhysicalVariation =
        (outputColVectorType == ColumnVector.Type.DECIMAL_64 ?
            DataTypePhysicalVariation.DECIMAL_64 : DataTypePhysicalVariation.NONE);

    this.vecAggrClass = vecAggrClass;
  }

  public AggregationDesc getAggrDesc() {
    return aggrDesc;
  }

  public TypeInfo getInputTypeInfo() {
    return inputTypeInfo;
  }

  public ColumnVector.Type getInputColVectorType() {
    return inputColVectorType;
  }

  public VectorExpression getInputExpression() {
    return inputExpression;
  }

  public TypeInfo getOutputTypeInfo() {
    return outputTypeInfo;
  }

  public ColumnVector.Type getOutputColVectorType() {
    return outputColVectorType;
  }

  public DataTypePhysicalVariation getOutputDataTypePhysicalVariation() {
    return outputDataTypePhysicalVariation;
  }

  public GenericUDAFEvaluator getEvaluator() {
    return evaluator;
  }

  public Class<? extends VectorAggregateExpression> getVecAggrClass() {
    return vecAggrClass;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(vecAggrClass.getSimpleName());
    if (inputExpression != null) {
      sb.append("(");
      sb.append(inputExpression.toString());
      sb.append(") -> ");
    } else {
      sb.append("(*) -> ");
    }
    sb.append(outputTypeInfo.toString());
    if (outputDataTypePhysicalVariation != null && outputDataTypePhysicalVariation != DataTypePhysicalVariation.NONE) {
      sb.append("/");
      sb.append(outputDataTypePhysicalVariation);
    }
    String aggregationName = aggrDesc.getGenericUDAFName();
    if (GenericUDAFVariance.isVarianceFamilyName(aggregationName)) {
      sb.append(" aggregation: ");
      sb.append(aggregationName);
    }
    return sb.toString();
  }
}