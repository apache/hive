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

import java.util.List;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * VectorAggregationDesc.
 *
 * Mode is GenericUDAFEvaluator.Mode.
 *
 * It is the different modes for an aggregate UDAF (User Defined Aggregation Function).
 *
 *    (Notice the these names are a subset of GroupByDesc.Mode...)
 *
 *        PARTIAL1       Original data            --&gt; Partial aggregation data
 *
 *        PARTIAL2       Partial aggregation data --&gt; Partial aggregation data
 *
 *        FINAL          Partial aggregation data --&gt; Full aggregation data
 *
 *        COMPLETE       Original data            --&gt; Full aggregation data
 *
 *
 * SIMPLEST CASE --&gt; The data type/semantics of original data, partial aggregation
 *     data, and full aggregation data ARE THE SAME.  E.g. MIN, MAX, SUM.  The different
 *     modes can be handled by one aggregation class.
 *
 *     This case has a null for the Mode.
 *
 * FOR OTHERS --&gt; The data type/semantics of partial aggregation data and full aggregation data
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
 * OTHERWISE FULL --&gt; The data type/semantics of partial aggregation data is different than
 *    original data and full aggregation data.
 *
 *    E.g. AVG uses a STRUCT with count and sum for partial aggregation data.  It divides
 *    sum by count to produce the average for final aggregation.
 *
 */
public class VectorAggregationDesc implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  private String aggregationName;

  private TypeInfo inputTypeInfo;
  private ColumnVector.Type inputColVectorType;
  private VectorExpression inputExpression;

  private TypeInfo outputTypeInfo;
  private ColumnVector.Type outputColVectorType;
  private DataTypePhysicalVariation outputDataTypePhysicalVariation;

  private Class<? extends VectorAggregateExpression> vecAggrClass;

  private List<ConstantVectorExpression> constants;

  private GenericUDAFEvaluator evaluator;
  private GenericUDAFEvaluator.Mode udafEvaluatorMode;

  public VectorAggregationDesc() {
    // empty constructor used by the builder
  }

  /**
   * @deprecated use VectorAggregationDescBuilder
   */
  @Deprecated
  public VectorAggregationDesc(String aggregationName, GenericUDAFEvaluator evaluator,
      GenericUDAFEvaluator.Mode udafEvaluatorMode,
      TypeInfo inputTypeInfo, ColumnVector.Type inputColVectorType,
      VectorExpression inputExpression, TypeInfo outputTypeInfo,
      ColumnVector.Type outputColVectorType,
      Class<? extends VectorAggregateExpression> vecAggrClass) {

    this.aggregationName = aggregationName;

    this.evaluator = evaluator;
    this.udafEvaluatorMode = udafEvaluatorMode;

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

  public String getAggregationName() {
    return aggregationName;
  }

  public GenericUDAFEvaluator.Mode getUdafEvaluatorMode() {
    return udafEvaluatorMode;
  }

  public static class VectorAggregationDescBuilder {
    private String aggregationName;
    private TypeInfo inputTypeInfo;
    private ColumnVector.Type inputColVectorType;
    private VectorExpression inputExpression;
    private TypeInfo outputTypeInfo;
    private ColumnVector.Type outputColVectorType;
    private DataTypePhysicalVariation outputDataTypePhysicalVariation;
    private Class<? extends VectorAggregateExpression> vecAggrClass;
    private List<ConstantVectorExpression> constants;
    private GenericUDAFEvaluator evaluator;
    private GenericUDAFEvaluator.Mode udafEvaluatorMode;

    public VectorAggregationDescBuilder aggregationName(String aggregationName) {
      this.aggregationName = aggregationName;
      return this;
    }

    public VectorAggregationDescBuilder evaluator(GenericUDAFEvaluator evaluator) {
      this.evaluator = evaluator;
      return this;
    }

    public VectorAggregationDescBuilder udafEvaluatorMode(GenericUDAFEvaluator.Mode udafEvaluatorMode) {
      this.udafEvaluatorMode = udafEvaluatorMode;
      return this;
    }

    public VectorAggregationDescBuilder inputTypeInfo(TypeInfo inputTypeInfo) {
      this.inputTypeInfo = inputTypeInfo;
      return this;
    }

    public VectorAggregationDescBuilder inputColVectorType(ColumnVector.Type inputColVectorType) {
      this.inputColVectorType = inputColVectorType;
      return this;
    }

    public VectorAggregationDescBuilder inputExpression(VectorExpression inputExpression) {
      this.inputExpression = inputExpression;
      return this;
    }

    public VectorAggregationDescBuilder outputTypeInfo(TypeInfo outputTypeInfo) {
      this.outputTypeInfo = outputTypeInfo;
      return this;
    }

    public VectorAggregationDescBuilder outputColVectorType(ColumnVector.Type outputColVectorType) {
      this.outputColVectorType = outputColVectorType;
      this.outputDataTypePhysicalVariation =
          (outputColVectorType == ColumnVector.Type.DECIMAL_64 ?
              DataTypePhysicalVariation.DECIMAL_64 : DataTypePhysicalVariation.NONE);
      return this;
    }

    public VectorAggregationDescBuilder vectorAggregationClass(
        Class<? extends VectorAggregateExpression> vecAggrClass) {
      this.vecAggrClass = vecAggrClass;
      return this;
    }

    public VectorAggregationDescBuilder constants(List<ConstantVectorExpression> constants) {
      this.constants = constants;
      return this;
    }

    public VectorAggregationDesc build() {
      final VectorAggregationDesc vectorAggregationDesc = new VectorAggregationDesc();

      vectorAggregationDesc.aggregationName = aggregationName;
      vectorAggregationDesc.evaluator = evaluator;
      vectorAggregationDesc.udafEvaluatorMode = udafEvaluatorMode;
      vectorAggregationDesc.inputTypeInfo = inputTypeInfo;
      vectorAggregationDesc.inputColVectorType = inputColVectorType;
      vectorAggregationDesc.inputExpression = inputExpression;
      vectorAggregationDesc.outputTypeInfo = outputTypeInfo;
      vectorAggregationDesc.outputColVectorType = outputColVectorType;
      vectorAggregationDesc.outputDataTypePhysicalVariation = outputDataTypePhysicalVariation;
      vectorAggregationDesc.vecAggrClass = vecAggrClass;
      vectorAggregationDesc.constants = constants;

      return vectorAggregationDesc;
    }
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

  public List<ConstantVectorExpression> getConstants() {
    return constants;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(vecAggrClass.getSimpleName());
    if (inputExpression != null) {
      sb.append("(");
      sb.append(inputExpression);
      sb.append(") -> ");
    } else {
      sb.append("(*) -> ");
    }
    sb.append(outputTypeInfo.toString());
    if (outputDataTypePhysicalVariation != null && outputDataTypePhysicalVariation != DataTypePhysicalVariation.NONE) {
      sb.append("/");
      sb.append(outputDataTypePhysicalVariation);
    }
    if (GenericUDAFVariance.isVarianceFamilyName(aggregationName)) {
      sb.append(" aggregation: ");
      sb.append(aggregationName);
    }
    return sb.toString();
  }
}
