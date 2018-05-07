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

package org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates;

import java.io.Serializable;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;

/**
 * Base class for aggregation expressions.
 */
public abstract class VectorAggregateExpression  implements Serializable {

  private static final long serialVersionUID = 1L;

  protected final VectorAggregationDesc vecAggrDesc;

  protected final VectorExpression inputExpression;
  protected final TypeInfo inputTypeInfo;

  protected final TypeInfo outputTypeInfo;
  protected final DataTypePhysicalVariation outputDataTypePhysicalVariation;
  protected final GenericUDAFEvaluator.Mode mode;

  public static final int AVERAGE_COUNT_FIELD_INDEX = 0;
  public static final int AVERAGE_SUM_FIELD_INDEX = 1;
  public static final int AVERAGE_SOURCE_FIELD_INDEX = 2;

  public static final int VARIANCE_COUNT_FIELD_INDEX = 0;
  public static final int VARIANCE_SUM_FIELD_INDEX = 1;
  public static final int VARIANCE_VARIANCE_FIELD_INDEX = 2;

  // This constructor is used to momentarily create the object so match can be called.
  public VectorAggregateExpression() {
    this.vecAggrDesc = null;

    // Null out final members.
    inputExpression = null;
    inputTypeInfo = null;

    outputTypeInfo = null;
    outputDataTypePhysicalVariation = null;

    mode = null;
  }

  public VectorAggregateExpression(VectorAggregationDesc vecAggrDesc) {
    this.vecAggrDesc = vecAggrDesc;

    inputExpression = vecAggrDesc.getInputExpression();
    if (inputExpression != null) {
      inputTypeInfo = inputExpression.getOutputTypeInfo();
    } else {
      inputTypeInfo = null;
    }

    outputTypeInfo =  vecAggrDesc.getOutputTypeInfo();
    outputDataTypePhysicalVariation = vecAggrDesc.getOutputDataTypePhysicalVariation();

    mode = vecAggrDesc.getAggrDesc().getMode();
  }

  public VectorExpression getInputExpression() {
    return inputExpression;
  }

  public TypeInfo getOutputTypeInfo() {
    return outputTypeInfo;
  }
  public DataTypePhysicalVariation getOutputDataTypePhysicalVariation() {
    return outputDataTypePhysicalVariation;
  }

  /**
   * Buffer interface to store aggregates.
   */
  public static interface AggregationBuffer extends Serializable {
    int getVariableSize();

    void reset();
  };

  /*
   *    VectorAggregateExpression()
   *    VectorAggregateExpression(VectorAggregationDesc vecAggrDesc)
   *
   *    AggregationBuffer getNewAggregationBuffer()
   *    void aggregateInput(AggregationBuffer agg, VectorizedRowBatch unit)
   *    void aggregateInputSelection(VectorAggregationBufferRow[] aggregationBufferSets,
   *                int aggregateIndex, VectorizedRowBatch vrg)
   *    void reset(AggregationBuffer agg)
   *    long getAggregationBufferFixedSize()
   *
   *    boolean matches(String name, ColumnVector.Type inputColVectorType,
   *                ColumnVector.Type outputColVectorType, Mode mode)
   *    assignRowColumn(VectorizedRowBatch batch, int batchIndex, int columnNum,
   *                AggregationBuffer agg)
   *
   */
  public abstract AggregationBuffer getNewAggregationBuffer() throws HiveException;
  public abstract void aggregateInput(AggregationBuffer agg, VectorizedRowBatch unit)
        throws HiveException;
  public abstract void aggregateInputSelection(VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex, VectorizedRowBatch vrg) throws HiveException;
  public abstract void reset(AggregationBuffer agg) throws HiveException;
  public abstract long getAggregationBufferFixedSize();
  public boolean hasVariableSize() {
    return false;
  }

  public abstract boolean matches(String name, ColumnVector.Type inputColVectorType,
      ColumnVector.Type outputColVectorType, Mode mode);

  public abstract void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int columnNum,
      AggregationBuffer agg) throws HiveException;

  @Override
  public String toString() {
    return vecAggrDesc.toString();
  }
}

