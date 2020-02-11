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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

/**
 * VectorUDAFCountStar. Vectorized implementation for COUNT(*) aggregates.
 */
@Description(name = "count", value = "_FUNC_(expr) - Returns count(*) (vectorized)")
public class VectorUDAFCountStar extends VectorAggregateExpression {

  private static final long serialVersionUID = 1L;

    /**
     * class for storing the current aggregate value.
     */
    static class Aggregation implements AggregationBuffer {

      private static final long serialVersionUID = 1L;

      transient private long count;

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset() {
        count = 0L;
      }
    }

    // This constructor is used to momentarily create the object so match can be called.
    public VectorUDAFCountStar() {
      super();
    }

    public VectorUDAFCountStar(VectorAggregationDesc vecAggrDesc) {
      super(vecAggrDesc);
      init();
    }

    private void init() {
    }

    private Aggregation getCurrentAggregationBuffer(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        int row) {
      VectorAggregationBufferRow mySet = aggregationBufferSets[row];
      Aggregation myagg = (Aggregation) mySet.getAggregationBuffer(aggregateIndex);
      return myagg;
    }

    @Override
    public void aggregateInputSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      VectorizedRowBatch batch) throws HiveException {

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      // count(*) cares not about NULLs nor selection
      for (int i=0; i < batchSize; ++i) {
        Aggregation myAgg = getCurrentAggregationBuffer(
            aggregationBufferSets, aggregateIndex, i);
        ++myAgg.count;
      }
    }

    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
    throws HiveException {

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      Aggregation myagg = (Aggregation)agg;
      myagg.count += batchSize;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new Aggregation();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      Aggregation myAgg = (Aggregation) agg;
      myAgg.reset();
    }

    @Override
    public long getAggregationBufferFixedSize() {
      JavaDataModel model = JavaDataModel.get();
      return JavaDataModel.alignUp(
        model.object() +
        model.primitive2() +
        model.primitive1(),
        model.memoryAlign());
    }

  @Override
  public boolean matches(String name, ColumnVector.Type inputColVectorType,
      ColumnVector.Type outputColVectorType, Mode mode) {

    /*
     * Count null input which is for COUNT(*) and output is LONG.
     *
     * Just modes (PARTIAL1, COMPLETE).
     */
    return
        name.equals("count") &&
        inputColVectorType == null &&
        outputColVectorType == ColumnVector.Type.LONG &&
        (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE);
  }

  @Override
  public void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int columnNum,
      AggregationBuffer agg) throws HiveException {

    LongColumnVector outputColVector = (LongColumnVector) batch.cols[columnNum];
    Aggregation myagg = (Aggregation) agg;
    outputColVector.isNull[batchIndex] = false;
    outputColVector.vector[batchIndex] = myagg.count;
  }
}
