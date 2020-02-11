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
 * VectorUDAFCountLong. Vectorized implementation for COUNT aggregates.
 */
@Description(name = "count", value = "_FUNC_(expr) - Returns the count (vectorized)")
public class VectorUDAFCount extends VectorAggregateExpression {

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
    public VectorUDAFCount() {
      super();
    }

    public VectorUDAFCount(VectorAggregationDesc vecAggrDesc) {
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

      inputExpression.evaluate(batch);

      ColumnVector inputVector = batch.cols[this.inputExpression.getOutputColumnNum()];

      if (inputVector.isRepeating) {
        if (inputVector.noNulls || !inputVector.isNull[0]) {
          iterateNoNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex, batchSize);
        }
      } else if (inputVector.noNulls) {
          // if there are no nulls then the iteration is the same on all cases
          iterateNoNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex, batchSize);
      } else if (!batch.selectedInUse) {
          iterateHasNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            batchSize, inputVector.isNull);
      } else if (batch.selectedInUse) {
          iterateHasNullsSelectionWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            batchSize, batch.selected, inputVector.isNull);
      }
    }

    private void iterateNoNullsWithAggregationSelection(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        int batchSize) {

        for (int i=0; i < batchSize; ++i) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
            i);
          myagg.count++;
        }
    }

    private void iterateHasNullsWithAggregationSelection(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        int batchSize,
        boolean[] isNull) {

        for (int i=0; i < batchSize; ++i) {
          if (!isNull[i]) {
            Aggregation myagg = getCurrentAggregationBuffer(
              aggregationBufferSets,
              aggregateIndex,
              i);
            myagg.count++;
          }
        }
    }

    private void iterateHasNullsSelectionWithAggregationSelection(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        int batchSize,
        int[] selection,
        boolean[] isNull) {

        for (int j=0; j < batchSize; ++j) {
          int i = selection[j];
          if (!isNull[i]) {
            Aggregation myagg = getCurrentAggregationBuffer(
              aggregationBufferSets,
              aggregateIndex,
              j);
            myagg.count++;
          }
        }
    }


    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
    throws HiveException {

      inputExpression.evaluate(batch);

      ColumnVector inputVector = batch.cols[this.inputExpression.getOutputColumnNum()];

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      Aggregation myagg = (Aggregation)agg;

      if (inputVector.isRepeating) {
        if (inputVector.noNulls || !inputVector.isNull[0]) {
          myagg.count += batchSize;
        }
        return;
      }

      if (inputVector.noNulls) {
        myagg.count += batchSize;
        return;
      }
      else if (!batch.selectedInUse) {
        iterateNoSelectionHasNulls(myagg, batchSize, inputVector.isNull);
      }
      else {
        iterateSelectionHasNulls(myagg, batchSize, inputVector.isNull, batch.selected);
      }
    }

    private void iterateSelectionHasNulls(
        Aggregation myagg,
        int batchSize,
        boolean[] isNull,
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          myagg.count += 1;
        }
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg,
        int batchSize,
        boolean[] isNull) {

      for (int i=0; i< batchSize; ++i) {
        if (!isNull[i]) {
          myagg.count += 1;
        }
      }
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
     * Count *any* input except null which is for COUNT(*) and output is LONG.
     *
     * Just modes (PARTIAL1, COMPLETE).
     */
    return
        name.equals("count") &&
        inputColVectorType != null &&
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
