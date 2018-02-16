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
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

/**
* VectorUDAFSumTimestamp. Vectorized implementation for SUM aggregates.
*/
@Description(name = "sum",
    value = "_FUNC_(expr) - Returns the sum value of expr (vectorized, type: <ValueType>)")
public class VectorUDAFSumTimestamp extends VectorAggregateExpression {

    private static final long serialVersionUID = 1L;

    /**
     * class for storing the current aggregate value.
     */
    private static final class Aggregation implements AggregationBuffer {

      private static final long serialVersionUID = 1L;

      transient private double sum;

      /**
      * Value is explicitly (re)initialized in reset()
      */
      transient private boolean isNull = true;

      public void sumValue(double value) {
        if (isNull) {
          sum = value;
          isNull = false;
        } else {
          sum += value;
        }
      }

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset () {
        isNull = true;
        sum = 0;;
      }
    }

    // This constructor is used to momentarily create the object so match can be called.
    public VectorUDAFSumTimestamp() {
      super();
    }

    public VectorUDAFSumTimestamp(VectorAggregationDesc vecAggrDesc) {
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

      TimestampColumnVector inputVector =
          (TimestampColumnVector)batch.cols[this.inputExpression.getOutputColumnNum()];

      if (inputVector.noNulls) {
        if (inputVector.isRepeating) {
          iterateNoNullsRepeatingWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputVector.getDouble(0), batchSize);
        } else {
          if (batch.selectedInUse) {
            iterateNoNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              inputVector, batch.selected, batchSize);
          } else {
            iterateNoNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              inputVector, batchSize);
          }
        }
      } else {
        if (inputVector.isRepeating) {
          if (batch.selectedInUse) {
            iterateHasNullsRepeatingSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              inputVector.getDouble(0), batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsRepeatingWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              inputVector.getDouble(0), batchSize, inputVector.isNull);
          }
        } else {
          if (batch.selectedInUse) {
            iterateHasNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              inputVector, batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              inputVector, batchSize, inputVector.isNull);
          }
        }
      }
    }

    private void iterateNoNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      double value,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(value);
      }
    }

    private void iterateNoNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      TimestampColumnVector inputVector,
      int[] selection,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(inputVector.getDouble(selection[i]));
      }
    }

    private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      TimestampColumnVector inputVector,
      int batchSize) {
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(inputVector.getDouble(i));
      }
    }

    private void iterateHasNullsRepeatingSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      double value,
      int batchSize,
      int[] selection,
      boolean[] isNull) {

      if (isNull[0]) {
        return;
      }

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(value);
      }

    }

    private void iterateHasNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      double value,
      int batchSize,
      boolean[] isNull) {

      if (isNull[0]) {
        return;
      }

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(value);
      }
    }

    private void iterateHasNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      TimestampColumnVector inputVector,
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
          myagg.sumValue(inputVector.getDouble(i));
        }
      }
   }

    private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      TimestampColumnVector inputVector,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
            i);
          myagg.sumValue(inputVector.getDouble(i));
        }
      }
    }

    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
    throws HiveException {

      inputExpression.evaluate(batch);

      TimestampColumnVector inputVector =
          (TimestampColumnVector)batch.cols[this.inputExpression.getOutputColumnNum()];

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      Aggregation myagg = (Aggregation)agg;

      if (inputVector.isRepeating) {
        if (inputVector.noNulls || !inputVector.isNull[0]) {
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.sum = 0;
          }
          myagg.sum += inputVector.getDouble(0) * batchSize;
        }
        return;
      }

      if (!batch.selectedInUse && inputVector.noNulls) {
        iterateNoSelectionNoNulls(myagg, inputVector, batchSize);
      }
      else if (!batch.selectedInUse) {
        iterateNoSelectionHasNulls(myagg, inputVector, batchSize, inputVector.isNull);
      }
      else if (inputVector.noNulls){
        iterateSelectionNoNulls(myagg, inputVector, batchSize, batch.selected);
      }
      else {
        iterateSelectionHasNulls(myagg, inputVector, batchSize, inputVector.isNull, batch.selected);
      }
    }

    private void iterateSelectionHasNulls(
        Aggregation myagg,
        TimestampColumnVector inputVector,
        int batchSize,
        boolean[] isNull,
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.sum = 0;
          }
          myagg.sum += inputVector.getDouble(i);
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg,
        TimestampColumnVector inputVector,
        int batchSize,
        int[] selected) {

      if (myagg.isNull) {
        myagg.sum = 0;
        myagg.isNull = false;
      }

      for (int i=0; i< batchSize; ++i) {
        myagg.sum += inputVector.getDouble(selected[i]);
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg,
        TimestampColumnVector inputVector,
        int batchSize,
        boolean[] isNull) {

      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          if (myagg.isNull) {
            myagg.sum = 0;
            myagg.isNull = false;
          }
          myagg.sum += inputVector.getDouble(i);
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg,
        TimestampColumnVector inputVector,
        int batchSize) {
      if (myagg.isNull) {
        myagg.sum = 0;
        myagg.isNull = false;
      }

      for (int i=0;i<batchSize;++i) {
        myagg.sum += inputVector.getDouble(i);
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
        model.object(),
        model.memoryAlign());
  }

  @Override
  public boolean matches(String name, ColumnVector.Type inputColVectorType,
      ColumnVector.Type outputColVectorType, Mode mode) {

    /*
     * Sum input TIMESTAMP and output DOUBLE.
     *
     * Just modes (PARTIAL1, COMPLETE).
     */
    return
        name.equals("sum") &&
        inputColVectorType == ColumnVector.Type.TIMESTAMP &&
        outputColVectorType == ColumnVector.Type.DOUBLE &&
        (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE);
  }

  @Override
  public void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int columnNum,
      AggregationBuffer agg) throws HiveException {

    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[columnNum];

    Aggregation myagg = (Aggregation) agg;
    if (myagg.isNull) {
      outputColVector.noNulls = false;
      outputColVector.isNull[batchIndex] = true;
      return;
    }
    outputColVector.isNull[batchIndex] = false;

    outputColVector.vector[batchIndex] = myagg.sum;
  }
}