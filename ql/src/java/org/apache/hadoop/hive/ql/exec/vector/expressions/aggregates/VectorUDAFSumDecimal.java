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

package org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
* VectorUDAFSumDecimal. Vectorized implementation for SUM aggregates.
*/
@Description(name = "sum",
    value = "_FUNC_(expr) - Returns the sum value of expr (vectorized, type: decimal)")
public class VectorUDAFSumDecimal extends VectorAggregateExpression {

    private static final long serialVersionUID = 1L;

    /**
     * class for storing the current aggregate value.
     */
    private static final class Aggregation implements AggregationBuffer {

      private static final long serialVersionUID = 1L;

      transient private final HiveDecimalWritable sum = new HiveDecimalWritable();
      transient private boolean isNull;

      public void sumValue(HiveDecimalWritable writable) {
        if (isNull) {
          // Make a copy since we intend to mutate sum.
          sum.set(writable);
          isNull = false;
        } else {
          sum.mutateAdd(writable);
        }
      }

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset() {
        isNull = true;
        sum.setFromLong(0L);
      }
    }

    private VectorExpression inputExpression;

    @Override
    public VectorExpression inputExpression() {
      return inputExpression;
    }

    public VectorUDAFSumDecimal(VectorExpression inputExpression) {
      this();
      this.inputExpression = inputExpression;
    }

    public VectorUDAFSumDecimal() {
      super();
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

      DecimalColumnVector inputVector = (DecimalColumnVector)batch.
        cols[this.inputExpression.getOutputColumn()];
      HiveDecimalWritable[] vector = inputVector.vector;

      if (inputVector.noNulls) {
        if (inputVector.isRepeating) {
          iterateNoNullsRepeatingWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            vector[0],
            batchSize);
        } else {
          if (batch.selectedInUse) {
            iterateNoNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector,
              batch.selected, batchSize);
          } else {
            iterateNoNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector,
              batchSize);
          }
        }
      } else {
        if (inputVector.isRepeating) {
          if (batch.selectedInUse) {
            iterateHasNullsRepeatingSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector[0],
              batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsRepeatingWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector[0],
              batchSize, inputVector.isNull);
          }
        } else {
          if (batch.selectedInUse) {
            iterateHasNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector,
              batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector,
              batchSize, inputVector.isNull);
          }
        }
      }
    }

    private void iterateNoNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      HiveDecimalWritable value,
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
      HiveDecimalWritable[] values,
      int[] selection,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(values[selection[i]]);
      }
    }

    private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      HiveDecimalWritable[] values,
      int batchSize) {
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(values[i]);
      }
    }

    private void iterateHasNullsRepeatingSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      HiveDecimalWritable value,
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
      HiveDecimalWritable value,
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
      HiveDecimalWritable[] values,
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
          myagg.sumValue(values[i]);
        }
      }
   }

    private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      HiveDecimalWritable[] values,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
            i);
          myagg.sumValue(values[i]);
        }
      }
   }


    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
    throws HiveException {

      inputExpression.evaluate(batch);

      DecimalColumnVector inputVector = (DecimalColumnVector)batch.
          cols[this.inputExpression.getOutputColumn()];

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      Aggregation myagg = (Aggregation)agg;

      HiveDecimalWritable[] vector = inputVector.vector;

      if (inputVector.isRepeating) {
        if ((inputVector.noNulls) || !inputVector.isNull[0]) {
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.sum.setFromLong(0L);
          }
          HiveDecimal value = vector[0].getHiveDecimal();
          HiveDecimal multiple = value.multiply(HiveDecimal.create(batchSize));
          myagg.sum.mutateAdd(multiple);
        }
        return;
      }

      if (!batch.selectedInUse && inputVector.noNulls) {
        iterateNoSelectionNoNulls(myagg, vector, batchSize);
      }
      else if (!batch.selectedInUse) {
        iterateNoSelectionHasNulls(myagg, vector, batchSize, inputVector.isNull);
      }
      else if (inputVector.noNulls){
        iterateSelectionNoNulls(myagg, vector, batchSize, batch.selected);
      }
      else {
        iterateSelectionHasNulls(myagg, vector, batchSize, inputVector.isNull, batch.selected);
      }
    }

    private void iterateSelectionHasNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        int batchSize,
        boolean[] isNull,
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.sum.setFromLong(0L);
          }
          myagg.sum.mutateAdd(vector[i]);
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        int batchSize,
        int[] selected) {

      if (myagg.isNull) {
        myagg.sum.setFromLong(0L);
        myagg.isNull = false;
      }

      for (int i=0; i< batchSize; ++i) {
        myagg.sum.mutateAdd(vector[selected[i]]);
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        int batchSize,
        boolean[] isNull) {

      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          if (myagg.isNull) {
            myagg.sum.setFromLong(0L);
            myagg.isNull = false;
          }
          myagg.sum.mutateAdd(vector[i]);
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        int batchSize) {
      if (myagg.isNull) {
        myagg.sum.setFromLong(0L);
        myagg.isNull = false;
      }

      for (int i=0;i<batchSize;++i) {
        myagg.sum.mutateAdd(vector[i]);
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
    public Object evaluateOutput(AggregationBuffer agg) throws HiveException {
      Aggregation myagg = (Aggregation) agg;
      // !isSet checks for overflow.
      if (myagg.isNull || !myagg.sum.isSet()) {
        return null;
      }
      else {
        return myagg.sum.getHiveDecimal();
      }
    }

    @Override
    public ObjectInspector getOutputObjectInspector() {
      return PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;
    }

  @Override
  public int getAggregationBufferFixedSize() {
      JavaDataModel model = JavaDataModel.get();
      return JavaDataModel.alignUp(
        model.object(),
        model.memoryAlign());
  }

  @Override
  public void init(AggregationDesc desc) throws HiveException {
    // No-op
  }

  public VectorExpression getInputExpression() {
    return inputExpression;
  }

  public void setInputExpression(VectorExpression inputExpression) {
    this.inputExpression = inputExpression;
  }
}

