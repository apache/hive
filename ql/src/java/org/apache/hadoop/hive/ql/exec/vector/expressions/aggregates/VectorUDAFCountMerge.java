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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;


/**
 * VectorUDAFCountMerge. Vectorized implementation for COUNT aggregate on reduce-side (merge).
 */
@Description(name = "count",     value = "_FUNC_(expr) - Returns the merged sum value of expr (vectorized, type: long)")

public class VectorUDAFCountMerge extends VectorAggregateExpression {

  private static final long serialVersionUID = 1L;

    /**
     * class for storing the current aggregate value.
     */
    static class Aggregation implements AggregationBuffer {

      private static final long serialVersionUID = 1L;

      transient private long value;

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset() {
        value = 0L;
      }
    }

    private VectorExpression inputExpression = null;
    transient private final LongWritable result;

    public VectorUDAFCountMerge(VectorExpression inputExpression) {
      this();
      this.inputExpression = inputExpression;
    }

    public VectorUDAFCountMerge() {
      super();
      result = new LongWritable(0);
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

      LongColumnVector inputVector = (LongColumnVector)batch.
                cols[this.inputExpression.getOutputColumn()];
      long[] vector = inputVector.vector;

      if (inputVector.noNulls) {
        if (inputVector.isRepeating) {
          iterateNoNullsRepeatingWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            vector[0], batchSize);
        } else {
          if (batch.selectedInUse) {
            iterateNoNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector, batch.selected, batchSize);
          } else {
            iterateNoNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector, batchSize);
          }
        }
      } else {
        if (inputVector.isRepeating) {
          if (batch.selectedInUse) {
            iterateHasNullsRepeatingSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector[0], batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsRepeatingWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector[0], batchSize, inputVector.isNull);
          }
        } else {
          if (batch.selectedInUse) {
            iterateHasNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector, batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector, batchSize, inputVector.isNull);
          }
        }
      }
    }

    private void iterateNoNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      long value,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets, 
          aggregateIndex,
          i);
        myagg.value += value;
      }
    } 

    private void iterateNoNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      long[] values,
      int[] selection,
      int batchSize) {
      
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets, 
          aggregateIndex,
          i);
        myagg.value += values[selection[i]];
      }
    }

    private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      long[] values,
      int batchSize) {
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets, 
          aggregateIndex,
          i);
        myagg.value += values[i];
      }
    }

    private void iterateHasNullsRepeatingSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      long value,
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
        myagg.value += value;
      }
      
    }

    private void iterateHasNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      long value,
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
        myagg.value += value;
      }
    }

    private void iterateHasNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      long[] values,
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
          myagg.value += values[i];
        }
      }
   }

    private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      long[] values,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets, 
            aggregateIndex,
            i);
          myagg.value += values[i];
        }
      }
   }

    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
    throws HiveException {

      inputExpression.evaluate(batch);

      LongColumnVector inputVector = (LongColumnVector)batch.
              cols[this.inputExpression.getOutputColumn()];

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      Aggregation myagg = (Aggregation)agg;

      long[] vector = inputVector.vector;
      
      if (inputVector.isRepeating) {
        if (inputVector.noNulls) {
          myagg.value += vector[0]*batchSize;
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
        long[] vector, 
        int batchSize,
        boolean[] isNull, 
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          myagg.value += vector[i];
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg, 
        long[] vector, 
        int batchSize, 
        int[] selected) {

      for (int i=0; i< batchSize; ++i) {
        myagg.value += vector[selected[i]];
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg, 
        long[] vector, 
        int batchSize,
        boolean[] isNull) {
      
      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          myagg.value += vector[i];
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg, 
        long[] vector, 
        int batchSize) {

      for (int i=0;i<batchSize;++i) {
        myagg.value += vector[i];
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
      result.set (myagg.value);
      return result;
    }

    @Override
    public ObjectInspector getOutputObjectInspector() {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    @Override
    public int getAggregationBufferFixedSize() {
      JavaDataModel model = JavaDataModel.get();
      return JavaDataModel.alignUp(
        model.object() +
        model.primitive2() +
        model.primitive1(),
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

