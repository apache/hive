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

import org.apache.hadoop.hive.common.type.Decimal128;
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

      transient private Decimal128 sum = new Decimal128();
      transient private boolean isNull;

      public void sumValue(Decimal128 value, short scale) {
        if (isNull) {
          sum.update(value, scale);
          isNull = false;
        } else {
          sum.addDestructive(value, scale);
        }
      }

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset() {
        isNull = true;
        sum.zeroClear();
      }
    }

    private VectorExpression inputExpression;
    transient private final Decimal128 scratchDecimal;

    public VectorUDAFSumDecimal(VectorExpression inputExpression) {
      this();
      this.inputExpression = inputExpression;
    }

    public VectorUDAFSumDecimal() {
      super();
      scratchDecimal = new Decimal128();
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
      Decimal128[] vector = inputVector.vector;

      if (inputVector.noNulls) {
        if (inputVector.isRepeating) {
          iterateNoNullsRepeatingWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            vector[0], inputVector.scale,
            batchSize);
        } else {
          if (batch.selectedInUse) {
            iterateNoNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector, inputVector.scale,
              batch.selected, batchSize);
          } else {
            iterateNoNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector, inputVector.scale,
              batchSize);
          }
        }
      } else {
        if (inputVector.isRepeating) {
          if (batch.selectedInUse) {
            iterateHasNullsRepeatingSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector[0], inputVector.scale,
              batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsRepeatingWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector[0], inputVector.scale,
              batchSize, inputVector.isNull);
          }
        } else {
          if (batch.selectedInUse) {
            iterateHasNullsSelectionWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector, inputVector.scale,
              batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              vector,inputVector.scale,
              batchSize, inputVector.isNull);
          }
        }
      }
    }

    private void iterateNoNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      Decimal128 value,
      short scale,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(value, scale);
      }
    }

    private void iterateNoNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      Decimal128[] values,
      short scale,
      int[] selection,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(values[selection[i]], scale);
      }
    }

    private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      Decimal128[] values,
      short scale,
      int batchSize) {
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        myagg.sumValue(values[i], scale);
      }
    }

    private void iterateHasNullsRepeatingSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      Decimal128 value,
      short scale,
      int batchSize,
      int[] selection,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[selection[i]]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
            i);
          myagg.sumValue(value, scale);
        }
      }

    }

    private void iterateHasNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      Decimal128 value,
      short scale,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
            i);
          myagg.sumValue(value, scale);
        }
      }
    }

    private void iterateHasNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      Decimal128[] values,
      short scale,
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
          myagg.sumValue(values[i], scale);
        }
      }
   }

    private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      Decimal128[] values,
      short scale,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
            i);
          myagg.sumValue(values[i], scale);
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

      Decimal128[] vector = inputVector.vector;

      if (inputVector.isRepeating) {
        if ((inputVector.noNulls) || !inputVector.isNull[0]) {
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.sum.zeroClear();
          }
          scratchDecimal.update(batchSize);
          scratchDecimal.multiplyDestructive(vector[0], inputVector.scale);
          myagg.sum.addDestructive(scratchDecimal, inputVector.scale);
        }
        return;
      }

      if (!batch.selectedInUse && inputVector.noNulls) {
        iterateNoSelectionNoNulls(myagg, vector, inputVector.scale, batchSize);
      }
      else if (!batch.selectedInUse) {
        iterateNoSelectionHasNulls(myagg, vector, inputVector.scale, batchSize, inputVector.isNull);
      }
      else if (inputVector.noNulls){
        iterateSelectionNoNulls(myagg, vector, inputVector.scale, batchSize, batch.selected);
      }
      else {
        iterateSelectionHasNulls(myagg, vector, inputVector.scale, batchSize, inputVector.isNull, batch.selected);
      }
    }

    private void iterateSelectionHasNulls(
        Aggregation myagg,
        Decimal128[] vector,
        short scale,
        int batchSize,
        boolean[] isNull,
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          Decimal128 value = vector[i];
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.sum.zeroClear();
          }
          myagg.sum.addDestructive(value, scale);
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg,
        Decimal128[] vector,
        short scale,
        int batchSize,
        int[] selected) {

      if (myagg.isNull) {
        myagg.sum.zeroClear();
        myagg.isNull = false;
      }

      for (int i=0; i< batchSize; ++i) {
        Decimal128 value = vector[selected[i]];
        myagg.sum.addDestructive(value, scale);
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg,
        Decimal128[] vector,
        short scale,
        int batchSize,
        boolean[] isNull) {

      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          Decimal128 value = vector[i];
          if (myagg.isNull) {
            myagg.sum.zeroClear();
            myagg.isNull = false;
          }
          myagg.sum.addDestructive(value, scale);
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg,
        Decimal128[] vector,
        short scale,
        int batchSize) {
      if (myagg.isNull) {
        myagg.sum.zeroClear();
        myagg.isNull = false;
      }

      for (int i=0;i<batchSize;++i) {
        Decimal128 value = vector[i];
        myagg.sum.addDestructive(value, scale);
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
      if (myagg.isNull) {
        return null;
      }
      else {
        return HiveDecimal.create(myagg.sum.toBigDecimal());
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

