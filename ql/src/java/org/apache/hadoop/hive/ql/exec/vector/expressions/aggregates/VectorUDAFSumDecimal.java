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

      transient private HiveDecimalWritable sum = new HiveDecimalWritable();
      transient private boolean isNull;

      // We use this to catch overflow.
      transient private boolean isOutOfRange;

      public void sumValue(HiveDecimalWritable writable, short scale) {
        if (isOutOfRange) {
          return;
        }
        HiveDecimal value = writable.getHiveDecimal();
        if (isNull) {
          sum.set(value);
          isNull = false;
        } else {
          HiveDecimal result;
          try {
            result = sum.getHiveDecimal().add(value);
          } catch (ArithmeticException e) {  // catch on overflow
            isOutOfRange = true;
            return;
          }
          sum.set(result);
        }
      }

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset() {
        isNull = true;
        isOutOfRange = false;
        sum.set(HiveDecimal.ZERO);
      }
    }

    private VectorExpression inputExpression;
    transient private final HiveDecimalWritable scratchDecimal;

    public VectorUDAFSumDecimal(VectorExpression inputExpression) {
      this();
      this.inputExpression = inputExpression;
    }

    public VectorUDAFSumDecimal() {
      super();
      scratchDecimal = new HiveDecimalWritable();
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
      HiveDecimalWritable value,
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
      HiveDecimalWritable[] values,
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
      HiveDecimalWritable[] values,
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
      HiveDecimalWritable value,
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
      HiveDecimalWritable value,
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
      HiveDecimalWritable[] values,
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
      HiveDecimalWritable[] values,
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
      if (myagg.isOutOfRange) {
        return;
      }

      HiveDecimalWritable[] vector = inputVector.vector;

      if (inputVector.isRepeating) {
        if ((inputVector.noNulls) || !inputVector.isNull[0]) {
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.sum.set(HiveDecimal.ZERO);
          }
          HiveDecimal value = vector[0].getHiveDecimal();
          HiveDecimal multiple;
          try {
            multiple = value.multiply(HiveDecimal.create(batchSize));
          } catch (ArithmeticException e) {  // catch on overflow
            myagg.isOutOfRange = true;
            return;
          }
          HiveDecimal result;
          try {
            result = myagg.sum.getHiveDecimal().add(multiple);
          } catch (ArithmeticException e) {  // catch on overflow
            myagg.isOutOfRange = true;
            return;
          }
          myagg.sum.set(result);
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
        HiveDecimalWritable[] vector,
        short scale,
        int batchSize,
        boolean[] isNull,
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          if (myagg.isNull) {
            myagg.isNull = false;
            myagg.sum.set(HiveDecimal.ZERO);
          }
          HiveDecimal value = vector[i].getHiveDecimal();
          HiveDecimal result;
          try {
            result = myagg.sum.getHiveDecimal().add(value);
          } catch (ArithmeticException e) {  // catch on overflow
            myagg.isOutOfRange = true;
            return;
          }
          myagg.sum.set(result);
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        short scale,
        int batchSize,
        int[] selected) {

      if (myagg.isNull) {
        myagg.sum.set(HiveDecimal.ZERO);
        myagg.isNull = false;
      }

      for (int i=0; i< batchSize; ++i) {
        HiveDecimal value = vector[selected[i]].getHiveDecimal();
        HiveDecimal result;
        try {
          result = myagg.sum.getHiveDecimal().add(value);
        } catch (ArithmeticException e) {  // catch on overflow
          myagg.isOutOfRange = true;
          return;
        }
        myagg.sum.set(result);
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        short scale,
        int batchSize,
        boolean[] isNull) {

      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          if (myagg.isNull) {
            myagg.sum.set(HiveDecimal.ZERO);
            myagg.isNull = false;
          }
          HiveDecimal value = vector[i].getHiveDecimal();
          HiveDecimal result;
          try {
            result = myagg.sum.getHiveDecimal().add(value);
          } catch (ArithmeticException e) {  // catch on overflow
            myagg.isOutOfRange = true;
            return;
          }
          myagg.sum.set(result);
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        short scale,
        int batchSize) {
      if (myagg.isNull) {
        myagg.sum.set(HiveDecimal.ZERO);
        myagg.isNull = false;
      }

      for (int i=0;i<batchSize;++i) {
        HiveDecimal value = vector[i].getHiveDecimal();
        HiveDecimal result;
        try {
          result = myagg.sum.getHiveDecimal().add(value);
        } catch (ArithmeticException e) {  // catch on overflow
          myagg.isOutOfRange = true;
          return;
        }
        myagg.sum.set(result);
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
      if (myagg.isNull || myagg.isOutOfRange) {
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

