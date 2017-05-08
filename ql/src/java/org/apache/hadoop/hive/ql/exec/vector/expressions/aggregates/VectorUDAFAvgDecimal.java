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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DecimalUtil;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

/**
 * Generated from template VectorUDAFAvg.txt.
 */
@Description(name = "avg",
    value = "_FUNC_(AVG) - Returns the average value of expr (vectorized, type: decimal)")
public class VectorUDAFAvgDecimal extends VectorAggregateExpression {

    private static final long serialVersionUID = 1L;

    /** class for storing the current aggregate value. */
    static class Aggregation implements AggregationBuffer {

      private static final long serialVersionUID = 1L;

      transient private final HiveDecimalWritable sum = new HiveDecimalWritable();
      transient private long count;
      transient private boolean isNull;

      public void sumValueWithNullCheck(HiveDecimalWritable writable) {
        if (isNull) {
          // Make a copy since we intend to mutate sum.
          sum.set(writable);
          count = 1;
          isNull = false;
        } else {
          // Note that if sum is out of range, mutateAdd will ignore the call.
          // At the end, sum.isSet() can be checked for null.
          sum.mutateAdd(writable);
          count++;
        }
      }

      public void sumValueNoNullCheck(HiveDecimalWritable writable) {
        sum.mutateAdd(writable);
        count++;
      }

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset() {
        isNull = true;
        sum.setFromLong(0L);
        count = 0;
      }
    }

    private VectorExpression inputExpression;

    @Override
    public VectorExpression inputExpression() {
      return inputExpression;
    }

    transient private Object[] partialResult;
    transient private LongWritable resultCount;
    transient private HiveDecimalWritable resultSum;
    transient private StructObjectInspector soi;

    /**
     * The scale of the SUM in the partial output
     */
    private short sumScale;

    /**
     * The precision of the SUM in the partial output
     */
    private short sumPrecision;

    /**
     * the scale of the input expression
     */
    private short inputScale;

    /**
     * the precision of the input expression
     */
    private short inputPrecision;

    public VectorUDAFAvgDecimal(VectorExpression inputExpression) {
      this();
      this.inputExpression = inputExpression;
    }

    public VectorUDAFAvgDecimal() {
      super();
      partialResult = new Object[2];
      resultCount = new LongWritable();
      resultSum = new HiveDecimalWritable();
      partialResult[0] = resultCount;
      partialResult[1] = resultSum;

    }

    private void initPartialResultInspector() {
      // the output type of the vectorized partial aggregate must match the
      // expected type for the row-mode aggregation
      // For decimal, the type is "same number of integer digits and 4 more decimal digits"
      
      DecimalTypeInfo dtiSum = GenericUDAFAverage.deriveSumFieldTypeInfo(inputPrecision, inputScale);
      this.sumScale = (short) dtiSum.scale();
      this.sumPrecision = (short) dtiSum.precision();
      
      List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
      foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
      foi.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(dtiSum));
      List<String> fname = new ArrayList<String>();
      fname.add("count");
      fname.add("sum");
      soi = ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
    }

    private Aggregation getCurrentAggregationBuffer(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int bufferIndex,
        int row) {
      VectorAggregationBufferRow mySet = aggregationBufferSets[row];
      Aggregation myagg = (Aggregation) mySet.getAggregationBuffer(bufferIndex);
      return myagg;
    }

    @Override
    public void aggregateInputSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      VectorizedRowBatch batch) throws HiveException {

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      inputExpression.evaluate(batch);

       DecimalColumnVector inputVector = ( DecimalColumnVector)batch.
        cols[this.inputExpression.getOutputColumn()];
      HiveDecimalWritable[] vector = inputVector.vector;

      if (inputVector.noNulls) {
        if (inputVector.isRepeating) {
          iterateNoNullsRepeatingWithAggregationSelection(
            aggregationBufferSets, bufferIndex,
            vector[0], batchSize);
        } else {
          if (batch.selectedInUse) {
            iterateNoNullsSelectionWithAggregationSelection(
              aggregationBufferSets, bufferIndex,
              vector, batch.selected, batchSize);
          } else {
            iterateNoNullsWithAggregationSelection(
              aggregationBufferSets, bufferIndex,
              vector, batchSize);
          }
        }
      } else {
        if (inputVector.isRepeating) {
          if (batch.selectedInUse) {
            iterateHasNullsRepeatingSelectionWithAggregationSelection(
              aggregationBufferSets, bufferIndex,
              vector[0], batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsRepeatingWithAggregationSelection(
              aggregationBufferSets, bufferIndex,
              vector[0], batchSize, inputVector.isNull);
          }
        } else {
          if (batch.selectedInUse) {
            iterateHasNullsSelectionWithAggregationSelection(
              aggregationBufferSets, bufferIndex,
              vector, batchSize, batch.selected, inputVector.isNull);
          } else {
            iterateHasNullsWithAggregationSelection(
              aggregationBufferSets, bufferIndex,
              vector, batchSize, inputVector.isNull);
          }
        }
      }
    }

    private void iterateNoNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      HiveDecimalWritable value,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          bufferIndex,
          i);
        myagg.sumValueWithNullCheck(value);
      }
    }

    private void iterateNoNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      HiveDecimalWritable[] values,
      int[] selection,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          bufferIndex,
          i);
        myagg.sumValueWithNullCheck(values[selection[i]]);
      }
    }

    private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      HiveDecimalWritable[] values,
      int batchSize) {
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          bufferIndex,
          i);
        myagg.sumValueWithNullCheck(values[i]);
      }
    }

    private void iterateHasNullsRepeatingSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
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
          bufferIndex,
          i);
        myagg.sumValueWithNullCheck(value);
      }

    }

    private void iterateHasNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      HiveDecimalWritable value,
      int batchSize,
      boolean[] isNull) {

      if (isNull[0]) {
        return;
      }

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          bufferIndex,
          i);
        myagg.sumValueWithNullCheck(value);
      }
    }

    private void iterateHasNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      HiveDecimalWritable[] values,
      int batchSize,
      int[] selection,
      boolean[] isNull) {

      for (int j=0; j < batchSize; ++j) {
        int i = selection[j];
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            bufferIndex,
            j);
          myagg.sumValueWithNullCheck(values[i]);
        }
      }
   }

    private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      HiveDecimalWritable[] values,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            bufferIndex,
            i);
          myagg.sumValueWithNullCheck(values[i]);
        }
      }
   }


    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
        throws HiveException {

        inputExpression.evaluate(batch);

        DecimalColumnVector inputVector =
            (DecimalColumnVector)batch.cols[this.inputExpression.getOutputColumn()];

        int batchSize = batch.size;

        if (batchSize == 0) {
          return;
        }

        Aggregation myagg = (Aggregation)agg;

        HiveDecimalWritable[] vector = inputVector.vector;

        if (inputVector.isRepeating) {
          if (inputVector.noNulls) {
            if (myagg.isNull) {
              myagg.isNull = false;
              myagg.sum.setFromLong(0L);
              myagg.count = 0;
            }
            HiveDecimal value = vector[0].getHiveDecimal();
            HiveDecimal multiple = value.multiply(HiveDecimal.create(batchSize));
            myagg.sum.mutateAdd(multiple);
            myagg.count += batchSize;
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
          myagg.sumValueWithNullCheck(vector[i]);
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        int batchSize,
        int[] selected) {

      if (myagg.isNull) {
        myagg.isNull = false;
        myagg.sum.setFromLong(0L);
        myagg.count = 0;
      }

      for (int i=0; i< batchSize; ++i) {
        myagg.sumValueNoNullCheck(vector[selected[i]]);
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        int batchSize,
        boolean[] isNull) {

      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          myagg.sumValueWithNullCheck(vector[i]);
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg,
        HiveDecimalWritable[] vector,
        int batchSize) {
      if (myagg.isNull) {
        myagg.isNull = false;
        myagg.sum.setFromLong(0L);
        myagg.count = 0;
      }

      for (int i=0;i<batchSize;++i) {
        myagg.sumValueNoNullCheck(vector[i]);
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
    public Object evaluateOutput(
        AggregationBuffer agg) throws HiveException {
      Aggregation myagg = (Aggregation) agg;
      // !isSet checks for overflow.
      if (myagg.isNull || !myagg.sum.isSet()) {
        return null;
      }
      else {
        assert(0 < myagg.count);
        resultCount.set (myagg.count);
        resultSum.set(myagg.sum);
        return partialResult;
      }
    }

  @Override
    public ObjectInspector getOutputObjectInspector() {
    return soi;
  }

  @Override
  public int getAggregationBufferFixedSize() {
    JavaDataModel model = JavaDataModel.get();
    return JavaDataModel.alignUp(
      model.object() +
      model.primitive2() * 2,
      model.memoryAlign());
  }

  @Override
  public void init(AggregationDesc desc) throws HiveException {
    ExprNodeDesc inputExpr = desc.getParameters().get(0);
    DecimalTypeInfo tiInput = (DecimalTypeInfo) inputExpr.getTypeInfo();
    this.inputScale = (short) tiInput.scale();
    this.inputPrecision = (short) tiInput.precision();

    initPartialResultInspector();
  }

  public VectorExpression getInputExpression() {
    return inputExpression;
  }

  public void setInputExpression(VectorExpression inputExpression) {
    this.inputExpression = inputExpression;
  }
}

