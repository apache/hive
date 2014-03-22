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
import org.apache.hive.common.util.Decimal128FastBuffer;

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

      transient private final Decimal128 sum = new Decimal128();
      transient private long count;
      transient private boolean isNull;

      public void sumValueWithCheck(Decimal128 value, short scale) {
        if (isNull) {
          sum.update(value);
          sum.changeScaleDestructive(scale);
          count = 1;
          isNull = false;
        } else {
          sum.addDestructive(value, scale);
          count++;
        }
      }

      public void sumValueNoCheck(Decimal128 value, short scale) {
        sum.addDestructive(value, scale);
        count++;
      }


      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset() {
        isNull = true;
        sum.zeroClear();
        count = 0L;
      }
    }

    private VectorExpression inputExpression;
    transient private Object[] partialResult;
    transient private LongWritable resultCount;
    transient private HiveDecimalWritable resultSum;
    transient private StructObjectInspector soi;

    transient private final Decimal128FastBuffer scratch;

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

    /**
     * A value used as scratch to avoid allocating at runtime.
     * Needed by computations like vector[0] * batchSize
     */
    transient private Decimal128 scratchDecimal = new Decimal128();

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
      scratch = new Decimal128FastBuffer();

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
      Decimal128[] vector = inputVector.vector;

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
      Decimal128 value,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          bufferIndex,
          i);
        myagg.sumValueWithCheck(value, this.sumScale);
      }
    }

    private void iterateNoNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      Decimal128[] values,
      int[] selection,
      int batchSize) {

      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          bufferIndex,
          i);
        myagg.sumValueWithCheck(values[selection[i]], this.sumScale);
      }
    }

    private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      Decimal128[] values,
      int batchSize) {
      for (int i=0; i < batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          bufferIndex,
          i);
        myagg.sumValueWithCheck(values[i], this.sumScale);
      }
    }

    private void iterateHasNullsRepeatingSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      Decimal128 value,
      int batchSize,
      int[] selection,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[selection[i]]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            bufferIndex,
            i);
          myagg.sumValueWithCheck(value, this.sumScale);
        }
      }

    }

    private void iterateHasNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      Decimal128 value,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            bufferIndex,
            i);
          myagg.sumValueWithCheck(value, this.sumScale);
        }
      }
    }

    private void iterateHasNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      Decimal128[] values,
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
          myagg.sumValueWithCheck(values[i], this.sumScale);
        }
      }
   }

    private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int bufferIndex,
      Decimal128[] values,
      int batchSize,
      boolean[] isNull) {

      for (int i=0; i < batchSize; ++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            bufferIndex,
            i);
          myagg.sumValueWithCheck(values[i], this.sumScale);
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

        Decimal128[] vector = inputVector.vector;

        if (inputVector.isRepeating) {
          if (inputVector.noNulls) {
            if (myagg.isNull) {
              myagg.isNull = false;
              myagg.sum.zeroClear();
              myagg.count = 0;
            }
            scratchDecimal.update(batchSize);
            scratchDecimal.multiplyDestructive(vector[0], vector[0].getScale());
            myagg.sum.update(scratchDecimal);
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
        Decimal128[] vector,
        int batchSize,
        boolean[] isNull,
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          Decimal128 value = vector[i];
          myagg.sumValueWithCheck(value, this.sumScale);
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg,
        Decimal128[] vector,
        int batchSize,
        int[] selected) {

      if (myagg.isNull) {
        myagg.isNull = false;
        myagg.sum.zeroClear();
        myagg.count = 0;
      }

      for (int i=0; i< batchSize; ++i) {
        Decimal128 value = vector[selected[i]];
        myagg.sumValueNoCheck(value, this.sumScale);
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg,
        Decimal128[] vector,
        int batchSize,
        boolean[] isNull) {

      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          Decimal128 value = vector[i];
          myagg.sumValueWithCheck(value, this.sumScale);
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg,
        Decimal128[] vector,
        int batchSize) {
      if (myagg.isNull) {
        myagg.isNull = false;
        myagg.sum.zeroClear();
        myagg.count = 0;
      }

      for (int i=0;i<batchSize;++i) {
        Decimal128 value = vector[i];
        myagg.sumValueNoCheck(value, this.sumScale);
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
      if (myagg.isNull) {
        return null;
      }
      else {
        assert(0 < myagg.count);
        resultCount.set (myagg.count);
        resultSum.set(HiveDecimal.create(myagg.sum.toBigDecimal()));
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

