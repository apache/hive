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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
* VectorUDAFVarSampTimestamp. Vectorized implementation for VARIANCE aggregates.
*/
@Description(name = "var_samp",
    value = "_FUNC_(x) - Returns the sample variance of a set of numbers (vectorized, double)")
public class VectorUDAFVarSampTimestamp extends VectorAggregateExpression {

    private static final long serialVersionUID = 1L;

    /**
    /* class for storing the current aggregate value.
    */
    private static final class Aggregation implements AggregationBuffer {

      private static final long serialVersionUID = 1L;

      transient private double sum;
      transient private long count;
      transient private double variance;

      /**
      * Value is explicitly (re)initialized in reset() (despite the init() bellow...)
      */
      transient private boolean isNull = true;

      public void init() {
        isNull = false;
        sum = 0;
        count = 0;
        variance = 0;
      }

      @Override
      public int getVariableSize() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reset () {
        isNull = true;
        sum = 0;
        count = 0;
        variance = 0;
      }
    }

    private VectorExpression inputExpression;

    @Override
    public VectorExpression inputExpression() {
      return inputExpression;
    }

    transient private LongWritable resultCount;
    transient private DoubleWritable resultSum;
    transient private DoubleWritable resultVariance;
    transient private Object[] partialResult;

    transient private ObjectInspector soi;


    public VectorUDAFVarSampTimestamp(VectorExpression inputExpression) {
      this();
      this.inputExpression = inputExpression;
    }

    public VectorUDAFVarSampTimestamp() {
      super();
      partialResult = new Object[3];
      resultCount = new LongWritable();
      resultSum = new DoubleWritable();
      resultVariance = new DoubleWritable();
      partialResult[0] = resultCount;
      partialResult[1] = resultSum;
      partialResult[2] = resultVariance;
      initPartialResultInspector();
    }

  private void initPartialResultInspector() {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("count");
        fname.add("sum");
        fname.add("variance");

        soi = ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
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

      inputExpression.evaluate(batch);

      TimestampColumnVector inputColVector = (TimestampColumnVector)batch.
        cols[this.inputExpression.getOutputColumn()];

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      if (inputColVector.isRepeating) {
        if (inputColVector.noNulls || !inputColVector.isNull[0]) {
          iterateRepeatingNoNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex, inputColVector.getDouble(0), batchSize);
        }
      }
      else if (!batch.selectedInUse && inputColVector.noNulls) {
        iterateNoSelectionNoNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex, inputColVector, batchSize);
      }
      else if (!batch.selectedInUse) {
        iterateNoSelectionHasNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex, inputColVector, batchSize, inputColVector.isNull);
      }
      else if (inputColVector.noNulls){
        iterateSelectionNoNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex, inputColVector, batchSize, batch.selected);
      }
      else {
        iterateSelectionHasNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex, inputColVector, batchSize,
            inputColVector.isNull, batch.selected);
      }

    }

    private void  iterateRepeatingNoNullsWithAggregationSelection(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        double value,
        int batchSize) {

      for (int i=0; i<batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        if (myagg.isNull) {
          myagg.init ();
        }
        myagg.sum += value;
        myagg.count += 1;
        if(myagg.count > 1) {
          double t = myagg.count*value - myagg.sum;
          myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
        }
      }
    }

    private void iterateSelectionHasNullsWithAggregationSelection(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        TimestampColumnVector inputColVector,
        int batchSize,
        boolean[] isNull,
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          j);
        int i = selected[j];
        if (!isNull[i]) {
          double value = inputColVector.getDouble(i);
          if (myagg.isNull) {
            myagg.init ();
          }
          myagg.sum += value;
          myagg.count += 1;
          if(myagg.count > 1) {
            double t = myagg.count*value - myagg.sum;
            myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
          }
        }
      }
    }

    private void iterateSelectionNoNullsWithAggregationSelection(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        TimestampColumnVector inputColVector,
        int batchSize,
        int[] selected) {

      for (int i=0; i< batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        double value = inputColVector.getDouble(selected[i]);
        if (myagg.isNull) {
          myagg.init ();
        }
        myagg.sum += value;
        myagg.count += 1;
        if(myagg.count > 1) {
          double t = myagg.count*value - myagg.sum;
          myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
        }
      }
    }

    private void iterateNoSelectionHasNullsWithAggregationSelection(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        TimestampColumnVector inputColVector,
        int batchSize,
        boolean[] isNull) {

      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
          i);
          double value = inputColVector.getDouble(i);
          if (myagg.isNull) {
            myagg.init ();
          }
          myagg.sum += value;
          myagg.count += 1;
        if(myagg.count > 1) {
          double t = myagg.count*value - myagg.sum;
          myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
        }
        }
      }
    }

    private void iterateNoSelectionNoNullsWithAggregationSelection(
        VectorAggregationBufferRow[] aggregationBufferSets,
        int aggregateIndex,
        TimestampColumnVector inputColVector,
        int batchSize) {

      for (int i=0; i<batchSize; ++i) {
        Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
        if (myagg.isNull) {
          myagg.init ();
        }
        double value = inputColVector.getDouble(i);
        myagg.sum += value;
        myagg.count += 1;
        if(myagg.count > 1) {
          double t = myagg.count*value - myagg.sum;
          myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
        }
      }
    }

    @Override
    public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
    throws HiveException {

      inputExpression.evaluate(batch);

      TimestampColumnVector inputColVector = (TimestampColumnVector)batch.
        cols[this.inputExpression.getOutputColumn()];

      int batchSize = batch.size;

      if (batchSize == 0) {
        return;
      }

      Aggregation myagg = (Aggregation)agg;

      if (inputColVector.isRepeating) {
        if (inputColVector.noNulls) {
          iterateRepeatingNoNulls(myagg, inputColVector.getDouble(0), batchSize);
        }
      }
      else if (!batch.selectedInUse && inputColVector.noNulls) {
        iterateNoSelectionNoNulls(myagg, inputColVector, batchSize);
      }
      else if (!batch.selectedInUse) {
        iterateNoSelectionHasNulls(myagg, inputColVector, batchSize, inputColVector.isNull);
      }
      else if (inputColVector.noNulls){
        iterateSelectionNoNulls(myagg, inputColVector, batchSize, batch.selected);
      }
      else {
        iterateSelectionHasNulls(myagg, inputColVector, batchSize, inputColVector.isNull, batch.selected);
      }
    }

    private void  iterateRepeatingNoNulls(
        Aggregation myagg,
        double value,
        int batchSize) {

      if (myagg.isNull) {
        myagg.init ();
      }

      // TODO: conjure a formula w/o iterating
      //

      myagg.sum += value;
      myagg.count += 1;
      if(myagg.count > 1) {
        double t = myagg.count*value - myagg.sum;
        myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
      }

      // We pulled out i=0 so we can remove the count > 1 check in the loop
      for (int i=1; i<batchSize; ++i) {
        myagg.sum += value;
        myagg.count += 1;
        double t = myagg.count*value - myagg.sum;
        myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
      }
    }

    private void iterateSelectionHasNulls(
        Aggregation myagg,
        TimestampColumnVector inputColVector,
        int batchSize,
        boolean[] isNull,
        int[] selected) {

      for (int j=0; j< batchSize; ++j) {
        int i = selected[j];
        if (!isNull[i]) {
          double value = inputColVector.getDouble(i);
          if (myagg.isNull) {
            myagg.init ();
          }
          myagg.sum += value;
          myagg.count += 1;
        if(myagg.count > 1) {
          double t = myagg.count*value - myagg.sum;
          myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
        }
        }
      }
    }

    private void iterateSelectionNoNulls(
        Aggregation myagg,
        TimestampColumnVector inputColVector,
        int batchSize,
        int[] selected) {

      if (myagg.isNull) {
        myagg.init ();
      }

      double value = inputColVector.getDouble(selected[0]);
      myagg.sum += value;
      myagg.count += 1;
      if(myagg.count > 1) {
        double t = myagg.count*value - myagg.sum;
        myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
      }

      // i=0 was pulled out to remove the count > 1 check in the loop
      //
      for (int i=1; i< batchSize; ++i) {
        value = inputColVector.getDouble(selected[i]);
        myagg.sum += value;
        myagg.count += 1;
        double t = myagg.count*value - myagg.sum;
        myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
      }
    }

    private void iterateNoSelectionHasNulls(
        Aggregation myagg,
        TimestampColumnVector inputColVector,
        int batchSize,
        boolean[] isNull) {

      for(int i=0;i<batchSize;++i) {
        if (!isNull[i]) {
          double value = inputColVector.getDouble(i);
          if (myagg.isNull) {
            myagg.init ();
          }
          myagg.sum += value;
          myagg.count += 1;
        if(myagg.count > 1) {
          double t = myagg.count*value - myagg.sum;
          myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
        }
        }
      }
    }

    private void iterateNoSelectionNoNulls(
        Aggregation myagg,
        TimestampColumnVector inputColVector,
        int batchSize) {

      if (myagg.isNull) {
        myagg.init ();
      }

      double value = inputColVector.getDouble(0);
      myagg.sum += value;
      myagg.count += 1;

      if(myagg.count > 1) {
        double t = myagg.count*value - myagg.sum;
        myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
      }

      // i=0 was pulled out to remove count > 1 check
      for (int i=1; i<batchSize; ++i) {
        value = inputColVector.getDouble(i);
        myagg.sum += value;
        myagg.count += 1;
        double t = myagg.count*value - myagg.sum;
        myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
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
        resultSum.set (myagg.sum);
        resultVariance.set (myagg.variance);
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
        model.primitive2()*3+
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

