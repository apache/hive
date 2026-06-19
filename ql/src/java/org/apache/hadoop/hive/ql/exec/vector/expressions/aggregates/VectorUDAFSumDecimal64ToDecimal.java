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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
* VectorUDAFSumLong. Vectorized implementation for SUM aggregates.
*/
@Description(name = "sum",
    value = "_FUNC_(expr) - Returns the sum value of expr (vectorized, type: decimal64)")
public class VectorUDAFSumDecimal64ToDecimal extends VectorAggregateExpression {

  private static final long serialVersionUID = 1L;

  private int inputScale;

  private DecimalTypeInfo outputDecimalTypeInfo;

  private transient final HiveDecimalWritable temp = new HiveDecimalWritable();

  /**
   * class for storing the current aggregate value.
   */
  private static final class Aggregation implements AggregationBuffer {

    private static final long serialVersionUID = 1L;

    // The max for 18 - 1 digits.
    private static final long nearDecimal64Max =
        HiveDecimalWritable.getDecimal64AbsMax(HiveDecimalWritable.DECIMAL64_DECIMAL_DIGITS - 1);

    private final int inputScale;
    private final HiveDecimalWritable temp;

    //----------------------------------------------------------------------------------------------

    private long sum;
    private final HiveDecimalWritable regularDecimalSum = new HiveDecimalWritable(0);

    /**
    * Value is explicitly (re)initialized in reset()
    */
    private boolean isNull = true;
    private boolean usingRegularDecimal = false;

    public Aggregation(int inputScale, HiveDecimalWritable temp) {
      this.inputScale = inputScale;
      this.temp = temp;
    }

    public void sumValue(long value) {
      if (isNull) {
        sum = value;
        isNull = false;
      } else {
        if (Math.abs(sum) > nearDecimal64Max) {
          if (!usingRegularDecimal) {
            usingRegularDecimal = true;
            regularDecimalSum.deserialize64(sum, inputScale);
          } else {
            temp.deserialize64(sum, inputScale);
            regularDecimalSum.mutateAdd(temp);
          }
          sum = value;
        } else {
          sum += value;
        }
      }
    }

    // The isNull check and work has already been performed.
    public void sumValueNoCheck(long value) {
      if (Math.abs(sum) > nearDecimal64Max) {
        if (!usingRegularDecimal) {
          usingRegularDecimal = true;
          regularDecimalSum.deserialize64(sum, inputScale);
        } else {
          temp.deserialize64(sum, inputScale);
          regularDecimalSum.mutateAdd(temp);
        }
        sum = value;
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
      usingRegularDecimal = false;
      sum = 0;
      regularDecimalSum.setFromLong(0);
    }
  }

  // This constructor is used to momentarily create the object so match can be called.
  public VectorUDAFSumDecimal64ToDecimal() {
    super();
  }

  public VectorUDAFSumDecimal64ToDecimal(VectorAggregationDesc vecAggrDesc) {
    super(vecAggrDesc);
    init();
  }

  private void init() {
    inputScale = ((DecimalTypeInfo) inputTypeInfo).getScale();
    outputDecimalTypeInfo = (DecimalTypeInfo) outputTypeInfo;
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

    Decimal64ColumnVector inputVector =
        (Decimal64ColumnVector) batch.cols[
            this.inputExpression.getOutputColumnNum()];

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
        iterateHasNullsRepeatingWithAggregationSelection(
          aggregationBufferSets, aggregateIndex,
          vector[0], batchSize, inputVector.isNull);
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
      myagg.sumValue(value);
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
      myagg.sumValue(values[selection[i]]);
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
      myagg.sumValue(values[i]);
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
      myagg.sumValue(value);
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
        myagg.sumValue(values[i]);
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
        myagg.sumValue(values[i]);
      }
    }
 }

  @Override
  public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
      throws HiveException {

    inputExpression.evaluate(batch);

    Decimal64ColumnVector inputVector =
        (Decimal64ColumnVector) batch.cols[
            this.inputExpression.getOutputColumnNum()];

    int batchSize = batch.size;

    if (batchSize == 0) {
      return;
    }

    Aggregation myagg = (Aggregation)agg;

    long[] vector = inputVector.vector;

    if (inputVector.isRepeating) {
      if (inputVector.noNulls || !inputVector.isNull[0]) {
        if (myagg.isNull) {
          myagg.isNull = false;
          myagg.sum = 0;
        }
        myagg.sumValueNoCheck(vector[0]*batchSize);
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
        long value = vector[i];
        if (myagg.isNull) {
          myagg.isNull = false;
          myagg.sum = 0;
        }
        myagg.sumValueNoCheck(value);
      }
    }
  }

  private void iterateSelectionNoNulls(
      Aggregation myagg,
      long[] vector,
      int batchSize,
      int[] selected) {

    if (myagg.isNull) {
      myagg.sum = 0;
      myagg.isNull = false;
    }

    for (int i=0; i< batchSize; ++i) {
      long value = vector[selected[i]];
      myagg.sumValueNoCheck(value);
    }
  }

  private void iterateNoSelectionHasNulls(
      Aggregation myagg,
      long[] vector,
      int batchSize,
      boolean[] isNull) {

    for(int i=0;i<batchSize;++i) {
      if (!isNull[i]) {
        long value = vector[i];
        if (myagg.isNull) {
          myagg.sum = 0;
          myagg.isNull = false;
        }
        myagg.sumValueNoCheck(value);
      }
    }
  }

  private void iterateNoSelectionNoNulls(
      Aggregation myagg,
      long[] vector,
      int batchSize) {
    if (myagg.isNull) {
      myagg.sum = 0;
      myagg.isNull = false;
    }

    for (int i=0;i<batchSize;++i) {
      long value = vector[i];
      myagg.sumValueNoCheck(value);
    }
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new Aggregation(inputScale, temp);
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
     * Sum input is DECIMAL_64 and output is DECIMAL.
     *
     * Any mode (PARTIAL1, PARTIAL2, FINAL, COMPLETE).
     */
    return
        name.equals("sum") &&
        inputColVectorType == ColumnVector.Type.DECIMAL_64 &&
        outputColVectorType == ColumnVector.Type.DECIMAL;
  }

  @Override
  public void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int columnNum,
      AggregationBuffer agg) throws HiveException {

    DecimalColumnVector outputColVector = (DecimalColumnVector) batch.cols[columnNum];

    Aggregation myagg = (Aggregation) agg;
    final boolean isNull;
    if (!myagg.isNull) {
      if (!myagg.usingRegularDecimal) {
        myagg.regularDecimalSum.deserialize64(myagg.sum, inputScale);
      } else {
        myagg.temp.deserialize64(myagg.sum, inputScale);
        myagg.regularDecimalSum.mutateAdd(myagg.temp);
      }

      // Now, check for overflow.
      myagg.regularDecimalSum.mutateEnforcePrecisionScale(
          outputDecimalTypeInfo.getPrecision(), outputDecimalTypeInfo.getScale());
      isNull = !myagg.regularDecimalSum.isSet();
    } else {
      isNull = true;
    }
    if (isNull) {
      outputColVector.noNulls = false;
      outputColVector.isNull[batchIndex] = true;
      return;
    }
    outputColVector.isNull[batchIndex] = false;

    outputColVector.set(batchIndex, myagg.regularDecimalSum);
  }
}