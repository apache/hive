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

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression.AggregationBuffer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.BloomFilter;

public class VectorUDAFBloomFilterMerge extends VectorAggregateExpression {

  private static final long serialVersionUID = 1L;

  private VectorExpression inputExpression;

  @Override
  public VectorExpression inputExpression() {
    return inputExpression;
  }

  private long expectedEntries = -1;
  transient private int aggBufferSize = -1;
  transient private BytesWritable bw = new BytesWritable();

  /**
   * class for storing the current aggregate value.
   */
  private static final class Aggregation implements AggregationBuffer {
    private static final long serialVersionUID = 1L;

    byte[] bfBytes;

    public Aggregation(long expectedEntries) {
      try {
        BloomFilter bf = new BloomFilter(expectedEntries);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        BloomFilter.serialize(bytesOut, bf);
        bfBytes = bytesOut.toByteArray();
      } catch (Exception err) {
        throw new IllegalArgumentException("Error creating aggregation buffer", err);
      }
    }

    @Override
    public int getVariableSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      // Do not change the initial bytes which contain NumHashFunctions/NumBits!
      Arrays.fill(bfBytes, BloomFilter.START_OF_SERIALIZED_LONGS, bfBytes.length, (byte) 0);
    }
  }

  public VectorUDAFBloomFilterMerge(VectorExpression inputExpression) {
    this();
    this.inputExpression = inputExpression;
  }

  public VectorUDAFBloomFilterMerge() {
    super();
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    if (expectedEntries < 0) {
      throw new IllegalStateException("expectedEntries not initialized");
    }
    return new Aggregation(expectedEntries);
  }

  @Override
  public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
      throws HiveException {

    inputExpression.evaluate(batch);

    ColumnVector inputColumn =  batch.cols[this.inputExpression.getOutputColumn()];

    int batchSize = batch.size;

    if (batchSize == 0) {
      return;
    }

    Aggregation myagg = (Aggregation) agg;

    if (inputColumn.isRepeating) {
      if (inputColumn.noNulls) {
        processValue(myagg, inputColumn, 0);
      }
      return;
    }

    if (!batch.selectedInUse && inputColumn.noNulls) {
      iterateNoSelectionNoNulls(myagg, inputColumn, batchSize);
    }
    else if (!batch.selectedInUse) {
      iterateNoSelectionHasNulls(myagg, inputColumn, batchSize);
    }
    else if (inputColumn.noNulls){
      iterateSelectionNoNulls(myagg, inputColumn, batchSize, batch.selected);
    }
    else {
      iterateSelectionHasNulls(myagg, inputColumn, batchSize, batch.selected);
    }
  }

  private void iterateNoSelectionNoNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize) {
    for (int i=0; i< batchSize; ++i) {
      processValue(myagg, inputColumn, i);
    }
  }

  private void iterateNoSelectionHasNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize) {

    for (int i=0; i< batchSize; ++i) {
      if (!inputColumn.isNull[i]) {
        processValue(myagg, inputColumn, i);
      }
    }
  }

  private void iterateSelectionNoNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize,
      int[] selected) {

    for (int j=0; j< batchSize; ++j) {
      int i = selected[j];
      processValue(myagg, inputColumn, i);
    }
  }

  private void iterateSelectionHasNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize,
      int[] selected) {

    for (int j=0; j< batchSize; ++j) {
      int i = selected[j];
      if (!inputColumn.isNull[i]) {
        processValue(myagg, inputColumn, i);
      }
    }
  }

  @Override
  public void aggregateInputSelection(
      VectorAggregationBufferRow[] aggregationBufferSets, int aggregateIndex,
      VectorizedRowBatch batch) throws HiveException {

    int batchSize = batch.size;

    if (batchSize == 0) {
      return;
    }

    inputExpression.evaluate(batch);

    ColumnVector inputColumn = batch.cols[this.inputExpression.getOutputColumn()];

    if (inputColumn.noNulls) {
      if (inputColumn.isRepeating) {
        iterateNoNullsRepeatingWithAggregationSelection(
          aggregationBufferSets, aggregateIndex,
          inputColumn, batchSize);
      } else {
        if (batch.selectedInUse) {
          iterateNoNullsSelectionWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputColumn, batch.selected, batchSize);
        } else {
          iterateNoNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputColumn, batchSize);
        }
      }
    } else {
      if (inputColumn.isRepeating) {
        // All nulls, no-op for min/max
      } else {
        if (batch.selectedInUse) {
          iterateHasNullsSelectionWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputColumn, batchSize, batch.selected);
        } else {
          iterateHasNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputColumn, batchSize);
        }
      }
    }
  }

  private void iterateNoNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      ColumnVector inputColumn,
      int batchSize) {

    for (int i=0; i < batchSize; ++i) {
      Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregrateIndex,
          i);
      processValue(myagg, inputColumn, 0);
    }
  }

  private void iterateNoNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      ColumnVector inputColumn,
      int[] selection,
      int batchSize) {

    for (int i=0; i < batchSize; ++i) {
      int row = selection[i];
      Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregrateIndex,
          i);
      processValue(myagg, inputColumn, row);
    }
  }

  private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      ColumnVector inputColumn,
      int batchSize) {
    for (int i=0; i < batchSize; ++i) {
      Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregrateIndex,
          i);
      processValue(myagg, inputColumn, i);
    }
  }

  private void iterateHasNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      ColumnVector inputColumn,
      int batchSize,
      int[] selection) {

    for (int i=0; i < batchSize; ++i) {
      int row = selection[i];
      if (!inputColumn.isNull[row]) {
        Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregrateIndex,
            i);
        processValue(myagg, inputColumn, i);
      }
    }
  }

  private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      ColumnVector inputColumn,
      int batchSize) {

    for (int i=0; i < batchSize; ++i) {
      if (!inputColumn.isNull[i]) {
        Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregrateIndex,
            i);
        processValue(myagg, inputColumn, i);
      }
    }
  }

  private Aggregation getCurrentAggregationBuffer(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregrateIndex,
      int row) {
    VectorAggregationBufferRow mySet = aggregationBufferSets[row];
    Aggregation myagg = (Aggregation) mySet.getAggregationBuffer(aggregrateIndex);
    return myagg;
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    agg.reset();
  }

  @Override
  public Object evaluateOutput(AggregationBuffer agg) throws HiveException {
    Aggregation bfAgg = (Aggregation) agg;
    bw.set(bfAgg.bfBytes, 0, bfAgg.bfBytes.length);
    return bw;
  }

  @Override
  public ObjectInspector getOutputObjectInspector() {
    return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
  }

  @Override
  public int getAggregationBufferFixedSize() {
    if (aggBufferSize < 0) {
      // Not pretty, but we need a way to get the size
      try {
        Aggregation agg = (Aggregation) getNewAggregationBuffer();
        aggBufferSize = agg.bfBytes.length;
      } catch (Exception e) {
        throw new RuntimeException("Unexpected error while creating AggregationBuffer", e);
      }
    }

    return aggBufferSize;
  }

  @Override
  public void init(AggregationDesc desc) throws HiveException {
    GenericUDAFBloomFilterEvaluator udafBloomFilter =
        (GenericUDAFBloomFilterEvaluator) desc.getGenericUDAFEvaluator();
    expectedEntries = udafBloomFilter.getExpectedEntries();
  }

  void processValue(Aggregation myagg, ColumnVector columnVector, int i) {
    // columnVector entry is byte array representing serialized BloomFilter.
    // BloomFilter.mergeBloomFilterBytes() does a simple byte ORing
    // which should be faster than deserialize/merge.
    BytesColumnVector inputColumn = (BytesColumnVector) columnVector;
    BloomFilter.mergeBloomFilterBytes(myagg.bfBytes, 0, myagg.bfBytes.length,
        inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
  }
}
