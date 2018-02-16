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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression.AggregationBuffer;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCount.Aggregation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.common.util.BloomKFilter;

public class VectorUDAFBloomFilter extends VectorAggregateExpression {

  private static final long serialVersionUID = 1L;

  private long expectedEntries = -1;
  private ValueProcessor valueProcessor;
  transient private int bitSetSize;
  transient private ByteArrayOutputStream byteStream;

  /**
   * class for storing the current aggregate value.
   */
  private static final class Aggregation implements AggregationBuffer {
    private static final long serialVersionUID = 1L;

    BloomKFilter bf;

    public Aggregation(long expectedEntries) {
      bf = new BloomKFilter(expectedEntries);
    }

    @Override
    public int getVariableSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      bf.reset();
    }
  }

  // This constructor is used to momentarily create the object so match can be called.
  public VectorUDAFBloomFilter() {
    super();
  }

  public VectorUDAFBloomFilter(VectorAggregationDesc vecAggrDesc) {
    super(vecAggrDesc);
    init();
  }

  private void init() {

    GenericUDAFBloomFilterEvaluator udafBloomFilter =
        (GenericUDAFBloomFilterEvaluator) vecAggrDesc.getEvaluator();
    expectedEntries = udafBloomFilter.getExpectedEntries();

    bitSetSize = -1;
    byteStream = new ByteArrayOutputStream();

    // Instantiate the ValueProcessor based on the input type
    ColumnVector.Type colVectorType;
    try {
    colVectorType = inputExpression.getOutputColumnVectorType();
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    switch (colVectorType) {
    case LONG:
      valueProcessor = new ValueProcessorLong();
      break;
    case DOUBLE:
      valueProcessor = new ValueProcessorDouble();
      break;
    case DECIMAL:
      valueProcessor = new ValueProcessorDecimal();
      break;
    case BYTES:
      valueProcessor = new ValueProcessorBytes();
      break;
    case TIMESTAMP:
      valueProcessor = new ValueProcessorTimestamp();
      break;
    default:
      throw new IllegalStateException("Unsupported column vector type " + colVectorType);
    }
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

    ColumnVector inputColumn =  batch.cols[this.inputExpression.getOutputColumnNum()];

    int batchSize = batch.size;

    if (batchSize == 0) {
      return;
    }

    Aggregation myagg = (Aggregation) agg;

    if (inputColumn.isRepeating) {
      if (inputColumn.noNulls || !inputColumn.isNull[0]) {
        valueProcessor.processValue(myagg, inputColumn, 0);
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
      valueProcessor.processValue(myagg, inputColumn, i);
    }
  }

  private void iterateNoSelectionHasNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize) {

    for (int i=0; i< batchSize; ++i) {
      if (!inputColumn.isNull[i]) {
        valueProcessor.processValue(myagg, inputColumn, i);
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
      valueProcessor.processValue(myagg, inputColumn, i);
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
        valueProcessor.processValue(myagg, inputColumn, i);
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

    ColumnVector inputColumn = batch.cols[this.inputExpression.getOutputColumnNum()];

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
        if (!inputColumn.isNull[0]) {
          iterateNoNullsRepeatingWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              inputColumn, batchSize);
        }
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
      valueProcessor.processValue(myagg, inputColumn, 0);
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
      valueProcessor.processValue(myagg, inputColumn, row);
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
      valueProcessor.processValue(myagg, inputColumn, i);
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
        valueProcessor.processValue(myagg, inputColumn, i);
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
        valueProcessor.processValue(myagg, inputColumn, i);
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
  public long getAggregationBufferFixedSize() {
    if (bitSetSize < 0) {
      // Not pretty, but we need a way to get the size
      try {
        Aggregation agg = (Aggregation) getNewAggregationBuffer();
        bitSetSize = agg.bf.getBitSet().length;
      } catch (Exception e) {
        throw new RuntimeException("Unexpected error while creating AggregationBuffer", e);
      }
    }

    // BloomFilter: object(BitSet: object(data: long[]), numBits: int, numHashFunctions: int)
    JavaDataModel model = JavaDataModel.get();
    long bloomFilterSize = JavaDataModel.alignUp(model.object() + model.lengthForLongArrayOfSize(bitSetSize),
        model.memoryAlign());
    return JavaDataModel.alignUp(
        model.object() + bloomFilterSize + model.primitive1() + model.primitive1(),
        model.memoryAlign());
  }

  public long getExpectedEntries() {
    return expectedEntries;
  }

  public void setExpectedEntries(long expectedEntries) {
    this.expectedEntries = expectedEntries;
  }

  // Type-specific handling done here
  private static abstract class ValueProcessor {
    abstract protected void processValue(Aggregation myagg, ColumnVector inputColumn, int index);
  }

  //
  // Type-specific implementations
  //

  public static class ValueProcessorBytes extends ValueProcessor {
    @Override
    protected void processValue(Aggregation myagg, ColumnVector columnVector, int i) {
      BytesColumnVector inputColumn = (BytesColumnVector) columnVector;
      myagg.bf.addBytes(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
    }
  }

  public static class ValueProcessorLong extends ValueProcessor {
    @Override
    protected void processValue(Aggregation myagg, ColumnVector columnVector, int i) {
      LongColumnVector inputColumn = (LongColumnVector) columnVector;
      myagg.bf.addLong(inputColumn.vector[i]);
    }
  }

  public static class ValueProcessorDouble extends ValueProcessor {
    @Override
    protected void processValue(Aggregation myagg, ColumnVector columnVector, int i) {
      DoubleColumnVector inputColumn = (DoubleColumnVector) columnVector;
      myagg.bf.addDouble(inputColumn.vector[i]);
    }
  }

  public static class ValueProcessorDecimal extends ValueProcessor {
    private byte[] scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];

    @Override
    protected void processValue(Aggregation myagg, ColumnVector columnVector, int i) {
      DecimalColumnVector inputColumn = (DecimalColumnVector) columnVector;
      int startIdx = inputColumn.vector[i].toBytes(scratchBuffer);
      myagg.bf.addBytes(scratchBuffer, startIdx, scratchBuffer.length - startIdx);
    }
  }

  public static class ValueProcessorTimestamp extends ValueProcessor {
    @Override
    protected void processValue(Aggregation myagg, ColumnVector columnVector, int i) {
      TimestampColumnVector inputColumn = (TimestampColumnVector) columnVector;
      myagg.bf.addLong(inputColumn.time[i]);
    }
  }

  @Override
  public boolean matches(String name, ColumnVector.Type inputColVectorType,
      ColumnVector.Type outputColVectorType, Mode mode) {

    /*
     * Bloom filter *any* input and output is BYTES.
     *
     * Just modes (PARTIAL1, COMPLETE).
     */
    return
        name.equals("bloom_filter") &&
        outputColVectorType == ColumnVector.Type.BYTES &&
        (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE);
  }

  @Override
  public void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int columnNum,
      AggregationBuffer agg) throws HiveException {

    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[columnNum];
    Aggregation myagg = (Aggregation) agg;
    outputColVector.isNull[batchIndex] = false;

    try {
      Aggregation bfAgg = (Aggregation) agg;
      byteStream.reset();
      BloomKFilter.serialize(byteStream, bfAgg.bf);
      byte[] bytes = byteStream.toByteArray();

      outputColVector.setVal(batchIndex, bytes);
    } catch (IOException err) {
      throw new HiveException("Error encountered while serializing bloomfilter", err);
    } finally {
      IOUtils.closeStream(byteStream);
    }
  }
}
