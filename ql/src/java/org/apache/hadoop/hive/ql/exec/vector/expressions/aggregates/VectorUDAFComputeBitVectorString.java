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

import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

@Description(name = "compute_bit_vector_hll",
    value = "_FUNC_(x) - Computes bit vector for NDV computation")
public class VectorUDAFComputeBitVectorString extends VectorAggregateExpression {

  public VectorUDAFComputeBitVectorString() {
    super();
  }

  public VectorUDAFComputeBitVectorString(VectorAggregationDesc vecAggrDesc) {
    super(vecAggrDesc);
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new Aggregation();
  }

  @Override
  public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch) throws HiveException {
    inputExpression.evaluate(batch);

    BytesColumnVector inputColumn = (BytesColumnVector) batch.cols[this.inputExpression.getOutputColumnNum()];

    int batchSize = batch.size;

    if (batchSize == 0) {
      return;
    }

    Aggregation myagg = (Aggregation) agg;

    myagg.prepare();
    if (inputColumn.noNulls) {
      if (inputColumn.isRepeating) {
        myagg.estimator.addToEstimator(inputColumn.vector[0], inputColumn.start[0], inputColumn.length[0]);
      } else {
        if (batch.selectedInUse) {
          for (int s = 0; s < batchSize; s++) {
            int i = batch.selected[s];
            myagg.estimator.addToEstimator(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            myagg.estimator.addToEstimator(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
          }
        }
      }
    } else {
      if (inputColumn.isRepeating) {
        if (!inputColumn.isNull[0]) {
          myagg.estimator.addToEstimator(inputColumn.vector[0], inputColumn.start[0], inputColumn.length[0]);
        }
      } else {
        if (batch.selectedInUse) {
          for (int j = 0; j < batchSize; ++j) {
            int i = batch.selected[j];
            if (!inputColumn.isNull[i]) {
              myagg.estimator.addToEstimator(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!inputColumn.isNull[i]) {
              myagg.estimator.addToEstimator(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
            }
          }
        }
      }
    }
  }

  private Aggregation getAggregation(VectorAggregationBufferRow[] sets, int rowid, int bufferIndex) {
    VectorAggregationBufferRow bufferRow = sets[rowid];
    Aggregation myagg = (Aggregation) bufferRow.getAggregationBuffer(bufferIndex);
    myagg.prepare();
    return myagg;
  }

  @Override
  public void aggregateInputSelection(VectorAggregationBufferRow[] aggregationBufferSets, int aggregateIndex, VectorizedRowBatch batch) throws HiveException {
    inputExpression.evaluate(batch);

    BytesColumnVector inputColumn = (BytesColumnVector) batch.cols[this.inputExpression.getOutputColumnNum()];

    int batchSize = batch.size;

    if (batchSize == 0) {
      return;
    }

    if (inputColumn.noNulls) {
      if (inputColumn.isRepeating) {
        for (int i = 0; i < batchSize; i++) {
          Aggregation myagg = getAggregation(aggregationBufferSets, i, aggregateIndex);
          myagg.estimator.addToEstimator(inputColumn.vector[0], inputColumn.start[0], inputColumn.length[0]);
        }
      } else {
        if (batch.selectedInUse) {
          for (int s = 0; s < batchSize; s++) {
            int i = batch.selected[s];
            Aggregation myagg = getAggregation(aggregationBufferSets, s, aggregateIndex);
            myagg.estimator.addToEstimator(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            Aggregation myagg = getAggregation(aggregationBufferSets, i, aggregateIndex);
            myagg.estimator.addToEstimator(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
          }
        }
      }
    } else {
      if (inputColumn.isRepeating) {
        if (!inputColumn.isNull[0]) {
          for (int i = 0; i < batchSize; i++) {
            Aggregation myagg = getAggregation(aggregationBufferSets, i, aggregateIndex);
            myagg.estimator.addToEstimator(inputColumn.vector[0], inputColumn.start[0], inputColumn.length[0]);
          }
        }
      } else {
        if (batch.selectedInUse) {
          for (int s = 0; s < batchSize; s++) {
            int i = batch.selected[s];
            if (!inputColumn.isNull[i]) {
              Aggregation myagg = getAggregation(aggregationBufferSets, s, aggregateIndex);
              myagg.estimator.addToEstimator(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!inputColumn.isNull[i]) {
              Aggregation myagg = getAggregation(aggregationBufferSets, i, aggregateIndex);
              myagg.estimator.addToEstimator(inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
            }
          }
        }
      }
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    agg.reset();
  }

  @Override
  public long getAggregationBufferFixedSize() {
    return Aggregation.FIXED_SIZE;
  }

  @Override
  public boolean matches(String name, ColumnVector.Type inputColVectorType, ColumnVector.Type outputColVectorType, GenericUDAFEvaluator.Mode mode) {
    return name.equals("compute_bit_vector_hll") &&
            inputColVectorType == ColumnVector.Type.BYTES &&
            outputColVectorType == ColumnVector.Type.BYTES &&
            (mode == GenericUDAFEvaluator.Mode.PARTIAL1 || mode == GenericUDAFEvaluator.Mode.COMPLETE);
  }

  @Override
  public void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int columnNum, AggregationBuffer agg) throws HiveException {
    Aggregation myagg = (Aggregation) agg;
    BytesColumnVector outputCol = (BytesColumnVector) batch.cols[columnNum];
    if (myagg.estimator == null) {
      outputCol.isNull[batchIndex] = true;
      outputCol.noNulls = false;
    } else {
      outputCol.isNull[batchIndex] = false;
      outputCol.isRepeating = false;
      byte[] outputbuf = myagg.estimator.serialize();
      outputCol.setRef(batchIndex, outputbuf, 0, outputbuf.length);
    }
  }

  static class Aggregation implements AggregationBuffer {

    private static final long FIXED_SIZE = HyperLogLog.builder().setSizeOptimized().build().lengthFor(null);

    HyperLogLog estimator;

    public Aggregation() {
      reset();
    }

    @Override
    public int getVariableSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      estimator = null;
    }

    public void prepare() {
      if (estimator == null) {
        estimator = HyperLogLog.builder().setSizeOptimized().build();
      }
    }
  }
}
