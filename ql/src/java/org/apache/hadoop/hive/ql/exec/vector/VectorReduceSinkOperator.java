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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TopNHash;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
// import org.apache.hadoop.util.StringUtils;

public class VectorReduceSinkOperator extends ReduceSinkOperator {

  private static final Log LOG = LogFactory.getLog(
      VectorReduceSinkOperator.class.getName());

  private static final long serialVersionUID = 1L;

  /**
   * The evaluators for the key columns. Key columns decide the sort order on
   * the reducer side. Key columns are passed to the reducer in the "key".
   */
  private VectorExpression[] keyEval;

  /**
   * The key value writers. These know how to write the necessary writable type
   * based on key column metadata, from the primitive vector type.
   */
  private transient VectorExpressionWriter[] keyWriters;

  /**
   * The evaluators for the value columns. Value columns are passed to reducer
   * in the "value".
   */
  private VectorExpression[] valueEval;

  /**
   * The output value writers. These know how to write the necessary writable type
   * based on value column metadata, from the primitive vector type.
   */
  private transient VectorExpressionWriter[] valueWriters;

  /**
   * The evaluators for the partition columns (CLUSTER BY or DISTRIBUTE BY in
   * Hive language). Partition columns decide the reducer that the current row
   * goes to. Partition columns are not passed to reducer.
   */
  private VectorExpression[] partitionEval;

  /**
   * The partition value writers. These know how to write the necessary writable type
   * based on partition column metadata, from the primitive vector type.
   */
  private transient VectorExpressionWriter[] partitionWriters;

  public VectorReduceSinkOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    ReduceSinkDesc desc = (ReduceSinkDesc) conf;
    this.conf = desc;
    keyEval = vContext.getVectorExpressions(desc.getKeyCols());
    valueEval = vContext.getVectorExpressions(desc.getValueCols());
    partitionEval = vContext.getVectorExpressions(desc.getPartitionCols());
  }

  public VectorReduceSinkOperator() {
    super();
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      numDistributionKeys = conf.getNumDistributionKeys();
      distinctColIndices = conf.getDistinctColumnIndices();
      numDistinctExprs = distinctColIndices.size();

      TableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer) keyTableDesc.getDeserializerClass()
          .newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);

      /*
       * Compute and assign the key writers and the key object inspector
       */
      VectorExpressionWriterFactory.processVectorExpressions(
          conf.getKeyCols(),
          conf.getOutputKeyColumnNames(),
          new VectorExpressionWriterFactory.SingleOIDClosure() {
            @Override
            public void assign(VectorExpressionWriter[] writers,
              ObjectInspector objectInspector) {
              keyWriters = writers;
              keyObjectInspector = objectInspector;
            }
          });

      String colNames = "";
      for(String colName : conf.getOutputKeyColumnNames()) {
        colNames = String.format("%s %s", colNames, colName);
      }

      LOG.info(String.format("keyObjectInspector [%s]%s => %s",
          keyObjectInspector.getClass(),
          keyObjectInspector,
          colNames));

      partitionWriters = VectorExpressionWriterFactory.getExpressionWriters(conf.getPartitionCols());

      TableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer) valueTableDesc.getDeserializerClass()
          .newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());

      /*
       * Compute and assign the value writers and the value object inspector
       */
      VectorExpressionWriterFactory.processVectorExpressions(
          conf.getValueCols(),
          conf.getOutputValueColumnNames(),
          new VectorExpressionWriterFactory.SingleOIDClosure() {
            @Override
            public void assign(VectorExpressionWriter[] writers,
                ObjectInspector objectInspector) {
                valueWriters = writers;
                valueObjectInspector = objectInspector;
              }
          });

      colNames = "";
      for(String colName : conf.getOutputValueColumnNames()) {
        colNames = String.format("%s %s", colNames, colName);
      }

      LOG.info(String.format("valueObjectInspector [%s]%s => %s",
          valueObjectInspector.getClass(),
          valueObjectInspector,
          colNames));

      int numKeys = numDistinctExprs > 0 ? numDistinctExprs : 1;
      int keyLen = numDistinctExprs > 0 ? numDistributionKeys + 1 :
        numDistributionKeys;
      cachedKeys = new Object[numKeys][keyLen];
      cachedValues = new Object[valueEval.length];

      int tag = conf.getTag();
      tagByte[0] = (byte) tag;
      LOG.info("Using tag = " + tag);

      int limit = conf.getTopN();
      float memUsage = conf.getTopNMemoryUsage();
      if (limit >= 0 && memUsage > 0) {
        reducerHash.initialize(limit, memUsage, conf.isMapGroupBy(), this);
      }
    } catch(Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    VectorizedRowBatch vrg = (VectorizedRowBatch) row;

    LOG.info(String.format("sinking %d rows, %d values, %d keys, %d parts",
        vrg.size,
        valueEval.length,
        keyEval.length,
        partitionEval.length));

    try {
      // Evaluate the keys
      for (int i = 0; i < keyEval.length; i++) {
        keyEval[i].evaluate(vrg);
      }

      // Determine which rows we need to emit based on topN optimization
      int startResult = reducerHash.startVectorizedBatch(vrg.size);
      if (startResult == TopNHash.EXCLUDE) {
        return; // TopN wants us to exclude all rows.
      }
      // TODO: can we do this later/only for the keys that are needed? E.g. update vrg.selected.
      for (int i = 0; i < partitionEval.length; i++) {
        partitionEval[i].evaluate(vrg);
      }
      // run the vector evaluations
      for (int i = 0; i < valueEval.length; i++) {
         valueEval[i].evaluate(vrg);
      }

      boolean useTopN = startResult != TopNHash.FORWARD;
      // Go thru the batch once. If we are not using TopN, we will forward all things and be done.
      // If we are using topN, we will make the first key for each row and store/forward it.
      // Values, hashes and additional distinct rows will be handled in the 2nd pass in that case.
      for (int batchIndex = 0 ; batchIndex < vrg.size; ++batchIndex) {
        int rowIndex = batchIndex;
        if (vrg.selectedInUse) {
          rowIndex = vrg.selected[batchIndex];
        }
        // First, make distrib key components for this row and determine distKeyLength.
        populatedCachedDistributionKeys(vrg, rowIndex, 0);
        HiveKey firstKey = toHiveKey(cachedKeys[0], tag, null);
        int distKeyLength = firstKey.getDistKeyLength();
        // Add first distinct expression, if any.
        if (numDistinctExprs > 0) {
          populateCachedDistinctKeys(vrg, rowIndex, 0);
          firstKey = toHiveKey(cachedKeys[0], tag, distKeyLength);
        }

        if (useTopN) {
          reducerHash.tryStoreVectorizedKey(firstKey, batchIndex);
        } else {
        // No TopN, just forward the first key and all others.
          int hashCode = computeHashCode(vrg, rowIndex);
          firstKey.setHashCode(hashCode);
          BytesWritable value = makeValueWritable(vrg, rowIndex);
          collect(firstKey, value);
          forwardExtraDistinctRows(vrg, rowIndex, hashCode, value, distKeyLength, tag, 0);
        }
      }

      if (!useTopN) return; // All done.

      // If we use topN, we have called tryStore on every key now. We can process the results.
      for (int batchIndex = 0 ; batchIndex < vrg.size; ++batchIndex) {
        int result = reducerHash.getVectorizedBatchResult(batchIndex);
        if (result == TopNHash.EXCLUDE) continue;
        int rowIndex = batchIndex;
        if (vrg.selectedInUse) {
          rowIndex = vrg.selected[batchIndex];
        }
        // Compute value and hashcode - we'd either store or forward them.
        int hashCode = computeHashCode(vrg, rowIndex);
        BytesWritable value = makeValueWritable(vrg, rowIndex);
        int distKeyLength = -1;
        if (result == TopNHash.FORWARD) {
          HiveKey firstKey = reducerHash.getVectorizedKeyToForward(batchIndex);
          firstKey.setHashCode(hashCode);
          distKeyLength = firstKey.getDistKeyLength();
          collect(firstKey, value);
        } else {
          reducerHash.storeValue(result, value, hashCode, true);
          distKeyLength = reducerHash.getVectorizedKeyDistLength(batchIndex);
        }
        // Now forward other the rows if there's multi-distinct (but see TODO in forward...).
        // Unfortunately, that means we will have to rebuild the cachedKeys. Start at 1.
        if (numDistinctExprs > 1) {
          populatedCachedDistributionKeys(vrg, rowIndex, 1);
          forwardExtraDistinctRows(vrg, rowIndex, hashCode, value, distKeyLength, tag, 1);
        }
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * This function creates and forwards all the additional KVs for the multi-distinct case,
   * after the first (0th) KV pertaining to the row has already been stored or forwarded.
   * @param vrg the batch
   * @param rowIndex the row index in the batch
   * @param hashCode the partitioning hash code to use; same as for the first KV
   * @param value the value to use; same as for the first KV
   * @param distKeyLength the distribution key length of the first key; TODO probably extraneous
   * @param tag the tag
   * @param baseIndex the index in cachedKeys where the pre-evaluated distribution keys are stored
   */
  private void forwardExtraDistinctRows(VectorizedRowBatch vrg, int rowIndex,int hashCode,
      BytesWritable value, int distKeyLength, int tag, int baseIndex)
          throws HiveException, SerDeException, IOException {
    // TODO: We don't have to forward extra distinct rows immediately (same in non-vector) if
    //       the first key has already been stored. There's few bytes difference between keys
    //       for different distincts, and the value/etc. are all the same.
    //       We could store deltas to re-gen extra rows when flushing TopN.
    for (int i = 1; i < numDistinctExprs; i++) {
      if (i != baseIndex) {
        System.arraycopy(cachedKeys[baseIndex], 0, cachedKeys[i], 0, numDistributionKeys);
      }
      populateCachedDistinctKeys(vrg, rowIndex, i);
      HiveKey hiveKey = toHiveKey(cachedKeys[i], tag, distKeyLength);
      hiveKey.setHashCode(hashCode);
      collect(hiveKey, value);
    }
  }

  /**
   * Populate distribution keys part of cachedKeys for a particular row from the batch.
   * @param vrg the batch
   * @param rowIndex the row index in the batch
   * @param index the cachedKeys index to write to
   */
  private void populatedCachedDistributionKeys(
      VectorizedRowBatch vrg, int rowIndex, int index) throws HiveException {
    for (int i = 0; i < numDistributionKeys; i++) {
      int batchColumn = keyEval[i].getOutputColumn();
      ColumnVector vectorColumn = vrg.cols[batchColumn];
      cachedKeys[index][i] = keyWriters[i].writeValue(vectorColumn, rowIndex);
    }
    if (cachedKeys[index].length > numDistributionKeys) {
      cachedKeys[index][numDistributionKeys] = null;
    }
  }

  /**
   * Populate distinct keys part of cachedKeys for a particular row from the batch.
   * @param vrg the batch
   * @param rowIndex the row index in the batch
   * @param index the cachedKeys index to write to
   */
  private void populateCachedDistinctKeys(
      VectorizedRowBatch vrg, int rowIndex, int index) throws HiveException {
    StandardUnion union;
    cachedKeys[index][numDistributionKeys] = union = new StandardUnion(
        (byte)index, new Object[distinctColIndices.get(index).size()]);
    Object[] distinctParameters = (Object[]) union.getObject();
    for (int distinctParamI = 0; distinctParamI < distinctParameters.length; distinctParamI++) {
      int distinctColIndex = distinctColIndices.get(index).get(distinctParamI);
      int batchColumn = keyEval[distinctColIndex].getOutputColumn();
      distinctParameters[distinctParamI] =
          keyWriters[distinctColIndex].writeValue(vrg.cols[batchColumn], rowIndex);
    }
    union.setTag((byte) index);
  }

  private BytesWritable makeValueWritable(VectorizedRowBatch vrg, int rowIndex)
      throws HiveException, SerDeException {
    for (int i = 0; i < valueEval.length; i++) {
      int batchColumn = valueEval[i].getOutputColumn();
      ColumnVector vectorColumn = vrg.cols[batchColumn];
      cachedValues[i] = valueWriters[i].writeValue(vectorColumn, rowIndex);
    }
    // Serialize the value
    return (BytesWritable)valueSerializer.serialize(cachedValues, valueObjectInspector);
  }

  private int computeHashCode(VectorizedRowBatch vrg, int rowIndex) throws HiveException {
    // Evaluate the HashCode
    int keyHashCode = 0;
    if (partitionEval.length == 0) {
      // If no partition cols, just distribute the data uniformly to provide better
      // load balance. If the requirement is to have a single reducer, we should set
      // the number of reducers to 1.
      // Use a constant seed to make the code deterministic.
      if (random == null) {
        random = new Random(12345);
      }
      keyHashCode = random.nextInt();
    } else {
      for (int p = 0; p < partitionEval.length; p++) {
        ColumnVector columnVector = vrg.cols[partitionEval[p].getOutputColumn()];
        Object partitionValue = partitionWriters[p].writeValue(columnVector, rowIndex);
        keyHashCode = keyHashCode
            * 31
            + ObjectInspectorUtils.hashCode(
                partitionValue,
                partitionWriters[p].getObjectInspector());
      }
    }
    return keyHashCode;
  }

  static public String getOperatorName() {
    return "RS";
  }

  public VectorExpression[] getPartitionEval() {
    return partitionEval;
  }

  public void setPartitionEval(VectorExpression[] partitionEval) {
    this.partitionEval = partitionEval;
  }

  public VectorExpression[] getValueEval() {
    return valueEval;
  }

  public void setValueEval(VectorExpression[] valueEval) {
    this.valueEval = valueEval;
  }

  public VectorExpression[] getKeyEval() {
    return keyEval;
  }

  public void setKeyEval(VectorExpression[] keyEval) {
    this.keyEval = keyEval;
  }
}
