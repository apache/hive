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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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

  private transient ObjectInspector keyObjectInspector;
  private transient ObjectInspector valueObjectInspector;
  private transient int [] keyHashCode = new int [VectorizedRowBatch.DEFAULT_SIZE];

  private transient int[] hashResult; // the pre-created array for reducerHash results

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

      for (int i = 0; i < partitionEval.length; i++) {
        partitionEval[i].evaluate(vrg);
      }

      // run the vector evaluations
      for (int i = 0; i < valueEval.length; i++) {
         valueEval[i].evaluate(vrg);
      }
      // Evaluate the keys
      for (int i = 0; i < keyEval.length; i++) {
        keyEval[i].evaluate(vrg);
      }

      Object[] distributionKeys = new Object[numDistributionKeys];

      // Determine which rows we need to emit based on topN optimization
      int startResult = reducerHash.startVectorizedBatch();
      if (startResult == TopNHash.EXCLUDED) {
        return; // TopN wants us to exclude all rows.
      }
      boolean useTopN = startResult != TopNHash.FORWARD;
      if (useTopN && (hashResult == null || hashResult.length < vrg.size)) {
        hashResult = new int[Math.max(vrg.size, VectorizedRowBatch.DEFAULT_SIZE)];
      }

      for (int j = 0 ; j < vrg.size; ++j) {
        int rowIndex = j;
        if (vrg.selectedInUse) {
          rowIndex = vrg.selected[j];
        }
        // First, evaluate the key - the way things stand we'd need it regardless.
        for (int i = 0; i < keyEval.length; i++) {
          int batchColumn = keyEval[i].getOutputColumn();
          ColumnVector vectorColumn = vrg.cols[batchColumn];
          distributionKeys[i] = keyWriters[i].writeValue(vectorColumn, rowIndex);
        }
        // no distinct key
        System.arraycopy(distributionKeys, 0, cachedKeys[0], 0, numDistributionKeys);
        // TopN is not supported for multi-distinct currently. If we have more cachedKeys
        // than one for every input key horrible things will happen (OOB error on array likely).
        assert !useTopN || cachedKeys.length <= 1;
        for (int i = 0; i < cachedKeys.length; i++) {
          // Serialize the keys and append the tag.
          Object keyObj = keySerializer.serialize(cachedKeys[i], keyObjectInspector);
          setKeyWritable(keyIsText ? (Text)keyObj : (BytesWritable)keyObj, tag);
          if (useTopN) {
            reducerHash.tryStoreVectorizedKey(keyWritable, j, hashResult);
          } else {
            // No TopN, just forward the key
            keyWritable.setHashCode(computeHashCode(vrg, rowIndex));
            collect(keyWritable, makeValueWritable(vrg, rowIndex));
           }
        }
      }

      if (!useTopN) return; // All done.

      // If we use topN, we have called tryStore on every key now. We can process the results.
      for (int j = 0 ; j < vrg.size; ++j) {
        int index = hashResult[j];
        if (index == TopNHash.EXCLUDED) continue;
        int rowIndex = j;
        if (vrg.selectedInUse) {
          rowIndex = vrg.selected[j];
        }
        // Compute everything now - we'd either store it, or forward it.
        int hashCode = computeHashCode(vrg, rowIndex);
        BytesWritable value = makeValueWritable(vrg, rowIndex);
        if (index < 0) {
          // Kinda hacky; see getVectorizedKeyToForward javadoc.
          byte[] key = reducerHash.getVectorizedKeyToForward(index);
          collect(key, value, hashCode);
        } else {
          reducerHash.storeValue(index, value, hashCode, true);
        }
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    } catch (IOException e) {
      throw new HiveException(e);
    }
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
      // If no partition cols, just distribute the data uniformly to provide
      // better
      // load balance. If the requirement is to have a single reducer, we
      // should set
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
