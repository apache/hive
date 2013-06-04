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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 * Vectorized GROUP BY operator implementation. Consumes the vectorized input and
 * stores the aggregate operators' intermediate states. Emits row mode output.
 *
 */
public class VectorGroupByOperator extends Operator<GroupByDesc> implements Serializable {

  private static final Log LOG = LogFactory.getLog(
      VectorGroupByOperator.class.getName());

  private final VectorizationContext vContext;

  /**
   * This is the vector of aggregators. They are stateless and only implement
   * the algorithm of how to compute the aggregation. state is kept in the 
   * aggregation buffers and is our responsibility to match the proper state for each key. 
   */
  private transient VectorAggregateExpression[] aggregators;
  
  /**
   * Key vector expressions.
   */
  private transient VectorExpression[] keyExpressions;
  
  private VectorExpressionWriter[] keyOutputWriters;
  
  /**
   * The aggregation buffers to use for the current batch.
   */
  private transient VectorAggregationBufferBatch aggregationBatchInfo;

  /**
   * The current batch key wrappers.
   * The very same instance gets reused for all batches.
   */
  private transient VectorHashKeyWrapperBatch keyWrappersBatch;

  /**
   * The global key-aggregation hash map.
   */
  private transient Map<KeyWrapper, VectorAggregationBufferRow> mapKeysAggregationBuffers;

  private static final long serialVersionUID = 1L;

  public VectorGroupByOperator(VectorizationContext ctxt, OperatorDesc conf) {
    super();
    this.vContext = ctxt;
    this.conf = (GroupByDesc) conf;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();

    try {
      vContext.setOperatorType(OperatorType.GROUPBY);

      List<ExprNodeDesc> keysDesc = conf.getKeys();
      keyExpressions = vContext.getVectorExpressions(keysDesc);
      
      keyOutputWriters = new VectorExpressionWriter[keyExpressions.length];
      
      for(int i = 0; i < keyExpressions.length; ++i) {
        keyOutputWriters[i] = VectorExpressionWriterFactory.
            genVectorExpressionWritable(keysDesc.get(i));
        objectInspectors.add(keyOutputWriters[i].getObjectInspector());
      }
      
      ArrayList<AggregationDesc> aggrDesc = conf.getAggregators();
      aggregators = new VectorAggregateExpression[aggrDesc.size()];
      for (int i = 0; i < aggrDesc.size(); ++i) {
        AggregationDesc desc = aggrDesc.get(i);
        aggregators[i] = vContext.getAggregatorExpression (desc);
        objectInspectors.add(aggregators[i].getOutputObjectInspector());
      }
      
      keyWrappersBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);
      aggregationBatchInfo = new VectorAggregationBufferBatch();
      mapKeysAggregationBuffers = new HashMap<KeyWrapper, VectorAggregationBufferRow>();

      List<String> outputFieldNames = conf.getOutputColumnNames();
      outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
          outputFieldNames, objectInspectors);

    } catch (HiveException he) {
      throw he;
    } catch (Throwable e) {
      throw new HiveException(e);
    }
    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;
    
    // First we traverse the batch to evaluate and prepare the KeyWrappers
    // After this the KeyWrappers are properly set and hash code is computed
    keyWrappersBatch.evaluateBatch(batch);

    // Next we locate the aggregation buffer set for each key
    prepareBatchAggregationBufferSets(batch);
    
    // Finally, evaluate the aggregators
    processAggregators(batch);
  }
   
  /**
   * Evaluates the aggregators on the current batch.
   * The aggregationBatchInfo must have been prepared
   * by calling {@link #prepareBatchAggregationBufferSets} first. 
   */
  private void processAggregators(VectorizedRowBatch batch) throws HiveException {
    // We now have a vector of aggregation buffer sets to use for each row
    // We can start computing the aggregates.
    // If the number of distinct keys in the batch is 1 we can
    // use the optimized code path of aggregateInput
    VectorAggregationBufferRow[] aggregationBufferSets = 
        aggregationBatchInfo.getAggregationBuffers();
    if (aggregationBatchInfo.getDistinctBufferSetCount() == 1) {
      VectorAggregateExpression.AggregationBuffer[] aggregationBuffers = 
          aggregationBufferSets[0].getAggregationBuffers();
      for (int i = 0; i < aggregators.length; ++i) {
        aggregators[i].aggregateInput(aggregationBuffers[i], batch);
      }
    } else {
      for (int i = 0; i < aggregators.length; ++i) {
        aggregators[i].aggregateInputSelection(
            aggregationBufferSets,
            i,
            batch);
      }
    }
  }
  
  /**
   * Locates the aggregation buffer sets to use for each key in the current batch.
   * The keyWrappersBatch must have evaluated the current batch first.
   */
  private void prepareBatchAggregationBufferSets(VectorizedRowBatch batch) throws HiveException {
    // The aggregation batch vector needs to know when we start a new batch
    // to bump its internal version. 
    aggregationBatchInfo.startBatch();
    
    // We now have to probe the global hash and find-or-allocate
    // the aggregation buffers to use for each key present in the batch
    VectorHashKeyWrapper[] keyWrappers = keyWrappersBatch.getVectorHashKeyWrappers();
    for (int i=0; i < batch.size; ++i) {
      VectorHashKeyWrapper kw = keyWrappers[i];
      VectorAggregationBufferRow aggregationBuffer = mapKeysAggregationBuffers.get(kw);
      if (null == aggregationBuffer) {
        // the probe failed, we must allocate a set of aggregation buffers 
        // and push the (keywrapper,buffers) pair into the hash.
        // is very important to clone the keywrapper, the one we have from our
        // keyWrappersBatch is going to be reset/reused on next batch.
        aggregationBuffer = allocateAggregationBuffer();
        mapKeysAggregationBuffers.put(kw.copyKey(), aggregationBuffer);
      }
      aggregationBatchInfo.mapAggregationBufferSet(aggregationBuffer, i);
    }
  }

  /**
   * allocates a new aggregation buffer set.
   */
  private VectorAggregationBufferRow allocateAggregationBuffer() throws HiveException {
    VectorAggregateExpression.AggregationBuffer[] aggregationBuffers = 
        new VectorAggregateExpression.AggregationBuffer[aggregators.length];
    for (int i=0; i < aggregators.length; ++i) {
      aggregationBuffers[i] = aggregators[i].getNewAggregationBuffer();
      aggregators[i].reset(aggregationBuffers[i]);
    }
    VectorAggregationBufferRow bufferSet = new VectorAggregationBufferRow(aggregationBuffers);
    return bufferSet;
  }

  @Override
  public void closeOp(boolean aborted) throws HiveException {
    if (!aborted) {
      Object[] forwardCache = new Object[keyExpressions.length + aggregators.length];
      if (keyExpressions.length == 0 && mapKeysAggregationBuffers.isEmpty()) {
        
        // if this is a global aggregation (no keys) and empty set, must still emit NULLs
        VectorAggregationBufferRow emptyBuffers = allocateAggregationBuffer();
        for (int i = 0; i < aggregators.length; ++i) {
          forwardCache[i] = aggregators[i].evaluateOutput(emptyBuffers.getAggregationBuffer(i));
        }
        forward(forwardCache, outputObjInspector);
      } else {
        
        /* Iterate the global (keywrapper,aggregationbuffers) map and emit
         a row for each key */
        for(Map.Entry<KeyWrapper, VectorAggregationBufferRow> pair: 
          mapKeysAggregationBuffers.entrySet()){
          int fi = 0;
          for (int i = 0; i < keyExpressions.length; ++i) {
            VectorHashKeyWrapper kw = (VectorHashKeyWrapper)pair.getKey();
            forwardCache[fi++] = keyWrappersBatch.getWritableKeyValue (
                kw, i, keyOutputWriters[i]);
          }
          for (int i = 0; i < aggregators.length; ++i) {
            forwardCache[fi++] = aggregators[i].evaluateOutput(pair.getValue()
                .getAggregationBuffer(i));
          }
          LOG.debug(String.format("forwarding keys: %s: %s", 
              pair.getKey().toString(), Arrays.toString(forwardCache)));
          forward(forwardCache, outputObjInspector);
        }
      }
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "GBY";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.GROUPBY;
  }

}

