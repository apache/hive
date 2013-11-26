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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.KeyWrapper;
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
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 * Vectorized GROUP BY operator implementation. Consumes the vectorized input and
 * stores the aggregate operators' intermediate states. Emits row mode output.
 *
 */
public class VectorGroupByOperator extends GroupByOperator {

  private static final Log LOG = LogFactory.getLog(
      VectorGroupByOperator.class.getName());

  /**
   * This is the vector of aggregators. They are stateless and only implement
   * the algorithm of how to compute the aggregation. state is kept in the
   * aggregation buffers and is our responsibility to match the proper state for each key.
   */
  private VectorAggregateExpression[] aggregators;

  /**
   * Key vector expressions.
   */
  private VectorExpression[] keyExpressions;

  private transient VectorExpressionWriter[] keyOutputWriters;

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
   * Total per hashtable entry fixed memory (does not depend on key/agg values).
   */
  private transient int fixedHashEntrySize;

  /**
   * Average per hashtable entry variable size memory (depends on key/agg value).
   */
  private transient int avgVariableSize;

  /**
   * Number of entries added to the hashtable since the last check if it should flush.
   */
  private transient int numEntriesSinceCheck;

  /**
   * Sum of batch size processed (ie. rows).
   */
  private transient long sumBatchSize;
  
  /**
   * Max number of entries in the vector group by aggregation hashtables. 
   * Exceeding this will trigger a flush irrelevant of memory pressure condition.
   */
  private transient int maxHtEntries = 1000000;

  /**
   * The number of new entries that must be added to the hashtable before a memory size check.
   */
  private transient int checkInterval = 10000;

  /**
   * Percent of entries to flush when memory threshold exceeded.
   */
  private transient float percentEntriesToFlush = 0.1f;

  /**
   * The global key-aggregation hash map.
   */
  private transient Map<KeyWrapper, VectorAggregationBufferRow> mapKeysAggregationBuffers;

  private static final long serialVersionUID = 1L;

  public VectorGroupByOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    GroupByDesc desc = (GroupByDesc) conf;
    this.conf = desc;
    List<ExprNodeDesc> keysDesc = desc.getKeys();
    keyExpressions = vContext.getVectorExpressions(keysDesc);
    ArrayList<AggregationDesc> aggrDesc = desc.getAggregators();
    aggregators = new VectorAggregateExpression[aggrDesc.size()];
    for (int i = 0; i < aggrDesc.size(); ++i) {
      AggregationDesc aggDesc = aggrDesc.get(i);
      aggregators[i] = vContext.getAggregatorExpression(aggDesc);
    }
  }

  public VectorGroupByOperator() {
    super();
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    
    // hconf is null in unit testing
    if (null != hconf) {
      this.percentEntriesToFlush = HiveConf.getFloatVar(hconf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT);
      this.checkInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_CHECKINTERVAL);
      this.maxHtEntries = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_MAXENTRIES);
    }

    List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();

    List<ExprNodeDesc> keysDesc = conf.getKeys();
    try {

      keyOutputWriters = new VectorExpressionWriter[keyExpressions.length];

      for(int i = 0; i < keyExpressions.length; ++i) {
        keyOutputWriters[i] = VectorExpressionWriterFactory.
            genVectorExpressionWritable(keysDesc.get(i));
        objectInspectors.add(keyOutputWriters[i].getObjectInspector());
      }

      for (int i = 0; i < aggregators.length; ++i) {
        aggregators[i].init(conf.getAggregators().get(i));
        objectInspectors.add(aggregators[i].getOutputObjectInspector());
      }

      keyWrappersBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);
      aggregationBatchInfo = new VectorAggregationBufferBatch();
      aggregationBatchInfo.compileAggregationBatchInfo(aggregators);
      mapKeysAggregationBuffers = new HashMap<KeyWrapper, VectorAggregationBufferRow>();

      List<String> outputFieldNames = conf.getOutputColumnNames();
      outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
          outputFieldNames, objectInspectors);

    } catch (HiveException he) {
      throw he;
    } catch (Throwable e) {
      throw new HiveException(e);
    }

    computeMemoryLimits();

    initializeChildren(hconf);
  }

  /**
   * Computes the memory limits for hash table flush (spill).
   */
  private void computeMemoryLimits() {
    JavaDataModel model = JavaDataModel.get();

    fixedHashEntrySize =
        model.hashMapEntry() +
        keyWrappersBatch.getKeysFixedSize() +
        aggregationBatchInfo.getAggregatorsFixedSize();

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();
    memoryThreshold = conf.getMemoryThreshold();
    // Tests may leave this unitialized, so better set it to 1
    if (memoryThreshold == 0.0f) {
      memoryThreshold = 1.0f;
    }

    maxHashTblMemory = (int)(maxMemory * memoryThreshold);

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("maxMemory:%dMb (%d * %f) fixSize:%d (key:%d agg:%d)",
          maxHashTblMemory/1024/1024,
          maxMemory/1024/1024,
          memoryThreshold,
          fixedHashEntrySize,
          keyWrappersBatch.getKeysFixedSize(),
          aggregationBatchInfo.getAggregatorsFixedSize()));
    }

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

    //Flush if memory limits were reached
    // We keep flushing until the memory is under threshold 
    int preFlushEntriesCount = numEntriesHashTable;
    while (shouldFlush(batch)) {
      flush(false);
      
      //Validate that some progress is being made
      if (!(numEntriesHashTable < preFlushEntriesCount)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Flush did not progress: %d entries before, %d entries after",
              preFlushEntriesCount,
              numEntriesHashTable));
        }
        break;
      }
      preFlushEntriesCount = numEntriesHashTable;
    }

    if (sumBatchSize == 0 && 0 != batch.size) {
      // Sample the first batch processed for variable sizes.
      updateAvgVariableSize(batch);
    }

    sumBatchSize += batch.size;
  }

  /**
   * Flushes the entries in the hash table by emiting output (forward).
   * When parameter 'all' is true all the entries are flushed.
   * @param all
   * @throws HiveException
   */
  private void flush(boolean all) throws HiveException {

    int entriesToFlush = all ? numEntriesHashTable :
      (int)(numEntriesHashTable * this.percentEntriesToFlush);
    int entriesFlushed = 0;

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Flush %d %s entries:%d fixed:%d variable:%d (used:%dMb max:%dMb)",
          entriesToFlush, all ? "(all)" : "",
          numEntriesHashTable, fixedHashEntrySize, avgVariableSize,
          numEntriesHashTable * (fixedHashEntrySize + avgVariableSize)/1024/1024,
          maxHashTblMemory/1024/1024));
    }

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
      Iterator<Map.Entry<KeyWrapper, VectorAggregationBufferRow>> iter =
          mapKeysAggregationBuffers.entrySet().iterator();
      while(iter.hasNext()) {
        Map.Entry<KeyWrapper, VectorAggregationBufferRow> pair = iter.next();
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
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("forwarding keys: %s: %s",
              pair.getKey().toString(), Arrays.toString(forwardCache)));
        }
        forward(forwardCache, outputObjInspector);

        if (!all) {
          iter.remove();
          --numEntriesHashTable;
          if (++entriesFlushed >= entriesToFlush) {
            break;
          }
        }
      }
    }

    if (all) {
      mapKeysAggregationBuffers.clear();
      numEntriesHashTable = 0;
    }
  }

  /**
   * Returns true if the memory threshold for the hash table was reached.
   */
  private boolean shouldFlush(VectorizedRowBatch batch) {
    if (batch.size == 0) {
      return false;
    }
    //numEntriesSinceCheck is the number of entries added to the hash table
    // since the last time we checked the average variable size
    if (numEntriesSinceCheck >= this.checkInterval) {
      // Were going to update the average variable row size by sampling the current batch
      updateAvgVariableSize(batch);
      numEntriesSinceCheck = 0;
    }
    return numEntriesHashTable > this.maxHtEntries ||
        numEntriesHashTable * (fixedHashEntrySize + avgVariableSize) > maxHashTblMemory;
  }

  /**
   * Updates the average variable size of the hash table entries.
   * The average is only updates by probing the batch that added the entry in the hash table
   * that caused the check threshold to be reached.
   */
  private void updateAvgVariableSize(VectorizedRowBatch batch) {
    int keyVariableSize = keyWrappersBatch.getVariableSize(batch.size);
    int aggVariableSize = aggregationBatchInfo.getVariableSize(batch.size);

    // This assumes the distribution of variable size keys/aggregates in the input
    // is the same as the distribution of variable sizes in the hash entries
    avgVariableSize = (int)((avgVariableSize * sumBatchSize + keyVariableSize +aggVariableSize) /
        (sumBatchSize + batch.size));
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
        numEntriesHashTable++;
        numEntriesSinceCheck++;
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
      flush(true);
    }
  }

  static public String getOperatorName() {
    return "GBY";
  }

  public VectorExpression[] getKeyExpressions() {
    return keyExpressions;
  }

  public void setKeyExpressions(VectorExpression[] keyExpressions) {
    this.keyExpressions = keyExpressions;
  }

  public VectorAggregateExpression[] getAggregators() {
    return aggregators;
  }

  public void setAggregators(VectorAggregateExpression[] aggregators) {
    this.aggregators = aggregators;
  }

}

