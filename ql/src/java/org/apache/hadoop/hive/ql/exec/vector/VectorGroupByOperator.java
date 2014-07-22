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
import java.lang.ref.SoftReference;
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

  private transient Object[] forwardCache;

  /**
   * Interface for processing mode: global, hash or streaming
   */
  private static interface IProcessingMode {
    public void initialize(Configuration hconf) throws HiveException;
    public void processBatch(VectorizedRowBatch batch) throws HiveException;
    public void close(boolean aborted) throws HiveException;
  }

  /**
   * Base class for all processing modes
   */
  private abstract class ProcessingModeBase implements IProcessingMode {
    /**
     * Evaluates the aggregators on the current batch.
     * The aggregationBatchInfo must have been prepared
     * by calling {@link #prepareBatchAggregationBufferSets} first.
     */
    protected void processAggregators(VectorizedRowBatch batch) throws HiveException {
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
     * allocates a new aggregation buffer set.
     */
    protected VectorAggregationBufferRow allocateAggregationBuffer() throws HiveException {
      VectorAggregateExpression.AggregationBuffer[] aggregationBuffers =
          new VectorAggregateExpression.AggregationBuffer[aggregators.length];
      for (int i=0; i < aggregators.length; ++i) {
        aggregationBuffers[i] = aggregators[i].getNewAggregationBuffer();
        aggregators[i].reset(aggregationBuffers[i]);
      }
      VectorAggregationBufferRow bufferSet = new VectorAggregationBufferRow(aggregationBuffers);
      return bufferSet;
    }

  }

  /**
   * Global aggregates (no GROUP BY clause, no keys)
   * This mode is very simple, there are no keys to consider, and only flushes one row at closing
   * The one row must flush even if no input was seen (NULLs)
   */
  private class ProcessingModeGlobalAggregate extends ProcessingModeBase {

    /**
     * In global processing mode there is only one set of aggregation buffers 
     */
    private VectorAggregationBufferRow aggregationBuffers;

    @Override
    public void initialize(Configuration hconf) throws HiveException {
      aggregationBuffers =  allocateAggregationBuffer();
      LOG.info("using global aggregation processing mode");
    }

    @Override
    public void processBatch(VectorizedRowBatch batch) throws HiveException {
      for (int i = 0; i < aggregators.length; ++i) {
        aggregators[i].aggregateInput(aggregationBuffers.getAggregationBuffer(i), batch);
      }
    }

    @Override
    public void close(boolean aborted) throws HiveException {
      if (!aborted) {
        flushSingleRow(null, aggregationBuffers);
      }
    }
  }

  /**
   * Hash Aggregate mode processing
   */
  private class ProcessingModeHashAggregate extends ProcessingModeBase {

    /**
     * The global key-aggregation hash map.
     */
    private Map<KeyWrapper, VectorAggregationBufferRow> mapKeysAggregationBuffers;

    /**
     * Total per hashtable entry fixed memory (does not depend on key/agg values).
     */
    private int fixedHashEntrySize;

    /**
     * Average per hashtable entry variable size memory (depends on key/agg value).
     */
    private int avgVariableSize;

    /**
     * Number of entries added to the hashtable since the last check if it should flush.
     */
    private int numEntriesSinceCheck;

    /**
     * Sum of batch size processed (ie. rows).
     */
    private long sumBatchSize;

    /**
     * Max number of entries in the vector group by aggregation hashtables. 
     * Exceeding this will trigger a flush irrelevant of memory pressure condition.
     */
    private int maxHtEntries = 1000000;

    /**
     * The number of new entries that must be added to the hashtable before a memory size check.
     */
    private int checkInterval = 10000;

    /**
     * Percent of entries to flush when memory threshold exceeded.
     */
    private float percentEntriesToFlush = 0.1f;
  
    /**
     * A soft reference used to detect memory pressure
     */
    private SoftReference<Object> gcCanary = new SoftReference<Object>(new Object());
    
    /**
     * Counts the number of time the gcCanary died and was resurrected
     */
    private long gcCanaryFlushes = 0L;

    /**
     * Count of rows since the last check for changing from aggregate to streaming mode
     */
    private long lastModeCheckRowCount = 0;

    /**
     * Minimum factor for hash table to reduce number of entries
     * If this is not met, the processing switches to streaming mode
     */
    private float minReductionHashAggr;

    /**
     * Number of rows processed between checks for minReductionHashAggr factor
     * TODO: there is overlap between numRowsCompareHashAggr and checkInterval
     */
    private long numRowsCompareHashAggr;

    @Override
    public void initialize(Configuration hconf) throws HiveException {
      // hconf is null in unit testing
      if (null != hconf) {
        this.percentEntriesToFlush = HiveConf.getFloatVar(hconf,
          HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT);
        this.checkInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_CHECKINTERVAL);
        this.maxHtEntries = HiveConf.getIntVar(hconf,
            HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_MAXENTRIES);
        this.minReductionHashAggr = HiveConf.getFloatVar(hconf,
            HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION);
          this.numRowsCompareHashAggr = HiveConf.getIntVar(hconf,
            HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL);
      } 
      else {
        this.percentEntriesToFlush =
            HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT.defaultFloatVal;
        this.checkInterval =
            HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_CHECKINTERVAL.defaultIntVal;
        this.maxHtEntries =
            HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_MAXENTRIES.defaultIntVal;
        this.minReductionHashAggr =
            HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION.defaultFloatVal;
          this.numRowsCompareHashAggr =
            HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL.defaultIntVal;
      }

      mapKeysAggregationBuffers = new HashMap<KeyWrapper, VectorAggregationBufferRow>();
      computeMemoryLimits();
      LOG.info("using hash aggregation processing mode");
    }

    @Override
    public void processBatch(VectorizedRowBatch batch) throws HiveException {

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

        if(gcCanary.get() == null) {
          gcCanaryFlushes++;
          gcCanary = new SoftReference<Object>(new Object()); 
        }

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
      lastModeCheckRowCount += batch.size;

      // Check if we should turn into streaming mode
      checkHashModeEfficiency();
    }

    @Override
    public void close(boolean aborted) throws HiveException {
      if (!aborted) {
        flush(true);
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
        LOG.debug(String.format(
            "Flush %d %s entries:%d fixed:%d variable:%d (used:%dMb max:%dMb) gcCanary:%s",
            entriesToFlush, all ? "(all)" : "",
            numEntriesHashTable, fixedHashEntrySize, avgVariableSize,
            numEntriesHashTable * (fixedHashEntrySize + avgVariableSize)/1024/1024,
            maxHashTblMemory/1024/1024,
            gcCanary.get() == null ? "dead" : "alive"));
      }

      /* Iterate the global (keywrapper,aggregationbuffers) map and emit
       a row for each key */
      Iterator<Map.Entry<KeyWrapper, VectorAggregationBufferRow>> iter =
          mapKeysAggregationBuffers.entrySet().iterator();
      while(iter.hasNext()) {
        Map.Entry<KeyWrapper, VectorAggregationBufferRow> pair = iter.next();

        flushSingleRow((VectorHashKeyWrapper) pair.getKey(), pair.getValue());

        if (!all) {
          iter.remove();
          --numEntriesHashTable;
          if (++entriesFlushed >= entriesToFlush) {
            break;
          }
        }
      }

      if (all) {
        mapKeysAggregationBuffers.clear();
        numEntriesHashTable = 0;
      }
      
      if (all && LOG.isDebugEnabled()) {
        LOG.debug(String.format("GC canary caused %d flushes", gcCanaryFlushes));
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
      if (numEntriesHashTable > this.maxHtEntries ||
          numEntriesHashTable * (fixedHashEntrySize + avgVariableSize) > maxHashTblMemory) {
        return true;
      }
      if (gcCanary.get() == null) {
        return true;
      }
      
      return false;
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
     * Checks if the HT reduces the number of entries by at least minReductionHashAggr factor 
     * @throws HiveException
     */
    private void checkHashModeEfficiency() throws HiveException {
      if (lastModeCheckRowCount > numRowsCompareHashAggr) {
        lastModeCheckRowCount = 0;
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("checkHashModeEfficiency: HT:%d RC:%d MIN:%d", 
              numEntriesHashTable, sumBatchSize, (long)(sumBatchSize * minReductionHashAggr)));
        }
        if (numEntriesHashTable > sumBatchSize * minReductionHashAggr) {
          flush(true);

          changeToStreamingMode();
        }
      }
    }
  }

  /**
   * Streaming processing mode. Intermediate values are flushed each time key changes.
   * In this mode we're relying on the MR shuffle and merge the intermediates in the reduce.
   */
  private class ProcessingModeStreaming extends ProcessingModeBase {

    /** 
     * The aggreagation buffers used in streaming mode
     */
    private VectorAggregationBufferRow currentStreamingAggregators;

    /**
     * The current key, used in streaming mode
     */
    private VectorHashKeyWrapper streamingKey;

    /**
     * The keys that needs to be flushed at the end of the current batch
     */
    private final VectorHashKeyWrapper[] keysToFlush = 
        new VectorHashKeyWrapper[VectorizedRowBatch.DEFAULT_SIZE];

    /**
     * The aggregates that needs to be flushed at the end of the current batch
     */
    private final VectorAggregationBufferRow[] rowsToFlush = 
        new VectorAggregationBufferRow[VectorizedRowBatch.DEFAULT_SIZE];

    /**
     * A pool of VectorAggregationBufferRow to avoid repeated allocations
     */
    private VectorUtilBatchObjectPool<VectorAggregationBufferRow> 
      streamAggregationBufferRowPool;

    @Override
    public void initialize(Configuration hconf) throws HiveException {
      streamAggregationBufferRowPool = new VectorUtilBatchObjectPool<VectorAggregationBufferRow>(
          VectorizedRowBatch.DEFAULT_SIZE,
          new VectorUtilBatchObjectPool.IAllocator<VectorAggregationBufferRow>() {

            @Override
            public VectorAggregationBufferRow alloc() throws HiveException {
              return allocateAggregationBuffer();
            }

            @Override
            public void free(VectorAggregationBufferRow t) {
              // Nothing to do
            }
          });
      LOG.info("using streaming aggregation processing mode");
    }

    @Override
    public void processBatch(VectorizedRowBatch batch) throws HiveException {
      // First we traverse the batch to evaluate and prepare the KeyWrappers
      // After this the KeyWrappers are properly set and hash code is computed
      keyWrappersBatch.evaluateBatch(batch);

      VectorHashKeyWrapper[] batchKeys = keyWrappersBatch.getVectorHashKeyWrappers();

      if (streamingKey == null) {
        // This is the first batch we process after switching from hash mode
        currentStreamingAggregators = streamAggregationBufferRowPool.getFromPool();
        streamingKey = (VectorHashKeyWrapper) batchKeys[0].copyKey();
      }

      aggregationBatchInfo.startBatch();
      int flushMark = 0;

      for(int i = 0; i < batch.size; ++i) {
        if (!batchKeys[i].equals(streamingKey)) {
          // We've encountered a new key, must save current one
          // We can't forward yet, the aggregators have not been evaluated
          rowsToFlush[flushMark] = currentStreamingAggregators;
          if (keysToFlush[flushMark] == null) {
            keysToFlush[flushMark] = (VectorHashKeyWrapper) streamingKey.copyKey();
          }
          else {
            streamingKey.duplicateTo(keysToFlush[flushMark]);
          }

          currentStreamingAggregators = streamAggregationBufferRowPool.getFromPool();
          batchKeys[i].duplicateTo(streamingKey);
          ++flushMark;
        }
        aggregationBatchInfo.mapAggregationBufferSet(currentStreamingAggregators, i);
      }

      // evaluate the aggregators
      processAggregators(batch);

      // Now flush/forward all keys/rows, except the last (current) one
      for (int i = 0; i < flushMark; ++i) {
        flushSingleRow(keysToFlush[i], rowsToFlush[i]);
        rowsToFlush[i].reset();
        streamAggregationBufferRowPool.putInPool(rowsToFlush[i]);
      }
    }

    @Override
    public void close(boolean aborted) throws HiveException {
      if (!aborted && null != streamingKey) {
        flushSingleRow(streamingKey, currentStreamingAggregators);
      }
    }
  }

  /**
   * Current processing mode. Processing mode can change (eg. hash -> streaming).
   */
  private transient IProcessingMode processingMode;

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

      List<String> outputFieldNames = conf.getOutputColumnNames();
      outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
          outputFieldNames, objectInspectors);

    } catch (HiveException he) {
      throw he;
    } catch (Throwable e) {
      throw new HiveException(e);
    }

    initializeChildren(hconf);

    forwardCache =new Object[keyExpressions.length + aggregators.length];

    if (keyExpressions.length == 0) {
      processingMode = this.new ProcessingModeGlobalAggregate();
    }
    else {
      //TODO: consider if parent can offer order guarantees
      // If input is sorted, is more efficient to use the streaming mode
      processingMode = this.new ProcessingModeHashAggregate();
    }
    processingMode.initialize(hconf);
  }

  /**
   * changes the processing mode to streaming
   * This is done at the request of the hash agg mode, if the number of keys 
   * exceeds the minReductionHashAggr factor
   * @throws HiveException 
   */
  private void changeToStreamingMode() throws HiveException {
    processingMode = this.new ProcessingModeStreaming();
    processingMode.initialize(null);
    LOG.trace("switched to streaming mode");
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;

    if (batch.size > 0) {
      processingMode.processBatch(batch);
    }
  }

  /**
   * Emits a single row, made from the key and the row aggregation buffers values
   * kw is null if keyExpressions.length is 0
   * @param kw
   * @param agg
   * @throws HiveException
   */
  private void flushSingleRow(VectorHashKeyWrapper kw, VectorAggregationBufferRow agg)
      throws HiveException {
    int fi = 0;
    for (int i = 0; i < keyExpressions.length; ++i) {
      forwardCache[fi++] = keyWrappersBatch.getWritableKeyValue (
          kw, i, keyOutputWriters[i]);
    }
    for (int i = 0; i < aggregators.length; ++i) {
      forwardCache[fi++] = aggregators[i].evaluateOutput(agg.getAggregationBuffer(i));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("forwarding keys: %s: %s",
          kw, Arrays.toString(forwardCache)));
    }
    forward(forwardCache, outputObjInspector);
  }

  @Override
  public void closeOp(boolean aborted) throws HiveException {
    processingMode.close(aborted);
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
