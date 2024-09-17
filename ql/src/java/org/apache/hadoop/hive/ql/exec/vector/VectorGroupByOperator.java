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

package org.apache.hadoop.hive.ql.exec.vector;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.ref.SoftReference;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.IConfigureJobConf;
import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperGeneral;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javolution.util.FastBitSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Vectorized GROUP BY operator implementation. Consumes the vectorized input and
 * stores the aggregate operators' intermediate states. Emits row mode output.
 *
 */
public class VectorGroupByOperator extends Operator<GroupByDesc>
    implements VectorizationOperator, VectorizationContextRegion, IConfigureJobConf {

  private static final Logger LOG = LoggerFactory.getLogger(
      VectorGroupByOperator.class.getName());

  private VectorizationContext vContext;
  private VectorGroupByDesc vectorDesc;

  /**
   * This is the vector of aggregators. They are stateless and only implement
   * the algorithm of how to compute the aggregation. state is kept in the
   * aggregation buffers and is our responsibility to match the proper state for each key.
   */
  private VectorAggregationDesc[] vecAggrDescs;

  /**
   * Key vector expressions.
   */
  private VectorExpression[] keyExpressions;
  private int outputKeyLength;

  private TypeInfo[] outputTypeInfos;
  private DataTypePhysicalVariation[] outputDataTypePhysicalVariations;

  // Create a new outgoing vectorization context because column name map will change.
  private VectorizationContext vOutContext = null;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  @VisibleForTesting
  transient VectorAggregateExpression[] aggregators;
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

  private transient VectorizedRowBatch outputBatch;
  private transient VectorizedRowBatchCtx vrbCtx;

  /*
   * Grouping sets members.
   */
  private transient boolean groupingSetsPresent;

  // The field bits (i.e. which fields to include) or "id" for each grouping set.
  private transient long[] groupingSets;

  // The position in the column keys of the dummy grouping set id column.
  private transient int groupingSetsPosition;

  // The planner puts a constant field in for the dummy grouping set id.  We will overwrite it
  // as we process the grouping sets.
  private transient ConstantVectorExpression groupingSetsDummyVectorExpression;

  // We translate the grouping set bit field into a boolean arrays.
  private transient boolean[][] allGroupingSetsOverrideIsNulls;

  private transient int numEntriesHashTable;
  private transient long numFlushedOutEntriesBeforeFinalFlush;

  private transient long maxHashTblMemory;

  private transient long maxMemory;

  private float memoryThreshold;

  private boolean isLlap = false;

  // tracks overall access count in map agg buffer any given time.
  private long totalAccessCount;
  private boolean batchNeedsClone;

  /**
   * Interface for processing mode: global, hash, unsorted streaming, or group batch
   */
  private static interface IProcessingMode {
    void initialize(Configuration hconf) throws HiveException;
    void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException;
    void processBatch(VectorizedRowBatch batch) throws HiveException;
    void close(boolean aborted) throws HiveException;
  }

  /**
   * Base class for all processing modes
   */
  private abstract class ProcessingModeBase implements IProcessingMode {
    /**
     * The VectorAggregationBufferRow instance. This field can be shared with ProcessingMode
     * subclasses, where is only 1 VectorAggregationBufferRow instance needed at the same time. This
     * is the case for ProcessingModeGlobalAggregate, ProcessingModeReduceMergePartial,
     * ProcessingModeStreaming, but not for ProcessingModeHashAggregate where this field is not
     * used.
     */
    protected VectorAggregationBufferRow aggregationBufferSet;

    // Overridden and used in ProcessingModeReduceMergePartial mode.
    @Override
    public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
      // Ignore it.
    }

    protected abstract void doProcessBatch(VectorizedRowBatch batch, boolean isFirstGroupingSet,
        boolean[] currentGroupingSetsOverrideIsNulls) throws HiveException;

    @Override
    public void processBatch(VectorizedRowBatch batch) throws HiveException {

      if (!groupingSetsPresent) {
        doProcessBatch(batch, false, null);
      } else {
        // We drive the doProcessBatch logic with the same batch but different
        // grouping set id and null variation.
        // PERFORMANCE NOTE: We do not try to reuse columns and generate the KeyWrappers anew...

        final int size = groupingSets.length;
        for (int i = 0; i < size; i++) {

          // NOTE: We are overwriting the constant vector value...
          groupingSetsDummyVectorExpression.setLongValue(groupingSets[i]);
          groupingSetsDummyVectorExpression.evaluate(batch);

          doProcessBatch(batch, (i == 0), allGroupingSetsOverrideIsNulls[i]);
        }
      }

      if (this instanceof ProcessingModeHashAggregate) {
        // Check if we should turn into streaming mode
        ((ProcessingModeHashAggregate)this).checkHashModeEfficiency();
      }
    }

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

    @Override
    public void close(boolean aborted) throws HiveException {
      finishAggregators(aggregationBufferSet, aborted);
    }

    public void finishAggregators(VectorAggregationBufferRow vectorAggregationBufferRow, boolean aborted) {
      if (vectorAggregationBufferRow != null) {
        for (int i = 0; i < aggregators.length; ++i) {
          aggregators[i].finish(vectorAggregationBufferRow.getAggregationBuffer(i), aborted);
        }
      }
    }
  }

  /**
   * Global aggregates (no GROUP BY clause, no keys)
   * This mode is very simple, there are no keys to consider, and only flushes one row at closing
   * The one row must flush even if no input was seen (NULLs)
   */
  final class ProcessingModeGlobalAggregate extends ProcessingModeBase {

    @Override
    public void initialize(Configuration hconf) throws HiveException {
      aggregationBufferSet =  allocateAggregationBuffer();
      LOG.info("using global aggregation processing mode");
    }

    @Override
    public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
      // Do nothing.
    }

    @Override
    public void doProcessBatch(VectorizedRowBatch batch, boolean isFirstGroupingSet,
        boolean[] currentGroupingSetsOverrideIsNulls) throws HiveException {
      for (int i = 0; i < aggregators.length; ++i) {
        aggregators[i].aggregateInput(aggregationBufferSet.getAggregationBuffer(i), batch);
      }
    }

    @Override
    public void close(boolean aborted) throws HiveException {
      super.close(aborted);

      if (!aborted) {
        writeSingleRow(null, aggregationBufferSet);
      }
    }
  }

  /**
   * Hash Aggregate mode processing
   */
   final class ProcessingModeHashAggregate extends ProcessingModeBase {

    private Queue<KeyWrapper> reusableKeyWrapperBuffer;

    /**
     * The global key-aggregation hash map.
     */
    @VisibleForTesting
    Map<KeyWrapper, VectorAggregationBufferRow> mapKeysAggregationBuffers;

    private Queue<VectorAggregationBufferRow> reusableAggregationBufferRows =
        new ArrayDeque<>(VectorizedRowBatch.DEFAULT_SIZE);

    /**
     * Total per hashtable entry fixed memory (does not depend on key/agg values).
     */
    private long fixedHashEntrySize;

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
    @VisibleForTesting
    SoftReference<Object> gcCanary = new SoftReference<Object>(new Object());

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
        this.numRowsCompareHashAggr = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVE_GROUPBY_MAP_INTERVAL);
      }
      else {
        this.percentEntriesToFlush =
            HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT.defaultFloatVal;
        this.checkInterval =
            HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_CHECKINTERVAL.defaultIntVal;
        this.maxHtEntries =
            HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_MAXENTRIES.defaultIntVal;
        this.numRowsCompareHashAggr =
            HiveConf.ConfVars.HIVE_GROUPBY_MAP_INTERVAL.defaultIntVal;
      }

      minReductionHashAggr = getConf().getMinReductionHashAggr();

      sumBatchSize = 0;

      mapKeysAggregationBuffers = new HashMap<KeyWrapper, VectorAggregationBufferRow>();
      /*
       * The grouping sets expand the hash sizes by producing intermediate keys. 3 grouping sets
       * of (),(col1),(col1,col2), will turn 10 rows into 30 rows. If the col1 has an nDV of 2 and
       * col2 has nDV of 5, then this turns into a maximum of 1+3+(2*5) or 14 keys into the
       * hashtable.
       *
       * So you get 10 rows in and 14 rows out, which is a reduction of ~2x vs Streaming mode,
       * but it is an increase if the grouping-set is not accounted for.
       *
       * For performance, it is definitely better to send 14 rows out to shuffle and not 30.
       *
       * Particularly if the same nDVs are repeated for a thousand rows, this would send a
       * thousand rows via streaming to a single reducer which owns the empty grouping set,
       * instead of sending 1 from the hash.
       *
       */
      if (groupingSets != null && groupingSets.length > 0) {
        /**
         * Adjust for grouping sets. For grouping sets, input records are processed 'n' number of times.
         * Ref processBatch(). Ensure that reduction is super effective, otherwise they become
         * memory intensive.
         */
        this.minReductionHashAggr = this.minReductionHashAggr / groupingSets.length;
        LOG.info("New maxHtEntries: {}, groupingSets len: {}, numRowsCompareHashAggr: {}, "
                + "minReductionHashAggr:{} ", maxHtEntries, groupingSets.length,
            numRowsCompareHashAggr, minReductionHashAggr);
      }
      computeMemoryLimits();
      LOG.debug("using hash aggregation processing mode");

      if (keyWrappersBatch.getVectorHashKeyWrappers()[0] instanceof VectorHashKeyWrapperGeneral) {
        reusableKeyWrapperBuffer = new ArrayDeque<>(VectorizedRowBatch.DEFAULT_SIZE);
      }
    }

    @VisibleForTesting
    int getMaxHtEntries() {
      return maxHtEntries;
    }

    @Override
    public void doProcessBatch(VectorizedRowBatch batch, boolean isFirstGroupingSet,
        boolean[] currentGroupingSetsOverrideIsNulls) throws HiveException {

      if (!groupingSetsPresent || isFirstGroupingSet) {

        // Evaluate the key expressions once.
        for(int i = 0; i < keyExpressions.length; ++i) {
          keyExpressions[i].evaluate(batch);
        }
      }

      // First we traverse the batch to evaluate and prepare the KeyWrappers
      // After this the KeyWrappers are properly set and hash code is computed
      if (!groupingSetsPresent) {
        keyWrappersBatch.evaluateBatch(batch);
      } else {
        keyWrappersBatch.evaluateBatchGroupingSets(batch, currentGroupingSetsOverrideIsNulls);
      }

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
          LOG.debug("Flush did not progress: {} entries before, {} entries after", preFlushEntriesCount,
              numEntriesHashTable);
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
    }

    @Override
    protected VectorAggregationBufferRow allocateAggregationBuffer() throws HiveException {
      VectorAggregationBufferRow bufferSet;
      if (reusableAggregationBufferRows.size() > 0) {
        bufferSet = reusableAggregationBufferRows.remove();
        bufferSet.setVersionAndIndex(0, 0);
        for (int i = 0; i < aggregators.length; i++) {
          aggregators[i].reset(bufferSet.getAggregationBuffer(i));
        }
        return bufferSet;
      } else {
        return super.allocateAggregationBuffer();
      }
    }

    @Override
    public void close(boolean aborted) throws HiveException {
      super.close(aborted);

      reusableAggregationBufferRows.clear();
      if (reusableKeyWrapperBuffer != null) {
        reusableKeyWrapperBuffer.clear();
      }
      if (!aborted) {
        flush(true);
      }
      if (!aborted && sumBatchSize == 0 && GroupByOperator.shouldEmitSummaryRow(conf)) {
        // in case the empty grouping set is preset; but no output has done
        // the "summary row" still needs to be emitted
        VectorHashKeyWrapperBase kw = keyWrappersBatch.getVectorHashKeyWrappers()[0];
        kw.setNull();
        int pos = conf.getGroupingSetPosition();
        if (pos >= 0) {
          long val = (1L << pos) - 1;
          keyWrappersBatch.setLongValue(kw, pos, val);
        }
        VectorAggregationBufferRow groupAggregators = allocateAggregationBuffer();
        finishAggregators(groupAggregators, false);
        writeSingleRow(kw, groupAggregators);
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

      if (batch.size == 0) {
        return;
      }

      // We now have to probe the global hash and find-or-allocate
      // the aggregation buffers to use for each key present in the batch
      VectorHashKeyWrapperBase[] keyWrappers = keyWrappersBatch.getVectorHashKeyWrappers();

      final int n = keyExpressions.length == 0 ? 1 : batch.size;
      // note - the row mapping is not relevant when aggregationBatchInfo::getDistinctBufferSetCount() == 1

      for (int i=0; i < n; ++i) {
        VectorHashKeyWrapperBase kw = keyWrappers[i];
        VectorAggregationBufferRow aggregationBuffer = mapKeysAggregationBuffers.get(kw);
        if (null == aggregationBuffer) {
          // the probe failed, we must allocate a set of aggregation buffers
          // and push the (keywrapper,buffers) pair into the hash.
          // is very important to clone the keywrapper, the one we have from our
          // keyWrappersBatch is going to be reset/reused on next batch.
          aggregationBuffer = allocateAggregationBuffer();
          KeyWrapper copyKeyWrapper = cloneKeyWrapper(kw);
          mapKeysAggregationBuffers.put(copyKeyWrapper, aggregationBuffer);
          numEntriesHashTable++;
          numEntriesSinceCheck++;
        } else {
          // for access tracking
          aggregationBuffer.incrementAccessCount();
          totalAccessCount++;
        }
        aggregationBatchInfo.mapAggregationBufferSet(aggregationBuffer, i);
      }
    }

    private KeyWrapper cloneKeyWrapper(VectorHashKeyWrapperBase from) {
      if (reusableKeyWrapperBuffer != null && reusableKeyWrapperBuffer.size() > 0) {
        KeyWrapper keyWrapper = reusableKeyWrapperBuffer.poll();
        from.copyKey(keyWrapper);
        return keyWrapper;
      } else {
        return from.copyKey();
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
      maxMemory = isLlap ? getConf().getMaxMemoryAvailable() : memoryMXBean.getHeapMemoryUsage().getMax();
      memoryThreshold = conf.getMemoryThreshold();
      // Tests may leave this unitialized, so better set it to 1
      if (memoryThreshold == 0.0f) {
        memoryThreshold = 1.0f;
      }

      maxHashTblMemory = (int)(maxMemory * memoryThreshold);

      if (LOG.isDebugEnabled()) {
        LOG.debug("GBY memory limits - isLlap: {} maxMemory: {} ({} * {}) fixSize:{} (key:{} agg:{})",
          isLlap,
          LlapUtil.humanReadableByteCount(maxHashTblMemory),
          LlapUtil.humanReadableByteCount(maxMemory),
          memoryThreshold,
          fixedHashEntrySize,
          keyWrappersBatch.getKeysFixedSize(),
          aggregationBatchInfo.getAggregatorsFixedSize());
      }
    }

    int computeAvgAccess() {
      if (numEntriesHashTable == 0) {
        return 0;
      }
      int avgAccess = (int) (totalAccessCount / numEntriesHashTable);
      LOG.debug("totalAccessCount:{}, numEntries:{}, avgAccess:{}",
          totalAccessCount, numEntriesHashTable, avgAccess);
      return avgAccess;
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
      int avgAccess = computeAvgAccess();

      /* Iterate the global (keywrapper,aggregationbuffers) map and emit
       a row for each key */
      Iterator<Map.Entry<KeyWrapper, VectorAggregationBufferRow>> iter =
          mapKeysAggregationBuffers.entrySet().iterator();
      while(iter.hasNext()) {
        Map.Entry<KeyWrapper, VectorAggregationBufferRow> pair = iter.next();
        KeyWrapper keyWrapper = pair.getKey();
        VectorAggregationBufferRow bufferRow = pair.getValue();
        if (!all && avgAccess >= 1) {
          if (bufferRow.getAccessCount() > avgAccess) {
            // resetting to give chance for other entries
            totalAccessCount -= bufferRow.getAccessCount();
            bufferRow.resetAccessCount();
            continue;
          }
        }

        finishAggregators(bufferRow, false);
        writeSingleRow((VectorHashKeyWrapperBase) keyWrapper, bufferRow);

        if (!all) {
          totalAccessCount -= bufferRow.getAccessCount();
          reusableAggregationBufferRows.add(bufferRow);
          bufferRow.resetAccessCount();
          if (reusableKeyWrapperBuffer != null) {
            reusableKeyWrapperBuffer.add(pair.getKey());
          }
          iter.remove();
          --numEntriesHashTable;
          if (++entriesFlushed >= entriesToFlush) {
            break;
          }
        }
      }

      if (!all) {
        numFlushedOutEntriesBeforeFinalFlush += entriesFlushed;
      }

      if (all) {
        mapKeysAggregationBuffers.clear();
        totalAccessCount = 0;
        numEntriesHashTable = 0;
        numFlushedOutEntriesBeforeFinalFlush = 0;
      }

      if (all && LOG.isDebugEnabled()) {
        LOG.debug(String.format("GC canary caused %d flushes", gcCanaryFlushes));
      }
    }

    /**
     * Returns true if the memory threshold for the hash table was reached.
     * WARN: Frequent flushing can reduce Op throughput
     */
    private boolean shouldFlush(VectorizedRowBatch batch) {
      if (batch.size == 0) {
        return false;
      }
      // numEntriesSinceCheck is the number of entries added to the hash table
      // since the last time we checked the average variable size
      if (numEntriesSinceCheck >= this.checkInterval) {
        // Were going to update the average variable row size by sampling the current batch
        updateAvgVariableSize(batch);
        numEntriesSinceCheck = 0;
      }
      long currMemUsed = numEntriesHashTable * (fixedHashEntrySize + avgVariableSize);
      // Protect against low maxHtEntries setting: if memory usage is below 30% avoid flushing
      if ( ((numEntriesHashTable > this.maxHtEntries) && (currMemUsed > 0.3 * maxHashTblMemory))  ||
          currMemUsed > maxHashTblMemory) {
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
        /**
         * For grouping sets, incoming data is processed multiple times depending on
         * grouping set length. sumBatchSize already accounts for this. Here we mainly check for
         * hashtable efficiency.
         */
        final long inputRecords = sumBatchSize;
        final long outputRecords = numEntriesHashTable + numFlushedOutEntriesBeforeFinalFlush;
        final float ratio = (outputRecords) / (inputRecords * 1.0f);
        if (ratio > minReductionHashAggr) {
          if (inputRecords > maxHtEntries) { // Don't bail out too soon.
            flush(true);
            changeToStreamingMode();
          }
        }
      }
    }
  }

  /**
   * Streaming processing mode on ALREADY GROUPED data. Each input VectorizedRowBatch may
   * have a mix of different keys.  Intermediate values are flushed each time key changes.
   */
  final class ProcessingModeStreaming extends ProcessingModeBase {

    /**
     * The current key, used in streaming mode
     */
    private VectorHashKeyWrapperBase streamingKey;

    /**
     * The keys that needs to be flushed at the end of the current batch
     */
    private final VectorHashKeyWrapperBase[] keysToFlush =
        new VectorHashKeyWrapperBase[VectorizedRowBatch.DEFAULT_SIZE];

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
      LOG.info("using unsorted streaming aggregation processing mode");
    }

    @Override
    public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
      // Do nothing.
    }

    @Override
    public void doProcessBatch(VectorizedRowBatch batch, boolean isFirstGroupingSet,
        boolean[] currentGroupingSetsOverrideIsNulls) throws HiveException {

      if (!groupingSetsPresent || isFirstGroupingSet) {

        // Evaluate the key expressions once.
        for(int i = 0; i < keyExpressions.length; ++i) {
          keyExpressions[i].evaluate(batch);
        }
      }

      // First we traverse the batch to evaluate and prepare the KeyWrappers
      // After this the KeyWrappers are properly set and hash code is computed
      if (!groupingSetsPresent) {
        keyWrappersBatch.evaluateBatch(batch);
      } else {
        keyWrappersBatch.evaluateBatchGroupingSets(batch, currentGroupingSetsOverrideIsNulls);
      }

      VectorHashKeyWrapperBase[] batchKeys = keyWrappersBatch.getVectorHashKeyWrappers();

      final VectorHashKeyWrapperBase prevKey = streamingKey;
      if (streamingKey == null) {
        // This is the first batch we process after switching from hash mode
        aggregationBufferSet = streamAggregationBufferRowPool.getFromPool();
        streamingKey = batchKeys[0];
      }

      aggregationBatchInfo.startBatch();
      int flushMark = 0;

      for(int i = 0; i < batch.size; ++i) {
        if (!batchKeys[i].equals(streamingKey)) {
          // We've encountered a new key, must save current one
          // We can't forward yet, the aggregators have not been evaluated
          rowsToFlush[flushMark] = aggregationBufferSet;
          keysToFlush[flushMark] = streamingKey;
          aggregationBufferSet = streamAggregationBufferRowPool.getFromPool();
          streamingKey = batchKeys[i];
          ++flushMark;
        }
        aggregationBatchInfo.mapAggregationBufferSet(aggregationBufferSet, i);
      }

      // evaluate the aggregators
      processAggregators(batch);

      // Now flush/forward all keys/rows, except the last (current) one
      for (int i = 0; i < flushMark; ++i) {
        finishAggregators(rowsToFlush[i], false); //finish aggregations before flushing
        writeSingleRow(keysToFlush[i], rowsToFlush[i]);
        rowsToFlush[i].reset();
        keysToFlush[i] = null;
        streamAggregationBufferRowPool.putInPool(rowsToFlush[i]);
      }

      if (streamingKey != prevKey) {
        streamingKey = (VectorHashKeyWrapperBase) streamingKey.copyKey();
      }
    }

    @Override
    public void close(boolean aborted) throws HiveException {
      super.close(aborted);
      if (!aborted && null != streamingKey) {
        writeSingleRow(streamingKey, aggregationBufferSet);
      }
    }
  }

  /**
   * Sorted reduce group batch processing mode. Each input VectorizedRowBatch will have the
   * same key.  On endGroup (or close), the intermediate values are flushed.
   *
   * We build the output rows one-at-a-time in the output vectorized row batch (outputBatch)
   * in 2 steps:
   *
   *   1) Just after startGroup, we copy the group key to the next position in the output batch,
   *      but don't increment the size in the batch (yet).  This is done with the copyGroupKey
   *      method of VectorGroupKeyHelper.  The next position is outputBatch.size
   *
   *      We know the same key is used for the whole batch (i.e. repeating) since that is how
   *      vectorized reduce-shuffle feeds the batches to us.
   *
   *   2) Later at endGroup after reduce-shuffle has fed us all the input batches for the group,
   *      we fill in the aggregation columns in outputBatch at outputBatch.size.  Our method
   *      writeGroupRow does this and finally increments outputBatch.size.
   *
   */
  final class ProcessingModeReduceMergePartial extends ProcessingModeBase {

    private boolean first;
    private boolean isLastGroupBatch;

    /**
     * The group vector key helper.
     */
    VectorGroupKeyHelper groupKeyHelper;

    /**
     * Buffer to hold string values.
     */
    private DataOutputBuffer buffer;

    @Override
    public void initialize(Configuration hconf) throws HiveException {
      isLastGroupBatch = true;

      // We do not include the dummy grouping set column in the output.  So we pass outputKeyLength
      // instead of keyExpressions.length
      groupKeyHelper = new VectorGroupKeyHelper(outputKeyLength);
      groupKeyHelper.init(keyExpressions);
      aggregationBufferSet = allocateAggregationBuffer();
      buffer = new DataOutputBuffer();
      LOG.info("using sorted group batch aggregation processing mode");
    }

    @Override
    public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
      if (this.isLastGroupBatch) {
        // Previous batch was the last of a group of batches.  Remember the next is the first batch
        // of a new group of batches.
        first = true;
      }
      this.isLastGroupBatch = isLastGroupBatch;
    }

    @Override
    public void doProcessBatch(VectorizedRowBatch batch, boolean isFirstGroupingSet,
        boolean[] currentGroupingSetsOverrideIsNulls) throws HiveException {
      if (first) {
        // Copy the group key to output batch now.  We'll copy in the aggregates at the end of the group.
        first = false;

        // Evaluate the key expressions of just this first batch to get the correct key.
        for (int i = 0; i < outputKeyLength; i++) {
          keyExpressions[i].evaluate(batch);
        }

        groupKeyHelper.copyGroupKey(batch, outputBatch, buffer);
      }

      // Aggregate this batch.
      for (int i = 0; i < aggregators.length; ++i) {
        aggregators[i].aggregateInput(aggregationBufferSet.getAggregationBuffer(i), batch);
      }

      if (isLastGroupBatch) {
        finishAggregators(aggregationBufferSet, false);
        writeGroupRow(aggregationBufferSet, buffer);
        aggregationBufferSet.reset();
      }
    }

    @Override
    public void close(boolean aborted) throws HiveException {
      super.close(aborted);
      if (!aborted && !first && !isLastGroupBatch) {
        writeGroupRow(aggregationBufferSet, buffer);
      }
    }
  }

  /**
   * Current processing mode. Processing mode can change (eg. hash -> streaming).
   */
  @VisibleForTesting
  transient IProcessingMode processingMode;

  private static final long serialVersionUID = 1L;

  public VectorGroupByOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    this(ctx);
    GroupByDesc desc = (GroupByDesc) conf;
    this.conf = desc;
    this.vContext = vContext;
    this.vectorDesc = (VectorGroupByDesc) vectorDesc;
    keyExpressions = this.vectorDesc.getKeyExpressions();
    vecAggrDescs = this.vectorDesc.getVecAggrDescs();

    // Grouping id should be pruned, which is the last of key columns
    // see ColumnPrunerGroupByProc
    outputKeyLength =
        this.conf.pruneGroupingSetId() ? keyExpressions.length - 1 : keyExpressions.length;

    final int aggregationCount = vecAggrDescs.length;
    final int outputCount = outputKeyLength + aggregationCount;

    outputTypeInfos = new TypeInfo[outputCount];
    outputDataTypePhysicalVariations = new DataTypePhysicalVariation[outputCount];
    for (int i = 0; i < outputKeyLength; i++) {
      VectorExpression keyExpression = keyExpressions[i];
      outputTypeInfos[i] = keyExpression.getOutputTypeInfo();
      outputDataTypePhysicalVariations[i] = keyExpression.getOutputDataTypePhysicalVariation();
    }
    for (int i = 0; i < aggregationCount; i++) {
      VectorAggregationDesc vecAggrDesc = vecAggrDescs[i];
      outputTypeInfos[i + outputKeyLength] = vecAggrDesc.getOutputTypeInfo();
      outputDataTypePhysicalVariations[i + outputKeyLength] =
          vecAggrDesc.getOutputDataTypePhysicalVariation();
    }

    vOutContext = new VectorizationContext(getName(), desc.getOutputColumnNames(),
        /* vContextEnvironment */ vContext);
    vOutContext.setInitialTypeInfos(Arrays.asList(outputTypeInfos));
    vOutContext.setInitialDataTypePhysicalVariations(Arrays.asList(outputDataTypePhysicalVariations));
  }

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorGroupByOperator() {
    super();
  }

  public VectorGroupByOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  private void setupGroupingSets() {

    groupingSetsPresent = conf.isGroupingSetsPresent();
    if (!groupingSetsPresent) {
      groupingSets = null;
      groupingSetsPosition = -1;
      groupingSetsDummyVectorExpression = null;
      allGroupingSetsOverrideIsNulls = null;
      return;
    }

    groupingSets = ArrayUtils.toPrimitive(conf.getListGroupingSets().toArray(new Long[0]));
    groupingSetsPosition = conf.getGroupingSetPosition();

    allGroupingSetsOverrideIsNulls = new boolean[groupingSets.length][];

    int pos = 0;
    for (long groupingSet: groupingSets) {

      // Create the mapping corresponding to the grouping set

      // Assume all columns are null, except the dummy column is always non-null.
      boolean[] groupingSetsOverrideIsNull = new boolean[keyExpressions.length];
      Arrays.fill(groupingSetsOverrideIsNull, true);
      groupingSetsOverrideIsNull[groupingSetsPosition] = false;

      // Add keys of this grouping set.
      FastBitSet bitset = GroupByOperator.groupingSet2BitSet(groupingSet, groupingSetsPosition);
      for (int keyPos = bitset.nextClearBit(0); keyPos < groupingSetsPosition;
        keyPos = bitset.nextClearBit(keyPos+1)) {
        groupingSetsOverrideIsNull[keyPos] = false;
      }

      allGroupingSetsOverrideIsNulls[pos] =  groupingSetsOverrideIsNull;
      pos++;
    }

    // The last key column is the dummy grouping set id.
    //
    // Figure out which (scratch) column was used so we can overwrite the dummy id.

    groupingSetsDummyVectorExpression = (ConstantVectorExpression) keyExpressions[groupingSetsPosition];
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    isLlap = LlapProxy.isDaemon();
    VectorExpression.doTransientInit(keyExpressions, hconf);

    List<ObjectInspector> objectInspectors = new ArrayList<>();

    List<ExprNodeDesc> keysDesc = conf.getKeys();
    try {
      List<String> outputFieldNames = conf.getOutputColumnNames();

      for(int i = 0; i < outputKeyLength; ++i) {
        VectorExpressionWriter vew = VectorExpressionWriterFactory.
            genVectorExpressionWritable(keysDesc.get(i));
        ObjectInspector oi = vew.getObjectInspector();
        objectInspectors.add(oi);
      }

      final int aggregateCount = vecAggrDescs.length;
      aggregators = new VectorAggregateExpression[aggregateCount];
      for (int i = 0; i < aggregateCount; ++i) {
        VectorAggregationDesc vecAggrDesc = vecAggrDescs[i];
        VectorAggregateExpression vecAggrExpr = instantiateExpression(vecAggrDesc);
        VectorExpression.doTransientInit(vecAggrExpr.getInputExpression(), hconf);
        aggregators[i] = vecAggrExpr;

        ObjectInspector objInsp =
            TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
                vecAggrDesc.getOutputTypeInfo());
        Preconditions.checkState(objInsp != null);
        objectInspectors.add(objInsp);
      }

      for (VectorAggregateExpression aggregator : aggregators) {
        if (aggregator.batchNeedsClone()) {
          batchNeedsClone = true;
          break;
        }
      }

      keyWrappersBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);
      aggregationBatchInfo = new VectorAggregationBufferBatch();
      aggregationBatchInfo.compileAggregationBatchInfo(aggregators);

      outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
          outputFieldNames, objectInspectors);

      vrbCtx = new VectorizedRowBatchCtx(
          outputFieldNames.toArray(new String[0]),
          outputTypeInfos,
          outputDataTypePhysicalVariations,
          /* dataColumnNums */ null,
          /* partitionColumnCount */ 0,
          /* virtualColumnCount */ 0,
          /* neededVirtualColumns */ null,
          vOutContext.getScratchColumnTypeNames(),
          vOutContext.getScratchDataTypePhysicalVariations());

      outputBatch = vrbCtx.createVectorizedRowBatch();

    } catch (HiveException he) {
      throw he;
    } catch (Throwable e) {
      throw new HiveException(e);
    }

    forwardCache = new Object[outputKeyLength + aggregators.length];

    setupGroupingSets();

    switch (vectorDesc.getProcessingMode()) {
    case GLOBAL:
      Preconditions.checkState(outputKeyLength == 0);
      Preconditions.checkState(!groupingSetsPresent);
      processingMode = this.new ProcessingModeGlobalAggregate();
      break;
    case HASH:
      processingMode = this.new ProcessingModeHashAggregate();
      break;
    case MERGE_PARTIAL:
      Preconditions.checkState(!groupingSetsPresent);
      processingMode = this.new ProcessingModeReduceMergePartial();
      break;
    case STREAMING:
      processingMode = this.new ProcessingModeStreaming();
      break;
    default:
      throw new RuntimeException("Unsupported vector GROUP BY processing mode " +
          vectorDesc.getProcessingMode().name());
    }
    processingMode.initialize(hconf);
  }

  @VisibleForTesting
  VectorAggregateExpression instantiateExpression(VectorAggregationDesc vecAggrDesc) throws HiveException {
    final Class<? extends VectorAggregateExpression> vecAggrClass = vecAggrDesc.getVecAggrClass();
    final Constructor<? extends VectorAggregateExpression> ctor;

    final List<ConstantVectorExpression> constants =
        vecAggrDesc.getConstants() == null ? Collections.emptyList() : vecAggrDesc.getConstants();

    final Class<?>[] ctorParamClasses = new Class<?>[constants.size() + 1];
    ctorParamClasses[0] = VectorAggregationDesc.class;

    final List<Object> values = new ArrayList<>(constants.size() + 1);
    values.add(vecAggrDesc);

    for (int i = 0; i < constants.size(); ++i) {
      ConstantVectorExpression constant = constants.get(i);
      String typeName = constant.getOutputTypeInfo().getTypeName();
      PrimitiveObjectInspectorUtils.PrimitiveTypeEntry primitiveTypeEntry =
          PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(typeName);
      if (primitiveTypeEntry == null) {
        throw new IllegalArgumentException(
            "Non-primitive type detected as " + i + "-th argument for a call to the vectorized aggregation class "
                + vecAggrClass.getSimpleName() + ", only primitive types are supported");
      }
      ctorParamClasses[i + 1] = primitiveTypeEntry.primitiveJavaType;

      // this is needed to bring back to the right type the value, e.g. int-family always gets back a long,
      // but in this way the constructor parameters won't match anymore, so we need to convert here
      switch (primitiveTypeEntry.primitiveCategory) {
      case BYTE:
        values.add(constant.getBytesValue());
        break;
      case FLOAT:
        values.add(new Double(constant.getDoubleValue()).floatValue());
        break;
      case DOUBLE:
        values.add(constant.getDoubleValue());
        break;
      case INT:
        values.add(new Long(constant.getLongValue()).intValue());
        break;
      case LONG:
        values.add(constant.getLongValue());
        break;
      case SHORT:
        values.add(new Long(constant.getLongValue()).shortValue());
        break;
      default:
        values.add(constant.getValue());
      }
    }

    try {
      ctor = vecAggrClass.getConstructor(ctorParamClasses);
    } catch (Exception e) {
      throw new HiveException(
          "Constructor " + vecAggrClass.getSimpleName() + "(" + Arrays.toString(ctorParamClasses) + ") not available", e);
    }
    try {
      return ctor.newInstance(values.toArray(new Object[0]));
    } catch (Exception e) {
      throw new HiveException(
          "Failed to create " + vecAggrClass.getSimpleName() + "(" + Arrays.toString(ctorParamClasses) + ") object ", e);
    }
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
    LOG.info("switched to streaming mode");
  }

  @Override
  public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
    processingMode.setNextVectorBatchGroupStatus(isLastGroupBatch);
  }

  @Override
  public void startGroup() throws HiveException {

    // We do not call startGroup on operators below because we are batching rows in
    // an output batch and the semantics will not work.
    // super.startGroup();
    throw new HiveException("Unexpected startGroup");
  }

  @Override
  public void endGroup() throws HiveException {

    // We do not call endGroup on operators below because we are batching rows in
    // an output batch and the semantics will not work.
    // super.endGroup();
    throw new HiveException("Unexpected startGroup");
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
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
  private void writeSingleRow(VectorHashKeyWrapperBase kw, VectorAggregationBufferRow agg)
      throws HiveException {

    int colNum = 0;
    final int batchIndex = outputBatch.size;

    // Output keys and aggregates into the output batch.
    for (int i = 0; i < outputKeyLength; ++i) {
      keyWrappersBatch.assignRowColumn(outputBatch, batchIndex, colNum++, kw);
    }
    for (int i = 0; i < aggregators.length; ++i) {
      aggregators[i].assignRowColumn(outputBatch, batchIndex, colNum++,
          agg.getAggregationBuffer(i));
    }
    ++outputBatch.size;
    if (outputBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
      flushOutput();
    }
  }

  /**
   * Emits a (reduce) group row, made from the key (copied in at the beginning of the group) and
   * the row aggregation buffers values
   * @param agg
   * @param buffer
   * @throws HiveException
   */
  private void writeGroupRow(VectorAggregationBufferRow agg, DataOutputBuffer buffer)
      throws HiveException {
    int colNum = outputKeyLength;   // Start after group keys.
    final int batchIndex = outputBatch.size;

    for (int i = 0; i < aggregators.length; ++i) {
      aggregators[i].assignRowColumn(outputBatch, batchIndex, colNum++,
          agg.getAggregationBuffer(i));
    }
    ++outputBatch.size;
    if (outputBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
      flushOutput();
      buffer.reset();
    }
  }

  private void flushOutput() throws HiveException {
    vectorForward(outputBatch);
    outputBatch.reset();
  }

  @Override
  public void closeOp(boolean aborted) throws HiveException {
    processingMode.close(aborted);
    if (!aborted && outputBatch.size > 0) {
      flushOutput();
    }
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

  @Override
  public VectorizationContext getOutputVectorizationContext() {
    return vOutContext;
  }

  @Override
  public OperatorType getType() {
    return OperatorType.GROUPBY;
  }

  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "GBY";
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

  @Override
  public void configureJobConf(JobConf job) {
    // only needed when grouping sets are present
    if (conf.getGroupingSetPosition() > 0 && GroupByOperator.shouldEmitSummaryRow(conf)) {
      job.setBoolean(Utilities.ENSURE_OPERATORS_EXECUTED, true);
    }
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public boolean batchNeedsClone() {
    return batchNeedsClone;
  }
}
