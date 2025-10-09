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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TopNKeyFilter;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorTopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * VectorTopNKeyOperator passes rows that contains top N keys only.
 */
public class VectorTopNKeyOperator extends Operator<TopNKeyDesc> implements VectorizationOperator {

  private static final long serialVersionUID = 1L;

  private VectorTopNKeyDesc vectorDesc;
  private VectorizationContext vContext;

  // Batch processing
  private transient int[] temporarySelected;
  private transient VectorHashKeyWrapperBatch partitionKeyWrapperBatch;
  private transient VectorHashKeyWrapperBatch keyWrappersBatch;
  private transient Map<KeyWrapper, TopNKeyFilter> topNKeyFilters;
  private transient Set<KeyWrapper> disabledPartitions;
  private transient Comparator<VectorHashKeyWrapperBase> keyWrapperComparator;
  private transient long incomingBatches;

  public VectorTopNKeyOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) {

    this(ctx);
    this.conf = (TopNKeyDesc) conf;
    this.vContext = vContext;
    this.vectorDesc = (VectorTopNKeyDesc) vectorDesc;
  }

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorTopNKeyOperator() {
    super();
  }

  public VectorTopNKeyOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    VectorExpression[] keyExpressions = vectorDesc.getKeyExpressions();
    initKeyExpressions(hconf, keyExpressions);

    VectorExpression[] partitionKeyExpressions = vectorDesc.getPartitionKeyColumns();
    initKeyExpressions(hconf, partitionKeyExpressions);

    temporarySelected = new int [VectorizedRowBatch.DEFAULT_SIZE];

    keyWrappersBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);
    keyWrapperComparator = keyWrappersBatch.getComparator(conf.getColumnSortOrder(), conf.getNullOrder());
    partitionKeyWrapperBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(partitionKeyExpressions);
    topNKeyFilters = new HashMap<>();
    disabledPartitions = new HashSet<>();
    incomingBatches = 0;
  }

  private void initKeyExpressions(Configuration hconf, VectorExpression[] keyExpressions) throws HiveException {
    VectorExpression.doTransientInit(keyExpressions, hconf);
    for (VectorExpression keyExpression : keyExpressions) {
      keyExpression.init(hconf);
    }
  }

  @Override
  public void process(Object data, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) data;
    if (!disabledPartitions.isEmpty() && disabledPartitions.size() == topNKeyFilters.size()) { // all filters are disabled due to efficiency check
      vectorForward(batch);
      return;
    }
    incomingBatches++;
    // The selected vector represents selected rows.
    // Clone the selected vector
    System.arraycopy(batch.selected, 0, temporarySelected, 0, batch.selected.length);
    int [] selectedBackup = batch.selected;
    batch.selected = temporarySelected;
    int sizeBackup = batch.size;
    boolean selectedInUseBackup = batch.selectedInUse;

    for (VectorExpression keyExpression : vectorDesc.getKeyExpressions()) {
      keyExpression.evaluate(batch);
    }

    partitionKeyWrapperBatch.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] partitionKeyWrappers = partitionKeyWrapperBatch.getVectorHashKeyWrappers();

    keyWrappersBatch.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] keyWrappers = keyWrappersBatch.getVectorHashKeyWrappers();

    // Filter rows with top n keys
    int size = 0;
    int[] selected = new int[batch.selected.length];
    for (int i = 0; i < batch.size; i++) {
      int j;
      if (batch.selectedInUse) {
        j = batch.selected[i];
      } else {
        j = i;
      }

      VectorHashKeyWrapperBase partitionKey = partitionKeyWrappers[i];
      if (disabledPartitions.contains(partitionKey)) { // filter for this partition is disabled
        selected[size++] = j;
      } else {
        TopNKeyFilter topNKeyFilter = topNKeyFilters.get(partitionKey);
        if (topNKeyFilter == null && topNKeyFilters.size() < conf.getMaxNumberOfPartitions()) {
          topNKeyFilter = new TopNKeyFilter(conf.getTopN(), keyWrapperComparator);
          topNKeyFilters.put(partitionKey.copyKey(), topNKeyFilter);
        }
        if (topNKeyFilter == null || topNKeyFilter.canForward(keyWrappers[i])) {
          selected[size++] = j;
        }
      }
    }

    // Apply selection to batch
    if (batch.size != size) {
      batch.selectedInUse = true;
      batch.selected = selected;
      batch.size = size;
    }

    // Forward the result
    if (size > 0) {
      vectorForward(batch);
    }

    // Restore the original selected vector
    batch.selected = selectedBackup;
    batch.size = sizeBackup;
    batch.selectedInUse = selectedInUseBackup;

    if (incomingBatches % conf.getCheckEfficiencyNumBatches() == 0) {
      checkTopNFilterEfficiency(
        topNKeyFilters, disabledPartitions, conf.getEfficiencyThreshold(), LOG, conf.getCheckEfficiencyNumRows());
    }
  }

  public static void checkTopNFilterEfficiency(Map<KeyWrapper, TopNKeyFilter> filters,
                                               Set<KeyWrapper> disabledPartitions,
                                               float efficiencyThreshold,
                                               Logger log, long checkEfficiencyNumRows)
  {
    Iterator<Map.Entry<KeyWrapper, TopNKeyFilter>> iterator = filters.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<KeyWrapper, TopNKeyFilter> each = iterator.next();
      KeyWrapper partitionKey = each.getKey();
      TopNKeyFilter filter = each.getValue();
      log.debug("Checking TopN Filter efficiency {}, threshold: {}", filter, efficiencyThreshold);
      if (filter.getTotal() >= checkEfficiencyNumRows && filter.forwardingRatio() >= efficiencyThreshold) {
        log.info("Disabling TopN Filter {}", filter);
        disabledPartitions.add(partitionKey);
      }
    }
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

  // Must send on to VectorPTFOperator...
  @Override
  public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setNextVectorBatchGroupStatus(isLastGroupBatch);
    }
  }

  @Override
  public String getName() {
    return TopNKeyOperator.getOperatorName();
  }

  @Override
  public OperatorType getType() {
    return OperatorType.TOPNKEY;
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
//    LOG.info("Closing TopNKeyFilter: {}.", topNKeyFilter);
    if (topNKeyFilters.size() == 1) {
      TopNKeyFilter filter = topNKeyFilters.values().iterator().next();
      LOG.info("Closing TopNKeyFilter: {}", filter);
      filter.clear();
    } else {
      LOG.info("Closing {} TopNKeyFilters", topNKeyFilters.size());
      for (TopNKeyFilter each : topNKeyFilters.values()) {
        LOG.debug("Closing TopNKeyFilter: {}", each);
        each.clear();
      }
    }
    topNKeyFilters.clear();
    disabledPartitions.clear();
    super.closeOp(abort);
  }

  // Because a TopNKeyOperator works like a FilterOperator with top n key condition, its properties
  // for optimizers has same values. Following methods are same with FilterOperator;
  // supportSkewJoinOptimization, columnNamesRowResolvedCanBeObtained,
  // supportAutomaticSortMergeJoin, and supportUnionRemoveOptimization.
  @Override
  public boolean supportSkewJoinOptimization() {
    return true;
  }

  @Override
  public boolean columnNamesRowResolvedCanBeObtained() {
    return true;
  }

  @Override
  public boolean supportAutomaticSortMergeJoin() {
    return true;
  }

  @Override
  public boolean supportUnionRemoveOptimization() {
    return true;
  }
}
