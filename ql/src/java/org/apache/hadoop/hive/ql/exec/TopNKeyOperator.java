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

package org.apache.hadoop.hive.ql.exec;

import static org.apache.hadoop.hive.ql.exec.vector.VectorTopNKeyOperator.checkTopNFilterEfficiency;
import static org.apache.hadoop.hive.ql.plan.api.OperatorType.TOPNKEY;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * TopNKeyOperator passes rows that contains top N keys only.
 */
public class TopNKeyOperator extends Operator<TopNKeyDesc> implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient Map<KeyWrapper, TopNKeyFilter> topNKeyFilters;

  private transient KeyWrapper partitionKeyWrapper;
  private transient KeyWrapper keyWrapper;

  private transient KeyWrapperComparator keyWrapperComparator;
  private transient Set<KeyWrapper> disabledPartitions;

  /** Kryo ctor. */
  public TopNKeyOperator() {
    super();
  }

  public TopNKeyOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    ObjectInspector rowInspector = inputObjInspectors[0];
    outputObjInspector = rowInspector;

    int numPartitionKeys = conf.getPartitionKeyColumns().size();
    List<ExprNodeDesc> keyColumns = conf.getKeyColumns().subList(numPartitionKeys, conf.getKeyColumns().size());
    String columnSortOrder = conf.getColumnSortOrder().substring(numPartitionKeys);
    String nullSortOrder = conf.getNullOrder().substring(numPartitionKeys);

    // init keyFields
    ObjectInspector[] keyObjectInspectors = new ObjectInspector[keyColumns.size()];
    ObjectInspector[] currentKeyObjectInspectors = new ObjectInspector[keyColumns.size()];
    keyWrapper = initObjectInspectors(hconf, keyColumns, rowInspector, keyObjectInspectors, currentKeyObjectInspectors);
    ObjectInspector[] partitionKeyObjectInspectors = new ObjectInspector[numPartitionKeys];
    ObjectInspector[] partitionCurrentKeyObjectInspectors = new ObjectInspector[numPartitionKeys];
    partitionKeyWrapper = initObjectInspectors(hconf, conf.getPartitionKeyColumns(), rowInspector,
            partitionKeyObjectInspectors, partitionCurrentKeyObjectInspectors);

    keyWrapperComparator = new KeyWrapperComparator(
            keyObjectInspectors, currentKeyObjectInspectors, columnSortOrder, nullSortOrder);

    this.topNKeyFilters = new HashMap<>();
    this.disabledPartitions = new HashSet<>();
  }

  private KeyWrapper initObjectInspectors(Configuration hconf,
                                    List<ExprNodeDesc> keyColumns,
                                    ObjectInspector rowInspector,
                                    ObjectInspector[] keyObjectInspectors,
                                    ObjectInspector[] currentKeyObjectInspectors) throws HiveException {
    ExprNodeEvaluator[] keyFields = new ExprNodeEvaluator[keyColumns.size()];
    for (int i = 0; i < keyColumns.size(); i++) {
      ExprNodeDesc key = keyColumns.get(i);
      keyFields[i] = ExprNodeEvaluatorFactory.get(key, hconf);
      keyObjectInspectors[i] = keyFields[i].initialize(rowInspector);
      currentKeyObjectInspectors[i] = ObjectInspectorUtils.getStandardObjectInspector(keyObjectInspectors[i],
              ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
    }

    KeyWrapperFactory keyWrapperFactory = new KeyWrapperFactory(keyFields, keyObjectInspectors,
            currentKeyObjectInspectors);
    return keyWrapperFactory.getKeyWrapper();
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    if (!disabledPartitions.isEmpty() && disabledPartitions.size() == topNKeyFilters.size()) { // all filters are disabled due to efficiency check
      forward(row, outputObjInspector);
      return;
    }

    partitionKeyWrapper.getNewKey(row, inputObjInspectors[tag]);
    partitionKeyWrapper.setHashKey();

    if (disabledPartitions.contains(partitionKeyWrapper)) { // filter for this partition is disabled
      forward(row, outputObjInspector);
      return;
    }

    TopNKeyFilter topNKeyFilter = topNKeyFilters.get(partitionKeyWrapper);
    if (topNKeyFilter == null && topNKeyFilters.size() < conf.getMaxNumberOfPartitions()) {
      topNKeyFilter = new TopNKeyFilter(conf.getTopN(), keyWrapperComparator);
      topNKeyFilters.put(partitionKeyWrapper.copyKey(), topNKeyFilter);
    }
    if (topNKeyFilter == null) {
      forward(row, outputObjInspector);
    } else {
      keyWrapper.getNewKey(row, inputObjInspectors[tag]);
      keyWrapper.setHashKey();
      if (topNKeyFilter.canForward(keyWrapper)) {
        forward(row, outputObjInspector);
      }
    }

    if (runTimeNumRows % conf.getCheckEfficiencyNumRows() == 0) { // check the efficiency at every nth rows
      checkTopNFilterEfficiency(
        topNKeyFilters, disabledPartitions, conf.getEfficiencyThreshold(), LOG, conf.getCheckEfficiencyNumRows());
    }
  }

  @Override
  protected final void closeOp(boolean abort) throws HiveException {
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

  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "TNK";
  }

  @Override
  public OperatorType getType() {
    return TOPNKEY;
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
