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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.PriorityQueue;

import static org.apache.hadoop.hive.ql.plan.api.OperatorType.TOPNKEY;

/**
 * TopNKeyOperator passes rows that contains top N keys only.
 */
public class TopNKeyOperator extends Operator<TopNKeyDesc> implements Serializable {

  private static final long serialVersionUID = 1L;

  // Maximum number of keys to hold
  private transient int topN;

  // Priority queue that holds occurred keys
  private transient PriorityQueue<KeyWrapper> priorityQueue;

  private transient KeyWrapper keyWrapper;

  /** Kryo ctor. */
  public TopNKeyOperator() {
    super();
  }

  public TopNKeyOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public static class KeyWrapperComparator implements Comparator<KeyWrapper> {
    private ObjectInspector[] objectInspectors1;
    private ObjectInspector[] objectInspectors2;
    private boolean[] columnSortOrderIsDesc;

    public KeyWrapperComparator(ObjectInspector[] objectInspectors1, ObjectInspector[]
        objectInspectors2, boolean[] columnSortOrderIsDesc) {
      this.objectInspectors1 = objectInspectors1;
      this.objectInspectors2 = objectInspectors2;
      this.columnSortOrderIsDesc = columnSortOrderIsDesc;
    }

    @Override
    public int compare(KeyWrapper key1, KeyWrapper key2) {
      return ObjectInspectorUtils.compare(key1.getKeyArray(), objectInspectors1,
          key2.getKeyArray(), objectInspectors2, columnSortOrderIsDesc);
    }
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    this.topN = conf.getTopN();

    String columnSortOrder = conf.getColumnSortOrder();
    boolean[] columnSortOrderIsDesc = new boolean[columnSortOrder.length()];
    for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
      columnSortOrderIsDesc[i] = (columnSortOrder.charAt(i) == '-');
    }

    ObjectInspector rowInspector = inputObjInspectors[0];
    ObjectInspector standardObjInspector = ObjectInspectorUtils.getStandardObjectInspector(rowInspector);
    outputObjInspector = rowInspector;

    // init keyFields
    int numKeys = conf.getKeyColumns().size();
    ExprNodeEvaluator[] keyFields = new ExprNodeEvaluator[numKeys];
    ObjectInspector[] keyObjectInspectors = new ObjectInspector[numKeys];
    ExprNodeEvaluator[] standardKeyFields = new ExprNodeEvaluator[numKeys];
    ObjectInspector[] standardKeyObjectInspectors = new ObjectInspector[numKeys];

    for (int i = 0; i < numKeys; i++) {
      ExprNodeDesc key = conf.getKeyColumns().get(i);
      keyFields[i] = ExprNodeEvaluatorFactory.get(key, hconf);
      keyObjectInspectors[i] = keyFields[i].initialize(rowInspector);
      standardKeyFields[i] = ExprNodeEvaluatorFactory.get(key, hconf);
      standardKeyObjectInspectors[i] = standardKeyFields[i].initialize(standardObjInspector);
    }

    priorityQueue = new PriorityQueue<>(topN + 1, new TopNKeyOperator.KeyWrapperComparator(
        standardKeyObjectInspectors, standardKeyObjectInspectors, columnSortOrderIsDesc));

    KeyWrapperFactory keyWrapperFactory = new KeyWrapperFactory(keyFields, keyObjectInspectors,
        standardKeyObjectInspectors);
    keyWrapper = keyWrapperFactory.getKeyWrapper();
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    if (canProcess(row, tag)) {
      forward(row, outputObjInspector);
    }
  }

  protected boolean canProcess(Object row, int tag) throws HiveException {
    keyWrapper.getNewKey(row, inputObjInspectors[tag]);
    keyWrapper.setHashKey();

    if (!priorityQueue.contains(keyWrapper)) {
      priorityQueue.offer(keyWrapper.copyKey());
    }
    if (priorityQueue.size() > topN) {
      priorityQueue.poll();
    }

    return priorityQueue.contains(keyWrapper);
  }

  @Override
  protected final void closeOp(boolean abort) throws HiveException {
    priorityQueue.clear();
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
