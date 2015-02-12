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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.exec.tez.RecordSource;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * With an aim to consolidate the join algorithms to either hash based joins (MapJoinOperator) or
 * sort-merge based joins, this operator is being introduced. This operator executes a sort-merge
 * based algorithm. It replaces both the JoinOperator and the SMBMapJoinOperator for the tez side of
 * things. It works in either the map phase or reduce phase.
 *
 * The basic algorithm is as follows:
 *
 * 1. The processOp receives a row from a "big" table.
 * 2. In order to process it, the operator does a fetch for rows from the other tables.
 * 3. Once we have a set of rows from the other tables (till we hit a new key), more rows are
 *    brought in from the big table and a join is performed.
 */

public class CommonMergeJoinOperator extends AbstractMapJoinOperator<CommonMergeJoinDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;
  private boolean isBigTableWork;
  private static final Log LOG = LogFactory.getLog(CommonMergeJoinOperator.class.getName());
  transient List<Object>[] keyWritables;
  transient List<Object>[] nextKeyWritables;
  transient RowContainer<List<Object>>[] nextGroupStorage;
  transient RowContainer<List<Object>>[] candidateStorage;

  transient String[] tagToAlias;
  private transient boolean[] fetchDone;
  private transient boolean[] foundNextKeyGroup;
  transient boolean firstFetchHappened = false;
  transient boolean localWorkInited = false;
  transient boolean initDone = false;
  transient List<Object> otherKey = null;
  transient List<Object> values = null;
  transient RecordSource[] sources;
  transient List<Operator<? extends OperatorDesc>> originalParents =
      new ArrayList<Operator<? extends OperatorDesc>>();

  public CommonMergeJoinOperator() {
    super();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    firstFetchHappened = false;
    initializeChildren(hconf);
    int maxAlias = 0;
    for (byte pos = 0; pos < order.length; pos++) {
      if (pos > maxAlias) {
        maxAlias = pos;
      }
    }
    maxAlias += 1;

    nextGroupStorage = new RowContainer[maxAlias];
    candidateStorage = new RowContainer[maxAlias];
    keyWritables = new ArrayList[maxAlias];
    nextKeyWritables = new ArrayList[maxAlias];
    fetchDone = new boolean[maxAlias];
    foundNextKeyGroup = new boolean[maxAlias];

    int bucketSize;

    int oldVar = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);
    if (oldVar != 100) {
      bucketSize = oldVar;
    } else {
      bucketSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVESMBJOINCACHEROWS);
    }

    for (byte pos = 0; pos < order.length; pos++) {
      RowContainer<List<Object>> rc =
          JoinUtil.getRowContainer(hconf, rowContainerStandardObjectInspectors[pos], pos,
              bucketSize, spillTableDesc, conf, !hasFilter(pos), reporter);
      nextGroupStorage[pos] = rc;
      RowContainer<List<Object>> candidateRC =
          JoinUtil.getRowContainer(hconf, rowContainerStandardObjectInspectors[pos], pos,
              bucketSize, spillTableDesc, conf, !hasFilter(pos), reporter);
      candidateStorage[pos] = candidateRC;
    }

    for (byte pos = 0; pos < order.length; pos++) {
      if (pos != posBigTable) {
        fetchDone[pos] = false;
      }
      foundNextKeyGroup[pos] = false;
    }

    sources = ((TezContext) MapredContext.get()).getRecordSources();
  }

  @Override
  public void endGroup() throws HiveException {
    // we do not want the end group to cause a checkAndGenObject
    defaultEndGroup();
  }

  @Override
  public void startGroup() throws HiveException {
    // we do not want the start group to clear the storage
    defaultStartGroup();
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.exec.Operator#processOp(java.lang.Object,
   * int) this processor has a push-pull model. First call to this method is a
   * push but the rest is pulled until we run out of records.
   */
  @Override
  public void processOp(Object row, int tag) throws HiveException {
    posBigTable = (byte) conf.getBigTablePosition();

    byte alias = (byte) tag;
    List<Object> value = getFilteredValue(alias, row);
    // compute keys and values as StandardObjects
    List<Object> key = mergeJoinComputeKeys(row, alias);

    if (!firstFetchHappened) {
      firstFetchHappened = true;
      // fetch the first group for all small table aliases
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != posBigTable) {
          fetchNextGroup(pos);
        }
      }
    }

    //have we reached a new key group?
    boolean nextKeyGroup = processKey(alias, key);
    if (nextKeyGroup) {
      //assert this.nextGroupStorage[alias].size() == 0;
      this.nextGroupStorage[alias].addRow(value);
      foundNextKeyGroup[tag] = true;
      if (tag != posBigTable) {
        return;
      }
    }

    reportProgress();
    numMapRowsRead++;

    // the big table has reached a new key group. try to let the small tables
    // catch up with the big table.
    if (nextKeyGroup) {
      assert tag == posBigTable;
      List<Byte> smallestPos = null;
      do {
        smallestPos = joinOneGroup();
        //jump out the loop if we need input from the big table
      } while (smallestPos != null && smallestPos.size() > 0
          && !smallestPos.contains(this.posBigTable));

      return;
    }

    assert !nextKeyGroup;
    candidateStorage[tag].addRow(value);

  }

  private List<Byte> joinOneGroup() throws HiveException {
    int[] smallestPos = findSmallestKey();
    List<Byte> listOfNeedFetchNext = null;
    if (smallestPos != null) {
      listOfNeedFetchNext = joinObject(smallestPos);
      if (listOfNeedFetchNext.size() > 0) {
        // listOfNeedFetchNext contains all tables that we have joined data in their
        // candidateStorage, and we need to clear candidate storage and promote their
        // nextGroupStorage to candidateStorage and fetch data until we reach a
        // new group.
        for (Byte b : listOfNeedFetchNext) {
          try {
            fetchNextGroup(b);
          } catch (Exception e) {
            throw new HiveException(e);
          }
        }
      }
    }
    return listOfNeedFetchNext;
  }

  private List<Byte> joinObject(int[] smallestPos) throws HiveException {
    List<Byte> needFetchList = new ArrayList<Byte>();
    byte index = (byte) (smallestPos.length - 1);
    for (; index >= 0; index--) {
      if (smallestPos[index] > 0 || keyWritables[index] == null) {
        putDummyOrEmpty(index);
        continue;
      }
      storage[index] = candidateStorage[index];
      needFetchList.add(index);
      if (smallestPos[index] < 0) {
        break;
      }
    }
    for (index--; index >= 0; index--) {
      putDummyOrEmpty(index);
    }
    checkAndGenObject();
    for (Byte pos : needFetchList) {
      this.candidateStorage[pos].clearRows();
      this.keyWritables[pos] = null;
    }
    return needFetchList;
  }

  private void putDummyOrEmpty(Byte i) {
    // put a empty list or null
    if (noOuterJoin) {
      storage[i] = emptyList;
    } else {
      storage[i] = dummyObjVectors[i];
    }
  }

  private int[] findSmallestKey() {
    int[] result = new int[order.length];
    List<Object> smallestOne = null;

    for (byte pos = 0; pos < order.length; pos++) {
      List<Object> key = keyWritables[pos];
      if (key == null) {
        continue;
      }
      if (smallestOne == null) {
        smallestOne = key;
        result[pos] = -1;
        continue;
      }
      result[pos] = compareKeys(key, smallestOne);
      if (result[pos] < 0) {
        smallestOne = key;
      }
    }
    return smallestOne == null ? null : result;
  }

  private void fetchNextGroup(Byte t) throws HiveException {
    if (foundNextKeyGroup[t]) {
      // first promote the next group to be the current group if we reached a
      // new group in the previous fetch
      if (this.nextKeyWritables[t] != null) {
        promoteNextGroupToCandidate(t);
      } else {
        this.keyWritables[t] = null;
        this.candidateStorage[t] = null;
        this.nextGroupStorage[t] = null;
      }
      foundNextKeyGroup[t] = false;
    }
    // for the big table, we only need to promote the next group to the current group.
    if (t == posBigTable) {
      return;
    }

    // for tables other than the big table, we need to fetch more data until reach a new group or
    // done.
    while (!foundNextKeyGroup[t]) {
      if (fetchDone[t]) {
        break;
      }
      fetchOneRow(t);
    }
    if (!foundNextKeyGroup[t] && fetchDone[t]) {
      this.nextKeyWritables[t] = null;
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    joinFinalLeftData();

    super.closeOp(abort);

    // clean up
    for (int pos = 0; pos < order.length; pos++) {
      if (pos != posBigTable) {
        fetchDone[pos] = false;
      }
      foundNextKeyGroup[pos] = false;
    }
  }

  private void fetchOneRow(byte tag) throws HiveException {
    try {
      fetchDone[tag] = !sources[tag].pushRecord();
      if (sources[tag].isGrouped()) {
        // instead of maintaining complex state for the fetch of the next group,
        // we know for sure that at the end of all the values for a given key,
        // we will definitely reach the next key group.
        foundNextKeyGroup[tag] = true;
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void joinFinalLeftData() throws HiveException {
    @SuppressWarnings("rawtypes")
    RowContainer bigTblRowContainer = this.candidateStorage[this.posBigTable];

    boolean allFetchDone = allFetchDone();
    // if all left data in small tables are less than and equal to the left data
    // in big table, let's them catch up
    while (bigTblRowContainer != null && bigTblRowContainer.rowCount() > 0 && !allFetchDone) {
      joinOneGroup();
      bigTblRowContainer = this.candidateStorage[this.posBigTable];
      allFetchDone = allFetchDone();
    }

    while (!allFetchDone) {
      List<Byte> ret = joinOneGroup();
      if (ret == null || ret.size() == 0) {
        break;
      }
      reportProgress();
      numMapRowsRead++;
      allFetchDone = allFetchDone();
    }

    boolean dataInCache = true;
    while (dataInCache) {
      for (byte pos = 0; pos < order.length; pos++) {
        if (this.foundNextKeyGroup[pos] && this.nextKeyWritables[pos] != null) {
          promoteNextGroupToCandidate(pos);
        }
      }
      joinOneGroup();
      dataInCache = false;
      for (byte pos = 0; pos < order.length; pos++) {
        if (candidateStorage[pos] == null) {
          continue;
        }
        if (this.candidateStorage[pos].rowCount() > 0) {
          dataInCache = true;
          break;
        }
      }
    }
  }

  private boolean allFetchDone() {
    boolean allFetchDone = true;
    for (byte pos = 0; pos < order.length; pos++) {
      if (pos == posBigTable) {
        continue;
      }
      allFetchDone = allFetchDone && fetchDone[pos];
    }
    return allFetchDone;
  }

  private void promoteNextGroupToCandidate(Byte t) throws HiveException {
    this.keyWritables[t] = this.nextKeyWritables[t];
    this.nextKeyWritables[t] = null;
    RowContainer<List<Object>> oldRowContainer = this.candidateStorage[t];
    oldRowContainer.clearRows();
    this.candidateStorage[t] = this.nextGroupStorage[t];
    this.nextGroupStorage[t] = oldRowContainer;
  }

  private boolean processKey(byte alias, List<Object> key) throws HiveException {
    List<Object> keyWritable = keyWritables[alias];
    if (keyWritable == null) {
      // the first group.
      keyWritables[alias] = key;
      return false;
    } else {
      int cmp = compareKeys(key, keyWritable);
      if (cmp != 0) {
        nextKeyWritables[alias] = key;
        return true;
      }
      return false;
    }
  }

  @SuppressWarnings("rawtypes")
  private int compareKeys(List<Object> k1, List<Object> k2) {
    int ret = 0;

    // join keys have difference sizes?
    ret = k1.size() - k2.size();
    if (ret != 0) {
      return ret;
    }

    for (int i = 0; i < k1.size(); i++) {
      WritableComparable key_1 = (WritableComparable) k1.get(i);
      WritableComparable key_2 = (WritableComparable) k2.get(i);
      if (key_1 == null && key_2 == null) {
        if (nullsafes != null && nullsafes[i]) {
          continue;
        } else {
          return -1;
        }
      } else if (key_1 == null) {
        return -1;
      } else if (key_2 == null) {
        return 1;
      }
      ret = WritableComparator.get(key_1.getClass()).compare(key_1, key_2);
      if (ret != 0) {
        return ret;
      }
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  private List<Object> mergeJoinComputeKeys(Object row, Byte alias) throws HiveException {
    if ((joinKeysObjectInspectors != null) && (joinKeysObjectInspectors[alias] != null)) {
      return JoinUtil.computeKeys(row, joinKeys[alias], joinKeysObjectInspectors[alias]);
    } else {
      row =
          ObjectInspectorUtils.copyToStandardObject(row, inputObjInspectors[alias],
              ObjectInspectorCopyOption.WRITABLE);
      StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[alias];
      StructField sf = soi.getStructFieldRef(Utilities.ReduceField.KEY.toString());
      return (List<Object>) soi.getStructFieldData(row, sf);
    }
  }

  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "MERGEJOIN";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MERGEJOIN;
  }

  @Override
  public void initializeLocalWork(Configuration hconf) throws HiveException {
    Operator<? extends OperatorDesc> parent = null;

    for (Operator<? extends OperatorDesc> parentOp : parentOperators) {
      if (parentOp != null) {
        parent = parentOp;
        break;
      }
    }

    if (parent == null) {
      throw new HiveException("No valid parents.");
    }
    Map<Integer, DummyStoreOperator> dummyOps = parent.getTagToOperatorTree();
    for (Entry<Integer, DummyStoreOperator> connectOp : dummyOps.entrySet()) {
      parentOperators.add(connectOp.getKey(), connectOp.getValue());
      connectOp.getValue().getChildOperators().add(this);
    }
    super.initializeLocalWork(hconf);
    return;
  }

  public boolean isBigTableWork() {
    return isBigTableWork;
  }

  public void setIsBigTableWork(boolean bigTableWork) {
    this.isBigTableWork = bigTableWork;
  }

  public int getTagForOperator(Operator<? extends OperatorDesc> op) {
    return originalParents.indexOf(op);
  }

  public void cloneOriginalParentsList(List<Operator<? extends OperatorDesc>> opList) {
    originalParents.addAll(opList);
  }
}
