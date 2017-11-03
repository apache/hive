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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.exec.tez.ReduceRecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.exec.tez.InterruptibleProcessing;
import org.apache.hadoop.hive.ql.exec.tez.RecordSource;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
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
  private static final Logger LOG = LoggerFactory.getLogger(CommonMergeJoinOperator.class.getName());
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
  transient WritableComparator[][] keyComparators;

  transient List<Operator<? extends OperatorDesc>> originalParents =
      new ArrayList<Operator<? extends OperatorDesc>>();
  transient Set<Integer> fetchInputAtClose;

  // A field because we cannot multi-inherit.
  transient InterruptibleProcessing interruptChecker;

  /** Kryo ctor. */
  protected CommonMergeJoinOperator() {
    super();
  }

  public CommonMergeJoinOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    firstFetchHappened = false;
    fetchInputAtClose = getFetchInputAtCloseList();

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
    keyComparators = new WritableComparator[maxAlias][];

    for (Entry<Byte, List<ExprNodeDesc>> entry : conf.getKeys().entrySet()) {
      keyComparators[entry.getKey().intValue()] = new WritableComparator[entry.getValue().size()];
    }

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
        if ((parentOperators != null) && !parentOperators.isEmpty()
            && (parentOperators.get(pos) instanceof TezDummyStoreOperator)) {
          TezDummyStoreOperator dummyStoreOp = (TezDummyStoreOperator) parentOperators.get(pos);
          fetchDone[pos] = dummyStoreOp.getFetchDone();
        } else {
          fetchDone[pos] = false;
        }
      }
      foundNextKeyGroup[pos] = false;
    }

    sources = ((TezContext) MapredContext.get()).getRecordSources();
    interruptChecker = new InterruptibleProcessing();

    if (sources[0] instanceof ReduceRecordSource &&
        parentOperators != null && !parentOperators.isEmpty()) {
      // Tell ReduceRecordSource to flush last record as this is a reduce
      // side SMB
      for (RecordSource source : sources) {
        ((ReduceRecordSource) source).setFlushLastRecord(true);
      }
    }
  }

  /*
   * In case of outer joins, we need to push records through even if one of the sides is done
   * sending records. For e.g. In the case of full outer join, the right side needs to send in data
   * for the join even after the left side has completed sending all the records on its side. This
   * can be done once at initialize time and at close, these tags will still forward records until
   * they have no more to send. Also, subsequent joins need to fetch their data as well since
   * any join following the outer join could produce results with one of the outer sides depending on
   * the join condition. We could optimize for the case of inner joins in the future here.
   */
  private Set<Integer> getFetchInputAtCloseList() {
    Set<Integer> retval = new TreeSet<Integer>();
    for (JoinCondDesc joinCondDesc : conf.getConds()) {
      retval.add(joinCondDesc.getLeft());
      retval.add(joinCondDesc.getRight());
    }

    return retval;
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
  public void process(Object row, int tag) throws HiveException {
    posBigTable = (byte) conf.getBigTablePosition();

    byte alias = (byte) tag;
    List<Object> value = getFilteredValue(alias, row);
    // compute keys and values as StandardObjects
    List<Object> key = mergeJoinComputeKeys(row, alias);
    // Fetch the first group for all small table aliases.
    doFirstFetchIfNeeded();

    //have we reached a new key group?
    boolean nextKeyGroup = processKey(alias, key);
    if (nextKeyGroup) {
      //assert this.nextGroupStorage[alias].size() == 0;
      this.nextGroupStorage[alias].addRow(value);
      foundNextKeyGroup[tag] = true;
      if (tag != posBigTable) {
        return;
      }
    } else {
      if ((tag == posBigTable) && (candidateStorage[tag].rowCount() == joinEmitInterval)) {
        boolean canEmit = true;
        for (byte i = 0; i < foundNextKeyGroup.length; i++) {
          if (i == posBigTable) {
            continue;
          }

          if (!foundNextKeyGroup[i]) {
            canEmit = false;
            break;
          }

          if (compareKeys(i, key, keyWritables[i]) != 0) {
            canEmit = false;
            break;
          }
        }
        // we can save ourselves from spilling once we have join emit interval worth of rows.
        if (canEmit) {
          LOG.info("We are emitting rows since we hit the join emit interval of "
              + joinEmitInterval);
          joinOneGroup(false);
          candidateStorage[tag].clearRows();
          storage[tag].clearRows();
        }
      }
    }

    reportProgress();
    numMapRowsRead++;

    // the big table has reached a new key group. try to let the small tables
    // catch up with the big table.
    if (nextKeyGroup) {
      assert tag == posBigTable;
      List<Byte> listOfFetchNeeded = null;
      do {
        listOfFetchNeeded = joinOneGroup();
        //jump out the loop if we need input from the big table
      } while (listOfFetchNeeded != null && listOfFetchNeeded.size() > 0
          && !listOfFetchNeeded.contains(this.posBigTable));
      return;
    }

    assert !nextKeyGroup;
    candidateStorage[tag].addRow(value);

  }

  private List<Byte> joinOneGroup() throws HiveException {
    return joinOneGroup(true);
  }

  private List<Byte> joinOneGroup(boolean clear) throws HiveException {
    int[] smallestPos = findSmallestKey();
    List<Byte> listOfNeedFetchNext = null;
    if (smallestPos != null) {
      listOfNeedFetchNext = joinObject(smallestPos, clear);
      if ((listOfNeedFetchNext.size() > 0) && clear) {
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

  private List<Byte> joinObject(int[] smallestPos, boolean clear) throws HiveException {
    List<Byte> needFetchList = new ArrayList<Byte>();
    byte index = (byte) (smallestPos.length - 1);
    for (; index >= 0; index--) {
      if (smallestPos[index] > 0 || keyWritables[index] == null) {
        putDummyOrEmpty(index);
        continue;
      }
      storage[index] = candidateStorage[index];
      if (clear) {
        needFetchList.add(index);
      }
      if (smallestPos[index] < 0) {
        break;
      }
    }
    for (index--; index >= 0; index--) {
      putDummyOrEmpty(index);
    }
    checkAndGenObject();
    if (clear) {
      for (Byte pos : needFetchList) {
        this.candidateStorage[pos].clearRows();
        this.keyWritables[pos] = null;
      }
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
      result[pos] = compareKeys(pos, key, smallestOne);
      if (result[pos] < 0) {
        smallestOne = key;
      }
    }
    return smallestOne == null ? null : result;
  }

  private void fetchNextGroup(Byte t) throws HiveException {
    if (keyWritables[t] != null) {
      return; // First process the current key.
    }
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
    interruptChecker.startAbortChecks(); // Reset the time, we only want to count it in the loop.
    while (!foundNextKeyGroup[t]) {
      if (fetchDone[t]) {
        break;
      }
      fetchOneRow(t);
      try {
        interruptChecker.addRowAndMaybeCheckAbort();
      } catch (InterruptedException e) {
        throw new HiveException(e);
      }
    }
    if (!foundNextKeyGroup[t] && fetchDone[t]) {
      this.nextKeyWritables[t] = null;
    }
  }
  
  @Override
  public void close(boolean abort) throws HiveException {
    joinFinalLeftData(); // Do this WITHOUT checking for parents
    super.close(abort);
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    super.closeOp(abort);

    // clean up
    LOG.debug("Cleaning up the operator state");
    for (int pos = 0; pos < order.length; pos++) {
      if (pos != posBigTable) {
        fetchDone[pos] = false;
      }
      foundNextKeyGroup[pos] = false;
    }
  }

  private void fetchOneRow(byte tag) throws HiveException {
    try {
      boolean hasMore = sources[tag].pushRecord();
      if (fetchDone[tag] && hasMore) {
        LOG.warn("fetchDone[" + tag + "] was set to true (by a recursive call) and will be reset");
      }// TODO: "else {"? This happened in the past due to a bug, see HIVE-11016.
        fetchDone[tag] = !hasMore;
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
      // if we are in close op phase, we have definitely exhausted the big table input
      fetchDone[posBigTable] = true;
      // First, handle the condition where the first fetch was never done (big table is empty).
      doFirstFetchIfNeeded();
      // in case of outer joins, we need to pull in records from the sides we still
      // need to produce output for apart from the big table. for e.g. full outer join
      // TODO: this reproduces the logic of the loop that was here before, assuming
      // firstFetchHappened == true. In reality it almost always calls joinOneGroup. Fix it?
      int lastPos = (fetchDone.length - 1);
      if (posBigTable != lastPos
          && (fetchInputAtClose.contains(lastPos)) && (fetchDone[lastPos] == false)) {
        // Do the join. It does fetching of next row groups itself.
        ret = joinOneGroup();
      }

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
          fetchNextGroup(pos);
        }
      }
      joinOneGroup();
      dataInCache = false;
      for (byte pos = 0; pos < order.length; pos++) {
        if (candidateStorage[pos] == null) {
          continue;
        }
        if (this.candidateStorage[pos].hasRows()) {
          dataInCache = true;
          break;
        }
      }
    }
  }

  private void doFirstFetchIfNeeded() throws HiveException {
    if (firstFetchHappened) return;
    firstFetchHappened = true;
    for (byte pos = 0; pos < order.length; pos++) {
      if (pos != posBigTable) {
        fetchNextGroup(pos);
      }
    }
  }

  private boolean allFetchDone() {
    for (byte pos = 0; pos < order.length; pos++) {
      if (pos != posBigTable && !fetchDone[pos]) return false;
    }
    return true;
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
      keyComparators[alias] = new WritableComparator[key.size()];
      return false;
    } else {
      int cmp = compareKeys(alias, key, keyWritable);
      if (cmp != 0) {
        // Cant overwrite existing keys
        if (nextKeyWritables[alias] != null) {
          throw new HiveException("Attempting to overwrite nextKeyWritables[" + alias + "]");
        }
        nextKeyWritables[alias] = key;
        return true;
      }
      return false;
    }
  }

  @SuppressWarnings("rawtypes")
  private int compareKeys(byte alias, List<Object> k1, List<Object> k2) {
    final WritableComparator[] comparators = keyComparators[alias];

    // join keys have difference sizes?
    if (k1.size() != k2.size()) {
      return k1.size() - k2.size();
    }

    if (comparators.length == 0) {
      // cross-product - no keys really
      return 0;
    }

    if (comparators.length > 1) {
      // rare case
      return compareKeysMany(comparators, k1, k2);
    } else {
      return compareKey(comparators, 0,
          (WritableComparable) k1.get(0),
          (WritableComparable) k2.get(0),
          nullsafes != null ? nullsafes[0]: false);
    }
  }

  @SuppressWarnings("rawtypes")
  private int compareKeysMany(WritableComparator[] comparators,
      final List<Object> k1,
      final List<Object> k2) {
    // invariant: k1.size == k2.size
    int ret = 0;
    final int size = k1.size();
    for (int i = 0; i < size; i++) {
      WritableComparable key_1 = (WritableComparable) k1.get(i);
      WritableComparable key_2 = (WritableComparable) k2.get(i);
      ret = compareKey(comparators, i, key_1, key_2,
          nullsafes != null ? nullsafes[i] : false);
      if (ret != 0) {
        return ret;
      }
    }
    return ret;
  }

  @SuppressWarnings("rawtypes")
  private int compareKey(final WritableComparator comparators[], final int pos,
      final WritableComparable key_1,
      final WritableComparable key_2,
      final boolean nullsafe) {

    if (key_1 == null && key_2 == null) {
      if (nullsafe) {
        return 0;
      } else {
        return -1;
      }
    } else if (key_1 == null) {
      return -1;
    } else if (key_2 == null) {
      return 1;
    }

    if (comparators[pos] == null) {
      comparators[pos] = WritableComparator.get(key_1.getClass());
    }
    return comparators[pos].compare(key_1, key_2);
  }

  @SuppressWarnings("unchecked")
  private List<Object> mergeJoinComputeKeys(Object row, Byte alias) throws HiveException {
    if ((joinKeysObjectInspectors != null) && (joinKeysObjectInspectors[alias] != null)) {
      return JoinUtil.computeKeys(row, joinKeys[alias], joinKeysObjectInspectors[alias]);
    } else {
      final List<Object> key = new ArrayList<Object>(1);
      ObjectInspectorUtils.partialCopyToStandardObject(key, row,
          Utilities.ReduceField.KEY.position, 1, (StructObjectInspector) inputObjInspectors[alias],
          ObjectInspectorCopyOption.WRITABLE);
      return (List<Object>) key.get(0); // this is always 0, even if KEY.position is not 
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

    if (parentOperators.size() == 1) {
      Map<Integer, DummyStoreOperator> dummyOps =
          ((TezContext) (MapredContext.get())).getDummyOpsMap();
      for (Entry<Integer, DummyStoreOperator> connectOp : dummyOps.entrySet()) {
        if (connectOp.getValue().getChildOperators() == null
            || connectOp.getValue().getChildOperators().isEmpty()) {
          parentOperators.add(connectOp.getKey(), connectOp.getValue());
          connectOp.getValue().getChildOperators().add(this);
        }
      }
    }
    super.initializeLocalWork(hconf);
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
