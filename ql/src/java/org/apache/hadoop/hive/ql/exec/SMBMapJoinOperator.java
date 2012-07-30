/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Sorted Merge Map Join Operator.
 */
public class SMBMapJoinOperator extends AbstractMapJoinOperator<SMBJoinDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  private static final Log LOG = LogFactory.getLog(SMBMapJoinOperator.class
      .getName());

  private MapredLocalWork localWork = null;
  private Map<String, FetchOperator> fetchOperators;
  transient ArrayList<Object>[] keyWritables;
  transient ArrayList<Object>[] nextKeyWritables;
  RowContainer<ArrayList<Object>>[] nextGroupStorage;
  RowContainer<ArrayList<Object>>[] candidateStorage;

  transient Map<Byte, String> tagToAlias;
  private transient boolean[] fetchOpDone;
  private transient boolean[] foundNextKeyGroup;
  transient boolean firstFetchHappened = false;
  private transient boolean inputFileChanged = false;
  transient boolean localWorkInited = false;

  public SMBMapJoinOperator() {
  }

  public SMBMapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mapJoinOp) {
    super(mapJoinOp);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    firstRow = true;

    closeCalled = false;

    this.firstFetchHappened = false;
    this.inputFileChanged = false;

    // get the largest table alias from order
    int maxAlias = 0;
    for (Byte alias: order) {
      if (alias > maxAlias) {
        maxAlias = alias;
      }
    }
    maxAlias += 1;

    nextGroupStorage = new RowContainer[maxAlias];
    candidateStorage = new RowContainer[maxAlias];
    keyWritables = new ArrayList[maxAlias];
    nextKeyWritables = new ArrayList[maxAlias];
    fetchOpDone = new boolean[maxAlias];
    foundNextKeyGroup = new boolean[maxAlias];

    int bucketSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);
    byte storePos = (byte) 0;
    for (Byte alias : order) {
      RowContainer rc = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors.get(storePos),
          alias, bucketSize,spillTableDesc, conf,noOuterJoin);
      nextGroupStorage[storePos] = rc;
      RowContainer candidateRC = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors.get((byte)storePos),
          alias,bucketSize,spillTableDesc, conf,noOuterJoin);
      candidateStorage[alias] = candidateRC;
      storePos++;
    }
    tagToAlias = conf.getTagToAlias();

    for (Byte alias : order) {
      if(alias != (byte) posBigTable) {
        fetchOpDone[alias] = false;
      }
      foundNextKeyGroup[alias] = false;
    }
  }

  @Override
  public void initializeLocalWork(Configuration hconf) throws HiveException {
    initializeMapredLocalWork(this.getConf(), hconf, this.getConf().getLocalWork(), LOG);
    super.initializeLocalWork(hconf);
  }

  public void initializeMapredLocalWork(MapJoinDesc conf, Configuration hconf,
      MapredLocalWork localWork, Log l4j) throws HiveException {
    if (localWork == null || localWorkInited) {
      return;
    }
    localWorkInited = true;
    this.localWork = localWork;
    fetchOperators = new HashMap<String, FetchOperator>();

    Map<FetchOperator, JobConf> fetchOpJobConfMap = new HashMap<FetchOperator, JobConf>();
    // create map local operators
    for (Map.Entry<String, FetchWork> entry : localWork.getAliasToFetchWork()
        .entrySet()) {
      JobConf jobClone = new JobConf(hconf);
      Operator<? extends Serializable> tableScan = localWork.getAliasToWork()
      .get(entry.getKey());
      if(tableScan instanceof TableScanOperator) {
        ArrayList<Integer> list = ((TableScanOperator)tableScan).getNeededColumnIDs();
        if (list != null) {
          ColumnProjectionUtils.appendReadColumnIDs(jobClone, list);
        }
      } else {
        ColumnProjectionUtils.setFullyReadColumns(jobClone);
      }
      FetchOperator fetchOp = new FetchOperator(entry.getValue(),jobClone);
      fetchOpJobConfMap.put(fetchOp, jobClone);
      fetchOperators.put(entry.getKey(), fetchOp);
      l4j.info("fetchoperator for " + entry.getKey() + " created");
    }

    for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
      Operator<? extends Serializable> forwardOp = localWork.getAliasToWork()
          .get(entry.getKey());
      // All the operators need to be initialized before process
      forwardOp.setExecContext(this.getExecContext());
      FetchOperator fetchOp = entry.getValue();
      JobConf jobConf = fetchOpJobConfMap.get(fetchOp);
      if (jobConf == null) {
        jobConf = this.getExecContext().getJc();
      }
      forwardOp.initialize(jobConf, new ObjectInspector[] {fetchOp.getOutputObjectInspector()});
      l4j.info("fetchoperator for " + entry.getKey() + " initialized");
    }
  }

  // The input file has changed - load the correct hash bucket
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    inputFileChanged = true;
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    if (tag == posBigTable) {
      if (inputFileChanged) {
        if (firstFetchHappened) {
          // we need to first join and flush out data left by the previous file.
          joinFinalLeftData();
        }
        // set up the fetch operator for the new input file.
        for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
          String alias = entry.getKey();
          FetchOperator fetchOp = entry.getValue();
          fetchOp.clearFetchContext();
          setUpFetchOpContext(fetchOp, alias);
        }
        firstFetchHappened = false;
        inputFileChanged = false;
      }
    }

    if (!firstFetchHappened) {
      firstFetchHappened = true;
      // fetch the first group for all small table aliases
      for (Byte t : order) {
        if(t != (byte)posBigTable) {
          fetchNextGroup(t);
        }
      }
    }

    byte alias = (byte) tag;
    // compute keys and values as StandardObjects

    // compute keys and values as StandardObjects
    ArrayList<Object> key = JoinUtil.computeKeys(row, joinKeys.get(alias),
        joinKeysObjectInspectors.get(alias));
    ArrayList<Object> value = JoinUtil.computeValues(row, joinValues.get(alias),
        joinValuesObjectInspectors.get(alias), joinFilters.get(alias),
        joinFilterObjectInspectors.get(alias), noOuterJoin);


    //have we reached a new key group?
    boolean nextKeyGroup = processKey(alias, key);
    if (nextKeyGroup) {
      //assert this.nextGroupStorage.get(alias).size() == 0;
      this.nextGroupStorage[alias].add(value);
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
      assert tag == (byte)posBigTable;
      List<Byte> smallestPos = null;
      do {
        smallestPos = joinOneGroup();
        //jump out the loop if we need input from the big table
      } while (smallestPos != null && smallestPos.size() > 0
          && !smallestPos.contains((byte)this.posBigTable));

      return;
    }

    assert !nextKeyGroup;
    candidateStorage[tag].add(value);
  }

  /*
   * this happens either when the input file of the big table is changed or in
   * closeop. It needs to fetch all the left data from the small tables and try
   * to join them.
   */
  private void joinFinalLeftData() throws HiveException {
    RowContainer bigTblRowContainer = this.candidateStorage[this.posBigTable];

    boolean allFetchOpDone = allFetchOpDone();
    // if all left data in small tables are less than and equal to the left data
    // in big table, let's them catch up
    while (bigTblRowContainer != null && bigTblRowContainer.size() > 0
        && !allFetchOpDone) {
      joinOneGroup();
      bigTblRowContainer = this.candidateStorage[this.posBigTable];
      allFetchOpDone = allFetchOpDone();
    }

    while (!allFetchOpDone) {
      List<Byte> ret = joinOneGroup();
      if (ret == null || ret.size() == 0) {
        break;
      }
      reportProgress();
      numMapRowsRead++;
      allFetchOpDone = allFetchOpDone();
    }

    boolean dataInCache = true;
    while (dataInCache) {
      for (byte t : order) {
        if (this.foundNextKeyGroup[t]
            && this.nextKeyWritables[t] != null) {
          promoteNextGroupToCandidate(t);
        }
      }
      joinOneGroup();
      dataInCache = false;
      for (byte r : order) {
        if (this.candidateStorage[r].size() > 0) {
          dataInCache = true;
          break;
        }
      }
    }
  }

  private boolean allFetchOpDone() {
    boolean allFetchOpDone = true;
    for (Byte tag : order) {
      if(tag == (byte) posBigTable) {
        continue;
      }
      allFetchOpDone = allFetchOpDone && fetchOpDone[tag];
    }
    return allFetchOpDone;
  }

  private List<Byte> joinOneGroup() throws HiveException {
    int[] smallestPos = findSmallestKey();
    List<Byte> listOfNeedFetchNext = null;
    if(smallestPos != null) {
      listOfNeedFetchNext = joinObject(smallestPos);
      if (listOfNeedFetchNext.size() > 0) {
        // listOfNeedFetchNext contains all tables that we have joined data in their
        // candidateStorage, and we need to clear candidate storage and promote their
        // nextGroupStorage to candidateStorage and fetch data until we reach a
        // new group.
        for (Byte b : listOfNeedFetchNext) {
          fetchNextGroup(b);
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
      storage.put(index, candidateStorage[index]);
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
      this.candidateStorage[pos].clear();
      this.keyWritables[pos] = null;
    }
    return needFetchList;
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
    //for the big table, we only need to promote the next group to the current group.
    if(t == (byte)posBigTable) {
      return;
    }

    //for tables other than the big table, we need to fetch more data until reach a new group or done.
    while (!foundNextKeyGroup[t]) {
      if (fetchOpDone[t]) {
        break;
      }
      fetchOneRow(t);
    }
    if (!foundNextKeyGroup[t] && fetchOpDone[t]) {
      this.nextKeyWritables[t] = null;
    }
  }

  private void promoteNextGroupToCandidate(Byte t) throws HiveException {
    this.keyWritables[t] = this.nextKeyWritables[t];
    this.nextKeyWritables[t] = null;
    RowContainer<ArrayList<Object>> oldRowContainer = this.candidateStorage[t];
    oldRowContainer.clear();
    this.candidateStorage[t] = this.nextGroupStorage[t];
    this.nextGroupStorage[t] = oldRowContainer;
  }

  private int compareKeys (ArrayList<Object> k1, ArrayList<Object> k2) {
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
        return nullsafes != null && nullsafes[i] ? 0 : -1; // just return k1 is smaller than k2
      } else if (key_1 == null) {
        return -1;
      } else if (key_2 == null) {
        return 1;
      }
      ret = WritableComparator.get(key_1.getClass()).compare(key_1, key_2);
      if(ret != 0) {
        return ret;
      }
    }
    return ret;
  }

  private void putDummyOrEmpty(Byte i) {
    // put a empty list or null
    if (noOuterJoin) {
      storage.put(i, emptyList);
    } else {
      storage.put(i, dummyObjVectors[i.intValue()]);
    }
  }

  private int[] findSmallestKey() {
    int[] result = new int[order.length];
    ArrayList<Object> smallestOne = null;

    for (byte i : order) {
      ArrayList<Object> key = keyWritables[i];
      if (key == null) {
        continue;
      }
      if (smallestOne == null) {
        smallestOne = key;
        result[i] = -1;
        continue;
      }
      result[i] = compareKeys(key, smallestOne);
      if (result[i] < 0) {
        smallestOne = key;
      }
    }
    return smallestOne == null ? null : result;
  }

  private boolean processKey(byte alias, ArrayList<Object> key)
      throws HiveException {
    ArrayList<Object> keyWritable = keyWritables[alias];
    if (keyWritable == null) {
      //the first group.
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

  private void setUpFetchOpContext(FetchOperator fetchOp, String alias) {
    String currentInputFile = getExecContext().getCurrentInputFile();
    BucketMapJoinContext bucketMatcherCxt = localWork.getBucketMapjoinContext();

    Class<? extends BucketMatcher> bucketMatcherCls = bucketMatcherCxt
        .getBucketMatcherClass();
    BucketMatcher bucketMatcher = (BucketMatcher) ReflectionUtils.newInstance(
        bucketMatcherCls, null);

    getExecContext().setFileId(bucketMatcherCxt.createFileId(currentInputFile));
    LOG.info("set task id: " + getExecContext().getFileId());

    bucketMatcher.setAliasBucketFileNameMapping(bucketMatcherCxt
        .getAliasBucketFileNameMapping());
    List<Path> aliasFiles = bucketMatcher.getAliasBucketFiles(currentInputFile,
        bucketMatcherCxt.getMapJoinBigTableAlias(), alias);

    Iterator<Path> iter = aliasFiles.iterator();
    fetchOp.setupContext(iter, null);
  }

  private void fetchOneRow(byte tag) {
    if (fetchOperators != null) {
      String tble = this.tagToAlias.get(tag);
      FetchOperator fetchOp = fetchOperators.get(tble);

      Operator<? extends Serializable> forwardOp = localWork.getAliasToWork()
          .get(tble);
      try {
        InspectableObject row = fetchOp.getNextRow();
        if (row == null) {
          this.fetchOpDone[tag] = true;
          return;
        }
        forwardOp.process(row.o, 0);
        // check if any operator had a fatal error or early exit during
        // execution
        if (forwardOp.getDone()) {
          this.fetchOpDone[tag] = true;
        }
      } catch (Throwable e) {
        if (e instanceof OutOfMemoryError) {
          // Don't create a new object if we are already out of memory
          throw (OutOfMemoryError) e;
        } else {
          throw new RuntimeException("Map local work failed", e);
        }
      }
    }
  }

  transient boolean closeCalled = false;
  @Override
  public void closeOp(boolean abort) throws HiveException {
    if(closeCalled) {
      return;
    }
    closeCalled = true;

    if (inputFileChanged || !firstFetchHappened) {
      //set up the fetch operator for the new input file.
      for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
        String alias = entry.getKey();
        FetchOperator fetchOp = entry.getValue();
        fetchOp.clearFetchContext();
        setUpFetchOpContext(fetchOp, alias);
      }
      firstFetchHappened = true;
      for (Byte t : order) {
        if(t != (byte)posBigTable) {
          fetchNextGroup(t);
        }
      }
      inputFileChanged = false;
    }

    joinFinalLeftData();

    //clean up
    for (Byte alias : order) {
      if(alias != (byte) posBigTable) {
        fetchOpDone[alias] = false;
      }
      foundNextKeyGroup[alias] = false;
    }

    localWorkInited = false;

    super.closeOp(abort);
    if (fetchOperators != null) {
      for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
        Operator<? extends Serializable> forwardOp = localWork
            .getAliasToWork().get(entry.getKey());
        forwardOp.close(abort);
      }
    }
  }

  @Override
  protected boolean allInitializedParentsAreClosed() {
    return true;
  }

  /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return "MAPJOIN";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MAPJOIN;
  }
}
