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
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
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
  transient Map<Byte, ArrayList<Object>> keyWritables;
  transient Map<Byte, ArrayList<Object>> nextKeyWritables;
  HashMap<Byte, RowContainer<ArrayList<Object>>> nextGroupStorage;
  HashMap<Byte, RowContainer<ArrayList<Object>>> candidateStorage;
  
  transient HashMap<Byte, String> tagToAlias;
  private transient HashMap<Byte, Boolean> fetchOpDone = new HashMap<Byte, Boolean>();
  private transient HashMap<Byte, Boolean> foundNextKeyGroup = new HashMap<Byte, Boolean>();
  transient boolean firstFetchHappened = false;
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

    nextGroupStorage = new HashMap<Byte, RowContainer<ArrayList<Object>>>();
    candidateStorage = new HashMap<Byte, RowContainer<ArrayList<Object>>>();
    int bucketSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);
    byte storePos = (byte) 0;
    for (Byte alias : order) {
      RowContainer rc = getRowContainer(hconf, storePos, alias, bucketSize);
      nextGroupStorage.put((byte) storePos, rc);
      RowContainer candidateRC = getRowContainer(hconf, storePos, alias,
          bucketSize);
      candidateStorage.put(alias, candidateRC);
      storePos++;
    }
    tagToAlias = conf.getTagToAlias();
    keyWritables = new HashMap<Byte, ArrayList<Object>>();
    nextKeyWritables = new HashMap<Byte, ArrayList<Object>>();
    
    for (Byte alias : order) {
      if(alias != (byte) posBigTable) {
        fetchOpDone.put(alias, Boolean.FALSE);;
      }
      foundNextKeyGroup.put(alias, Boolean.FALSE);
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
    // create map local operators
    for (Map.Entry<String, FetchWork> entry : localWork.getAliasToFetchWork()
        .entrySet()) {
      fetchOperators.put(entry.getKey(), new FetchOperator(entry.getValue(),
          new JobConf(hconf)));
      if (l4j != null) {
        l4j.info("fetchoperator for " + entry.getKey() + " created");
      }
    }
    
    for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
      Operator<? extends Serializable> forwardOp = localWork.getAliasToWork()
          .get(entry.getKey());
      // All the operators need to be initialized before process
      forwardOp.setExecContext(this.getExecContext());
      forwardOp.initialize(this.getExecContext().jc, new ObjectInspector[] {entry.getValue()
          .getOutputObjectInspector()});
      l4j.info("fetchoperator for " + entry.getKey() + " initialized");
    }
  }
  
  @Override
  public void processOp(Object row, int tag) throws HiveException {

    if (this.getExecContext().inputFileChanged) {
      if(firstFetchHappened) {
        //we need to first join and flush out data left by the previous file. 
        joinFinalLeftData();        
      }
      //set up the fetch operator for the new input file.
      for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
        String alias = entry.getKey();
        FetchOperator fetchOp = entry.getValue();
        fetchOp.clearFetchContext();
        setUpFetchOpContext(fetchOp, alias);
      }
      this.getExecContext().inputFileChanged = false;
      firstFetchHappened = false;
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
    ArrayList<Object> key = computeValues(row, joinKeys.get(alias),
        joinKeysObjectInspectors.get(alias));
    ArrayList<Object> value = computeValues(row, joinValues.get(alias),
        joinValuesObjectInspectors.get(alias));

    //have we reached a new key group?
    boolean nextKeyGroup = processKey(alias, key);
    if (nextKeyGroup) {
      //assert this.nextGroupStorage.get(alias).size() == 0;
      this.nextGroupStorage.get(alias).add(value);
      foundNextKeyGroup.put((byte) tag, Boolean.TRUE);
      if (tag != posBigTable) {
        return;
      }
    }
    
    reportProgress();

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
    candidateStorage.get((byte) tag).add(value);
  }

  /*
   * this happens either when the input file of the big table is changed or in
   * closeop. It needs to fetch all the left data from the small tables and try
   * to join them.
   */
  private void joinFinalLeftData() throws HiveException {
    RowContainer bigTblRowContainer = this.candidateStorage.get((byte)this.posBigTable);
    
    boolean allFetchOpDone = allFetchOpDone();
    // if all left data in small tables are less than and equal to the left data
    // in big table, let's them catch up
    while (bigTblRowContainer != null && bigTblRowContainer.size() > 0
        && !allFetchOpDone) {
      joinOneGroup();
      bigTblRowContainer = this.candidateStorage.get((byte)this.posBigTable);
      allFetchOpDone = allFetchOpDone();
    }
    
    if (allFetchOpDone
        && this.candidateStorage.get((byte) this.posBigTable).size() > 0) {
      // if all fetch operator for small tables are done and there are data left
      // in big table 
      for (byte t : order) {
        if(this.foundNextKeyGroup.get(t) && this.nextKeyWritables.get(t) != null) {
          promoteNextGroupToCandidate(t);
        }
      }
      joinOneGroup();
    } else {
      while (!allFetchOpDone) {
        List<Byte> ret = joinOneGroup();
        if (ret == null || ret.size() == 0) {
          break;
        }
        
        reportProgress();
        
        allFetchOpDone = allFetchOpDone();
      }
      //one final table left
      for (byte t : order) {
        if(this.foundNextKeyGroup.get(t) && this.nextKeyWritables.get(t) != null) {
          promoteNextGroupToCandidate(t);
        }
      }
      joinOneGroup();
    }
  }

  private boolean allFetchOpDone() {
    boolean allFetchOpDone = true;
    for (Byte tag : order) {
      if(tag == (byte) posBigTable) {
        continue;
      }
      allFetchOpDone = allFetchOpDone && fetchOpDone.get(tag);
    }
    return allFetchOpDone;
  }

  private List<Byte> joinOneGroup() throws HiveException {
    int smallestPos = -1;
    smallestPos = findMostSmallKey();
    List<Byte> listOfNeedFetchNext = null;
    if(smallestPos >= 0) {
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

  private List<Byte> joinObject(int smallestPos) throws HiveException {
    List<Byte> needFetchList = new ArrayList<Byte>();
    ArrayList<Object> smallKey = keyWritables.get((byte) smallestPos);
    needFetchList.add((byte)smallestPos);
    this.storage.put((byte) smallestPos, this.candidateStorage.get((byte) smallestPos));
    for (Byte i : order) {
      if ((byte) smallestPos == i) {
        continue;
      }
      ArrayList<Object> key = keyWritables.get(i);
      if (key == null) {
        putDummyOrEmpty(i);
      } else {
        int cmp = compareKeys(key, smallKey);
        if (cmp == 0) {
          this.storage.put((byte) i, this.candidateStorage
              .get((byte) i));
          needFetchList.add(i);
          continue;
        } else {
          putDummyOrEmpty(i);
        }
      }
    }
    checkAndGenObject();
    for (Byte pos : needFetchList) {
      this.candidateStorage.get(pos).clear();
      this.keyWritables.remove(pos);
    }
    return needFetchList;
  }
  
  private void fetchNextGroup(Byte t) throws HiveException {
    if (foundNextKeyGroup.get(t)) {
      // first promote the next group to be the current group if we reached a
      // new group in the previous fetch
      if (this.nextKeyWritables.get(t) != null) {
        promoteNextGroupToCandidate(t);
      } else {
        this.keyWritables.remove(t);
        this.candidateStorage.remove(t);
        this.nextGroupStorage.remove(t);
      }
      foundNextKeyGroup.put(t, Boolean.FALSE);
    }
    //for the big table, we only need to promote the next group to the current group.
    if(t == (byte)posBigTable) {
      return;
    }
    
    //for tables other than the big table, we need to fetch more data until reach a new group or done.
    while (!foundNextKeyGroup.get(t)) {
      if (fetchOpDone.get(t)) {
        break;
      }
      fetchOneRow(t);
    }
    if (!foundNextKeyGroup.get(t) && fetchOpDone.get(t)) {
      this.nextKeyWritables.remove(t);
    }
  }

  private void promoteNextGroupToCandidate(Byte t) throws HiveException {
    this.keyWritables.put(t, this.nextKeyWritables.get(t));
    this.nextKeyWritables.remove(t);
    RowContainer<ArrayList<Object>> oldRowContainer = this.candidateStorage.get(t);
    oldRowContainer.clear();
    this.candidateStorage.put(t, this.nextGroupStorage.get(t));
    this.nextGroupStorage.put(t, oldRowContainer);
  }
  
  private int compareKeys (ArrayList<Object> k1, ArrayList<Object> k2) {
    int ret = 0;
    for (int i = 0; i < k1.size() && i < k1.size(); i++) {
      WritableComparable key_1 = (WritableComparable) k1.get(i);
      WritableComparable key_2 = (WritableComparable) k2.get(i);
      ret = WritableComparator.get(key_1.getClass()).compare(key_1, key_2);
      if(ret != 0) {
        return ret;
      }
    }
    return k1.size() - k2.size();
  }

  private void putDummyOrEmpty(Byte i) {
    // put a empty list or null
    if (noOuterJoin) {
      storage.put(i, emptyList);
    } else {
      storage.put(i, dummyObjVectors[i.intValue()]);
    }
  }

  private int findMostSmallKey() {
    byte index = -1;
    ArrayList<Object> mostSmallOne = null;

    for (byte i : order) {
      ArrayList<Object> key = keyWritables.get(i);
      if (key == null) {
        continue;
      }
      if (mostSmallOne == null) {
        mostSmallOne = key;
        index = i;
        continue;
      }
      int cmp = compareKeys(key, mostSmallOne);
      if (cmp < 0) {
        mostSmallOne = key;
        index = i;
        continue;
      }
    }
    return index;
  }

  private boolean processKey(byte alias, ArrayList<Object> key)
      throws HiveException {
    ArrayList<Object> keyWritable = keyWritables.get(alias);
    if (keyWritable == null) {
      //the first group.
      keyWritables.put(alias, key);
      return false;
    } else {
      int cmp = compareKeys(key, keyWritable);;
      if (cmp != 0) {
        nextKeyWritables.put(alias, key);
        return true;
      }
      return false;
    }
  }

  private void setUpFetchOpContext(FetchOperator fetchOp, String alias) {
    String currentInputFile = this.getExecContext().currentInputFile;
    BucketMapJoinContext bucketMatcherCxt = this.localWork
        .getBucketMapjoinContext();
    Class<? extends BucketMatcher> bucketMatcherCls = bucketMatcherCxt
        .getBucketMatcherClass();
    BucketMatcher bucketMatcher = (BucketMatcher) ReflectionUtils.newInstance(
        bucketMatcherCls, null);
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
          this.fetchOpDone.put(tag, Boolean.TRUE);
          return;
        }
        forwardOp.process(row.o, 0);
        // check if any operator had a fatal error or early exit during
        // execution
        if (forwardOp.getDone()) {
          this.fetchOpDone.put(tag, Boolean.TRUE);
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
    joinFinalLeftData();
    this.firstFetchHappened = false;
    //clean up 
    for (Byte alias : order) {
      if(alias != (byte) posBigTable) {
        fetchOpDone.put(alias, Boolean.FALSE);;
      }
      foundNextKeyGroup.put(alias, Boolean.FALSE);
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
  public int getType() {
    return OperatorType.MAPJOIN;
  }
}
