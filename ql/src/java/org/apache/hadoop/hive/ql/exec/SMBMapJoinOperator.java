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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.util.ObjectPair;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.PriorityQueue;
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
  private Map<String, MergeQueue> aliasToMergeQueue = Collections.emptyMap();

  transient ArrayList<Object>[] keyWritables;
  transient ArrayList<Object>[] nextKeyWritables;
  RowContainer<ArrayList<Object>>[] nextGroupStorage;
  RowContainer<ArrayList<Object>>[] candidateStorage;

  transient Map<Byte, String> tagToAlias;
  private transient boolean[] fetchDone;
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
    fetchDone = new boolean[maxAlias];
    foundNextKeyGroup = new boolean[maxAlias];

    int bucketSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);
    byte storePos = (byte) 0;
    for (Byte alias : order) {
      RowContainer rc = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors.get(storePos),
          alias, bucketSize,spillTableDesc, conf, !hasFilter(storePos));
      nextGroupStorage[storePos] = rc;
      RowContainer candidateRC = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors.get((byte)storePos),
          alias,bucketSize,spillTableDesc, conf, !hasFilter(storePos));
      candidateStorage[alias] = candidateRC;
      storePos++;
    }
    tagToAlias = conf.getTagToAlias();

    for (Byte alias : order) {
      if(alias != (byte) posBigTable) {
        fetchDone[alias] = false;
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
    aliasToMergeQueue = new HashMap<String, MergeQueue>();

    // create map local operators
    Map<String,FetchWork> aliasToFetchWork = localWork.getAliasToFetchWork();
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = localWork.getAliasToWork();

    for (Map.Entry<String, FetchWork> entry : aliasToFetchWork.entrySet()) {
      String alias = entry.getKey();
      FetchWork fetchWork = entry.getValue();

      Operator<? extends OperatorDesc> forwardOp = aliasToWork.get(alias);
      forwardOp.setExecContext(getExecContext());

      JobConf jobClone = cloneJobConf(hconf, forwardOp);
      FetchOperator fetchOp = new FetchOperator(fetchWork, jobClone);
      forwardOp.initialize(jobClone, new ObjectInspector[]{fetchOp.getOutputObjectInspector()});
      fetchOp.clearFetchContext();

      MergeQueue mergeQueue = new MergeQueue(alias, fetchWork, jobClone);

      aliasToMergeQueue.put(alias, mergeQueue);
      l4j.info("fetch operators for " + alias + " initialized");
    }
  }

  private JobConf cloneJobConf(Configuration hconf, Operator<?> op) {
    JobConf jobClone = new JobConf(hconf);
    if (op instanceof TableScanOperator) {
      List<Integer> list = ((TableScanOperator)op).getNeededColumnIDs();
      if (list != null) {
        ColumnProjectionUtils.appendReadColumnIDs(jobClone, list);
      }
    } else {
      ColumnProjectionUtils.setFullyReadColumns(jobClone);
    }
    return jobClone;
  }

  private byte tagForAlias(String alias) {
    for (Map.Entry<Byte, String> entry : tagToAlias.entrySet()) {
      if (entry.getValue().equals(alias)) {
        return entry.getKey();
      }
    }
    return -1;
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
        for (Map.Entry<String, MergeQueue> entry : aliasToMergeQueue.entrySet()) {
          String alias = entry.getKey();
          MergeQueue mergeQueue = entry.getValue();
          setUpFetchContexts(alias, mergeQueue);
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
    ArrayList<Object> key = JoinUtil.computeKeys(row, joinKeys.get(alias),
        joinKeysObjectInspectors.get(alias));
    ArrayList<Object> value = JoinUtil.computeValues(row, joinValues.get(alias),
        joinValuesObjectInspectors.get(alias), joinFilters.get(alias),
        joinFilterObjectInspectors.get(alias),
        filterMap == null ? null : filterMap[alias]);


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

    boolean allFetchDone = allFetchDone();
    // if all left data in small tables are less than and equal to the left data
    // in big table, let's them catch up
    while (bigTblRowContainer != null && bigTblRowContainer.size() > 0
        && !allFetchDone) {
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

  private boolean allFetchDone() {
    boolean allFetchDone = true;
    for (Byte tag : order) {
      if(tag == (byte) posBigTable) {
        continue;
      }
      allFetchDone = allFetchDone && fetchDone[tag];
    }
    return allFetchDone;
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
      if (fetchDone[t]) {
        break;
      }
      fetchOneRow(t);
    }
    if (!foundNextKeyGroup[t] && fetchDone[t]) {
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

  private int compareKeys (List<Object> k1, List<Object> k2) {
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

  private void setUpFetchContexts(String alias, MergeQueue mergeQueue) throws HiveException {
    mergeQueue.clearFetchContext();

    String currentInputFile = getExecContext().getCurrentInputFile();

    BucketMapJoinContext bucketMatcherCxt = localWork.getBucketMapjoinContext();
    Class<? extends BucketMatcher> bucketMatcherCls = bucketMatcherCxt.getBucketMatcherClass();
    BucketMatcher bucketMatcher = ReflectionUtils.newInstance(bucketMatcherCls, null);

    getExecContext().setFileId(bucketMatcherCxt.createFileId(currentInputFile));
    LOG.info("set task id: " + getExecContext().getFileId());

    bucketMatcher.setAliasBucketFileNameMapping(bucketMatcherCxt
        .getAliasBucketFileNameMapping());

    List<Path> aliasFiles = bucketMatcher.getAliasBucketFiles(currentInputFile,
        bucketMatcherCxt.getMapJoinBigTableAlias(), alias);

    mergeQueue.setupContext(aliasFiles);
  }

  private void fetchOneRow(byte tag) {
    String table = tagToAlias.get(tag);
    MergeQueue mergeQueue = aliasToMergeQueue.get(table);

    Operator<? extends OperatorDesc> forwardOp = localWork.getAliasToWork()
        .get(table);
    try {
      InspectableObject row = mergeQueue.getNextRow();
      if (row == null) {
        fetchDone[tag] = true;
        return;
      }
      forwardOp.process(row.o, 0);
      // check if any operator had a fatal error or early exit during
      // execution
      if (forwardOp.getDone()) {
        fetchDone[tag] = true;
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

  transient boolean closeCalled = false;
  @Override
  public void closeOp(boolean abort) throws HiveException {
    if(closeCalled) {
      return;
    }
    closeCalled = true;

    if (inputFileChanged || !firstFetchHappened) {
      //set up the fetch operator for the new input file.
      for (Map.Entry<String, MergeQueue> entry : aliasToMergeQueue.entrySet()) {
        String alias = entry.getKey();
        MergeQueue mergeQueue = entry.getValue();
        setUpFetchContexts(alias, mergeQueue);
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
        fetchDone[alias] = false;
      }
      foundNextKeyGroup[alias] = false;
    }

    localWorkInited = false;

    super.closeOp(abort);
    for (Map.Entry<String, MergeQueue> entry : aliasToMergeQueue.entrySet()) {
      String alias = entry.getKey();
      MergeQueue mergeQueue = entry.getValue();
      Operator forwardOp = localWork.getAliasToWork().get(alias);
      forwardOp.close(abort);
      mergeQueue.clearFetchContext();
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
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "MAPJOIN";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MAPJOIN;
  }

  // returns rows from possibly multiple bucket files of small table in ascending order
  // by utilizing primary queue (borrowed from hadoop)
  // elements of queue (Integer) are index to FetchOperator[] (segments)
  private class MergeQueue extends PriorityQueue<Integer> {

    private final String alias;
    private final FetchWork fetchWork;
    private final JobConf jobConf;

    // for keeping track of the number of elements read. just for debugging
    transient int counter;

    transient FetchOperator[] segments;
    transient List<ExprNodeEvaluator> keyFields;
    transient List<ObjectInspector> keyFieldOIs;

    // index of FetchOperator which is providing smallest one
    transient Integer currentMinSegment;
    transient ObjectPair<List<Object>, InspectableObject>[] keys;

    public MergeQueue(String alias, FetchWork fetchWork, JobConf jobConf) {
      this.alias = alias;
      this.fetchWork = fetchWork;
      this.jobConf = jobConf;
    }

    // paths = bucket files of small table for current bucket file of big table
    // initializes a FetchOperator for each file in paths, reuses FetchOperator if possible
    // currently, number of paths is always the same (bucket numbers are all the same over
    // all partitions in a table).
    // But if hive supports assigning bucket number for each partition, this can be vary
    public void setupContext(List<Path> paths) throws HiveException {
      int segmentLen = paths.size();
      FetchOperator[] segments = segmentsForSize(segmentLen);
      for (int i = 0 ; i < segmentLen; i++) {
        Path path = paths.get(i);
        if (segments[i] == null) {
          segments[i] = new FetchOperator(fetchWork, new JobConf(jobConf));
        }
        segments[i].setupContext(Arrays.asList(path));
      }
      initialize(segmentLen);
      for (int i = 0; i < segmentLen; i++) {
        if (nextHive(i)) {
          put(i);
        }
      }
      counter = 0;
    }

    @SuppressWarnings("unchecked")
    private FetchOperator[] segmentsForSize(int segmentLen) {
      if (segments == null || segments.length < segmentLen) {
        FetchOperator[] newSegments = new FetchOperator[segmentLen];
        ObjectPair<List<Object>, InspectableObject>[] newKeys = new ObjectPair[segmentLen];
        if (segments != null) {
          System.arraycopy(segments, 0, newSegments, 0, segments.length);
          System.arraycopy(keys, 0, newKeys, 0, keys.length);
        }
        segments = newSegments;
        keys = newKeys;
      }
      return segments;
    }

    public void clearFetchContext() throws HiveException {
      if (segments != null) {
        for (FetchOperator op : segments) {
          if (op != null) {
            op.clearFetchContext();
          }
        }
      }
    }

    protected boolean lessThan(Object a, Object b) {
      return compareKeys(keys[(Integer) a].getFirst(), keys[(Integer)b].getFirst()) < 0;
    }

    public final InspectableObject getNextRow() throws IOException {
      if (currentMinSegment != null) {
        adjustPriorityQueue(currentMinSegment);
      }
      Integer current = top();
      if (current == null) {
        LOG.info("MergeQueue forwarded " + counter + " rows");
        return null;
      }
      counter++;
      return keys[currentMinSegment = current].getSecond();
    }

    private void adjustPriorityQueue(Integer current) throws IOException {
      if (nextIO(current)) {
        adjustTop();  // sort
      } else {
        pop();
      }
    }

    // wrapping for exception handling
    private boolean nextHive(Integer current) throws HiveException {
      try {
        return next(current);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }

    // wrapping for exception handling
    private boolean nextIO(Integer current) throws IOException {
      try {
        return next(current);
      } catch (HiveException e) {
        throw new IOException(e);
      }
    }

    // return true if current min segment(FetchOperator) has next row
    private boolean next(Integer current) throws IOException, HiveException {
      if (keyFields == null) {
        // joinKeys/joinKeysOI are initialized after making merge queue, so setup lazily at runtime
        byte tag = tagForAlias(alias);
        keyFields = joinKeys.get(tag);
        keyFieldOIs = joinKeysObjectInspectors.get(tag);
      }
      InspectableObject nextRow = segments[current].getNextRow();
      if (nextRow != null) {
        if (keys[current] == null) {
          keys[current] = new ObjectPair<List<Object>, InspectableObject>();
        }
        // todo this should be changed to be evaluated lazily, especially for single segment case
        keys[current].setFirst(JoinUtil.computeKeys(nextRow.o, keyFields, keyFieldOIs));
        keys[current].setSecond(nextRow);
        return true;
      }
      keys[current] = null;
      return false;
    }
  }
}
