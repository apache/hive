/*
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hive.common.util.ReflectionUtil;

/**
 * Sorted Merge Map Join Operator.
 */
public class SMBMapJoinOperator extends AbstractMapJoinOperator<SMBJoinDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(SMBMapJoinOperator.class
      .getName());

  private MapredLocalWork localWork = null;
  private Map<String, MergeQueue> aliasToMergeQueue = Collections.emptyMap();

  transient List<Object>[] keyWritables;
  transient List<Object>[] nextKeyWritables;
  RowContainer<List<Object>>[] nextGroupStorage;
  RowContainer<List<Object>>[] candidateStorage;

  transient String[] tagToAlias;
  private transient boolean[] fetchDone;
  private transient boolean[] foundNextKeyGroup;
  transient boolean firstFetchHappened = false;
  private transient boolean inputFileChanged = false;
  transient boolean localWorkInited = false;
  transient boolean initDone = false;

  // This join has been converted to a SMB join by the hive optimizer. The user did not
  // give a mapjoin hint in the query. The hive optimizer figured out that the join can be
  // performed as a smb join, based on all the tables/partitions being joined.
  private transient boolean convertedAutomaticallySMBJoin = false;

  /** Kryo ctor. */
  protected SMBMapJoinOperator() {
    super();
  }

  public SMBMapJoinOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public SMBMapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mapJoinOp) {
    super(mapJoinOp);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    // If there is a sort-merge join followed by a regular join, the SMBJoinOperator may not
    // get initialized at all. Consider the following query:
    // A SMB B JOIN C
    // For the mapper processing C, The SMJ is not initialized, no need to close it either.
    initDone = true;

    super.initializeOp(hconf);

    closeCalled = false;

    this.firstFetchHappened = false;
    this.inputFileChanged = false;

    // get the largest table alias from order
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

    // For backwards compatibility reasons we honor the older
    // HIVE_MAPJOIN_BUCKET_CACHE_SIZE if set different from default.
    // By hive 0.13 we should remove this code.
    int oldVar = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVE_MAPJOIN_BUCKET_CACHE_SIZE);
    if (oldVar != 100) {
      bucketSize = oldVar;
    } else {
      bucketSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVE_SMBJOIN_CACHE_ROWS);
    }

    for (byte pos = 0; pos < order.length; pos++) {
      RowContainer<List<Object>> rc = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors[pos],
          pos, bucketSize,spillTableDesc, conf, !hasFilter(pos),
          reporter);
      nextGroupStorage[pos] = rc;
      RowContainer<List<Object>> candidateRC = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors[pos],
          pos, bucketSize,spillTableDesc, conf, !hasFilter(pos),
          reporter);
      candidateStorage[pos] = candidateRC;
    }
    tagToAlias = conf.convertToArray(conf.getTagToAlias(), String.class);

    for (byte pos = 0; pos < order.length; pos++) {
      if (pos != posBigTable) {
        fetchDone[pos] = false;
      }
      foundNextKeyGroup[pos] = false;
    }
  }

  @Override
  public void initializeLocalWork(Configuration hconf) throws HiveException {
    initializeMapredLocalWork(this.getConf(), hconf, this.getConf().getLocalWork(), LOG);
    super.initializeLocalWork(hconf);
  }

  public void initializeMapredLocalWork(MapJoinDesc mjConf, Configuration hconf,
      MapredLocalWork localWork, Logger l4j) throws HiveException {
    if (localWork == null || localWorkInited) {
      return;
    }
    localWorkInited = true;
    this.localWork = localWork;
    aliasToMergeQueue = new HashMap<String, MergeQueue>();

    // create map local operators
    Map<String,FetchWork> aliasToFetchWork = localWork.getAliasToFetchWork();
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = localWork.getAliasToWork();
    Map<String, DummyStoreOperator> aliasToSinkWork = conf.getAliasToSink();

    // The operator tree till the sink operator needs to be processed while
    // fetching the next row to fetch from the priority queue (possibly containing
    // multiple files in the small table given a file in the big table). The remaining
    // tree will be processed while processing the join.
    // Look at comments in DummyStoreOperator for additional explanation.
    for (Map.Entry<String, FetchWork> entry : aliasToFetchWork.entrySet()) {
      String alias = entry.getKey();
      FetchWork fetchWork = entry.getValue();

      JobConf jobClone = new JobConf(hconf);
      if (UserGroupInformation.isSecurityEnabled()) {
        String hadoopAuthToken = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
        if(hadoopAuthToken != null){
          jobClone.set("mapreduce.job.credentials.binary", hadoopAuthToken);
        }
      }

      TableScanOperator ts = (TableScanOperator)aliasToWork.get(alias);
      // push down projections
      ColumnProjectionUtils.appendReadColumns(jobClone, ts.getNeededColumnIDs(), ts.getNeededColumns(),
              ts.getNeededNestedColumnPaths(), ts.conf.hasVirtualCols());
      // push down filters and as of information
      HiveInputFormat.pushFiltersAndAsOf(jobClone, ts, null);

      AcidUtils.setAcidOperationalProperties(jobClone, ts.getConf().isTranscationalTable(),
          ts.getConf().getAcidOperationalProperties());
      AcidUtils.setValidWriteIdList(jobClone, ts.getConf());

      ts.passExecContext(getExecContext());

      FetchOperator fetchOp = new FetchOperator(fetchWork, jobClone);
      ts.initialize(jobClone, new ObjectInspector[]{fetchOp.getOutputObjectInspector()});
      fetchOp.clearFetchContext();

      DummyStoreOperator sinkOp = aliasToSinkWork.get(alias);

      MergeQueue mergeQueue = new MergeQueue(alias, fetchWork, jobClone, ts, sinkOp);

      aliasToMergeQueue.put(alias, mergeQueue);
      l4j.info("fetch operators for " + alias + " initialized");
    }
  }

  private byte tagForAlias(String alias) {
    for (byte tag = 0; tag < tagToAlias.length; tag++) {
      if (alias.equals(tagToAlias[tag])) {
        return tag;
      }
    }
    return -1;
  }

  // The input file has changed - load the correct hash bucket
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    inputFileChanged = true;
  }

  protected List<Object> smbJoinComputeKeys(Object row, byte alias) throws HiveException {
    return JoinUtil.computeKeys(row, joinKeys[alias],
          joinKeysObjectInspectors[alias]);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

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
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != posBigTable) {
          fetchNextGroup(pos);
        }
      }
    }

    byte alias = (byte) tag;

    // compute keys and values as StandardObjects
    List<Object> key = smbJoinComputeKeys(row, alias);

    List<Object> value = getFilteredValue(alias, row);


    //have we reached a new key group?
    boolean nextKeyGroup = processKey(alias, key);
    addToAliasFilterTags(alias, value, nextKeyGroup);
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
    while (bigTblRowContainer != null && bigTblRowContainer.rowCount() > 0
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
      for (byte pos = 0; pos < order.length; pos++) {
        if (this.foundNextKeyGroup[pos]
            && this.nextKeyWritables[pos] != null) {
          promoteNextGroupToCandidate(pos);
        }
      }
      joinOneGroup();
      dataInCache = false;
      for (byte pos = 0; pos < order.length; pos++) {
        if (this.candidateStorage[pos] != null && this.candidateStorage[pos].hasRows()) {
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
    if(t == posBigTable) {
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
    RowContainer<List<Object>> oldRowContainer = this.candidateStorage[t];
    oldRowContainer.clearRows();
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
      ret = WritableComparatorFactory.get(k1.get(i), nullsafes == null ? false : nullsafes[i], null)
              .compare(k1.get(i), k2.get(i));
      if(ret != 0) {
        return ret;
      }
    }
    return ret;
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

  private boolean processKey(byte alias, List<Object> key)
      throws HiveException {
    List<Object> keyWritable = keyWritables[alias];
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

    Path currentInputPath = getExecContext().getCurrentInputPath();

    BucketMapJoinContext bucketMatcherCxt = localWork.getBucketMapjoinContext();
    Class<? extends BucketMatcher> bucketMatcherCls = bucketMatcherCxt.getBucketMatcherClass();
    BucketMatcher bucketMatcher = ReflectionUtil.newInstance(bucketMatcherCls, null);

    getExecContext().setFileId(bucketMatcherCxt.createFileId(currentInputPath.toString()));
    LOG.info("set task id: " + getExecContext().getFileId());

    bucketMatcher.setAliasBucketFileNameMapping(bucketMatcherCxt
        .getAliasBucketFileNameMapping());

    List<Path> aliasFiles = bucketMatcher.getAliasBucketFiles(currentInputPath.toString(),
        bucketMatcherCxt.getMapJoinBigTableAlias(), alias);

    mergeQueue.setupContext(aliasFiles);
  }

  private void fetchOneRow(byte tag) {
    String table = tagToAlias[tag];
    MergeQueue mergeQueue = aliasToMergeQueue.get(table);

    // The operator tree till the sink operator has already been processed while
    // fetching the next row to fetch from the priority queue (possibly containing
    // multiple files in the small table given a file in the big table). Now, process
    // the remaining tree. Look at comments in DummyStoreOperator for additional
    // explanation.
    Operator<? extends OperatorDesc> forwardOp =
        conf.getAliasToSink().get(table).getChildOperators().get(0);
    try {
      InspectableObject row = mergeQueue.getNextRow();
      if (row == null) {
        fetchDone[tag] = true;
        return;
      }
      forwardOp.process(row.o, tag);
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

    // If there is a sort-merge join followed by a regular join, the SMBJoinOperator may not
    // get initialized at all. Consider the following query:
    // A SMB B JOIN C
    // For the mapper processing C, The SMJ is not initialized, no need to close it either.
    if (!initDone) {
      return;
    }


    if (inputFileChanged || !firstFetchHappened) {
      //set up the fetch operator for the new input file.
      for (Map.Entry<String, MergeQueue> entry : aliasToMergeQueue.entrySet()) {
        String alias = entry.getKey();
        MergeQueue mergeQueue = entry.getValue();
        setUpFetchContexts(alias, mergeQueue);
      }
      firstFetchHappened = true;
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != posBigTable) {
          fetchNextGroup(pos);
        }
      }
      inputFileChanged = false;
    }

    joinFinalLeftData();

    //clean up
    for (int pos = 0; pos < order.length; pos++) {
      if (pos != posBigTable) {
        fetchDone[pos] = false;
      }
      foundNextKeyGroup[pos] = false;
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

  public boolean isConvertedAutomaticallySMBJoin() {
    return convertedAutomaticallySMBJoin;
  }

  public void setConvertedAutomaticallySMBJoin(boolean convertedAutomaticallySMBJoin) {
    this.convertedAutomaticallySMBJoin = convertedAutomaticallySMBJoin;
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
    transient Operator<? extends OperatorDesc> forwardOp;
    transient DummyStoreOperator sinkOp;

    // index of FetchOperator which is providing smallest one
    transient Integer currentMinSegment;
    transient MutablePair<List<Object>, InspectableObject>[] keys;

    public MergeQueue(String alias, FetchWork fetchWork, JobConf jobConf,
        Operator<? extends OperatorDesc> forwardOp,
        DummyStoreOperator sinkOp) {
      this.alias = alias;
      this.fetchWork = fetchWork;
      this.jobConf = jobConf;
      this.forwardOp = forwardOp;
      this.sinkOp = sinkOp;
    }

    // paths = bucket files of small table for current bucket file of big table
    // initializes a FetchOperator for each file in paths, reuses FetchOperator if possible
    // currently, number of paths is always the same (bucket numbers are all the same over
    // all partitions in a table).
    // But if hive supports assigning bucket number for each partition, this can be vary
    public void setupContext(List<Path> paths) throws HiveException {
      int segmentLen = paths.size();
      FetchOperator.setFetchOperatorContext(jobConf, fetchWork.getPartDir());
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
        MutablePair<List<Object>, InspectableObject>[] newKeys = new MutablePair[segmentLen];
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

    @Override
    protected boolean lessThan(Object a, Object b) {
      return compareKeys(keys[(Integer) a].getLeft(), keys[(Integer)b].getLeft()) < 0;
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
      currentMinSegment = current;
      return keys[currentMinSegment].getRight();
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
        byte tag = tagForAlias(alias);
        // joinKeys/joinKeysOI are initialized after making merge queue, so setup lazily at runtime
        keyFields = joinKeys[tag];
        keyFieldOIs = joinKeysObjectInspectors[tag];
      }
      InspectableObject nextRow = segments[current].getNextRow();
      while (nextRow != null) {
        sinkOp.reset();
        if (keys[current] == null) {
          keys[current] = new MutablePair<List<Object>, InspectableObject>();
        }

        // Pass the row though the operator tree. It is guaranteed that not more than 1 row can
        // be produced from a input row.
        forwardOp.process(nextRow.o, 0);
        nextRow = sinkOp.getResult();

        // It is possible that the row got absorbed in the operator tree.
        if (nextRow.o != null) {
          // todo this should be changed to be evaluated lazily, especially for single segment case
          keys[current].setLeft(JoinUtil.computeKeys(nextRow.o, keyFields, keyFieldOIs));
          keys[current].setRight(nextRow);
          return true;
        }
        nextRow = segments[current].getNextRow();
      }
      keys[current] = null;
      return false;
    }
  }

  @Override
  public boolean opAllowedConvertMapJoin() {
    return false;
  }
}
