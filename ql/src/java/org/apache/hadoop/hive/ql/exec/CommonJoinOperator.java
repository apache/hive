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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Join operator implementation.
 */
public abstract class CommonJoinOperator<T extends JoinDesc> extends
    Operator<T> implements Serializable {
  private static final long serialVersionUID = 1L;
  protected static final Logger LOG = LoggerFactory.getLogger(CommonJoinOperator.class
      .getName());

  protected transient int numAliases; // number of aliases
  /**
   * The expressions for join inputs.
   */
  protected transient List<ExprNodeEvaluator>[] joinValues;

  /**
   * The filters for join
   */
  protected transient List<ExprNodeEvaluator>[] joinFilters;

  /**
   * List of evaluators for conditions which appear on on-clause and needs to be
   * evaluated before emitting rows. Currently, relevant only for outer joins.
   *
   * For instance, given the query:
   *     select * from t1 right outer join t2 on t1.c1 + t2.c2 &gt; t1.c3;
   * The expression evaluator for t1.c1 + t2.c2 &gt; t1.c3 will be stored in this list.
   */
  protected transient List<ExprNodeEvaluator> residualJoinFilters;

  protected transient int[][] filterMaps;

  /**
   * The ObjectInspectors for the join inputs.
   */
  protected transient List<ObjectInspector>[] joinValuesObjectInspectors;

  /**
   * The ObjectInspectors for join filters.
   */
  protected transient List<ObjectInspector>[] joinFilterObjectInspectors;

  /**
   * OIs corresponding to residualJoinFilters.
   */
  protected transient List<ObjectInspector> residualJoinFiltersOIs;

  /**
   * Will be true depending on content of residualJoinFilters.
   */
  protected transient boolean needsPostEvaluation;

  /**
   * This data structure is used to keep track of rows on which residualFilters
   * evaluated to false. We will iterate on this container afterwards and emit
   * rows appending NULL values if it was not done. Key is relation index.
   */
  protected transient Map<Integer, Object[]> rowContainerPostFilteredOuterJoin = null;

  /**
   * The standard ObjectInspectors for the join inputs.
   */
  protected transient List<ObjectInspector>[] joinValuesStandardObjectInspectors;
  /**
   * The standard ObjectInspectors for the row container.
   */
  protected transient List<ObjectInspector>[] rowContainerStandardObjectInspectors;

  protected transient Byte[] order; // order in which the results should
  // be output
  protected transient JoinCondDesc[] condn;
  protected transient boolean[] nullsafes;

  public transient boolean noOuterJoin;

  // for outer joins, contains the potential nulls for the concerned aliases
  protected transient ArrayList<Object>[] dummyObj;

  // empty rows for each table
  protected transient RowContainer<List<Object>>[] dummyObjVectors;

  protected transient int totalSz; // total size of the composite object

  // keys are the column names. basically this maps the position of the column
  // in
  // the output of the CommonJoinOperator to the input columnInfo.
  private transient Map<Integer, Set<String>> posToAliasMap;

  transient LazyBinarySerDe[] spillTableSerDe;
  protected transient TableDesc[] spillTableDesc; // spill tables are
  // used if the join
  // input is too large
  // to fit in memory

  AbstractRowContainer<List<Object>>[] storage; // map b/w table alias
  // to RowContainer
  int joinEmitInterval = -1;
  int joinCacheSize = 0;
  long nextSz = 0;
  transient Byte lastAlias = null;
  private long logEveryNRows = 0L;

  transient boolean handleSkewJoin = false;

  transient boolean hasLeftSemiJoin = false;

  transient boolean hasLeftAntiSemiJoin = false;

  protected transient int countAfterReport;
  protected transient int heartbeatInterval;
  protected static final int NOTSKIPBIGTABLE = -1;

  private transient boolean closeOpCalled = false;

  /** Kryo ctor. */
  protected CommonJoinOperator() {
    super();
  }

  public CommonJoinOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public CommonJoinOperator(CommonJoinOperator<T> clone) {
    super(clone.id, clone.cContext);
    this.joinEmitInterval = clone.joinEmitInterval;
    this.joinCacheSize = clone.joinCacheSize;
    this.nextSz = clone.nextSz;
    this.logEveryNRows = clone.logEveryNRows;
    this.childOperators = clone.childOperators;
    this.parentOperators = clone.parentOperators;
    this.done = clone.done;
    this.storage = clone.storage;
    this.condn = clone.condn;
    this.conf = clone.getConf();
    this.setSchema(clone.getSchema());
    this.alias = clone.alias;
    this.childOperatorsArray = clone.childOperatorsArray;
    this.childOperatorsTag = clone.childOperatorsTag;
    this.setColumnExprMap(clone.getColumnExprMap());
    this.dummyObj = clone.dummyObj;
    this.dummyObjVectors = clone.dummyObjVectors;
    this.forwardCache = clone.forwardCache;
    this.groupKeyObject = clone.groupKeyObject;
    this.handleSkewJoin = clone.handleSkewJoin;
    this.hconf = clone.hconf;
    this.inputObjInspectors = clone.inputObjInspectors;
    this.noOuterJoin = clone.noOuterJoin;
    this.numAliases = clone.numAliases;
    this.operatorId = clone.operatorId;
    this.posToAliasMap = clone.posToAliasMap;
    this.spillTableDesc = clone.spillTableDesc;
    this.statsMap = clone.statsMap;
    this.joinFilters = clone.joinFilters;
    this.joinFilterObjectInspectors = clone.joinFilterObjectInspectors;
    this.residualJoinFilters = clone.residualJoinFilters;
    this.residualJoinFiltersOIs = clone.residualJoinFiltersOIs;
    this.needsPostEvaluation = clone.needsPostEvaluation;
  }

  private <T extends JoinDesc> ObjectInspector getJoinOutputObjectInspector(
      Byte[] order, List<ObjectInspector>[] aliasToObjectInspectors, T conf) {
    List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    for (Byte alias : order) {
      List<ObjectInspector> oiList = getValueObjectInspectors(alias, aliasToObjectInspectors);
      if (oiList != null && !oiList.isEmpty()) {
        structFieldObjectInspectors.addAll(oiList);
      }
    }

    StructObjectInspector joinOutputObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf.getOutputColumnNames(),
        structFieldObjectInspectors);
    return joinOutputObjectInspector;
  }

  protected List<ObjectInspector> getValueObjectInspectors(
      byte alias, List<ObjectInspector>[] aliasToObjectInspectors) {
    return aliasToObjectInspectors[alias];
  }

  protected Configuration hconf;

  @Override
  @SuppressWarnings("unchecked")
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    closeOpCalled = false;
    this.handleSkewJoin = conf.getHandleSkewJoin();
    this.hconf = hconf;

    heartbeatInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVE_SEND_HEARTBEAT);
    countAfterReport = 0;

    totalSz = 0;

    int tagLen = conf.getTagLength();
    // Map that contains the rows for each alias
    storage = new AbstractRowContainer[tagLen];

    numAliases = conf.getExprs().size();

    joinValues = new List[tagLen];

    joinFilters = new List[tagLen];

    order = conf.getTagOrder();
    condn = conf.getConds();
    nullsafes = conf.getNullSafes();
    noOuterJoin = conf.isNoOuterJoin();

    totalSz = JoinUtil.populateJoinKeyValue(joinValues, conf.getExprs(),
        order,NOTSKIPBIGTABLE, hconf);

    //process join filters
    joinFilters = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinFilters, conf.getFilters(),order,NOTSKIPBIGTABLE, hconf);


    joinValuesObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinValues,
        inputObjInspectors,NOTSKIPBIGTABLE, tagLen);
    joinFilterObjectInspectors = JoinUtil.getObjectInspectorsFromEvaluators(joinFilters,
        inputObjInspectors,NOTSKIPBIGTABLE, tagLen);
    joinValuesStandardObjectInspectors = JoinUtil.getStandardObjectInspectors(
        joinValuesObjectInspectors,NOTSKIPBIGTABLE, tagLen);

    filterMaps = conf.getFilterMap();

    if (noOuterJoin) {
      rowContainerStandardObjectInspectors = joinValuesStandardObjectInspectors;
    } else {
      List<ObjectInspector>[] rowContainerObjectInspectors = new List[tagLen];
      for (Byte alias : order) {
        ArrayList<ObjectInspector> rcOIs = new ArrayList<ObjectInspector>();
        rcOIs.addAll(joinValuesObjectInspectors[alias]);
        // for each alias, add object inspector for short as the last element
        rcOIs.add(
            PrimitiveObjectInspectorFactory.writableShortObjectInspector);
        rowContainerObjectInspectors[alias] = rcOIs;
      }
      rowContainerStandardObjectInspectors =
        JoinUtil.getStandardObjectInspectors(rowContainerObjectInspectors, NOTSKIPBIGTABLE, tagLen);
    }

    dummyObj = new ArrayList[numAliases];
    dummyObjVectors = new RowContainer[numAliases];

    joinEmitInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVE_JOIN_EMIT_INTERVAL);
    joinCacheSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVE_JOIN_CACHE_SIZE);

    logEveryNRows = HiveConf.getLongVar(hconf,
        HiveConf.ConfVars.HIVE_LOG_N_RECORDS);

    // construct dummy null row (indicating empty table) and
    // construct spill table serde which is used if input is too
    // large to fit into main memory.
    byte pos = 0;
    for (Byte alias : order) {
      int sz = conf.getExprs().get(alias).size();
      ArrayList<Object> nr = new ArrayList<Object>(sz);

      for (int j = 0; j < sz; j++) {
        nr.add(null);
      }

      if (!noOuterJoin) {
        // add whether the row is filtered or not
        // this value does not matter for the dummyObj
        // because the join values are already null
        nr.add(new ShortWritable());
      }
      dummyObj[pos] = nr;
      // there should be only 1 dummy object in the RowContainer
      RowContainer<List<Object>> values = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors[pos],
          alias, 1, spillTableDesc, conf, !hasFilter(pos), reporter);

      values.addRow(dummyObj[pos]);
      dummyObjVectors[pos] = values;

      // if serde is null, the input doesn't need to be spilled out
      // e.g., the output columns does not contains the input table
      RowContainer<List<Object>> rc = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors[pos],
          alias, joinCacheSize, spillTableDesc, conf, !hasFilter(pos), reporter);
      storage[pos] = rc;

      pos++;
    }

    forwardCache = new Object[totalSz];
    aliasFilterTags = new short[numAliases];
    Arrays.fill(aliasFilterTags, (byte)0xff);
    aliasFilterTagsNext = new short[numAliases];
    Arrays.fill(aliasFilterTagsNext, (byte) 0xff);

    filterTags = new short[numAliases];
    skipVectors = new boolean[numAliases][];
    for(int i = 0; i < skipVectors.length; i++) {
      skipVectors[i] = new boolean[i + 1];
    }
    intermediate = new List[numAliases];

    offsets = new int[numAliases + 1];
    int sum = 0;
    for (int i = 0; i < numAliases; i++) {
      offsets[i] = sum;
      sum += joinValues[order[i]].size();
    }
    offsets[numAliases] = sum;

    outputObjInspector = getJoinOutputObjectInspector(order,
        joinValuesStandardObjectInspectors, conf);

    for( int i = 0; i < condn.length; i++ ) {
      if(condn[i].getType() == JoinDesc.LEFT_SEMI_JOIN) {
        hasLeftSemiJoin = true;
      } else if(condn[i].getType() == JoinDesc.ANTI_JOIN) {
        hasLeftAntiSemiJoin = true;
      }
    }

    // Create post-filtering evaluators if needed
    if (conf.getResidualFilterExprs() != null) {
      residualJoinFilters = new ArrayList<>(conf.getResidualFilterExprs().size());
      residualJoinFiltersOIs = new ArrayList<>(conf.getResidualFilterExprs().size());
      for (int i = 0; i < conf.getResidualFilterExprs().size(); i++) {
        ExprNodeDesc expr = conf.getResidualFilterExprs().get(i);
        residualJoinFilters.add(ExprNodeEvaluatorFactory.get(expr));
        residualJoinFiltersOIs.add(
                residualJoinFilters.get(i).initialize(outputObjInspector));
      }
      needsPostEvaluation = true;
      if (!noOuterJoin) {
        // We need to disable join emit interval, since for outer joins with post conditions
        // we need to have the full view on the right matching rows to know whether we need
        // to produce a row with NULL values or not
        joinEmitInterval = -1;
      }
    }

    LOG.info("JOIN " + outputObjInspector.getTypeName() + " totalsz = " + totalSz);
  }

  transient boolean newGroupStarted = false;

  @Override
  public void startGroup() throws HiveException {
    newGroupStarted = true;
    for (AbstractRowContainer<List<Object>> alw : storage) {
      alw.clearRows();
    }
    super.startGroup();
  }

  /**
   * Determine the frequency with which to emit a log message instead of
   * one for every for every event.
   *
   * @param sz The current number of events
   * @return The next event count to emit a log message
   */
  protected long getNextSize(long sz) {
    Preconditions.checkArgument(sz >= 0L);
    // If no logging is configured, log every 1, 10, 100, 1000, ..., 100000
    if (this.logEveryNRows == 0L) {
      final long next = (long) Math.pow(10.0, Math.ceil(Math.log10(sz + 1)));
      return Math.min(100000L, next);
    }
    // Log every N rows
    return ((sz / this.logEveryNRows) + 1L) * this.logEveryNRows;
  }

  protected transient Byte alias;
  protected transient Object[] forwardCache;

  // pre-calculated offset values for each alias
  protected transient int[] offsets;

  // a array of bitvectors where each entry denotes whether the element is to
  // be used or not (whether it is null or not). The size of the bitvector is
  // same as the number of inputs(aliases) under consideration currently.
  // When all inputs are accounted for, the output is forwarded appropriately.
  protected transient boolean[][] skipVectors;

  // caches objects before constructing forward cache
  protected transient List[] intermediate;

  // filter tags for objects
  protected transient short[] filterTags;

  /**
   * On filterTags
   *
   * ANDed value of all filter tags in current join group
   * if any of values passes on outer join alias (which makes zero for the tag alias),
   * it means there exists a pair for it and safely regarded as a inner join
   *
   * for example, with table a, b something like,
   *   a = 100, 10 | 100, 20 | 100, 30
   *   b = 100, 10 | 100, 20 | 100, 30
   *
   * the query "a FO b ON a.k=b.k AND a.v&gt;10 AND b.v&gt;30" makes filter map
   *   0(a) = [1(b),1] : a.v&gt;10
   *   1(b) = [0(a),1] : b.v&gt;30
   *
   * for filtered rows in a (100,10) create a-NULL
   * for filtered rows in b (100,10) (100,20) (100,30) create NULL-b
   *
   * with 0(a) = [1(b),1] : a.v&gt;10
   *   100, 10 = 00000010 (filtered)
   *   100, 20 = 00000000 (valid)
   *   100, 30 = 00000000 (valid)
   * -------------------------
   *       sum = 00000000 : for valid rows in b, there is at least one pair in a
   *
   * with 1(b) = [0(a),1] : b.v&gt;30
   *   100, 10 = 00000001 (filtered)
   *   100, 20 = 00000001 (filtered)
   *   100, 30 = 00000001 (filtered)
   * -------------------------
   *       sum = 00000001 : for valid rows in a (100,20) (100,30), there is no pair in b
   *
   * result :
   *   100, 10 :   N,  N
   *     N,  N : 100, 10
   *     N,  N : 100, 20
   *     N,  N : 100, 30
   *   100, 20 :   N,  N
   *   100, 30 :   N,  N
   */
  protected transient short[] aliasFilterTags;
  protected transient short[] aliasFilterTagsNext;

  // all evaluation should be processed here for valid aliasFilterTags
  //
  // for MapJoin, filter tag is pre-calculated in MapredLocalTask and stored with value.
  // when reading the hashtable, MapJoinObjectValue calculates alias filter and provide it to join
  protected List<Object> getFilteredValue(byte alias, Object row) throws HiveException {
    boolean hasFilter = hasFilter(alias);
    List<Object> nr = JoinUtil.computeValues(row, joinValues[alias],
        joinValuesObjectInspectors[alias], hasFilter);
    if (hasFilter) {
      short filterTag = JoinUtil.isFiltered(row, joinFilters[alias],
          joinFilterObjectInspectors[alias], filterMaps[alias]);
      nr.add(new ShortWritable(filterTag));
    }
    return nr;
  }

  protected void addToAliasFilterTags(byte alias, List<Object> object, boolean isNextGroup) {
    boolean hasFilter = hasFilter(alias);
    if (hasFilter) {
      if (isNextGroup) {
        aliasFilterTagsNext[alias] &= ((ShortWritable) (object.get(object.size() - 1))).get();
      } else {
        aliasFilterTags[alias] &= ((ShortWritable) (object.get(object.size() - 1))).get();
      }
    }
  }

  private void createForwardJoinObjectForAntiJoin(boolean[] skip) throws HiveException {
    boolean forward = fillForwardCache(skip);
    if (forward) {
      internalForward(forwardCache, outputObjInspector);
      countAfterReport = 0;
    }
  }

  // fill forwardCache with skipvector
  private boolean fillForwardCache(boolean[] skip) {
    Arrays.fill(forwardCache, null);
    boolean forward = false;
    for (int i = 0; i < numAliases; i++) {
      if (!skip[i]) {
        for (int j = offsets[i]; j < offsets[i + 1]; j++) {
          forwardCache[j] = intermediate[i].get(j - offsets[i]);
        }
        forward = true;
      }
    }
    return forward;
  }

  // returns whether a record was forwarded
  private boolean createForwardJoinObject(boolean[] skip, boolean antiJoin) throws HiveException {
    boolean forward = fillForwardCache(skip);
    if (forward) {
      if (needsPostEvaluation) {
        forward = !JoinUtil.isFiltered(forwardCache, residualJoinFilters, residualJoinFiltersOIs);
      }

      // For anti join, check all right side and if nothing is matched then only forward.
      if (forward && !antiJoin) {
        // If it is not an outer join, or the post-condition filters
        // are empty or the row passed them
        internalForward(forwardCache, outputObjInspector);
        countAfterReport = 0;
      }
    }

    return forward;
  }

  // entry point (aliasNum = 0)
  private void genJoinObject() throws HiveException {
    if (needsPostEvaluation && 0 == numAliases - 2) {
      int nextType = condn[0].getType();
      if (nextType == JoinDesc.RIGHT_OUTER_JOIN || nextType == JoinDesc.FULL_OUTER_JOIN) {
        // Initialize container to use for storing tuples before emitting them
        rowContainerPostFilteredOuterJoin = new HashMap<>();
      }
    }

    boolean rightFirst = true;
    boolean hasFilter = hasFilter(order[0]);
    AbstractRowContainer.RowIterator<List<Object>> iter = storage[order[0]].rowIter();
    for (List<Object> rightObj = iter.first(); rightObj != null; rightObj = iter.next()) {
      boolean rightNull = rightObj == dummyObj[0];
      if (hasFilter) {
        filterTags[0] = getFilterTag(rightObj);
      }
      skipVectors[0][0] = rightNull;
      intermediate[0] = rightObj;

      genObject(1, rightFirst, rightNull);
      rightFirst = false;
    }

    // Consolidation for outer joins
    if (needsPostEvaluation && 0 == numAliases - 2) {
      int nextType = condn[0].getType();
      if (nextType == JoinDesc.RIGHT_OUTER_JOIN || nextType == JoinDesc.FULL_OUTER_JOIN) {
        // If it is a RIGHT / FULL OUTER JOIN, we need to iterate through the row container
        // that contains all the right records that did not produce results. Then, for each
        // of those records, we replace the left side with NULL values, and produce the
        // records.
        // Observe that we only enter this block when we have finished iterating through
        // all the left and right records (aliasNum == numAliases - 2), and thus, we have
        // tried to evaluate the post-filter condition on every possible combination.
        // NOTE: the left records that do not produce results (for LEFT / FULL OUTER JOIN)
        // will always be caught in the genObject method
        Arrays.fill(forwardCache, null);
        for (Object[] row : rowContainerPostFilteredOuterJoin.values()) {
          if (row == null) {
            continue;
          }
          System.arraycopy(row, 0, forwardCache, offsets[numAliases - 1], row.length);
          internalForward(forwardCache, outputObjInspector);
          countAfterReport = 0;
        }
      }
    }
  }

  // creates objects in recursive manner
  private void genObject(int aliasNum, boolean allLeftFirst, boolean allLeftNull)
      throws HiveException {
    JoinCondDesc joinCond = condn[aliasNum - 1];
    int type = joinCond.getType();
    int left = joinCond.getLeft();
    int right = joinCond.getRight();

    if (needsPostEvaluation && aliasNum == numAliases - 2) {
      int nextType = condn[aliasNum].getType();
      if (nextType == JoinDesc.RIGHT_OUTER_JOIN || nextType == JoinDesc.FULL_OUTER_JOIN) {
        // Initialize container to use for storing tuples before emitting them
        rowContainerPostFilteredOuterJoin = new HashMap<>();
      }
    }

    boolean[] skip = skipVectors[aliasNum];
    boolean[] prevSkip = skipVectors[aliasNum - 1];

    // search for match in the rhs table
    AbstractRowContainer<List<Object>> aliasRes = storage[order[aliasNum]];

    boolean needToProduceLeftRow = false;
    boolean producedRow = false;
    boolean done = false;
    boolean loopAgain = false;
    boolean tryLOForFO = type == JoinDesc.FULL_OUTER_JOIN;

    boolean rightFirst = true;
    AbstractRowContainer.RowIterator<List<Object>> iter = aliasRes.rowIter();
    int pos = 0;

    for (List<Object> rightObj = iter.first(); !done && rightObj != null;
         rightObj = loopAgain ? rightObj : iter.next(), rightFirst = loopAgain = false, pos++) {
      // Keep a copy of the skip vector and update the bit for current alias only in the loop.
      System.arraycopy(prevSkip, 0, skip, 0, prevSkip.length);
      boolean rightNull = rightObj == dummyObj[aliasNum];
      if (hasFilter(order[aliasNum])) {
        filterTags[aliasNum] = getFilterTag(rightObj);
      }
      skip[right] = rightNull;

      if (type == JoinDesc.INNER_JOIN) {
        innerJoin(skip, left, right);
      } else if (type == JoinDesc.LEFT_SEMI_JOIN) {
        if (innerJoin(skip, left, right)) {
          // if left-semi-join found a match and we do not have any additional predicates,
          // skipping the rest of the rows in the rhs table of the semijoin
          done = !needsPostEvaluation;
        }
      } else if (type == JoinDesc.ANTI_JOIN) {
        if (innerJoin(skip, left, right)) {
          // if inner join found a match then the condition is not matched for anti join, so we can skip rest of the
          // record. But if there is some post evaluation we have to handle that.
          done = !needsPostEvaluation;
        }
      } else if (type == JoinDesc.LEFT_OUTER_JOIN ||
          (type == JoinDesc.FULL_OUTER_JOIN && rightNull)) {
        int result = leftOuterJoin(skip, left, right);
        if (result < 0) {
          continue;
        }
        done = result > 0;
      } else if (type == JoinDesc.RIGHT_OUTER_JOIN ||
          (type == JoinDesc.FULL_OUTER_JOIN && allLeftNull)) {
        if (allLeftFirst && !rightOuterJoin(skip, left, right) ||
          !allLeftFirst && !innerJoin(skip, left, right)) {
          continue;
        }
      } else if (type == JoinDesc.FULL_OUTER_JOIN) {
        if (tryLOForFO && leftOuterJoin(skip, left, right) > 0) {
          loopAgain = allLeftFirst;
          done = !loopAgain;
          tryLOForFO = false;
        } else if (allLeftFirst && !rightOuterJoin(skip, left, right) ||
          !allLeftFirst && !innerJoin(skip, left, right)) {
          continue;
        }
      }
      intermediate[aliasNum] = rightObj;

      if (aliasNum == numAliases - 1) {
        if (!(allLeftNull && rightNull)) {
          needToProduceLeftRow = true;
          if (needsPostEvaluation) {
            // This is only executed for outer joins with residual filters
            boolean forward = createForwardJoinObject(skipVectors[numAliases - 1],
                                                    type == JoinDesc.ANTI_JOIN);
            producedRow |= forward;
            done = (type == JoinDesc.LEFT_SEMI_JOIN) && forward;
            if (!rightNull &&
                    (type == JoinDesc.RIGHT_OUTER_JOIN || type == JoinDesc.FULL_OUTER_JOIN)) {
              if (forward) {
                // This record produced a result this time, remove it from the storage
                // as it will not need to produce a result with NULL values anymore
                rowContainerPostFilteredOuterJoin.put(pos, null);
              } else {
                // We need to store this record (if it is not done yet) in case
                // we should produce a result
                if (!rowContainerPostFilteredOuterJoin.containsKey(pos)) {
                  Object[] row = Arrays.copyOfRange(forwardCache, offsets[aliasNum], offsets[aliasNum + 1]);
                  rowContainerPostFilteredOuterJoin.put(pos, row);
                }
              }
            }
          } else {
            createForwardJoinObject(skipVectors[numAliases - 1], type == JoinDesc.ANTI_JOIN);
          }
        }
      } else {
        // recursively call the join the other rhs tables
        genObject(aliasNum + 1, allLeftFirst && rightFirst, allLeftNull && rightNull);
      }
    }

    // For anti join, we should proceed to emit records if the right side is empty or not matching.
    if (type == JoinDesc.ANTI_JOIN && !producedRow) {
      System.arraycopy(prevSkip, 0, skip, 0, prevSkip.length);
      skip[right] = true;
      if (aliasNum == numAliases - 1) {
        createForwardJoinObjectForAntiJoin(skipVectors[numAliases - 1]);
      } else {
        genObject(aliasNum + 1, allLeftFirst, allLeftNull);
      }
    }

    // Consolidation for outer joins
    if (needsPostEvaluation && aliasNum == numAliases - 1 &&
            needToProduceLeftRow && !producedRow && !allLeftNull) {
      if (type == JoinDesc.LEFT_OUTER_JOIN || type == JoinDesc.FULL_OUTER_JOIN) {
        // If it is a LEFT / FULL OUTER JOIN and the left record did not produce
        // results, we need to take that record, replace the right side with NULL
        // values, and produce the records
        int i = numAliases - 1;
        for (int j = offsets[i]; j < offsets[i + 1]; j++) {
          forwardCache[j] = null;
        }
        internalForward(forwardCache, outputObjInspector);
        countAfterReport = 0;
      }
    } else if (needsPostEvaluation && aliasNum == numAliases - 2) {
      int nextType = condn[aliasNum].getType();
      if (nextType == JoinDesc.RIGHT_OUTER_JOIN || nextType == JoinDesc.FULL_OUTER_JOIN) {
        // If it is a RIGHT / FULL OUTER JOIN, we need to iterate through the row container
        // that contains all the right records that did not produce results. Then, for each
        // of those records, we replace the left side with NULL values, and produce the
        // records.
        // Observe that we only enter this block when we have finished iterating through
        // all the left and right records (aliasNum == numAliases - 2), and thus, we have
        // tried to evaluate the post-filter condition on every possible combination.
        Arrays.fill(forwardCache, null);
        for (Object[] row : rowContainerPostFilteredOuterJoin.values()) {
          if (row == null) {
            continue;
          }
          System.arraycopy(row, 0, forwardCache, offsets[numAliases - 1], row.length);
          internalForward(forwardCache, outputObjInspector);
          countAfterReport = 0;
        }
      }
    }
  }

  // inner join
  private boolean innerJoin(boolean[] skip, int left, int right) {
    if (!isInnerJoin(skip, left, right)) {
      Arrays.fill(skip, true);
      return false;
    }
    return true;
  }

  // LO
  //
  // LEFT\RIGHT   skip  filtered   valid
  // skip        --(1)     --(1)    --(1)
  // filtered    +-(1)     +-(1)    +-(1)
  // valid       +-(1)     +-(4*)   ++(2)
  //
  // * If right alias has any pair for left alias, continue (3)
  // -1 for continue : has pair but not in this turn
  //  0 for inner join (++) : join and continue LO
  //  1 for left outer join (+-) : join and skip further LO
  private int leftOuterJoin(boolean[] skip, int left, int right) {
    if (skip[left] || skip[right] || !isLeftValid(left, right)) {
      skip[right] = true;
      return 1;   // case 1
    }
    if (isRightValid(left, right)) {
      return 0;   // case 2
    }
    if (hasRightPairForLeft(left, right)) {
      return -1;  // case 3
    }
    skip[right] = true;
    return 1;     // case 4
  }

  // RO
  //
  // LEFT\RIGHT   skip  filtered   valid
  // skip        --(1)     -+(1)   -+(1)
  // filtered    --(1)     -+(1)   -+(4*)
  // valid       --(1)     -+(1)   ++(2)
  //
  // * If left alias has any pair for right alias, continue (3)
  // false for continue : has pair but not in this turn
  private boolean rightOuterJoin(boolean[] skip, int left, int right) {
    if (skip[left] || skip[right] || !isRightValid(left, right)) {
      Arrays.fill(skip, 0, right, true);
      return true;  // case 1
    }
    if (isLeftValid(left, right)) {
      return true;  // case 2
    }
    if (hasLeftPairForRight(left, right)) {
      return false; // case 3
    }
    Arrays.fill(skip, 0, right, true);
    return true;    // case 4
  }

  // If left and right aliases are all valid, two values will be inner joined,
  private boolean isInnerJoin(boolean[] skip, int left, int right) {
    return !skip[left] && !skip[right] &&
        isLeftValid(left, right) && isRightValid(left, right);
  }

  // check if left is valid
  private boolean isLeftValid(int left, int right) {
    return !hasFilter(left) || !JoinUtil.isFiltered(filterTags[left], right);
  }

  // check if right is valid
  private boolean isRightValid(int left, int right) {
    return !hasFilter(right) || !JoinUtil.isFiltered(filterTags[right], left);
  }

  // check if any left pair exists for right objects
  private boolean hasLeftPairForRight(int left, int right) {
    return !JoinUtil.isFiltered(aliasFilterTags[left], right);
  }

  // check if any right pair exists for left objects
  private boolean hasRightPairForLeft(int left, int right) {
    return !JoinUtil.isFiltered(aliasFilterTags[right], left);
  }

  private boolean hasAnyFiltered(int alias, List<Object> row) {
    if (row == dummyObj[alias]) {
      return true;
    }
    if (hasFilter(alias) && row != null) {
      ShortWritable shortWritable = (ShortWritable) row.get(row.size() - 1);
      if (shortWritable != null) {
        return JoinUtil.hasAnyFiltered(shortWritable.get());
      }
    }
    return false;
  }

  protected final boolean hasFilter(int alias) {
    return filterMaps != null && filterMaps[alias] != null;
  }

  // get tag value from object (last of list)
  protected final short getFilterTag(List<Object> row) {
    return ((ShortWritable) row.get(row.size() - 1)).get();
  }

  /**
   * Forward a record of join results.
   *
   * @throws HiveException
   */
  @Override
  public void endGroup() throws HiveException {
    checkAndGenObject();
  }

  protected void internalForward(Object row, ObjectInspector outputOI) throws HiveException {
    forward(row, outputOI);
  }

  private void genUniqueJoinObject(int aliasNum, int forwardCachePos)
      throws HiveException {
    AbstractRowContainer.RowIterator<List<Object>> iter = storage[order[aliasNum]].rowIter();
    for (List<Object> row = iter.first(); row != null; row = iter.next()) {
      reportProgress();
      int sz = joinValues[order[aliasNum]].size();
      int p = forwardCachePos;
      for (int j = 0; j < sz; j++) {
        forwardCache[p++] = row.get(j);
      }
      if (aliasNum == numAliases - 1) {
        internalForward(forwardCache, outputObjInspector);
        countAfterReport = 0;
      } else {
        genUniqueJoinObject(aliasNum + 1, p);
      }
    }
  }

  private void genAllOneUniqueJoinObject()
      throws HiveException {
    int p = 0;
    for (int i = 0; i < numAliases; i++) {
      int sz = joinValues[order[i]].size();
      List<Object> obj = storage[order[i]].rowIter().first();
      for (int j = 0; j < sz; j++) {
        forwardCache[p++] = obj.get(j);
      }
    }
    internalForward(forwardCache, outputObjInspector);
    countAfterReport = 0;
  }

  protected void checkAndGenObject() throws HiveException {
    if (closeOpCalled) {
      LOG.warn("checkAndGenObject is called after operator " +
          id + " " + getName() + " called closeOp");
      return;
    }

    if (condn[0].getType() == JoinDesc.UNIQUE_JOIN) {

      // Check if results need to be emitted.
      // Results only need to be emitted if there is a non-null entry in a table
      // that is preserved or if there are no non-null entries
      boolean preserve = false; // Will be true if there is a non-null entry
      // in a preserved table
      boolean hasNulls = false; // Will be true if there are null entries
      boolean allOne = true;
      for (int i = 0; i < numAliases; i++) {
        Byte alias = order[i];
        AbstractRowContainer<List<Object>> alw = storage[alias];

        if (!alw.isSingleRow()) {
          allOne = false;
        }

        if (!alw.hasRows()) {
          alw.addRow(dummyObj[i]);
          hasNulls = true;
        } else if (condn[i].getPreserved()) {
          preserve = true;
        }
      }

      if (hasNulls && !preserve) {
        return;
      }

      if (allOne) {
        genAllOneUniqueJoinObject();
      } else {
        genUniqueJoinObject(0, 0);
      }
    } else {
      // does any result need to be emitted
      boolean mayHasMoreThanOne = false;
      boolean hasEmpty = false;
      for (int i = 0; i < numAliases; i++) {
        Byte alias = order[i];
        AbstractRowContainer<List<Object>> alw = storage[alias];
        boolean isRightOfAntiJoin = (i != 0 && condn[i-1].getType() == JoinDesc.ANTI_JOIN);

        if (noOuterJoin) {
          if (!alw.hasRows()) {
            if (!isRightOfAntiJoin) {
              // For anti join the right side can be empty.
              return;
            }
          } else if (isRightOfAntiJoin && !needsPostEvaluation)  {
            // For anti join the right side should be empty. For needsPostEvaluation case we will
            // wait till evaluation is done. For other cases we can directly return from here.
            return;
          } else if (!alw.isSingleRow()) {
            mayHasMoreThanOne = true;
          }
        } else {
          if (!alw.hasRows()) {
            hasEmpty = true;
            alw.addRow(dummyObj[i]);
          } else if (!hasEmpty && alw.isSingleRow()) {
            if (hasAnyFiltered(alias, alw.rowIter().first())) {
              hasEmpty = true;
            }
          } else {
            mayHasMoreThanOne = true;
            if (!hasEmpty) {
              AbstractRowContainer.RowIterator<List<Object>> iter = alw.rowIter();
              for (List<Object> row = iter.first(); row != null; row = iter.next()) {
                reportProgress();
                if (hasAnyFiltered(alias, row)) {
                  hasEmpty = true;
                  break;
                }
              }
            }
          }
        }
      }

      if (!needsPostEvaluation && !hasEmpty && !mayHasMoreThanOne) {
        genAllOneUniqueJoinObject();
      } else if (!needsPostEvaluation && !hasEmpty && !hasLeftSemiJoin && !hasLeftAntiSemiJoin) {
        genUniqueJoinObject(0, 0);
      } else {
        genJoinObject();
      }
    }
    System.arraycopy(aliasFilterTagsNext, 0, aliasFilterTags, 0, aliasFilterTagsNext.length);
    Arrays.fill(aliasFilterTagsNext, (byte) 0xff);
  }

  protected void reportProgress() {
    // Send some status periodically
    countAfterReport++;

    if ((countAfterReport % heartbeatInterval) == 0
        && (reporter != null)) {
      reporter.progress();
      countAfterReport = 0;
    }
  }

  /**
   * All done.
   *
   */
  @Override
  public void closeOp(boolean abort) throws HiveException {
    closeOpCalled = true;
    for (AbstractRowContainer<List<Object>> alw : storage) {
      if (alw != null) {
        alw.clearRows(); // clean up the temp files
      }
    }
    Arrays.fill(storage, null);
    super.closeOp(abort);
  }

  @Override
  public String getName() {
    return CommonJoinOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "JOIN";
  }

  /**
   * @return the posToAliasMap
   */
  public Map<Integer, Set<String>> getPosToAliasMap() {
    return posToAliasMap;
  }

  /**
   * @param posToAliasMap
   *          the posToAliasMap to set
   */
  public void setPosToAliasMap(Map<Integer, Set<String>> posToAliasMap) {
    this.posToAliasMap = posToAliasMap;
  }

  @Override
  public boolean opAllowedBeforeMapJoin() {
    return false;
  }

  @Override
  public boolean opAllowedAfterMapJoin() {
    return false;
  }
}
