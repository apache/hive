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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Join operator implementation.
 */
public abstract class CommonJoinOperator<T extends JoinDesc> extends
    Operator<T> implements Serializable {
  private static final long serialVersionUID = 1L;
  protected static final Log LOG = LogFactory.getLog(CommonJoinOperator.class
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

  transient boolean handleSkewJoin = false;

  transient boolean hasLeftSemiJoin = false;

  protected transient int countAfterReport;
  protected transient int heartbeatInterval;
  protected static final int NOTSKIPBIGTABLE = -1;

  public CommonJoinOperator() {
  }

  public CommonJoinOperator(CommonJoinOperator<T> clone) {
    this.joinEmitInterval = clone.joinEmitInterval;
    this.joinCacheSize = clone.joinCacheSize;
    this.nextSz = clone.nextSz;
    this.childOperators = clone.childOperators;
    this.parentOperators = clone.parentOperators;
    this.counterNames = clone.counterNames;
    this.counterNameToEnum = clone.counterNameToEnum;
    this.done = clone.done;
    this.operatorId = clone.operatorId;
    this.storage = clone.storage;
    this.condn = clone.condn;
    this.conf = clone.getConf();
    this.setSchema(clone.getSchema());
    this.alias = clone.alias;
    this.beginTime = clone.beginTime;
    this.inputRows = clone.inputRows;
    this.childOperatorsArray = clone.childOperatorsArray;
    this.childOperatorsTag = clone.childOperatorsTag;
    this.colExprMap = clone.colExprMap;
    this.counters = clone.counters;
    this.dummyObj = clone.dummyObj;
    this.dummyObjVectors = clone.dummyObjVectors;
    this.forwardCache = clone.forwardCache;
    this.groupKeyObject = clone.groupKeyObject;
    this.handleSkewJoin = clone.handleSkewJoin;
    this.hconf = clone.hconf;
    this.id = clone.id;
    this.inputObjInspectors = clone.inputObjInspectors;
    this.inputRows = clone.inputRows;
    this.noOuterJoin = clone.noOuterJoin;
    this.numAliases = clone.numAliases;
    this.operatorId = clone.operatorId;
    this.posToAliasMap = clone.posToAliasMap;
    this.spillTableDesc = clone.spillTableDesc;
    this.statsMap = clone.statsMap;
    this.joinFilters = clone.joinFilters;
    this.joinFilterObjectInspectors = clone.joinFilterObjectInspectors;
  }


  protected static <T extends JoinDesc> ObjectInspector getJoinOutputObjectInspector(
      Byte[] order, List<ObjectInspector>[] aliasToObjectInspectors,
      T conf) {
    List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    for (Byte alias : order) {
      List<ObjectInspector> oiList = aliasToObjectInspectors[alias];
      if (oiList != null) {
        structFieldObjectInspectors.addAll(oiList);
      }
    }

    StructObjectInspector joinOutputObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf.getOutputColumnNames(),
        structFieldObjectInspectors);
    return joinOutputObjectInspector;
  }

  protected Configuration hconf;

  @Override
  @SuppressWarnings("unchecked")
  protected void initializeOp(Configuration hconf) throws HiveException {
    this.handleSkewJoin = conf.getHandleSkewJoin();
    this.hconf = hconf;

    heartbeatInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVESENDHEARTBEAT);
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
        order,NOTSKIPBIGTABLE);

    //process join filters
    joinFilters = new List[tagLen];
    JoinUtil.populateJoinKeyValue(joinFilters, conf.getFilters(),order,NOTSKIPBIGTABLE);


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
        JoinUtil.getStandardObjectInspectors(rowContainerObjectInspectors,NOTSKIPBIGTABLE, tagLen);
    }

    dummyObj = new ArrayList[numAliases];
    dummyObjVectors = new RowContainer[numAliases];

    joinEmitInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEJOINEMITINTERVAL);
    nextSz = joinEmitInterval;
    joinCacheSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEJOINCACHESIZE);

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

      values.add(dummyObj[pos]);
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
      }
    }

    LOG.info("JOIN " + outputObjInspector.getTypeName() + " totalsz = " + totalSz);
  }

  transient boolean newGroupStarted = false;

  @Override
  public void startGroup() throws HiveException {
    LOG.trace("Join: Starting new group");
    newGroupStarted = true;
    for (AbstractRowContainer<List<Object>> alw : storage) {
      alw.clear();
    }
    super.startGroup();
  }

  protected long getNextSize(long sz) {
    // A very simple counter to keep track of join entries for a key
    if (sz >= 100000) {
      return sz + 100000;
    }

    return 2 * sz;
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
   * the query "a FO b ON a.k=b.k AND a.v>10 AND b.v>30" makes filter map
   *   0(a) = [1(b),1] : a.v>10
   *   1(b) = [0(a),1] : b.v>30
   *
   * for filtered rows in a (100,10) create a-NULL
   * for filtered rows in b (100,10) (100,20) (100,30) create NULL-b
   *
   * with 0(a) = [1(b),1] : a.v>10
   *   100, 10 = 00000010 (filtered)
   *   100, 20 = 00000000 (valid)
   *   100, 30 = 00000000 (valid)
   * -------------------------
   *       sum = 00000000 : for valid rows in b, there is at least one pair in a
   *
   * with 1(b) = [0(a),1] : b.v>30
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
      aliasFilterTags[alias] &= filterTag;
    }
    return nr;
  }

  // fill forwardCache with skipvector
  private void createForwardJoinObject(boolean[] skip) throws HiveException {
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
    if (forward) {
      internalForward(forwardCache, outputObjInspector);
      countAfterReport = 0;
    }
  }

  // entry point (aliasNum = 0)
  private void genJoinObject() throws HiveException {
    boolean rightFirst = true;
    boolean hasFilter = hasFilter(order[0]);
    AbstractRowContainer<List<Object>> aliasRes = storage[order[0]];
    for (List<Object> rightObj = aliasRes.first(); rightObj != null; rightObj = aliasRes.next()) {
      boolean rightNull = rightObj == dummyObj[0];
      if (hasFilter) {
        filterTags[0] = getFilterTag(rightObj);
      }
      skipVectors[0][0] = rightNull;
      intermediate[0] = rightObj;

      genObject(1, rightFirst, rightNull);
      rightFirst = false;
    }
  }

  // creates objects in recursive manner
  private void genObject(int aliasNum, boolean allLeftFirst, boolean allLeftNull)
      throws HiveException {
    if (aliasNum < numAliases) {

      boolean[] skip = skipVectors[aliasNum];
      boolean[] prevSkip = skipVectors[aliasNum - 1];

      JoinCondDesc joinCond = condn[aliasNum - 1];
      int type = joinCond.getType();
      int left = joinCond.getLeft();
      int right = joinCond.getRight();

      // search for match in the rhs table
      AbstractRowContainer<List<Object>> aliasRes = storage[order[aliasNum]];

      boolean done = false;
      boolean loopAgain = false;
      boolean tryLOForFO = type == JoinDesc.FULL_OUTER_JOIN;

      boolean rightFirst = true;
      for (List<Object> rightObj = aliasRes.first(); !done && rightObj != null;
           rightObj = loopAgain ? rightObj : aliasRes.next(), rightFirst = loopAgain = false) {
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
            // if left-semi-join found a match, skipping the rest of the rows in the
            // rhs table of the semijoin
            done = true;
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

        // recursively call the join the other rhs tables
        genObject(aliasNum + 1, allLeftFirst && rightFirst, allLeftNull && rightNull);
      }
    } else if (!allLeftNull) {
      createForwardJoinObject(skipVectors[numAliases - 1]);
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
    return row == dummyObj[alias] || hasFilter(alias) && JoinUtil.hasAnyFiltered(getFilterTag(row));
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
    LOG.trace("Join Op: endGroup called: numValues=" + numAliases);

    checkAndGenObject();
  }

  protected void internalForward(Object row, ObjectInspector outputOI) throws HiveException {
    forward(row, outputOI);
  }

  private void genUniqueJoinObject(int aliasNum, int forwardCachePos)
      throws HiveException {
    AbstractRowContainer<List<Object>> alias = storage[order[aliasNum]];
    for (List<Object> row = alias.first(); row != null; row = alias.next()) {
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
      List<Object> obj = storage[order[i]].first();
      for (int j = 0; j < sz; j++) {
        forwardCache[p++] = obj.get(j);
      }
    }

    internalForward(forwardCache, outputObjInspector);
    countAfterReport = 0;
  }

  protected void checkAndGenObject() throws HiveException {
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

        if (alw.size() != 1) {
          allOne = false;
        }

        if (alw.size() == 0) {
          alw.add(dummyObj[i]);
          hasNulls = true;
        } else if (condn[i].getPreserved()) {
          preserve = true;
        }
      }

      if (hasNulls && !preserve) {
        return;
      }

      if (allOne) {
        LOG.info("calling genAllOneUniqueJoinObject");
        genAllOneUniqueJoinObject();
        LOG.info("called genAllOneUniqueJoinObject");
      } else {
        LOG.trace("calling genUniqueJoinObject");
        genUniqueJoinObject(0, 0);
        LOG.trace("called genUniqueJoinObject");
      }
    } else {
      // does any result need to be emitted
      boolean mayHasMoreThanOne = false;
      boolean hasEmpty = false;
      for (int i = 0; i < numAliases; i++) {
        Byte alias = order[i];
        AbstractRowContainer<List<Object>> alw = storage[alias];

        if (noOuterJoin) {
          if (alw.size() == 0) {
            LOG.trace("No data for alias=" + i);
            return;
          } else if (alw.size() > 1) {
            mayHasMoreThanOne = true;
          }
        } else {
          if (alw.size() == 0) {
            hasEmpty = true;
            alw.add(dummyObj[i]);
          } else if (!hasEmpty && alw.size() == 1) {
            if (hasAnyFiltered(alias, alw.first())) {
              hasEmpty = true;
            }
          } else {
            mayHasMoreThanOne = true;
            if (!hasEmpty) {
              for (List<Object> row = alw.first(); row != null; row = alw.next()) {
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

      if (!hasEmpty && !mayHasMoreThanOne) {
        LOG.trace("calling genAllOneUniqueJoinObject");
        genAllOneUniqueJoinObject();
        LOG.trace("called genAllOneUniqueJoinObject");
      } else if (!hasEmpty && !hasLeftSemiJoin) {
        LOG.trace("calling genUniqueJoinObject");
        genUniqueJoinObject(0, 0);
        LOG.trace("called genUniqueJoinObject");
      } else {
        LOG.trace("calling genObject");
        genJoinObject();
        LOG.trace("called genObject");
      }
    }
    Arrays.fill(aliasFilterTags, (byte)0xff);
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
    LOG.trace("Join Op close");
    for (AbstractRowContainer<List<Object>> alw : storage) {
      if (alw != null) {
        alw.clear(); // clean up the temp files
      }
    }
    Arrays.fill(storage, null);
  }

  @Override
  public String getName() {
    return getOperatorName();
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
