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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TezDummyStoreOperator;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests merge-join reducer estimates and traits in {@link GenTezUtils#createReduceWork}.
 * Sibling {@link ReduceWork}s are combined into one Tez vertex, so aligning their estimates
 * must not change input hashing or disable that vertex's adaptive parallelism.
 */
public class TestGenTezUtilsMergeJoinNumReducers {

  private static final int BIG_TABLE_NUM_REDUCERS = 5;
  private static final int SMALL_TABLE_NUM_REDUCERS = 20;

  private GenTezProcContext ctx;
  private TezWork tezWork;

  private ReduceSinkOperator rsBigTable;
  private GroupByOperator groupBy;

  private ReduceSinkOperator rsSmallTable;
  private TezDummyStoreOperator dummyStore;

  @Before
  public void setUp() {
    HiveConf conf = new HiveConfForTest(getClass());
    SessionState.start(conf);

    ParseContext pctx = new ParseContext();
    pctx.setContext(new Context(conf));

    ctx = new GenTezProcContext(conf, pctx, Collections.EMPTY_LIST,
        new ArrayList<Task<?>>(), Collections.EMPTY_SET, Collections.EMPTY_SET);

    tezWork = new TezWork("test");

    CompilationOpContext cCtx = new CompilationOpContext();

    rsBigTable = newReduceSink(cCtx, BIG_TABLE_NUM_REDUCERS);
    groupBy = new GroupByOperator(cCtx);
    groupBy.setConf(new GroupByDesc());
    groupBy.getConf().setKeys(new ArrayList<>());
    OperatorFactory.makeChild(rsBigTable, groupBy);

    rsSmallTable = newReduceSink(cCtx, SMALL_TABLE_NUM_REDUCERS);
    // AUTOPARALLEL marks this input's reducer count as an estimate rather than a fixed count.
    rsSmallTable.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    dummyStore = new TezDummyStoreOperator(cCtx);
    OperatorFactory.makeChild(rsSmallTable, dummyStore);

    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator(cCtx);
    groupBy.setChildOperators(newList(mergeJoin));
    dummyStore.setChildOperators(newList(mergeJoin));
    mergeJoin.setParentOperators(newList(groupBy, dummyStore));
  }

  @After
  public void tearDown() throws IOException {
    SessionState.get().close();
  }

  /**
   * Inputs represented by separate ReduceWorks start with different reducer estimates.
   * Normalize the estimates before the works are combined into the merge-join vertex.
   */
  @Test
  public void testSiblingReduceSinksFeedingMergeJoinGetSameNumReducers() {
    ctx.conf.setBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM, false);
    ReduceWork rwBigTable = createReduceWorkFor(rsBigTable, groupBy);

    assertEquals(SMALL_TABLE_NUM_REDUCERS, rsBigTable.getConf().getNumReducers());
    assertEquals(SMALL_TABLE_NUM_REDUCERS, rsSmallTable.getConf().getNumReducers());
    ReduceWork rwSmallTable = createReduceWorkFor(rsSmallTable, dummyStore);

    assertEquals(SMALL_TABLE_NUM_REDUCERS, rwBigTable.getNumReduceTasks().intValue());
    assertEquals(rwBigTable.getNumReduceTasks(), rwSmallTable.getNumReduceTasks());
  }

  @Test
  public void testSiblingReduceSinksAreNormalizedWhenDummyStoreIsVisitedFirst() {
    ctx.conf.setBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM, false);
    ReduceWork rwSmallTable = createReduceWorkFor(rsSmallTable, dummyStore);

    assertEquals(SMALL_TABLE_NUM_REDUCERS, rsBigTable.getConf().getNumReducers());
    assertEquals(SMALL_TABLE_NUM_REDUCERS, rsSmallTable.getConf().getNumReducers());
    ReduceWork rwBigTable = createReduceWorkFor(rsBigTable, groupBy);

    assertEquals(SMALL_TABLE_NUM_REDUCERS, rwSmallTable.getNumReduceTasks().intValue());
    assertEquals(rwSmallTable.getNumReduceTasks(), rwBigTable.getNumReduceTasks());
  }

  /**
   * Plain ReduceSink parents are handled as tags of one ReduceWork rather than as separate
   * ReduceWorks. Their counts and traits must remain untouched by this normalization.
   */
  @Test
  public void testMergeJoinWithoutDummyStoreSiblingIsNotNormalized() {
    CompilationOpContext cCtx = new CompilationOpContext();
    ReduceSinkOperator rsLeft = newReduceSink(cCtx, BIG_TABLE_NUM_REDUCERS);
    rsLeft.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    ReduceSinkOperator rsRight = newReduceSink(cCtx, SMALL_TABLE_NUM_REDUCERS);
    rsRight.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));

    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator(cCtx);
    rsLeft.setChildOperators(newList(mergeJoin));
    rsRight.setChildOperators(newList(mergeJoin));
    mergeJoin.setParentOperators(newList(rsLeft, rsRight));

    createReduceWorkFor(rsLeft, mergeJoin);

    assertEquals("numReducers must be untouched when there is no DummyStore sibling",
        BIG_TABLE_NUM_REDUCERS, rsLeft.getConf().getNumReducers());
    assertEquals("numReducers must be untouched when there is no DummyStore sibling",
        SMALL_TABLE_NUM_REDUCERS, rsRight.getConf().getNumReducers());
    assertTrue("AUTOPARALLEL must survive when there is no DummyStore sibling to protect against",
        rsLeft.getConf().getReducerTraits().contains(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    assertTrue("AUTOPARALLEL must survive when there is no DummyStore sibling to protect against",
        rsRight.getConf().getReducerTraits().contains(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
  }

  /**
   * A DummyStore sibling alone isn't sufficient to normalize: genuine bucketed SMB joins (e.g.
   * a 2-bucket table joined with a 3-bucket table) legitimately have different, FIXED
   * numReducers per side, each anchored to that side's own on-disk bucket layout. Neither side
   * carries AUTOPARALLEL/UNIFORM, so there is no runtime-drift risk - forcing them to match
   * would break the bucket-to-reducer correspondence the join relies on.
   */
  @Test
  public void testMergeJoinWithFixedDifferentBucketCountsIsNotNormalized() {
    CompilationOpContext cCtx = new CompilationOpContext();
    ReduceSinkOperator rsTwoBuckets = newReduceSink(cCtx, 2);
    rsTwoBuckets.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.FIXED));
    GroupByOperator bigTableSide = new GroupByOperator(cCtx);
    bigTableSide.setConf(new GroupByDesc());
    bigTableSide.getConf().setKeys(new ArrayList<>());
    OperatorFactory.makeChild(rsTwoBuckets, bigTableSide);

    ReduceSinkOperator rsThreeBuckets = newReduceSink(cCtx, 3);
    rsThreeBuckets.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.FIXED));
    TezDummyStoreOperator smallTableSide = new TezDummyStoreOperator(cCtx);
    OperatorFactory.makeChild(rsThreeBuckets, smallTableSide);

    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator(cCtx);
    bigTableSide.setChildOperators(newList(mergeJoin));
    smallTableSide.setChildOperators(newList(mergeJoin));
    mergeJoin.setParentOperators(newList(bigTableSide, smallTableSide));

    ReduceWork rwTwoBuckets = createReduceWorkFor(rsTwoBuckets, bigTableSide);
    ReduceWork rwThreeBuckets = createReduceWorkFor(rsThreeBuckets, smallTableSide);

    assertEquals("a FIXED bucket count must not be overwritten by a sibling's count",
        2L, (long) rwTwoBuckets.getNumReduceTasks());
    assertEquals("a FIXED bucket count must not be overwritten by a sibling's count",
        3L, (long) rwThreeBuckets.getNumReduceTasks());
  }

  /**
   * If one merge-join input's lineage is too deep for the search to resolve, normalizing only the
   * inputs that were found would leave the unresolved one with a different numReducers.
   * No input may be touched in that case.
   */
  @Test
  public void testUnresolvableSiblingLineageLeavesAllInputsUntouched() {
    ctx.conf.setBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM, false);
    CompilationOpContext cCtx = new CompilationOpContext();
    ReduceSinkOperator rsLeft = newReduceSink(cCtx, BIG_TABLE_NUM_REDUCERS);
    rsLeft.getConf().setReducerTraits(
        EnumSet.of(ReduceSinkDesc.ReducerTraits.UNIFORM, ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    GroupByOperator leftSide = newGroupBy(cCtx);
    OperatorFactory.makeChild(rsLeft, leftSide);

    ReduceSinkOperator rsRight = newReduceSink(cCtx, SMALL_TABLE_NUM_REDUCERS);
    rsRight.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    // Longer than the lineage search depth, so rsRight cannot be reached from the DummyStore.
    GroupByOperator rightRoot = newGroupBy(cCtx);
    OperatorFactory.makeChild(rsRight, rightRoot);
    Operator<?> rightSide = rightRoot;
    for (int i = 1; i < 10; i++) {
      GroupByOperator op = newGroupBy(cCtx);
      OperatorFactory.makeChild(rightSide, op);
      rightSide = op;
    }
    TezDummyStoreOperator rightDummyStore = new TezDummyStoreOperator(cCtx);
    OperatorFactory.makeChild(rightSide, rightDummyStore);

    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator(cCtx);
    leftSide.setChildOperators(newList(mergeJoin));
    rightDummyStore.setChildOperators(newList(mergeJoin));
    mergeJoin.setParentOperators(newList(leftSide, rightDummyStore));

    createReduceWorkFor(rsLeft, leftSide);

    assertTrue("UNIFORM must survive: clearing it changes the hash function used for this input",
        rsLeft.getConf().getReducerTraits().contains(ReduceSinkDesc.ReducerTraits.UNIFORM));
    assertTrue("AUTOPARALLEL must survive when the merge join's lineage is unresolved",
        rsLeft.getConf().getReducerTraits().contains(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    assertEquals("numReducers must be untouched when the merge join's lineage is unresolved",
        (long) BIG_TABLE_NUM_REDUCERS, (long) rsLeft.getConf().getNumReducers());
    assertEquals("numReducers must be untouched when the merge join's lineage is unresolved",
        (long) SMALL_TABLE_NUM_REDUCERS, (long) rsRight.getConf().getNumReducers());
    assertTrue("AUTOPARALLEL must survive when the merge join's lineage is unresolved",
        rsRight.getConf().getReducerTraits().contains(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));

    ReduceWork rwRight = createReduceWorkFor(rsRight, rightRoot);
    assertEquals(BIG_TABLE_NUM_REDUCERS, rsLeft.getConf().getNumReducers());
    assertEquals(SMALL_TABLE_NUM_REDUCERS, rsRight.getConf().getNumReducers());
    assertEquals(SMALL_TABLE_NUM_REDUCERS, rwRight.getNumReduceTasks().intValue());
  }

  /**
   * A merge join reached from an upstream ReduceSink only by passing through another
   * ReduceSink belongs to that later shuffle stage. Creating the upstream work must not normalize
   * that later stage's inputs or disable the upstream stage's adaptive parallelism.
   */
  @Test
  public void testMergeJoinBehindAnotherShuffleLeavesLaterInputsUntouched() {
    ctx.conf.setBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM, true);
    CompilationOpContext cCtx = new CompilationOpContext();

    ReduceSinkOperator rsUpstream = newReduceSink(cCtx, BIG_TABLE_NUM_REDUCERS);
    rsUpstream.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    GroupByOperator upstreamGroupBy = newGroupBy(cCtx);
    OperatorFactory.makeChild(rsUpstream, upstreamGroupBy);

    ReduceSinkOperator rsMid = newReduceSink(cCtx, BIG_TABLE_NUM_REDUCERS);
    rsMid.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    OperatorFactory.makeChild(upstreamGroupBy, rsMid);
    GroupByOperator midGroupBy = newGroupBy(cCtx);
    OperatorFactory.makeChild(rsMid, midGroupBy);

    ReduceSinkOperator rsSmall = newReduceSink(cCtx, SMALL_TABLE_NUM_REDUCERS);
    rsSmall.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    TezDummyStoreOperator smallDummyStore = new TezDummyStoreOperator(cCtx);
    OperatorFactory.makeChild(rsSmall, smallDummyStore);

    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator(cCtx);
    midGroupBy.setChildOperators(newList(mergeJoin));
    smallDummyStore.setChildOperators(newList(mergeJoin));
    mergeJoin.setParentOperators(newList(midGroupBy, smallDummyStore));

    ReduceWork rwUpstream = createReduceWorkFor(rsUpstream, upstreamGroupBy);

    assertTrue("an upstream stage behind another shuffle must keep its adaptive parallelism",
        rwUpstream.isAutoReduceParallelism());
    assertEquals("a later shuffle must not be normalized while creating upstream work",
        BIG_TABLE_NUM_REDUCERS, rsMid.getConf().getNumReducers());
    assertEquals("a later shuffle must not be normalized while creating upstream work",
        SMALL_TABLE_NUM_REDUCERS, rsSmall.getConf().getNumReducers());
  }

  @Test
  public void testMergeJoinPreservesUniformHashingWithoutAutoParallelism() {
    ctx.conf.setBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM, false);
    EnumSet<ReduceSinkDesc.ReducerTraits> traits =
        EnumSet.of(ReduceSinkDesc.ReducerTraits.UNIFORM, ReduceSinkDesc.ReducerTraits.AUTOPARALLEL);
    rsBigTable.getConf().setReducerTraits(traits);
    rsSmallTable.getConf().setReducerTraits(traits);
    rsBigTable.getConf().setNumReducers(8);
    rsSmallTable.getConf().setNumReducers(32);

    ReduceWork rwBigTable = createReduceWorkFor(rsBigTable, groupBy);
    ReduceWork rwSmallTable = createReduceWorkFor(rsSmallTable, dummyStore);

    // UNIFORM selects whole-key Murmur hashing. Clearing it can put correlated composite keys
    // such as (a, a) in a single partition even when there are 32 reducers.
    assertEquals(traits, rsBigTable.getConf().getReducerTraits());
    assertEquals(traits, rsSmallTable.getConf().getReducerTraits());
    assertTrue(rwBigTable.isUniformDistribution());
    assertTrue(rwSmallTable.isUniformDistribution());
    assertEquals(32, rwBigTable.getNumReduceTasks().intValue());
    assertEquals(32, rwSmallTable.getNumReduceTasks().intValue());
    assertFalse(rwBigTable.isAutoReduceParallelism());
    assertFalse(rwSmallTable.isAutoReduceParallelism());
  }

  @Test
  public void testMergeJoinPreservesAdaptiveParallelism() {
    ctx.conf.setBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM, true);
    ctx.conf.setFloatVar(HiveConf.ConfVars.TEZ_MIN_PARTITION_FACTOR, 0.25f);
    ctx.conf.setFloatVar(HiveConf.ConfVars.TEZ_MAX_PARTITION_FACTOR, 2.0f);
    ctx.conf.setIntVar(HiveConf.ConfVars.MAX_REDUCERS, 1000);
    EnumSet<ReduceSinkDesc.ReducerTraits> traits =
        EnumSet.of(ReduceSinkDesc.ReducerTraits.UNIFORM, ReduceSinkDesc.ReducerTraits.AUTOPARALLEL);
    rsBigTable.getConf().setReducerTraits(traits);
    rsSmallTable.getConf().setReducerTraits(traits);
    rsBigTable.getConf().setNumReducers(100);
    rsSmallTable.getConf().setNumReducers(400);

    ReduceWork rwBigTable = createReduceWorkFor(rsBigTable, groupBy);
    ReduceWork rwSmallTable = createReduceWorkFor(rsSmallTable, dummyStore);

    assertEquals(traits, rsBigTable.getConf().getReducerTraits());
    assertEquals(traits, rsSmallTable.getConf().getReducerTraits());
    for (ReduceWork work : new ReduceWork[] {rwBigTable, rwSmallTable}) {
      assertTrue("merge-join inputs share one vertex and may adapt together",
          work.isAutoReduceParallelism());
      assertEquals(100, work.getMinReduceTasks());
      assertEquals(800, work.getMaxReduceTasks());
      assertTrue(work.getEdgePropRef().isAutoReduce());
      assertEquals(100, work.getEdgePropRef().getMinReducer());
      assertEquals(800, work.getEdgePropRef().getMaxReducer());
    }
  }

  private static GroupByOperator newGroupBy(CompilationOpContext cCtx) {
    GroupByOperator op = new GroupByOperator(cCtx);
    op.setConf(new GroupByDesc());
    op.getConf().setKeys(new ArrayList<>());
    return op;
  }

  private ReduceWork createReduceWorkFor(ReduceSinkOperator rs, Operator<?> root) {
    MapWork preceedingWork = new MapWork("Map for " + rs);
    tezWork.add(preceedingWork);
    ctx.preceedingWork = preceedingWork;
    ctx.parentOfRoot = rs;
    ReduceWork work = GenTezUtils.createReduceWork(ctx, root, tezWork);
    // GenTezWork cuts every shuffle edge after creating a ReduceWork.
    root.removeParents();
    return work;
  }

  private static ReduceSinkOperator newReduceSink(CompilationOpContext cCtx, int numReducers) {
    ReduceSinkOperator rs = new ReduceSinkOperator(cCtx);
    rs.setConf(new ReduceSinkDesc());
    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(new Properties());
    rs.getConf().setKeySerializeInfo(tableDesc);
    rs.getConf().setValueSerializeInfo(tableDesc);
    rs.getConf().setNumReducers(numReducers);
    return rs;
  }

  @SafeVarargs
  private static <T> ArrayList<T> newList(T... items) {
    ArrayList<T> list = new ArrayList<>();
    Collections.addAll(list, items);
    return list;
  }
}
