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
package org.apache.hadoop.hive.ql.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TezDummyStoreOperator;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;

/**
 * Reproduces a reduce-side merge join bug: {@link GenTezUtils#createReduceWork} sets each
 * {@link ReduceWork}'s numReduceTasks purely from that branch's own ReduceSinkOperator, with no
 * cross-check against sibling ReduceSinkOperators that feed the same downstream
 * {@link CommonMergeJoinOperator}. When two such siblings end up with different numReducers
 * (e.g. because Tez auto-parallelism at runtime shrinks one side's task count but not the
 * other's), rows with the same join key can land in different reduce tasks on each side,
 * causing the merge join to silently miss matches.
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
  public void setUp() throws Exception {
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
    wireParentChild(rsBigTable, groupBy);

    rsSmallTable = newReduceSink(cCtx, SMALL_TABLE_NUM_REDUCERS);
    dummyStore = new TezDummyStoreOperator(cCtx);
    wireParentChild(rsSmallTable, dummyStore);

    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator(cCtx);
    groupBy.setChildOperators(newList(mergeJoin));
    dummyStore.setChildOperators(newList(mergeJoin));
    mergeJoin.setParentOperators(newList(groupBy, dummyStore));
  }

  /**
   * Both ReduceSinkOperators feed the same CommonMergeJoinOperator (one via a GroupBy, one via
   * a DummyStore), but start out with different numReducers. Today, createReduceWork does not
   * reconcile them: this assertion fails against unfixed code (5 != 20) and should pass once
   * GenTezUtils normalizes numReducers across merge-join siblings.
   */
  @Test
  public void testSiblingReduceSinksFeedingMergeJoinGetSameNumReducers() throws Exception {
    ReduceWork rwBigTable = createReduceWorkFor(rsBigTable, groupBy);
    ReduceWork rwSmallTable = createReduceWorkFor(rsSmallTable, dummyStore);

    assertEquals("both sides of the merge join must partition into the same number of tasks",
        rwBigTable.getNumReduceTasks(), rwSmallTable.getNumReduceTasks());
  }

  /**
   * When a CommonMergeJoinOperator's parents are plain ReduceSinkOperators with no
   * TezDummyStoreOperator sibling, both sides already run as tags of the same single Tez
   * vertex and share its one parallelism setting. Normalization must be a no-op here: it must
   * not touch numReducers or strip the AUTOPARALLEL trait, since there is no independent-vertex
   * mismatch to protect against.
   */
  @Test
  public void testMergeJoinWithoutDummyStoreSiblingIsNotNormalized() throws Exception {
    CompilationOpContext cCtx = new CompilationOpContext();
    ReduceSinkOperator rsLeft = newReduceSink(cCtx, BIG_TABLE_NUM_REDUCERS);
    rsLeft.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    ReduceSinkOperator rsRight = newReduceSink(cCtx, SMALL_TABLE_NUM_REDUCERS);
    rsRight.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));

    CommonMergeJoinOperator mergeJoin = new CommonMergeJoinOperator(cCtx);
    rsLeft.setChildOperators(newList(mergeJoin));
    rsRight.setChildOperators(newList(mergeJoin));
    mergeJoin.setParentOperators(newList(rsLeft, rsRight));

    ReduceWork rwLeft = createReduceWorkFor(rsLeft, mergeJoin);
    ReduceWork rwRight = createReduceWorkFor(rsRight, mergeJoin);

    assertEquals("numReducers must be untouched when there is no DummyStore sibling",
        (long) BIG_TABLE_NUM_REDUCERS, (long) rwLeft.getNumReduceTasks());
    assertEquals("numReducers must be untouched when there is no DummyStore sibling",
        (long) SMALL_TABLE_NUM_REDUCERS, (long) rwRight.getNumReduceTasks());
    assertTrue("AUTOPARALLEL must survive when there is no DummyStore sibling to protect against",
        rsLeft.getConf().getReducerTraits().contains(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    assertTrue("AUTOPARALLEL must survive when there is no DummyStore sibling to protect against",
        rsRight.getConf().getReducerTraits().contains(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
  }

  private ReduceWork createReduceWorkFor(ReduceSinkOperator rs, Operator<?> root) throws Exception {
    MapWork preceedingWork = new MapWork("Map for " + rs);
    tezWork.add(preceedingWork);
    ctx.preceedingWork = preceedingWork;
    ctx.parentOfRoot = rs;
    return GenTezUtils.createReduceWork(ctx, root, tezWork);
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

  private static void wireParentChild(
      Operator<? extends OperatorDesc> parent, Operator<? extends OperatorDesc> child) {
    parent.setChildOperators(newList(child));
    child.setParentOperators(newList(parent));
  }

  @SafeVarargs
  private static <T> ArrayList<T> newList(T... items) {
    ArrayList<T> list = new ArrayList<>();
    Collections.addAll(list, items);
    return list;
  }
}
