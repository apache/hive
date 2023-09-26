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

package org.apache.hadoop.hive.ql.optimizer;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.SharedWorkOptimizer.SharedWorkOptimizerCache;
import org.apache.hadoop.hive.ql.optimizer.SharedWorkOptimizer.TSComparator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.AUTOPARALLEL;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.FIXED;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNSET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSharedWorkOptimizer {

  private static final EnumSet<ReduceSinkDesc.ReducerTraits> unset = EnumSet.of(UNSET);
  private static final EnumSet<ReduceSinkDesc.ReducerTraits> fixed = EnumSet.of(FIXED);
  private static final EnumSet<ReduceSinkDesc.ReducerTraits> uniform = EnumSet.of(UNIFORM);
  private static final EnumSet<ReduceSinkDesc.ReducerTraits> autoparallel = EnumSet.of(AUTOPARALLEL);
  private static final EnumSet<ReduceSinkDesc.ReducerTraits> uniformAutoparallel = EnumSet.of(UNIFORM, AUTOPARALLEL);

  private void ensureDeduplicate(
      EnumSet<ReduceSinkDesc.ReducerTraits> traits1, int numReducers1,
      EnumSet<ReduceSinkDesc.ReducerTraits> traits2, int numReducers2,
      EnumSet<ReduceSinkDesc.ReducerTraits> expectedTraits, int expectedNumReducers) {

    ReduceSinkDesc rsConf1;
    ReduceSinkDesc rsConf2;
    boolean deduplicated;

    rsConf1 = new ReduceSinkDesc();
    rsConf1.setReducerTraits(traits1);
    rsConf1.setNumReducers(numReducers1);
    rsConf2 = new ReduceSinkDesc();
    rsConf2.setReducerTraits(traits2);
    rsConf2.setNumReducers(numReducers2);
    deduplicated = SharedWorkOptimizer.deduplicateReduceTraits(rsConf1, rsConf2);
    assertTrue(deduplicated);
    assertEquals(expectedTraits, rsConf1.getReducerTraits());
    assertEquals(expectedNumReducers, rsConf1.getNumReducers());

    rsConf1 = new ReduceSinkDesc();
    rsConf1.setReducerTraits(traits1);
    rsConf1.setNumReducers(numReducers1);
    rsConf2 = new ReduceSinkDesc();
    rsConf2.setReducerTraits(traits2);
    rsConf2.setNumReducers(numReducers2);
    deduplicated = SharedWorkOptimizer.deduplicateReduceTraits(rsConf2, rsConf1);
    assertTrue(deduplicated);
    assertEquals(expectedTraits, rsConf2.getReducerTraits());
    assertEquals(expectedNumReducers, rsConf2.getNumReducers());
  }

  private void ensureNotDeduplicate(
      EnumSet<ReduceSinkDesc.ReducerTraits> traits1, int numReducers1,
      EnumSet<ReduceSinkDesc.ReducerTraits> traits2, int numReducers2) {

    ReduceSinkDesc rsConf1;
    ReduceSinkDesc rsConf2;
    boolean deduplicated;

    rsConf1 = new ReduceSinkDesc();
    rsConf1.setReducerTraits(traits1);
    rsConf1.setNumReducers(numReducers1);
    rsConf2 = new ReduceSinkDesc();
    rsConf2.setReducerTraits(traits2);
    rsConf2.setNumReducers(numReducers2);
    deduplicated = SharedWorkOptimizer.deduplicateReduceTraits(rsConf1, rsConf2);
    assertFalse(deduplicated);

    rsConf1 = new ReduceSinkDesc();
    rsConf1.setReducerTraits(traits1);
    rsConf1.setNumReducers(numReducers1);
    rsConf2 = new ReduceSinkDesc();
    rsConf2.setReducerTraits(traits2);
    rsConf2.setNumReducers(numReducers2);
    deduplicated = SharedWorkOptimizer.deduplicateReduceTraits(rsConf1, rsConf2);
    assertFalse(deduplicated);
  }

  @Test
  public void testDeduplicate() {
    // UNSET
    ensureDeduplicate(unset, 0, unset, 0, unset, 0);
    ensureDeduplicate(unset, 0, fixed, 1, fixed, 1);
    ensureDeduplicate(unset, 0, uniform, 1, uniform, 1);
    ensureDeduplicate(unset, 0, autoparallel, 1, autoparallel, 1);
    ensureDeduplicate(unset, 0, uniformAutoparallel, 1, uniformAutoparallel, 1);

    // FIXED
    ensureDeduplicate(fixed, 1, fixed, 1, fixed, 1);
    ensureNotDeduplicate(fixed, 1, fixed, 2);
    ensureDeduplicate(fixed, 1, uniform, 1, fixed, 1);
    ensureDeduplicate(fixed, 1, autoparallel, 2, fixed, 1);
    ensureDeduplicate(fixed, 1, uniformAutoparallel, 2, fixed, 1);

    // UNIFORM
    ensureDeduplicate(uniform, 1, uniform, 2, uniform, 2);
    ensureNotDeduplicate(uniform, 1, autoparallel, 2);
    ensureDeduplicate(uniform, 1, uniformAutoparallel, 2, uniform, 2);

    // AUTOPARALLEL
    ensureDeduplicate(autoparallel, 1, uniformAutoparallel, 2, autoparallel, 2);

    // UNIFORM and AUTOPARALLEL
    ensureDeduplicate(uniformAutoparallel, 1, uniformAutoparallel, 2, uniformAutoparallel, 2);
  }

  @Test
  public void testTSCmp() {

    ArrayList<TableScanOperator> li = Lists.newArrayList(addFilter(getTsOp(), 1), getTsOp());
    li.sort(new TSComparator());
    assertNull(li.get(0).getConf().getFilterExpr());

  }

  @Test
  public void testTSCmpOrdersById() {
    TableScanOperator ts1 = getTsOp();
    TableScanOperator ts2 = getTsOp();
    ArrayList<TableScanOperator> li1 = Lists.newArrayList(ts1, ts2);
    ArrayList<TableScanOperator> li2 = Lists.newArrayList(ts2, ts1);
    li1.sort(new TSComparator());
    li2.sort(new TSComparator());
    assertTrue(li1.get(0) == li2.get(0));
  }

  @Test
  public void testTSCmpOrdersByDataSizeDesc() {
    TableScanOperator ts1 = getTsOp();
    TableScanOperator ts2 = getTsOp();
    TableScanOperator ts3 = getTsOp();
    ts1.setStatistics(new Statistics(100, 100, 1, 1));
    ts2.setStatistics(new Statistics(1000, 1000, 1, 1));
    ts3.setStatistics(new Statistics(10, 10, 1, 1));
    ArrayList<TableScanOperator> li1 = Lists.newArrayList(ts1, ts3, ts2);
    li1.sort(new TSComparator());

    assertTrue(li1.get(0).getStatistics().getDataSize() == 1000);
    assertTrue(li1.get(1).getStatistics().getDataSize() == 100);
    assertTrue(li1.get(2).getStatistics().getDataSize() == 10);
  }


  CompilationOpContext cCtx = new CompilationOpContext();

  private TableScanOperator getTsOp() {
    Table tblMetadata = new Table("db", "table");
    TableScanDesc desc = new TableScanDesc("alias_" + cCtx.nextOperatorId(), tblMetadata);
    Operator<TableScanDesc> ts = OperatorFactory.get(cCtx, desc);
    return (TableScanOperator) ts;
  }

  private TableScanOperator addFilter(TableScanOperator ts, int i) {
    TableScanDesc desc = ts.getConf();
    List<ExprNodeDesc> as =
        Lists.newArrayList(new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, Integer.valueOf(i)),
            new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "c1", "aa", false));
    GenericUDF udf = new GenericUDFConcat();
    ExprNodeGenericFuncDesc f1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo, udf, as);
    desc.setFilterExpr(f1);
    return ts;
  }

  private Operator<? extends OperatorDesc> getFilterOp(int constVal) {
    ExprNodeDesc pred = new ExprNodeConstantDesc(constVal);
    FilterDesc fd = new FilterDesc(pred, true);
    Operator<? extends OperatorDesc> op = OperatorFactory.get(cCtx, fd);
    return op;
  }

  @Test
  public void testSharedWorkOptimizerCache() {

    List<Operator<?>> ops = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      ops.add(getFilterOp(i));
    }

    SharedWorkOptimizerCache c = new SharedWorkOptimizerCache();

    for (int i = 1; i < 10; i++) {
      int u = 10 * i;
      c.addWorkGroup(ops.subList(u, u + 10));
    }

    // unknowns
    for (int i = 0; i < 10; i++) {
      assertTrue(c.getWorkGroup(ops.get(i)).isEmpty());
    }

    // equiv group
    for (int i = 40; i < 50; i++) {
      assertEquals(c.getWorkGroup(ops.get(40)), c.getWorkGroup(ops.get(i)));
    }

    // non equiv
    for (int i = 10; i < 100; i += 10) {
      for (int j = i + 10; j < 100; j += 10) {
        assertNotEquals(c.getWorkGroup(ops.get(i)), c.getWorkGroup(ops.get(j)));
      }
    }

    c.removeOpAndCombineWork(ops.get(10), ops.get(20));
    assertTrue(c.getWorkGroup(ops.get(10)).isEmpty());
    assertEquals(19, c.getWorkGroup(ops.get(11)).size());
    for (int i = 11; i < 20; i++) {
      assertTrue(c.getWorkGroup(ops.get(11)).contains(ops.get(i)));
    }

    c.putIfWorkExists(ops.get(0), ops.get(1));
    assertTrue(c.getWorkGroup(ops.get(0)).isEmpty());
    assertTrue(c.getWorkGroup(ops.get(1)).isEmpty());

    c.putIfWorkExists(ops.get(0), ops.get(30));
    assertFalse(c.getWorkGroup(ops.get(0)).isEmpty());
    assertTrue(c.getWorkGroup(ops.get(31)).contains(ops.get(0)));

    c.removeOp(ops.get(1));

    c.removeOp(ops.get(50));
    assertTrue(c.getWorkGroup(ops.get(50)).isEmpty());
    assertFalse(c.getWorkGroup(ops.get(51)).contains(ops.get(50)));

  }

  private ReduceSinkDesc getReduceSinkDesc() {
    TableDesc dummyKeySerializeInfo = new TableDesc();
    dummyKeySerializeInfo.setProperties(new Properties());

    ReduceSinkDesc conf = new ReduceSinkDesc(new ArrayList<>(), 0, new ArrayList<>(), new ArrayList<>(),
        new ArrayList<>(), new ArrayList<>(), 0, new ArrayList<>(), 0, null, null, null);
    conf.setKeySerializeInfo(dummyKeySerializeInfo);
    return conf;
  }

  private MapJoinDesc getMapJoinDesc(int posBigTable) {
    MapJoinDesc conf = new MapJoinDesc();
    conf.setPosBigTable(posBigTable);
    return conf;
  }

  private void runMapJoinCacheReuseOptimization(Operator<?> op1, Operator<?> op2) {
    SharedWorkOptimizer sharedWorkOptimizer = new SharedWorkOptimizer();
    SharedWorkOptimizerCache optimizerCache = new SharedWorkOptimizerCache();

    optimizerCache.addWorkGroup(Arrays.asList(op1, op2));
    try {
      sharedWorkOptimizer.runMapJoinCacheReuseOptimization(null, optimizerCache);
    } catch (SemanticException se) {
      fail();
    }
  }

  @Test
  public void testMapJoinCacheReuse() {
    // Big tables
    TableScanOperator ts1 = getTsOp();
    TableScanOperator ts2 = getTsOp();

    // Small tables
    Operator<?> smallTableA = getFilterOp(0);
    Operator<?> smallTableB = getFilterOp(0);
    Operator<?> smallTableC = getFilterOp(0);

    Operator<?> rsA1 = OperatorFactory.getAndMakeChild(getReduceSinkDesc(), smallTableA);
    Operator<?> rsA2 = OperatorFactory.getAndMakeChild(getReduceSinkDesc(), smallTableA);

    Operator<?> rsB1 = OperatorFactory.getAndMakeChild(getReduceSinkDesc(), smallTableB);
    Operator<?> rsB2 = OperatorFactory.getAndMakeChild(getReduceSinkDesc(), smallTableB);

    Operator<?> rsC2 = OperatorFactory.getAndMakeChild(getReduceSinkDesc(), smallTableC);

    // case 1. MapJoin1: (big, A, B), MapJoin2: (big, A, B)
    List<Operator<?>> joinSource1 = Arrays.asList(ts1, rsA1, rsB1);
    List<Operator<?>> joinSource2 = Arrays.asList(ts2, rsA2, rsB2);

    MapJoinOperator mapJoin1 = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        cCtx, getMapJoinDesc(0), joinSource1);
    MapJoinOperator mapJoin2 = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        cCtx, getMapJoinDesc(0), joinSource2);

    runMapJoinCacheReuseOptimization(mapJoin1, mapJoin2);
    assertEquals(mapJoin1.getConf().getCacheKey(), mapJoin2.getConf().getCacheKey());

    // case 2. MapJoin3: (big, A, B), MapJoin4: (big, A, C)
    List<Operator<?>> joinSource3 = Arrays.asList(ts1, rsA1, rsB1);
    List<Operator<?>> joinSource4 = Arrays.asList(ts2, rsA2, rsC2);

    MapJoinOperator mapJoin3 = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        cCtx, getMapJoinDesc(0), joinSource3);
    MapJoinOperator mapJoin4 = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        cCtx, getMapJoinDesc(0), joinSource4);

    runMapJoinCacheReuseOptimization(mapJoin3, mapJoin4);
    assertNotEquals(mapJoin3.getConf().getCacheKey(), mapJoin4.getConf().getCacheKey());

    // case 3. MapJoin5: (big, A, B), MapJoin6: (A, big, B)
    List<Operator<?>> joinSource5 = Arrays.asList(ts1, rsA1, rsB1);
    List<Operator<?>> joinSource6 = Arrays.asList(rsA2, ts2, rsB2);

    MapJoinOperator mapJoin5 = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        cCtx, getMapJoinDesc(0), joinSource5);
    MapJoinOperator mapJoin6 = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        cCtx, getMapJoinDesc(1), joinSource6);

    runMapJoinCacheReuseOptimization(mapJoin5, mapJoin6);
    assertNotEquals(mapJoin5.getConf().getCacheKey(), mapJoin6.getConf().getCacheKey());
  }
}
