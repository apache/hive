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

package org.apache.hadoop.hive.ql.optimizer.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph.Cluster;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TezCompiler;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOperatorGraph {

  TezCompilerMimic tezCompiler;
  ParseContext pctx;

  TableScanOperator ts1, ts2;
  UnionOperator union;
  SelectOperator sel;
  ReduceSinkOperator rs;
  GroupByOperator gby;
  FileSinkOperator fs;

  private class TezCompilerMimic extends TezCompiler {
    public List<BaseWork> runGenerateTaskTree(ParseContext pCtx) throws SemanticException {
      List<Task<?>> rootTasks = new ArrayList<>();
      generateTaskTree(rootTasks, pCtx, new ArrayList<>(), new HashSet<>(), new HashSet<>());

      assertEquals(1, rootTasks.size());
      assertTrue(rootTasks.get(0) instanceof TezTask);
      TezTask tezTask = (TezTask) rootTasks.get(0);

      return tezTask.getWork().getAllWork();
    }
  }

  private boolean isMatchingOperatorSet(Set<Operator<?>> opSet1, Set<Operator<?>> opSet2) {
    List<String> opSetTypes1 = opSet1.stream()
        .filter(op -> !(op instanceof UnionOperator))
        .map(Operator::getName)
        .sorted()
        .collect(Collectors.toList());
    List<String> opSetTypes2 = opSet2.stream()
        .filter(op -> !(op instanceof UnionOperator))
        .map(Operator::getName)
        .sorted()
        .collect(Collectors.toList());
    
    if (opSetTypes1.size() == opSetTypes2.size()) {
      boolean result = true;
      for (int i = 0; i < opSet1.size(); i++) {
        result = result && opSetTypes1.get(i) == opSetTypes2.get(i);
      }
      return result;
    } else {
      return false;
    }
  }

  private void connectOperators(Operator<?> parent, Operator<?> child) {
    parent.getChildOperators().add(child);
    child.getParentOperators().add(parent);
  }

  @Before
  public void setUp() throws SemanticException {
    CompilationOpContext cCtx = new CompilationOpContext();

    ts1 = new TableScanOperator(cCtx);
    ts2 = new TableScanOperator(cCtx);
    union = new UnionOperator(cCtx);
    sel = new SelectOperator(cCtx);
    rs = new ReduceSinkOperator(cCtx);
    gby = new GroupByOperator(cCtx);
    fs = new FileSinkOperator(cCtx);

    TableScanDesc tsConf1 = new TableScanDesc(new Table("db", "table1"));
    tsConf1.setAlias("table1");
    tsConf1.getTableMetadata().getSd().setLocation("file:///table1");

    TableScanDesc tsConf2 = new TableScanDesc(new Table("db", "table2"));
    tsConf2.setAlias("table2");
    tsConf2.getTableMetadata().getSd().setLocation("file:///table2");

    TableDesc rsTableDesc = new TableDesc();
    rsTableDesc.setProperties(new Properties());
    ReduceSinkDesc rsConf = new ReduceSinkDesc();
    rsConf.setKeySerializeInfo(rsTableDesc);

    GroupByDesc gbyConf = new GroupByDesc();
    gbyConf.setKeys(new ArrayList<>());

    TableDesc fsTableDesc = new TableDesc();
    fsTableDesc.setProperties(new Properties());
    FileSinkDesc fsConf = new FileSinkDesc();
    fsConf.setTableInfo(fsTableDesc);

    ts1.setConf(tsConf1);
    ts2.setConf(tsConf2);
    union.setConf(new UnionDesc());
    sel.setConf(new SelectDesc());
    rs.setConf(rsConf);
    gby.setConf(gbyConf);
    fs.setConf(fsConf);

    connectOperators(ts1, union);
    connectOperators(ts2, union);
    connectOperators(union, sel);
    connectOperators(sel, rs);
    connectOperators(rs, gby);
    connectOperators(gby, fs);

    HiveConf conf = new HiveConf();
    conf.setBoolean(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION.varname, false);
    SessionState.start(conf);
    QueryState queryState = QueryState.getNewQueryState(conf, null);
    SemanticAnalyzer sem = new SemanticAnalyzer(queryState);

    tezCompiler = new TezCompilerMimic();
    tezCompiler.init(queryState, null, null);

    pctx = sem.getParseContext();
    pctx.setContext(new Context(conf));

    pctx.setTopOps(new HashMap<>());
    pctx.getTopOps().put("table1", ts1);
    pctx.getTopOps().put("table2", ts2);
  }

  @After
  public void tearDown() {
    tezCompiler = null;
    pctx = null;
    ts1 = null;
    ts2 = null;
    union = null;
    sel = null;
    rs = null;
    gby = null;
    fs = null;
  }

  @Test
  public void testCompareDAGIncludingUnion() throws SemanticException {
    OperatorGraph og = new OperatorGraph(pctx);
    HashSet<Cluster> operatorGraphClusters = new HashSet<>(og.getClusters());

    List<BaseWork> nonUnionBaseWorks = tezCompiler.runGenerateTaskTree(pctx)
        .stream().filter(work -> !(work instanceof UnionWork))
        .collect(Collectors.toList());

    assertEquals(nonUnionBaseWorks.size(), operatorGraphClusters.size());
    for (BaseWork work: nonUnionBaseWorks) {
      Optional<Cluster> matchingCluster = operatorGraphClusters.stream()
          .filter(cluster -> isMatchingOperatorSet(work.getAllOperators(), cluster.getMembers()))
          .findFirst();

      assertTrue(matchingCluster.isPresent());

      operatorGraphClusters.remove(matchingCluster.get());
    }
  }
}

