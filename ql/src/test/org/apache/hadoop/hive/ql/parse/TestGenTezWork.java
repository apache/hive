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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for GenTezWork.
 *
 */
public class TestGenTezWork {

  GenTezProcContext ctx;
  GenTezWork proc;
  ReduceSinkOperator rs;
  FileSinkOperator fs;
  TableScanOperator ts;

  /**
   * @throws java.lang.Exception
   */
  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    // Init conf
    final HiveConf conf = new HiveConf(SemanticAnalyzer.class);
    SessionState.start(conf);

    // Init parse context
    final ParseContext pctx = new ParseContext();
    pctx.setContext(new Context(conf));

    ctx = new GenTezProcContext(
        conf,
        pctx,
        Collections.EMPTY_LIST,
        new ArrayList<Task<?>>(),
        Collections.EMPTY_SET,
        Collections.EMPTY_SET);

    proc = new GenTezWork(new GenTezUtils() {
      @Override
        protected void setupMapWork(MapWork mapWork, GenTezProcContext context,
          PrunedPartitionList partitions, TableScanOperator root, String alias)
        throws SemanticException {

        LinkedHashMap<String, Operator<? extends OperatorDesc>> map
          = new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
        map.put("foo", root);
        mapWork.setAliasToWork(map);
        return;
      }
    });

    CompilationOpContext cCtx = new CompilationOpContext();
    fs = new FileSinkOperator(cCtx);
    fs.setConf(new FileSinkDesc());
    rs = new ReduceSinkOperator(cCtx);
    rs.setConf(new ReduceSinkDesc());
    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(new Properties());
    rs.getConf().setKeySerializeInfo(tableDesc);
    ts = new TableScanOperator(cCtx);
    ts.setConf(new TableScanDesc(null));
    ts.getChildOperators().add(rs);
    rs.getParentOperators().add(ts);
    rs.getChildOperators().add(fs);
    fs.getParentOperators().add(rs);
    ctx.preceedingWork = null;
    ctx.currentRootOperator = ts;
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    ctx = null;
    proc = null;
    ts = null;
    rs = null;
    fs = null;
  }

  @Test
  public void testCreateMap() throws SemanticException {
    proc.process(rs, null, ctx, (Object[])null);

    assertNotNull(ctx.currentTask);
    assertTrue(ctx.rootTasks.contains(ctx.currentTask));

    TezWork work = ctx.currentTask.getWork();
    assertEquals(work.getAllWork().size(),1);

    BaseWork w = work.getAllWork().get(0);
    assertTrue(w instanceof MapWork);

    MapWork mw = (MapWork)w;

    // need to make sure names are set for tez to connect things right
    assertNotNull(w.getName());

    // map work should start with our ts op
    assertSame(mw.getAliasToWork().entrySet().iterator().next().getValue(),ts);

    // preceeding work must be set to the newly generated map
    assertSame(ctx.preceedingWork, mw);

    // should have a new root now
    assertSame(ctx.currentRootOperator, fs);
  }

  @Test
  public void testCreateReduce() throws SemanticException {
    // create map
    proc.process(rs,  null,  ctx,  (Object[])null);

    // create reduce
    proc.process(fs, null, ctx, (Object[])null);

    TezWork work = ctx.currentTask.getWork();
    assertEquals(work.getAllWork().size(),2);

    BaseWork w = work.getAllWork().get(1);
    assertTrue(w instanceof ReduceWork);
    assertTrue(work.getParents(w).contains(work.getAllWork().get(0)));

    ReduceWork rw = (ReduceWork)w;

    // need to make sure names are set for tez to connect things right
    assertNotNull(w.getName());

    // map work should start with our ts op
    assertSame(rw.getReducer(),fs);

    // should have severed the ties
    assertEquals(fs.getParentOperators().size(),0);
  }
}
