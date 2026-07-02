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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Note: this test is not thread-safe!
 */
public class TestCalcitePlanner {
  static QueryState queryState;

  ParseDriver pd;
  CalcitePlanner planner;

  @BeforeClass
  public static void initialize() throws Exception {
    HiveConf conf = new HiveConfForTest(TestCalcitePlanner.class);
    conf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, "false");
    conf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.varname,
        SQLStdConfOnlyAuthorizerFactory.class.getCanonicalName());
    queryState = new QueryState.Builder().withHiveConf(conf).build();
    SessionState.start(conf);
  }

  @Before
  public void setup() throws SemanticException {
    pd = new ParseDriver();
    planner = new CalcitePlanner(queryState);
  }

  ASTNode parse(String query) throws ParseException {
    ASTNode nd = pd.parse(query).getTree();
    return (ASTNode) nd.getChild(0);
  }

  private Context getContext(String sql) throws ParseException, SemanticException {
    ASTNode ast = parse(sql);
    Context ctx = new Context(queryState.getConf());
    planner.init(false);
    planner.initCtx(ctx);
    SemanticAnalyzer.PlannerContext pctx = new CalcitePlanner.PreCboCtx();
    planner.genResolvedParseTree(ast, pctx);
    Operator<?> operator = planner.genOPTree(ast, pctx);
    assertNotNull(operator);
    return ctx;
  }

  /**
   * The planner should store the Calcite plan in the context when HIVE_LOG_EXPLAIN_OUTPUT is enabled.
   */
  @Test
  public void testCBOLogging() throws Exception {
    queryState.getConf().setBoolVar(HiveConf.ConfVars.HIVE_LOG_EXPLAIN_OUTPUT, true);
    Context ctx = getContext("select 1");
    String calcitePlan = ctx.getCalcitePlan();
    assertNotNull(calcitePlan);
    assertTrue("Expected a RelNode plan containing \"HiveProject\", but was:\n" + calcitePlan,
        calcitePlan.contains("HiveProject"));
  }

  /**
   * The planner shall not store the Calcite plan in the context when HIVE_LOG_EXPLAIN_OUTPUT is disabled.
   */
  @Test
  public void testNoCBOLogging() throws Exception {
    queryState.getConf().setBoolVar(HiveConf.ConfVars.HIVE_LOG_EXPLAIN_OUTPUT, false);
    Context ctx = getContext("select 1");
    String calcitePlan = ctx.getCalcitePlan();
    assertNull(calcitePlan);
  }
}
