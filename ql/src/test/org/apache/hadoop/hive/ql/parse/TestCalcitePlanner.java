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

import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCalcitePlanner {
  static QueryState queryState;

  ParseDriver pd;
  CalcitePlanner planner;

  @BeforeClass
  public static void initialize() {
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
    return getContext(sql, ctx -> {
    });
  }

  private Context getContext(String sql, Consumer<Context> prepare) throws ParseException, SemanticException {
    ASTNode ast = parse(sql);
    Context ctx = new Context(queryState.getConf());
    planner.init(false);
    planner.initCtx(ctx);
    prepare.accept(ctx);
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
  public void testCBOLogExplainEnabled() throws ParseException, SemanticException {
    queryState.getConf().setBoolVar(HiveConf.ConfVars.HIVE_LOG_EXPLAIN_OUTPUT, true);
    Context context = getContext("select 1");
    String calcitePlan = context.getCalcitePlan();
    assertNotNull(calcitePlan);
    assertTrue("Expected a RelNode plan containing \"HiveProject\", but was:\n" + calcitePlan,
        calcitePlan.contains("HiveProject"));
  }

  /**
   * The planner shall not overwrite the explain configuration.
   */
  @Test
  public void testCBOLogExplainEnabledExplainFormatted() throws ParseException, SemanticException {
    queryState.getConf().setBoolVar(HiveConf.ConfVars.HIVE_LOG_EXPLAIN_OUTPUT, true);
    Context context = getContext("explain formatted cbo select 1", ctx -> {
      ExplainConfiguration explConf = new ExplainConfiguration();
      explConf.setFormatted(true);
      explConf.setCbo(true);
      explConf.setCboJoinCost(false);
      ctx.setExplainConfig(explConf);
      ctx.setExplainPlan(true);
    });
    String calcitePlan = context.getCalcitePlan();
    assertNotNull(calcitePlan);
    assertTrue("Expected a JSON plan, but was\n" + calcitePlan, calcitePlan.trim().startsWith("{"));
  }

  /**
   * The planner shall not overwrite the explain configuration.
   */
  @Test
  public void testCBOLogExplainEnabledExplainNotFormatted() throws ParseException, SemanticException {
    queryState.getConf().setBoolVar(HiveConf.ConfVars.HIVE_LOG_EXPLAIN_OUTPUT, true);
    Context context = getContext("explain cbo select 1", ctx -> {
      ExplainConfiguration explConf = new ExplainConfiguration();
      explConf.setFormatted(false);
      explConf.setCbo(true);
      explConf.setCboJoinCost(false);
      ctx.setExplainConfig(explConf);
      ctx.setExplainPlan(true);
    });
    String calcitePlan = context.getCalcitePlan();
    assertNotNull(calcitePlan);
    assertFalse("Expected a non-JSON plan, but was\n" + calcitePlan, calcitePlan.trim().startsWith("{"));
  }

  /**
   * The planner shall not store the Calcite plan in the context when HIVE_LOG_EXPLAIN_OUTPUT is disabled.
   */
  @Test
  public void testCBOLogExplainDisabled() throws ParseException, SemanticException {
    queryState.getConf().setBoolVar(HiveConf.ConfVars.HIVE_LOG_EXPLAIN_OUTPUT, false);
    Context context = getContext("select 1");
    String calcitePlan = context.getCalcitePlan();
    assertNull(calcitePlan);
  }
}
