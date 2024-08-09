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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQueryProperties {
  private static HiveConf conf;
  private static CalcitePlanner analyzer;

  @BeforeClass
  public static void setUpAll() throws Exception {
    conf = new HiveConfForTest(TestSemanticAnalyzer.class);
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        "org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator");
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");

    SessionState ss = new SessionState(conf);
    SessionState.setCurrentSessionState(ss);
    SessionState.start(ss);

    Table testTable = new Table("test_db", "test_table");
    testTable.getSd().setLocation("dummy");
    List<FieldSchema> columns = testTable.getCols();
    columns.add(new FieldSchema("t1", "string", ""));
    columns.add(new FieldSchema("t2", "string", ""));
    Map<String, String> parameters = testTable.getParameters();
    parameters.put(StatsSetupConst.ROW_COUNT, "10");
    parameters.put(StatsSetupConst.RAW_DATA_SIZE, "10");
    parameters.put(StatsSetupConst.TOTAL_SIZE, "10");
    ss.getTempTables().put("test_db", Collections.singletonMap("test_table", testTable));

    QueryState state = new QueryState.Builder().build();
    analyzer = new CalcitePlanner(state);
  }

  private QueryProperties analyze(String sql) throws Exception {
    Context context = new Context(conf);
    ASTNode node = ParseUtils.parse(sql, context);
    analyzer.initCtx(context);
    analyzer.init(false);
    // CalcitePlanner#genOPTree is invoked in analyzeInternal, and it cleans up QueryProperties
    // That's why we call only genResolvedParseTree here
    analyzer.genResolvedParseTree(node, null);
    return analyzer.getQueryProperties();
  }

  @Test
  public void testExceptAll() throws Exception {
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table EXCEPT ALL SELECT t2 FROM test_db.test_table");
    Assert.assertTrue(properties.hasExcept());
  }

  @Test
  public void testExceptDistinct() throws Exception {
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table EXCEPT DISTINCT SELECT t2 FROM test_db.test_table");
    Assert.assertTrue(properties.hasExcept());
  }

  @Test
  public void testIntersectAll() throws Exception {
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table INTERSECT ALL SELECT t2 FROM test_db.test_table");
    Assert.assertTrue(properties.hasIntersect());
  }

  @Test
  public void testIntersectDistinct() throws Exception {
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table INTERSECT DISTINCT SELECT t2 FROM test_db.test_table");
    Assert.assertTrue(properties.hasIntersect());
  }

  @Test
  public void testQualify() throws Exception {
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table QUALIFY row_number() OVER (PARTITION BY t1 ORDER BY t2) = 1");
    Assert.assertTrue(properties.hasQualify());
  }
}
