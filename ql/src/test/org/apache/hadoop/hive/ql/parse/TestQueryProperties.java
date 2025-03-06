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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestQueryProperties {
  private static HiveConf conf;
  private static Hive db;
  @Parameterized.Parameter(0)
  public boolean useCbo;
  private SemanticAnalyzer analyzer;

  @Parameterized.Parameters(name = "useCbo={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {true},  // CBO
        {false}  // non-CBO
    });
  }

  @BeforeClass
  public static void setUpAll() throws Exception {
    conf = new HiveConfForTest(TestQueryProperties.class);
    db = Hive.get(conf);
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        "org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator");
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");

    Database database = new Database();
    database.setName("test_db");
    db.createDatabase(database);

    Table testTable = new Table("test_db", "test_table");
    List<FieldSchema> columns = testTable.getCols();
    columns.add(new FieldSchema("t1", "string", ""));
    columns.add(new FieldSchema("t2", "string", ""));
    Map<String, String> parameters = testTable.getParameters();
    parameters.put(StatsSetupConst.ROW_COUNT, "10");
    parameters.put(StatsSetupConst.RAW_DATA_SIZE, "10");
    parameters.put(StatsSetupConst.TOTAL_SIZE, "10");
    testTable.setProperty("transactional", "true");
    testTable.setInputFormatClass("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    testTable.setOutputFormatClass("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
    testTable.setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
    db.createTable(testTable);
  }

  /**
   * Some unit tests expect the whole analyze process to run, which is the expected, preferred way.
   * @param sql the query to analyze
   * @param fullAnalyze whether to run the full analyze or just some parts of it
   * @return QueryProperties from the analyzer
   */
  private QueryProperties analyze(String sql, boolean fullAnalyze) throws Exception {
    SessionState.start(conf);

    Context context = new Context(conf);
    ASTNode node = ParseUtils.parse(sql, context);

    QueryState state = new QueryState.Builder().withHiveConf(conf).build();
    HiveTxnManager txnManager = mock(DbTxnManager.class);
    when(txnManager.supportsAcid()).thenReturn(true);
    state.setTxnManager(txnManager);

    HiveConf.setBoolVar(state.getConf(), ConfVars.HIVE_CBO_ENABLED, useCbo);
    analyzer = (SemanticAnalyzer) SemanticAnalyzerFactory.get(state, node);

    analyzer.initCtx(context);
    analyzer.init(false);
    if (fullAnalyze) {
      analyzer.analyze(node, context);
    } else {
      // CalcitePlanner#genOPTree is invoked in analyzeInternal, and it cleans up QueryProperties
      // That's why we call only genResolvedParseTree here
      analyzer.genResolvedParseTree(node, null);
    }
    return analyzer.getQueryProperties();
  }

  @Test
  public void testExceptAll() throws Exception {
    if (!useCbo){
      // Semantic EXCEPT and INTERSECT operations are only supported with Cost Based Optimizations enabled
      return;
    }
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table EXCEPT ALL SELECT t2 FROM test_db.test_table", false);
    Assert.assertTrue(properties.hasExcept());
  }

  @Test
  public void testExceptDistinct() throws Exception {
    if (!useCbo){
      // Semantic EXCEPT and INTERSECT operations are only supported with Cost Based Optimizations enabled
      return;
    }
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table EXCEPT DISTINCT SELECT t2 FROM test_db.test_table", false);
    Assert.assertTrue(properties.hasExcept());
  }

  @Test
  public void testIntersectAll() throws Exception {
    if (!useCbo){
      // Semantic EXCEPT and INTERSECT operations are only supported with Cost Based Optimizations enabled
      return;
    }
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table INTERSECT ALL SELECT t2 FROM test_db.test_table", false);
    Assert.assertTrue(properties.hasIntersect());
  }

  @Test
  public void testIntersectDistinct() throws Exception {
    if (!useCbo){
      // Semantic EXCEPT and INTERSECT operations are only supported with Cost Based Optimizations enabled
      return;
    }
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table INTERSECT DISTINCT SELECT t2 FROM test_db.test_table", false);
    Assert.assertTrue(properties.hasIntersect());
  }

  @Test
  public void testQualify() throws Exception {
    if (!useCbo){
      // The following functionality requires CBO (hive.cbo.enable): Qualify clause
      return;
    }
    QueryProperties properties = analyze(
        "SELECT t1 FROM test_db.test_table QUALIFY row_number() OVER (PARTITION BY t1 ORDER BY t2) = 1", false);
    Assert.assertTrue(properties.hasQualify());
  }

  @Test
  public void testIsDML() throws Exception {
    checkIsDML("SELECT * FROM test_db.test_table", false);
    checkIsDML("INSERT INTO test_db.test_table VALUES ('a', 'b')", true);
    checkIsDML("INSERT OVERWRITE TABLE test_db.test_table SELECT * FROM test_db.test_table", true);
    checkIsDML("UPDATE test_db.test_table SET t1 = 'c' WHERE t2 = 'd'", true);
    checkIsDML("DELETE FROM test_db.test_table WHERE t2 = 'asdf'", true);
    checkIsDML("MERGE INTO test_db.test_table AS target USING test_db.test_table AS source ON source.t1 = target" +
        ".t1 WHEN MATCHED THEN UPDATE SET t1 = 'a' WHEN NOT MATCHED THEN INSERT VALUES ('q', 'r')", true);
  }

  private void checkIsDML(String query, boolean expectedIsDML) throws Exception {
    // fullAnalyze=true: this unit test expects the whole SemanticAnalyzer.analyze to be run
    QueryProperties properties = analyze(query, true);
    Assert.assertEquals(expectedIsDML, QueryProperties.QueryType.DML.equals(properties.getQueryType()));
  }

  @Test
  public void testFieldsRemainUntouchedOnClear() {
    QueryProperties queryProperties = new QueryProperties();

    // check default values
    Assert.assertNull(queryProperties.getQueryType());

    queryProperties.setQueryType(QueryProperties.QueryType.DDL);

    queryProperties.clear();

    // check that some values are not affected by clear()
    Assert.assertEquals("queryType is not supposed to be cleared", QueryProperties.QueryType.DDL,
        queryProperties.getQueryType());
  }
}
