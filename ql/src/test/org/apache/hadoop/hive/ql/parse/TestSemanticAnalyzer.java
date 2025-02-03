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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.common.MaterializationSnapshot;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryProperties.QueryType;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.MaterializedViewMetadata;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(TestSemanticAnalyzer.class.getName());

  private static Hive db;
  private static HiveConf conf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = new HiveConfForTest(TestSemanticAnalyzer.class);
    conf.set("hive.security.authorization.enabled", "false");
    conf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory");
    db = Hive.get(conf);

    createDatabase("other_db");

    // table1 (col1 string, col2 int)
    Table table1 = createKeyValueTable("table1");
    createKeyValueTable("table2");
    createKeyValueTable("table3");
    createPartitionedTable("table_part");
    createAcidTable("table_acid");

    createKeyValueTable("other_db", "table1");

    createView("view1", table1);
    createMaterializedView("mview1", table1);
  }

  private static void createDatabase(String dbName) throws Exception {
    Database database = new Database();
    database.setName(dbName);
    db.createDatabase(database);
  }

  private static Table createKeyValueTable(String tableName) throws Exception {
    return createKeyValueTable("default", tableName);
  }

  private static Table createKeyValueTable(String dbName, String tableName) throws Exception {
    Table table = createSimpleTableWithColumns(dbName, tableName);
    db.createTable(table);
    return table;
  }

  private static void createAcidTable(String tableAcid) throws HiveException {
    Table table = createSimpleTableWithColumns("default", tableAcid);
    table.setProperty("transactional", "true");
    // The table must be stored using an ACID compliant format (such as ORC)
    setOrc(table);
    db.createTable(table);
  }

  private static void createPartitionedTable(String tablePart) throws HiveException {
    Table table = createSimpleTableWithColumns("default", tablePart);

    List<FieldSchema> partitionColumns = new ArrayList<>();
    partitionColumns.add(new FieldSchema("part_col", "string", "Partition column description"));
    table.setPartCols(partitionColumns);

    db.createTable(table);
  }

  private static void setOrc(Table table) throws HiveException {
    table.setInputFormatClass("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    table.setOutputFormatClass("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
    table.setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
  }

  private static Table createSimpleTableWithColumns(String dbName, String tableName) {
    Table table = new Table(dbName, tableName);
    List<FieldSchema> columns = new ArrayList<>();
    columns.add(new FieldSchema("key", "string", "First column"));
    columns.add(new FieldSchema("value", "int", "Second column"));
    table.setFields(columns); // Set columns
    return table;
  }

  private static void createView(String viewName, Table sourceTable) throws HiveException {
    Table view = new Table("default", viewName);
    view.setTableType(TableType.VIRTUAL_VIEW);
    view.setViewOriginalText(String.format("SELECT * FROM %s", sourceTable.getTableName()));
    view.setViewExpandedText(String.format("SELECT * FROM %s", sourceTable.getTableName()));

    db.createTable(view);
  }

  private static void createMaterializedView(String materializedViewName, Table sourceTable) throws HiveException {
    Table materializedView = new Table("default", materializedViewName);
    materializedView.setTableType(TableType.MATERIALIZED_VIEW);
    materializedView.setViewOriginalText(String.format("SELECT * FROM %s", sourceTable.getTableName()));
    materializedView.setViewExpandedText(String.format("SELECT * FROM %s", sourceTable.getTableName()));

    MaterializedViewMetadata metadata = new MaterializedViewMetadata(
        MetaStoreUtils.getDefaultCatalog(conf),
        sourceTable.getDbName(),
        materializedViewName,
        Sets.newHashSet(sourceTable.createSourceTable()),
        mock(MaterializationSnapshot.class));
    materializedView.setMaterializedViewMetadata(metadata);

    db.createTable(materializedView);
  }

  @AfterClass
  public static void afterClass() {
    db.close(true);
  }

  @Test
  public void testNormalizeColSpec() throws Exception {
    // Hive normalizes partition spec for dates to yyyy-mm-dd format. Some versions of Java will
    // accept other formats for Date.valueOf, e.g. yyyy-m-d, and who knows what else in the future;
    // some will not accept other formats, so we cannot test normalization with them - type check
    // will fail before it can ever happen. Thus, test in isolation.
    checkNormalization("date", "2010-01-01", "2010-01-01", Date.valueOf("2010-01-01"));
    checkNormalization("date", "2010-1-01", "2010-01-01", Date.valueOf("2010-01-01"));
    checkNormalization("date", "2010-1-1", "2010-01-01", Date.valueOf("2010-01-01"));
    checkNormalization("string", "2010-1-1", "2010-1-1", "2010-1-1");

    try {
      checkNormalization("date", "foo", "", "foo"); // Bad format.
      fail("should throw");
    } catch (SemanticException ex) {
    }

    try {
      checkNormalization("date", "2010-01-01", "2010-01-01", "2010-01-01"); // Bad value type.
      fail("should throw");
    } catch (SemanticException ex) {
    }
  }


  public void checkNormalization(String colType, String originalColSpec,
      String result, Object colValue) throws SemanticException {
    final String colName = "col";
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(colName, originalColSpec);
    BaseSemanticAnalyzer.normalizeColSpec(partSpec, colName, colType, originalColSpec, colValue);
    assertEquals(result, partSpec.get(colName));
    if (colValue instanceof Date) {
      DateWritableV2 dw = new DateWritableV2((Date)colValue);
      BaseSemanticAnalyzer.normalizeColSpec(partSpec, colName, colType, originalColSpec, dw);
      assertEquals(result, partSpec.get(colName));
    }
  }

  @Test
  public void testUnescapeSQLString() {
    assertEquals("abcdefg", BaseSemanticAnalyzer.unescapeSQLString("\"abcdefg\""));

    // String enclosed by single quotes.
    assertEquals("C0FFEE", BaseSemanticAnalyzer.unescapeSQLString("\'C0FFEE\'"));

    // Strings including single escaped characters.
    assertEquals("\u0000", BaseSemanticAnalyzer.unescapeSQLString("'\\0'"));
    assertEquals("\'", BaseSemanticAnalyzer.unescapeSQLString("\"\\'\""));
    assertEquals("\"", BaseSemanticAnalyzer.unescapeSQLString("'\\\"'"));
    assertEquals("\b", BaseSemanticAnalyzer.unescapeSQLString("\"\\b\""));
    assertEquals("\n", BaseSemanticAnalyzer.unescapeSQLString("'\\n'"));
    assertEquals("\r", BaseSemanticAnalyzer.unescapeSQLString("\"\\r\""));
    assertEquals("\t", BaseSemanticAnalyzer.unescapeSQLString("'\\t'"));
    assertEquals("\u001A", BaseSemanticAnalyzer.unescapeSQLString("\"\\Z\""));
    assertEquals("\\", BaseSemanticAnalyzer.unescapeSQLString("'\\\\'"));
    assertEquals("\\%", BaseSemanticAnalyzer.unescapeSQLString("\"\\%\""));
    assertEquals("\\_", BaseSemanticAnalyzer.unescapeSQLString("'\\_'"));

    // String including '\000' style literal characters.
    assertEquals("3 + 5 = \u0038", BaseSemanticAnalyzer.unescapeSQLString("'3 + 5 = \\070'"));
    assertEquals("\u0000", BaseSemanticAnalyzer.unescapeSQLString("\"\\000\""));

    // String including invalid '\000' style literal characters.
    assertEquals("256", BaseSemanticAnalyzer.unescapeSQLString("\"\\256\""));

    // String including a '\u0000' style literal characters (\u732B is a cat in Kanji).
    assertEquals("How cute \u732B are",
      BaseSemanticAnalyzer.unescapeSQLString("\"How cute \\u732B are\""));

    // String including a surrogate pair character
    // (\uD867\uDE3D is Okhotsk atka mackerel in Kanji).
    assertEquals("\uD867\uDE3D is a fish",
      BaseSemanticAnalyzer.unescapeSQLString("\"\\uD867\uDE3D is a fish\""));
  }

  @Test
  public void testSkipAuthorization() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_SERVICE_USERS, "u1,u2");
    SessionState ss = new SessionState(hiveConf);
    ss.setIsHiveServerQuery(true);
    ss.setAuthenticator(new HadoopDefaultAuthenticator() {
      @Override
      public String getUserName() {
        return "u3";
      }
    });
    SessionState.setCurrentSessionState(ss);
    BaseSemanticAnalyzer analyzer = new BaseSemanticAnalyzer(new QueryState.Builder()
        .withHiveConf(hiveConf).nonIsolated().build(), null) {

      @Override
      public void analyzeInternal(ASTNode ast) throws SemanticException {
        // no op
      }
    };
    assertFalse(analyzer.skipAuthorization());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_SERVICE_USERS, "u1,u2,u3");
    assertTrue(analyzer.skipAuthorization());
  }

  @Test
  public void testSelectCacheable() throws Exception {
    checkQueryCanUseCache("SELECT key from table1", true);
  }

  @Test
  public void testInsertCacheable() throws Exception {
    checkQueryCanUseCache("INSERT INTO table1 VALUES ('asdf', 2)", false);
  }

  @Test
  public void testInsertOverwriteDirectoryCacheable() throws Exception {
    checkQueryCanUseCache("INSERT OVERWRITE DIRECTORY '/tmp' SELECT key FROM table2", false);
  }

  @Test
  public void testInsertOverwriteDirectoryWithNonTrivialSubqueryCacheable() throws Exception {
    checkQueryCanUseCache("insert overwrite directory '/tmp' " +
        "SELECT a.key, MAX(b.value) AS MAX_VALUE, COUNT(DISTINCT b.key) AS UNIQUE_KEYS, AVG(c.value) AS VALS " +
            "FROM table1 a " +
            "JOIN table2 b ON a.key = b.key " +
            "JOIN table3 c ON a.key = c.key " +
            "GROUP BY a.key HAVING AVG(LENGTH(a.key) + LENGTH(b.key)) > 5 " +
            "ORDER BY MAX_VALUE DESC, UNIQUE_KEYS ASC"
        , false);
  }

  private void checkQueryCanUseCache(String query, boolean canUseCache) throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    QueryResultsCache.initialize(conf);
    QueryResultsCache cache = QueryResultsCache.getInstance();
    String cacheDirPath = cache.getCacheDirPath().toUri().getPath();

    SessionState.start(conf);
    Context ctx = new Context(conf);
    ASTNode astNode = ParseUtils.parse(query, ctx);
    QueryState queryState = new QueryState.Builder().withHiveConf(conf).build();

    SemanticAnalyzer analyzer = spy((SemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, astNode));

    analyzer.initCtx(ctx);

    List<Operator<?>> capturedValues = new ArrayList<>();
    doAnswer((Answer<Operator<?>>) invocation -> {
      Operator<?> fileSinkOperator = (Operator<?>) invocation.callRealMethod(); // Call the actual method
      capturedValues.add(fileSinkOperator);
      return fileSinkOperator;
    }).when(analyzer).genFileSinkPlan(anyString(), any(QB.class), any(Operator.class));

    analyzer.analyze(astNode, ctx);

    // this is a soft assertion, and doesn't reflect the goal of this unit test,
    assertEquals("genFileSinkPlan is supposed to be called once during semantic analysis",
        1, capturedValues.size());
    FileSinkOperator operator = (FileSinkOperator) capturedValues.get(0);
    String finalPath = operator.getConf().getDestPath().toUri().toString();

    if (canUseCache) {
      Assert.assertTrue(String.format("Final path %s is not in the cache folder (%s), which is unexpected",
          finalPath, cacheDirPath), finalPath.contains(cacheDirPath));
    } else {
      assertFalse(String.format("Final path %s is in cache folder (%s), which is unexpected",
          finalPath, cacheDirPath), finalPath.contains(cacheDirPath));
    }
  }

  @Test
  public void testQueryTypes() throws Exception {
    // QUERY
    checkQueryType("SELECT key FROM table1", QueryType.QUERY);
    checkQueryType("SELECT key FROM table_non_existing", QueryType.QUERY);
    checkQueryType("SELECT a.value, b.value FROM table1 a JOIN table2 b ON a.key = b.key", QueryType.QUERY);

    // STATS
    checkQueryType("ANALYZE TABLE table1 COMPUTE STATISTICS", QueryType.STATS);
    checkQueryType("ANALYZE TABLE table1 COMPUTE STATISTICS FOR COLUMNS", QueryType.STATS);

    // DML
    checkQueryType("INSERT INTO table1 VALUES ('1', 1)", QueryType.DML);
    checkQueryType("INSERT OVERWRITE TABLE table1 SELECT * FROM table2", QueryType.DML);
    checkQueryType("UPDATE table_acid SET value = 2 WHERE key = '1'", QueryType.DML);
    checkQueryType("DELETE FROM table_acid WHERE key = '1'", QueryType.DML);
    checkQueryType("MERGE INTO table_acid AS target USING table1 AS source ON source.key = target.key " +
        "WHEN MATCHED THEN UPDATE SET key = 3 WHEN NOT MATCHED THEN INSERT VALUES ('3', 4)", QueryType.DML);
    // EXPORT acid table is handled by RewriteSemanticAnalyzer, but it's not a DML
    checkQueryType("EXPORT TABLE table_acid TO '/path'", QueryType.OTHER);

    // DDL
    checkQueryType("SHOW DATABASES", QueryType.DDL);
    checkQueryType("SHOW TABLES", QueryType.DDL);
    checkQueryType("CREATE DATABASE test_database_to_create", QueryType.DDL);
    checkQueryType("CREATE EXTERNAL TABLE test_part(id int) PARTITIONED BY(dt string) STORED AS ORC", QueryType.DDL);
    checkQueryType("ALTER TABLE table_part ADD PARTITION (part_col='3')", QueryType.DDL);
    checkQueryType("DROP TABLE table1", QueryType.DDL);
    checkQueryType("CREATE TABLE target AS SELECT * FROM table1", QueryType.DDL);
    // DDL: view
    checkQueryType("CREATE VIEW v1 AS SELECT * FROM table1", QueryType.DDL);
    checkQueryType("DROP VIEW IF EXISTS v1", QueryType.DDL);
    checkQueryType("ALTER VIEW view1 AS SELECT * FROM table1", QueryType.DDL);
    // DDL: materialized view
    checkQueryType("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM table1", QueryType.DDL);
    checkQueryType("DROP MATERIALIZED VIEW IF EXISTS mv1", QueryType.DDL);
    checkQueryType("ALTER MATERIALIZED VIEW mview1 REBUILD", QueryType.DDL);

    // OTHER
    // EXPLAIN is a utility, cannot classified as any of the categories above
    checkQueryType("EXPLAIN SELECT key FROM table1", QueryType.OTHER);
  }

  private void checkQueryType(String query, QueryProperties.QueryType expectedQueryType) throws Exception {
    SessionState.start(new SessionState(conf));
    Context ctx = new Context(conf);
    ASTNode astNode = ParseUtils.parse(query, ctx);
    QueryState queryState = new QueryState.Builder().withHiveConf(conf).build();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, "test_query_id");
    SessionState.get().addQueryState("test_query_id", queryState);
    // RewriteSemanticAnalyzer needs a txn manager that supportsAcid()
    HiveTxnManager txnManager = mock(DbTxnManager.class);
    when(txnManager.supportsAcid()).thenReturn(true);
    queryState.setTxnManager(txnManager);

    BaseSemanticAnalyzer analyzer = spy(SemanticAnalyzerFactory.get(queryState, astNode));

    analyzer.initCtx(ctx);
    try {
      analyzer.analyze(astNode, ctx);
    } catch (SemanticException e) {
      LOG.info("Ignoring semantic exception because the inner state of the Semantic Analyzer still can and will be " +
          "asserted in this unit test.", e);
    } finally {
      analyzer.endAnalysis(astNode); // this is called by Compiler in production
    }

    QueryType queryType = analyzer.getQueryProperties() == null ? null : analyzer.getQueryProperties().getQueryType();
    assertEquals(String.format("Expected query type for query '%s' is '%s', seen '%s'", query, expectedQueryType,
        queryType), expectedQueryType, queryType);
  }

  @Test
  public void testUsedTables() throws Exception {
    checkTablesUsed("SELECT key FROM table1", Sets.newHashSet("default.table1"));
    checkTablesUsed("INSERT OVERWRITE TABLE table1 SELECT * FROM table2", Sets.newHashSet("default.table1",
        "default.table2"));
    checkTablesUsed("INSERT OVERWRITE TABLE table1 SELECT * FROM other_db.table1",
        Sets.newHashSet("default.table1", "other_db.table1"));
    checkTablesUsed("SELECT a.value, b.value FROM table1 a JOIN table2 b ON a.key = b.key",
        Sets.newHashSet("default.table1",
            "default.table2"));
  }

  private void checkTablesUsed(String query, Set<String> tables) throws Exception {
    SessionState.start(conf);
    Context ctx = new Context(conf);
    ASTNode astNode = ParseUtils.parse(query, ctx);
    QueryState queryState = new QueryState.Builder().withHiveConf(conf).build();
    SemanticAnalyzer analyzer = spy((SemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, astNode));
    analyzer.initCtx(ctx);
    analyzer.analyze(astNode, ctx);

    Set<String> result = analyzer.getQueryProperties().getUsedTables();

    Assert.assertEquals(new TreeSet<>(tables), new TreeSet<>(result));
  }
}
