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
package org.apache.hadoop.hive.ql;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientWithLocalCache;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorTestUtilities.CompactorThreadType;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorThread;
import org.apache.hadoop.hive.ql.txn.compactor.Initiator;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.hive.metastore.DatabaseProduct.determineDatabaseProduct;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public abstract class TxnCommandsBaseForTests {
  private static final Logger LOG = LoggerFactory.getLogger(TxnCommandsBaseForTests.class);

  //bucket count for test tables; set it to 1 for easier debugging
  final static int BUCKET_COUNT = 2;
  @Rule
  public TestName testName = new TestName();
  
  protected HiveConf hiveConf;
  protected Driver d;
  protected TxnStore txnHandler;
  private DatabaseProduct databaseProduct;

  public enum Table {
    ACIDTBL("acidTbl"),
    ACIDTBLPART("acidTblPart"),
    ACIDTBLNESTEDPART("acidTblNestedPart"),
    ACIDTBL2("acidTbl2"),
    NONACIDORCTBL("nonAcidOrcTbl"),
    NONACIDORCTBL2("nonAcidOrcTbl2"),
    NONACIDNONBUCKET("nonAcidNonBucket");

    final String name;
    @Override
    public String toString() {
      return name;
    }
    Table(String name) {
      this.name = name;
    }
  }

  public TxnStore getTxnStore() {
    return txnHandler;
  }

  @Before
  @BeforeEach
  public void setUp() throws Exception {
    setUpInternal();

    // set up metastore client cache
    if (hiveConf.getBoolVar(HiveConf.ConfVars.MSC_CACHE_ENABLED)) {
      HiveMetaStoreClientWithLocalCache.init(hiveConf);
    }
  }
  void initHiveConf() {
    hiveConf = new HiveConfForTest(this.getClass());
    // Multiple tests requires more than one buckets per write. Use a very small value for grouping size to create
    // multiple mapper instances with FileSinkOperators. The number of buckets are depends on the size of the data
    // written and the grouping size. Most test cases expects 2 buckets.
    hiveConf.set("tez.grouping.max-size", "10");
    hiveConf.set("tez.grouping.min-size", "1");
    databaseProduct = determineDatabaseProduct(DatabaseProduct.DERBY_NAME, hiveConf);
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_TABLE_OPTIMIZERS,
        "org.apache.hadoop.hive.ql.txn.compactor.AcidTableOptimizer");
  }

  void setUpInternal() throws Exception {
    initHiveConf();
    Path workDir = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"));
    hiveConf.set("mapred.local.dir", workDir + File.separator + this.getClass().getSimpleName()
        + File.separator + "mapred" + File.separator + "local");
    hiveConf.set("mapred.system.dir", workDir + File.separator + this.getClass().getSimpleName()
        + File.separator + "mapred" + File.separator + "system");
    hiveConf.set("mapreduce.jobtracker.staging.root.dir", workDir + File.separator + this.getClass().getSimpleName()
        + File.separator + "mapred" + File.separator + "staging");
    hiveConf.set("mapred.temp.dir", workDir + File.separator + this.getClass().getSimpleName()
        + File.separator + "mapred" + File.separator + "temp");
    hiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, getWarehouseDir());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_INPUT_FORMAT, HiveInputFormat.class.getName());
    hiveConf
      .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, true);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.SPLIT_UPDATE, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_COL_AUTOGATHER, false);
    hiveConf.setBoolean("mapred.input.dir.recursive", true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);
      
    TestTxnDbUtil.setConfValues(hiveConf);
    TestTxnDbUtil.prepDb(hiveConf);
    txnHandler = TxnUtils.getTxnStore(hiveConf);
    File f = new File(getWarehouseDir());
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(getWarehouseDir()).mkdirs())) {
      throw new RuntimeException("Could not create " + getWarehouseDir());
    }
    SessionState ss = SessionState.start(hiveConf);
    ss.applyAuthorizationPolicy();
    d = new Driver(new QueryState.Builder().withHiveConf(hiveConf).nonIsolated().build());
    d.setMaxRows(10000);
    dropTables();
    setUpSchema();
  }

  protected void setUpSchema() throws Exception {
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.ACIDTBLPART + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.ACIDTBLNESTEDPART + "(a int, b int) partitioned by (p1 string, p2 string, p3 string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL2 + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create temporary  table " + Table.ACIDTBL2 + "(a int, b int, c int) clustered by (c) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.NONACIDNONBUCKET + "(a int, b int) stored as orc TBLPROPERTIES ('transactional'='false')");
  }
  protected void dropTables() throws Exception {
    for (TxnCommandsBaseForTests.Table t : TxnCommandsBaseForTests.Table.values()) {
      runStatementOnDriver("drop table if exists " + t);
    }
  }
  @After
  @AfterEach
  public void tearDown() throws Exception {
    try {
      if (d != null) {
        dropTables();
        d.close();
        d.destroy();
        d = null;
      }
    } finally {
      TestTxnDbUtil.cleanDb(hiveConf);
      FileUtils.deleteDirectory(new File(getTestDataDir()));
    }
  }
  protected String getWarehouseDir() {
    return getTestDataDir() + "/warehouse";
  }
  protected abstract String getTestDataDir();
  /**
   * takes raw data and turns it into a string as if from Driver.getResults()
   * sorts rows in dictionary order
   */
  public static List<String> stringifyValues(int[][] rowsIn) {
    assert rowsIn.length > 0;
    int[][] rows = rowsIn.clone();
    Arrays.sort(rows, new RowComp());
    List<String> rs = new ArrayList<>();
    for(int[] row : rows) {
      assert row.length > 0;
      StringBuilder sb = new StringBuilder();
      for(int value : row) {
        sb.append(value).append("\t");
      }
      sb.setLength(sb.length() - 1);
      rs.add(sb.toString());
    }
    return rs;
  }
  static class RowComp implements Comparator<int[]> {
    @Override
    public int compare(int[] row1, int[] row2) {
      assert row1 != null && row2 != null && row1.length == row2.length;
      for(int i = 0; i < row1.length; i++) {
        int comp = Integer.compare(row1[i], row2[i]);
        if(comp != 0) {
          return comp;
        }
      }
      return 0;
    }
  }
  public static String makeValuesClause(int[][] rows) {
    assert rows.length > 0;
    StringBuilder sb = new StringBuilder(" values");
    for (int[] row : rows) {
      assert row.length > 0;
      if (row.length > 1) {
        sb.append("(");
      }
      for (int value : row) {
        sb.append(value).append(",");
      }
      sb.setLength(sb.length() - 1);//remove trailing comma
      if (row.length > 1) {
        sb.append(")");
      }
      sb.append(",");
    }
    sb.setLength(sb.length() - 1);//remove trailing comma
    return sb.toString();
  }

  public static void runInitiator(HiveConf hiveConf) throws Exception {
    runCompactorThread(hiveConf, CompactorThreadType.INITIATOR);
  }
  public static void runWorker(HiveConf hiveConf) throws Exception {
    runCompactorThread(hiveConf, CompactorThreadType.WORKER);
  }
  public static void runWorker(HiveConf hiveConf, String poolName) throws Exception {
    runCompactorThread(hiveConf, CompactorThreadType.WORKER, poolName);
  }
  public static void runCleaner(HiveConf hiveConf) throws Exception {
    // Wait for the cooldown period so the Cleaner can see the last committed txn as the highest committed watermark
    Thread.sleep(MetastoreConf.getTimeVar(hiveConf, MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS));
    runCompactorThread(hiveConf, CompactorThreadType.CLEANER);
  }
  private static void runCompactorThread(HiveConf hiveConf, CompactorThreadType type) throws Exception {
    runCompactorThread(hiveConf, type, Constants.COMPACTION_DEFAULT_POOL);
  }
  private static void runCompactorThread(HiveConf hiveConf, CompactorThreadType type, String poolName)
      throws Exception {
    AtomicBoolean stop = new AtomicBoolean(true);
    CompactorThread t;
    switch (type) {
      case INITIATOR:
        t = new Initiator();
        break;
      case WORKER:
        t = new Worker();
        if (poolName != null && !poolName.equals(Constants.COMPACTION_DEFAULT_POOL)) {
          ((Worker)t).setPoolName(poolName); 
        }
        break;
      case CLEANER:
        t = new Cleaner();
        break;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
    t.setConf(hiveConf);
    t.init(stop);
    t.run();
  }

  protected List<String> runStatementOnDriver(String stmt) throws Exception {
    LOG.info("Running the query: " + stmt);
    try {
      d.run(stmt);
    } catch (CommandProcessorException e) {
      throw new RuntimeException(stmt + " failed: " + e);
    }
    List<String> rs = new ArrayList<>();
    d.getResults(rs);
    return rs;
  }

  protected CommandProcessorException runStatementOnDriverNegative(String stmt) {
    try {
      d.run(stmt);
    } catch (CommandProcessorException e) {
      return e;
    }
    throw new RuntimeException("Didn't get expected failure!");
  }

  protected void assertMappersAreVectorized(String query)
          throws Exception {
    List<String> rs = runStatementOnDriver("EXPLAIN FORMATTED VECTORIZATION DETAIL " + query);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> plan = objectMapper.readValue(rs.get(0), Map.class);
    Map<String, Object> stages = (Map<String, Object>) plan.get("STAGE PLANS");
    Map<String, Object> tezStage = null;
    if (stages == null) {
      Assert.fail("Execution plan of query does not have have stages: " + rs.get(0));
    }
    for (Map.Entry<String, Object> stageEntry : stages.entrySet()) {
      Map<String, Object> stage = (Map<String, Object>) stageEntry.getValue();
      tezStage = (Map<String, Object>) stage.get("Tez");
      if (tezStage != null) {
        break;
      }
    }
    if (tezStage == null) {
      Assert.fail("Execution plan of query does not contain a Tez stage: " + rs.get(0));
    }
    Map<String, Object> vertices = (Map<String, Object>) tezStage.get("Vertices:");
    if (vertices == null) {
      Assert.fail("Execution plan of query does not contain Tez vertices: " + rs.get(0));
    }
    for (Map.Entry<String, Object> vertexEntry : stages.entrySet()) {
      if (vertexEntry.getKey() == null || !vertexEntry.getKey().startsWith("Map")) {
        continue;
      }
      Map<String, Object> mapVertex = (Map<String, Object>) vertexEntry.getValue();
      String executionMode = (String) mapVertex.get("Execution mode");
      boolean vectorized = isNotBlank(executionMode) && executionMode.contains("vectorized");
      String message = "Mapper was " + (shouldVectorized() ? "not vectorized: " : "vectorized but was not expected: ");
      Assert.assertTrue(message + rs.get(0),
              shouldVectorized() ^ vectorized);
    }
  }
  /**
   * Will assert that actual files match expected.
   * @param expectedFiles - suffixes of expected Paths.  Must be the same length
   * @param rootPath - table or partition root where to start looking for actual files, recursively
   */
  void assertExpectedFileSet(Set<String> expectedFiles, String rootPath, String tableName) throws Exception {
    Pattern pattern = Pattern.compile("(.+)/(" + tableName + "/[delete_delta|delta|base].+)");
    FileSystem fs = FileSystem.get(hiveConf);
    Set<String> actualFiles = new HashSet<>();
    RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path(rootPath), true);
    while (remoteIterator.hasNext()) {
      LocatedFileStatus lfs = remoteIterator.next();
      if(!lfs.isDirectory() && org.apache.hadoop.hive.common.FileUtils.HIDDEN_FILES_PATH_FILTER.accept(lfs.getPath())) {
        String p = lfs.getPath().toString();
        Matcher matcher = pattern.matcher(p);
        if (matcher.matches()) {
          actualFiles.add(matcher.group(2));
        }
      }
    }
    Assert.assertEquals("Unexpected file list", expectedFiles, actualFiles);
  }
  void checkExpected(List<String> rs, String[][] expected, String msg, Logger LOG) {
    LOG.warn(testName.getMethodName() + ": read data(" + msg + "): ");
    logResult(LOG, rs);
    Assert.assertEquals(testName.getMethodName() + ": " + msg + "; " + rs,
        expected.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line (data) " + i + " data: " + rs.get(i) + "; expected " + expected[i][0], rs.get(i).startsWith(expected[i][0]));
      if (expected[i].length == 2) {
        Assert.assertTrue("Actual line(file) " + i + " file: " + rs.get(i),
                rs.get(i).endsWith(expected[i][1]) || rs.get(i).matches(expected[i][1]));
      }
    }
  }
  void logResult(Logger LOG, List<String> rs) {
    StringBuilder sb = new StringBuilder();
    for(String s : rs) {
      sb.append(s).append('\n');
    }
    LOG.info(sb.toString());
  }
  /**
   * We have to use a different query to check results for Vectorized tests because to get the
   * file name info we need to use {@link org.apache.hadoop.hive.ql.metadata.VirtualColumn#FILENAME}
   * which will currently make the query non-vectorizable.  This means we can't check the file name
   * for vectorized version of the test.
   */
  protected void checkResultAndVectorization(String[][] expectedResult, String query, String msg, Logger LOG)
          throws Exception {
    checkResult(expectedResult, query, msg, LOG);
    assertMappersAreVectorized(query);
  }
  protected void checkResult(String[][] expectedResult, String query, String msg, Logger LOG)
          throws Exception {
    List<String> rs = runStatementOnDriver(query);
    checkExpected(rs, expectedResult, msg + (shouldVectorized() ? " vect" : ""), LOG);
  }
  void dropTables(String... tables) throws Exception {
    HiveConf queryConf = d.getQueryState().getConf();
    queryConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    for (String table : tables) {
      d.run("drop table if exists " + table);
    }
    queryConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
  }
  Driver swapDrivers(Driver otherDriver) {
    Driver tmp = d;
    d = otherDriver;
    return tmp;
  }

  protected void waitUntilAllTxnFinished() throws Exception {
    long openTxnTimeOutMillis = MetastoreConf.getTimeVar(
            hiveConf, MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS);
    while (getOpenTxnCount(openTxnTimeOutMillis) > 0) {
      Thread.sleep(openTxnTimeOutMillis);
    }
  }

  protected int getOpenTxnCount(long openTxnTimeOutMillis) throws Exception {
    return TestTxnDbUtil.countQueryAgent(hiveConf,
            "select count(*) from TXNS where TXN_STATE = '" + TxnStatus.OPEN.getSqlConst() + "' " +
                    "or TXN_STARTED >= (" + getEpochFn(databaseProduct) +
                    " - " + openTxnTimeOutMillis + ")");
  }

  protected boolean shouldVectorized() {
    return hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
  }
}
