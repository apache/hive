package org.apache.hadoop.hive.ql;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class TxnCommandsBaseForTests {
  //bucket count for test tables; set it to 1 for easier debugging
  final static int BUCKET_COUNT = 2;
  @Rule
  public TestName testName = new TestName();
  HiveConf hiveConf;
  Driver d;
  enum Table {
    ACIDTBL("acidTbl"),
    ACIDTBLPART("acidTblPart"),
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

  @Before
  public void setUp() throws Exception {
    setUpInternal();
  }
  void setUpInternal() throws Exception {
    tearDown();
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, getWarehouseDir());
    hiveConf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    hiveConf
      .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, true);
    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.prepDb();
    File f = new File(getWarehouseDir());
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(getWarehouseDir()).mkdirs())) {
      throw new RuntimeException("Could not create " + getWarehouseDir());
    }
    SessionState.start(new SessionState(hiveConf));
    d = new Driver(hiveConf);
    d.setMaxRows(10000);
    dropTables();
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.ACIDTBLPART + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL2 + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create temporary  table " + Table.ACIDTBL2 + "(a int, b int, c int) clustered by (c) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.NONACIDNONBUCKET + "(a int, b int) stored as orc");
  }
  private void dropTables() throws Exception {
    for(TxnCommandsBaseForTests.Table t : TxnCommandsBaseForTests.Table.values()) {
      runStatementOnDriver("drop table if exists " + t);
    }
  }
  @After
  public void tearDown() throws Exception {
    try {
      if (d != null) {
        dropTables();
        d.destroy();
        d.close();
        d = null;
      }
    } finally {
      TxnDbUtil.cleanDb();
      FileUtils.deleteDirectory(new File(getTestDataDir()));
    }
  }
  String getWarehouseDir() {
    return getTestDataDir() + "/warehouse";
  }
  abstract String getTestDataDir();
  /**
   * takes raw data and turns it into a string as if from Driver.getResults()
   * sorts rows in dictionary order
   */
  List<String> stringifyValues(int[][] rowsIn) {
    return TestTxnCommands2.stringifyValues(rowsIn);
  }
  String makeValuesClause(int[][] rows) {
    return TestTxnCommands2.makeValuesClause(rows);
  }

  List<String> runStatementOnDriver(String stmt) throws Exception {
    CommandProcessorResponse cpr = d.run(stmt);
    if(cpr.getResponseCode() != 0) {
      throw new RuntimeException(stmt + " failed: " + cpr);
    }
    List<String> rs = new ArrayList<String>();
    d.getResults(rs);
    return rs;
  }
  CommandProcessorResponse runStatementOnDriverNegative(String stmt) throws Exception {
    CommandProcessorResponse cpr = d.run(stmt);
    if(cpr.getResponseCode() != 0) {
      return cpr;
    }
    throw new RuntimeException("Didn't get expected failure!");
  }
  /**
   * Will assert that actual files match expected.
   * @param expectedFiles - suffixes of expected Paths.  Must be the same length
   * @param rootPath - table or patition root where to start looking for actual files, recursively
   */
  void assertExpectedFileSet(Set<String> expectedFiles, String rootPath) throws Exception {
    int suffixLength = 0;
    for(String s : expectedFiles) {
      if(suffixLength > 0) {
        assert suffixLength == s.length() : "all entries must be the same length. current: " + s;
      }
      suffixLength = s.length();
    }
    FileSystem fs = FileSystem.get(hiveConf);
    Set<String> actualFiles = new HashSet<>();
    RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path(rootPath), true);
    while (remoteIterator.hasNext()) {
      LocatedFileStatus lfs = remoteIterator.next();
      if(!lfs.isDirectory() && org.apache.hadoop.hive.common.FileUtils.HIDDEN_FILES_PATH_FILTER.accept(lfs.getPath())) {
        String p = lfs.getPath().toString();
        actualFiles.add(p.substring(p.length() - suffixLength, p.length()));
      }
    }
    Assert.assertEquals("Unexpected file list", expectedFiles, actualFiles);
  }
}
