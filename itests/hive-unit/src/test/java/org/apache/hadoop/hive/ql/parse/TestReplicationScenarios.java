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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.MessageFormatFilter;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hive.hcatalog.api.repl.ReplicationV1CompatRule;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestReplicationScenarios {

  @Rule
  public final TestName testName = new TestName();

  private final static String DBNOTIF_LISTENER_CLASSNAME =
      "org.apache.hive.hcatalog.listener.DbNotificationListener";
      // FIXME : replace with hive copy once that is copied
  private final static String tid =
      TestReplicationScenarios.class.getCanonicalName().toLowerCase().replace('.','_') + "_" + System.currentTimeMillis();
  private final static String TEST_PATH =
      System.getProperty("test.warehouse.dir", "/tmp") + Path.SEPARATOR + tid;

  private static HiveConf hconf;
  private static int msPort;
  private static IDriver driver;
  private static HiveMetaStoreClient metaStoreClient;
  private static String proxySettingName;
  static HiveConf hconfMirror;
  static int msPortMirror;
  static IDriver driverMirror;
  static HiveMetaStoreClient metaStoreClientMirror;

  @Rule
  public TestRule replV1BackwardCompatibleRule =
      new ReplicationV1CompatRule(metaStoreClient, hconf,
          new ArrayList<>(Arrays.asList("testEventFilters")));
  // Make sure we skip backward-compat checking for those tests that don't generate events

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private ArrayList<String> lastResults;

  private final boolean VERIFY_SETUP_STEPS = true;
  // if verifySetup is set to true, all the test setup we do will perform additional
  // verifications as well, which is useful to verify that our setup occurred
  // correctly when developing and debugging tests. These verifications, however
  // do not test any new functionality for replication, and thus, are not relevant
  // for testing replication itself. For steady state, we want this to be false.

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    hconf = new HiveConf(TestReplicationScenarios.class);
    String metastoreUri = System.getProperty("test."+HiveConf.ConfVars.METASTOREURIS.varname);
    if (metastoreUri != null) {
      hconf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
      return;
    }

    hconf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS,
        DBNOTIF_LISTENER_CLASSNAME); // turn on db notification listener on metastore
    hconf.setBoolVar(HiveConf.ConfVars.REPLCMENABLED, true);
    hconf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    hconf.setVar(HiveConf.ConfVars.REPLCMDIR, TEST_PATH + "/cmroot/");
    proxySettingName = "hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts";
    hconf.set(proxySettingName, "*");
    msPort = MetaStoreTestUtils.startMetaStore(hconf);
    hconf.setVar(HiveConf.ConfVars.REPLDIR,TEST_PATH + "/hrepl/");
    hconf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
    hconf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hconf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hconf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname,
        "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
    hconf.set(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname,
              "org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore");
    hconf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, true);
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

    Path testPath = new Path(TEST_PATH);
    FileSystem fs = FileSystem.get(testPath.toUri(),hconf);
    fs.mkdirs(testPath);

    driver = DriverFactory.newDriver(hconf);
    SessionState.start(new CliSessionState(hconf));
    metaStoreClient = new HiveMetaStoreClient(hconf);

    FileUtils.deleteDirectory(new File("metastore_db2"));
    HiveConf hconfMirrorServer = new HiveConf();
    hconfMirrorServer.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:;databaseName=metastore_db2;create=true");
    msPortMirror = MetaStoreTestUtils.startMetaStore(hconfMirrorServer);
    hconfMirror = new HiveConf(hconf);
    hconfMirror.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPortMirror);
    driverMirror = DriverFactory.newDriver(hconfMirror);
    metaStoreClientMirror = new HiveMetaStoreClient(hconfMirror);

    ObjectStore.setTwoMetastoreTesting(true);
  }

  @AfterClass
  public static void tearDownAfterClass(){
    // FIXME : should clean up TEST_PATH, but not doing it now, for debugging's sake
  }

  @Before
  public void setUp(){
    // before each test
    SessionState.get().setCurrentDatabase("default");
  }

  @After
  public void tearDown(){
    // after each test
  }

  private static  int next = 0;
  private synchronized void advanceDumpDir() {
    next++;
    ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next));
  }

 static class Tuple {
    final String dumpLocation;
    final String lastReplId;

    Tuple(String dumpLocation, String lastReplId) {
      this.dumpLocation = dumpLocation;
      this.lastReplId = lastReplId;
    }
  }

  private Tuple bootstrapLoadAndVerify(String dbName, String replDbName) throws IOException {
    return incrementalLoadAndVerify(dbName, null, replDbName);
  }

  private Tuple incrementalLoadAndVerify(String dbName, String fromReplId, String replDbName) throws IOException {
    Tuple dump = replDumpDb(dbName, fromReplId, null, null);
    loadAndVerify(replDbName, dump.dumpLocation, dump.lastReplId);
    return dump;
  }

  private Tuple dumpDbFromLastDump(String dbName, Tuple lastDump) throws IOException {
    return replDumpDb(dbName, lastDump.lastReplId, null, null);
  }

  private Tuple replDumpDb(String dbName, String fromReplID, String toReplID, String limit) throws IOException {
    advanceDumpDir();
    String dumpCmd = "REPL DUMP " + dbName;
    if (null != fromReplID) {
      dumpCmd = dumpCmd + " FROM " + fromReplID;
    }
    if (null != toReplID) {
      dumpCmd = dumpCmd + " TO " + toReplID;
    }
    if (null != limit) {
      dumpCmd = dumpCmd + " LIMIT " + limit;
    }
    run(dumpCmd, driver);
    String dumpLocation = getResult(0, 0, driver);
    String lastReplId = getResult(0, 1, true, driver);
    LOG.info("Dumped to {} with id {} for command: {}", dumpLocation, lastReplId, dumpCmd);
    return new Tuple(dumpLocation, lastReplId);
  }

  private void loadAndVerify(String replDbName, String dumpLocation, String lastReplId) throws IOException {
    run("REPL LOAD " + replDbName + " FROM '" + dumpLocation + "'", driverMirror);
    verifyRun("REPL STATUS " + replDbName, lastReplId, driverMirror);
    return;
  }

  /**
   * Tests basic operation - creates a db, with 4 tables, 2 ptned and 2 unptned.
   * Inserts data into one of the ptned tables, and one of the unptned tables,
   * and verifies that a REPL DUMP followed by a REPL LOAD is able to load it
   * appropriately. This tests bootstrap behaviour primarily.
   */
  @Test
  public void testBasic() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, name + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, name + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty, driver);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty, driver);

    String replicatedDbName = dbName + "_dupe";
    bootstrapLoadAndVerify(dbName, replicatedDbName);

    verifyRun("SELECT * from " + replicatedDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned WHERE b=2", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned_empty", empty, driverMirror);
    verifyRun("SELECT * from " + replicatedDbName + ".unptned_empty", empty, driverMirror);
  }

  @Test
  public void testBasicWithCM() throws Exception {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, name + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, name + "_ptn2").toUri().getPath();
    String ptn_locn_2_later = new Path(TEST_PATH, name + "_ptn2_later").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    run("SELECT * from " + dbName + ".unptned", driver);
    verifyResults(unptn_data, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    run("SELECT a from " + dbName + ".ptned WHERE b=1", driver);
    verifyResults(ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    run("SELECT a from " + dbName + ".ptned WHERE b=2", driver);
    verifyResults(ptn_data_2, driver);
    run("SELECT a from " + dbName + ".ptned_empty", driver);
    verifyResults(empty, driver);
    run("SELECT * from " + dbName + ".unptned_empty", driver);
    verifyResults(empty, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0, driver);
    String replDumpId = getResult(0,1,true, driver);

    // Table dropped after "repl dump"
    run("DROP TABLE " + dbName + ".unptned", driver);

    // Partition droppped after "repl dump"
    run("ALTER TABLE " + dbName + ".ptned " + "DROP PARTITION(b=1)", driver);

    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    run("REPL STATUS " + dbName + "_dupe", driverMirror);
    verifyResults(new String[] {replDumpId}, driverMirror);

    run("SELECT * from " + dbName + "_dupe.unptned", driverMirror);
    verifyResults(unptn_data, driverMirror);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", driverMirror);
    verifyResults(ptn_data_1, driverMirror);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", driverMirror);
    verifyResults(ptn_data_2, driverMirror);
    run("SELECT a from " + dbName + ".ptned_empty", driverMirror);
    verifyResults(empty, driverMirror);
    run("SELECT * from " + dbName + ".unptned_empty", driverMirror);
    verifyResults(empty, driverMirror);
  }

  @Test
  public void testBootstrapLoadOnExistingDb() throws IOException {
    String testName = "bootstrapLoadOnExistingDb";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    createTestDataFile(unptn_locn, unptn_data);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    // Create an empty database to load
    run("CREATE DATABASE " + dbName + "_empty", driverMirror);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0,driver);
    String replDumpId = getResult(0,1,true,driver);
    // Load to an empty database
    run("REPL LOAD " + dbName + "_empty FROM '" + replDumpLocn + "'", driverMirror);

    // REPL STATUS should return same repl ID as dump
    verifyRun("REPL STATUS " + dbName + "_empty", replDumpId, driverMirror);
    verifyRun("SELECT * from " + dbName + "_empty.unptned", unptn_data, driverMirror);

    String[] nullReplId = new String[]{ "NULL" };

    // Create a database with a table
    run("CREATE DATABASE " + dbName + "_withtable", driverMirror);
    run("CREATE TABLE " + dbName + "_withtable.unptned(a string) STORED AS TEXTFILE", driverMirror);
    // Load using same dump to a DB with table. It should fail as DB is not empty.
    verifyFail("REPL LOAD " + dbName + "_withtable FROM '" + replDumpLocn + "'", driverMirror);

    // REPL STATUS should return NULL
    verifyRun("REPL STATUS " + dbName + "_withtable", nullReplId, driverMirror);

    // Create a database with a view
    run("CREATE DATABASE " + dbName + "_withview", driverMirror);
    run("CREATE TABLE " + dbName + "_withview.unptned(a string) STORED AS TEXTFILE", driverMirror);
    run("CREATE VIEW " + dbName + "_withview.view AS SELECT * FROM " + dbName + "_withview.unptned", driverMirror);
    // Load using same dump to a DB with view. It should fail as DB is not empty.
    verifyFail("REPL LOAD " + dbName + "_withview FROM '" + replDumpLocn + "'", driverMirror);

    // REPL STATUS should return NULL
    verifyRun("REPL STATUS " + dbName + "_withview", nullReplId, driverMirror);
  }

  @Test
  public void testBootstrapWithConcurrentDropTable() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, name + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, name + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);

    advanceDumpDir();

    BehaviourInjection<Table,Table> ptnedTableNuller = new BehaviourInjection<Table,Table>(){
      @Nullable
      @Override
      public Table apply(@Nullable Table table) {
        if (table.getTableName().equalsIgnoreCase("ptned")){
          injectionPathCalled = true;
          return null;
        } else {
          nonInjectedPathCalled = true;
          return table;
        }
      }
    };
    InjectableBehaviourObjectStore.setGetTableBehaviour(ptnedTableNuller);

    // The ptned table will not be dumped as getTable will return null
    run("REPL DUMP " + dbName, driver);
    ptnedTableNuller.assertInjectionsPerformed(true,true);
    InjectableBehaviourObjectStore.resetGetTableBehaviour(); // reset the behaviour

    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    // The ptned table should miss in target as the table was marked virtually as dropped
    verifyRun("SELECT * from " + dbName + "_dupe.unptned", unptn_data, driverMirror);
    verifyFail("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", driverMirror);
    verifyIfTableNotExist(dbName + "_dupe", "ptned", metaStoreClient);

    // Verify if Drop table on a non-existing table is idempotent
    run("DROP TABLE " + dbName + ".ptned", driver);
    verifyIfTableNotExist(dbName, "ptned", metaStoreClient);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String postDropReplDumpLocn = getResult(0,0, driver);
    String postDropReplDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);
    assert(run("REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'", true, driverMirror));

    verifyRun("SELECT * from " + dbName + "_dupe.unptned", unptn_data, driverMirror);
    verifyIfTableNotExist(dbName + "_dupe", "ptned", metaStoreClientMirror);
    verifyFail("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", driverMirror);
  }

  @Test
  public void testBootstrapWithConcurrentDropPartition() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String ptn_locn_1 = new Path(TEST_PATH, name + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, name + "_ptn2").toUri().getPath();

    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);

    advanceDumpDir();

    BehaviourInjection<List<String>, List<String>> listPartitionNamesNuller
            = new BehaviourInjection<List<String>, List<String>>(){
      @Nullable
      @Override
      public List<String> apply(@Nullable List<String> partitions) {
        injectionPathCalled = true;
        return new ArrayList<String>();
      }
    };
    InjectableBehaviourObjectStore.setListPartitionNamesBehaviour(listPartitionNamesNuller);

    // None of the partitions will be dumped as the partitions list was empty
    run("REPL DUMP " + dbName, driver);
    listPartitionNamesNuller.assertInjectionsPerformed(true, false);
    InjectableBehaviourObjectStore.resetListPartitionNamesBehaviour(); // reset the behaviour

    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    // All partitions should miss in target as it was marked virtually as dropped
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", empty, driverMirror);
    verifyIfPartitionNotExist(dbName + "_dupe", "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClientMirror);
    verifyIfPartitionNotExist(dbName + "_dupe", "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClientMirror);

    // Verify if drop partition on a non-existing partition is idempotent and just a noop.
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b=1)", driver);
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b=2)", driver);
    verifyIfPartitionNotExist(dbName, "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClient);
    verifyIfPartitionNotExist(dbName, "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClient);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", empty, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String postDropReplDumpLocn = getResult(0,0,driver);
    String postDropReplDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);
    assert(run("REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'", true, driverMirror));

    verifyIfPartitionNotExist(dbName + "_dupe", "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClientMirror);
    verifyIfPartitionNotExist(dbName + "_dupe", "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClientMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", empty, driverMirror);
  }

  @Test
  public void testBootstrapWithConcurrentRename() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] ptn_data = new String[]{ "eleven" , "twelve" };
    String[] empty = new String[]{};
    String ptn_locn = new Path(TEST_PATH, name + "_ptn").toUri().getPath();

    createTestDataFile(ptn_locn, ptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);

    BehaviourInjection<Table,Table> ptnedTableRenamer = new BehaviourInjection<Table,Table>(){
      boolean success = false;

      @Nullable
      @Override
      public Table apply(@Nullable Table table) {
        if (injectionPathCalled) {
          nonInjectedPathCalled = true;
        } else {
          // getTable is invoked after fetching the table names
          injectionPathCalled = true;
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              LOG.info("Entered new thread");
              IDriver driver2 = DriverFactory.newDriver(hconf);
              SessionState.start(new CliSessionState(hconf));
              CommandProcessorResponse ret =
                  driver2.run("ALTER TABLE " + dbName + ".ptned PARTITION (b=1) RENAME TO PARTITION (b=10)");
              success = (ret.getException() == null);
              assertFalse(success);
              ret = driver2.run("ALTER TABLE " + dbName + ".ptned RENAME TO " + dbName + ".ptned_renamed");
              success = (ret.getException() == null);
              assertFalse(success);
              LOG.info("Exit new thread success - {}", success);
            }
          });
          t.start();
          LOG.info("Created new thread {}", t.getName());
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return table;
      }
    };
    InjectableBehaviourObjectStore.setGetTableBehaviour(ptnedTableRenamer);

    // The intermediate rename would've failed as bootstrap dump in progress
    bootstrapLoadAndVerify(dbName, replDbName);

    ptnedTableRenamer.assertInjectionsPerformed(true,true);
    InjectableBehaviourObjectStore.resetGetTableBehaviour(); // reset the behaviour

    // The ptned table should be there in both source and target as rename was not successful
    verifyRun("SELECT a from " + dbName + ".ptned WHERE (b=1) ORDER BY a", ptn_data, driver);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE (b=1) ORDER BY a", ptn_data, driverMirror);

    // Verify if Rename after bootstrap is successful
    run("ALTER TABLE " + dbName + ".ptned PARTITION (b=1) RENAME TO PARTITION (b=10)", driver);
    verifyIfPartitionNotExist(dbName, "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClient);
    run("ALTER TABLE " + dbName + ".ptned RENAME TO " + dbName + ".ptned_renamed", driver);
    verifyIfTableNotExist(dbName, "ptned", metaStoreClient);
    verifyRun("SELECT a from " + dbName + ".ptned_renamed WHERE (b=10) ORDER BY a", ptn_data, driver);
  }

  @Test
  public void testBootstrapWithDropPartitionedTable() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] ptn_data = new String[]{ "eleven" , "twelve" };
    String[] empty = new String[]{};
    String ptn_locn = new Path(TEST_PATH, name + "_ptn").toUri().getPath();

    createTestDataFile(ptn_locn, ptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);

    BehaviourInjection<Table,Table> ptnedTableRenamer = new BehaviourInjection<Table,Table>(){
      boolean success = false;

      @Nullable
      @Override
      public Table apply(@Nullable Table table) {
        if (injectionPathCalled) {
          nonInjectedPathCalled = true;
        } else {
          // getTable is invoked after fetching the table names
          injectionPathCalled = true;
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              LOG.info("Entered new thread");
              IDriver driver2 = DriverFactory.newDriver(hconf);
              SessionState.start(new CliSessionState(hconf));
              CommandProcessorResponse ret = driver2.run("DROP TABLE " + dbName + ".ptned");
              success = (ret.getException() == null);
              assertTrue(success);
              LOG.info("Exit new thread success - {}", success);
            }
          });
          t.start();
          LOG.info("Created new thread {}", t.getName());
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return table;
      }
    };
    InjectableBehaviourObjectStore.setGetTableBehaviour(ptnedTableRenamer);

    Tuple bootstrap = bootstrapLoadAndVerify(dbName, replDbName);

    ptnedTableRenamer.assertInjectionsPerformed(true,true);
    InjectableBehaviourObjectStore.resetGetTableBehaviour(); // reset the behaviour

    incrementalLoadAndVerify(dbName, bootstrap.lastReplId, replDbName);
    verifyIfTableNotExist(replDbName, "ptned", metaStoreClientMirror);

  }

  @Test
  public void testIncrementalAdds() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0,driver);
    String replDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}",replDumpLocn,replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, name + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, name + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty, driverMirror);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty, driverMirror);

    // Now, we load data into the tables, and see if an incremental
    // repl drop/load can duplicate it.

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    run("CREATE TABLE " + dbName + ".unptned_late AS SELECT * from " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptn_data, driver);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);

    run("CREATE TABLE " + dbName + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1",ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptn_data_2, driver);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0,0,driver);
    String incrementalDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'", driverMirror);

    run("REPL STATUS " + dbName + "_dupe", driverMirror);
    verifyResults(new String[] {incrementalDumpId}, driverMirror);

    // VERIFY tables and partitions on destination for equivalence.

    verifyRun("SELECT * from " + dbName + "_dupe.unptned_empty", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_empty", empty, driverMirror);

//    verifyRun("SELECT * from " + dbName + "_dupe.unptned", unptn_data);
    // TODO :this does not work because LOAD DATA LOCAL INPATH into an unptned table seems
    // to use ALTER_TABLE only - it does not emit an INSERT or CREATE - re-enable after
    // fixing that.
    verifyRun("SELECT * from " + dbName + "_dupe.unptned_late", unptn_data, driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", ptn_data_2, driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=2", ptn_data_2, driverMirror);
  }

  @Test
  public void testIncrementalLoadWithVariableLengthEventId() throws IOException, TException {
    String testName = "incrementalLoadWithVariableLengthEventId";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('ten')", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    // CREATE_TABLE - TRUNCATE - INSERT - The result is just one record.
    // Creating dummy table to control the event ID of TRUNCATE not to be 10 or 100 or 1000...
    String[] unptn_data = new String[]{ "eleven" };
    run("CREATE TABLE " + dbName + ".dummy(a string) STORED AS TEXTFILE", driver);
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);

    // Inject a behaviour where all events will get ID less than 100 except TRUNCATE which will get ID 100.
    // This enesures variable length of event ID in the incremental dump
    BehaviourInjection<NotificationEventResponse,NotificationEventResponse> eventIdModifier
            = new BehaviourInjection<NotificationEventResponse,NotificationEventResponse>(){
      private long nextEventId = 0; // Initialize to 0 as for increment dump, 0 won't be used.

      @Nullable
      @Override
      public NotificationEventResponse apply(@Nullable NotificationEventResponse eventIdList) {
        if (null != eventIdList) {
          List<NotificationEvent> eventIds = eventIdList.getEvents();
          List<NotificationEvent> outEventIds = new ArrayList<NotificationEvent>();
          for (int i = 0; i < eventIds.size(); i++) {
            NotificationEvent event = eventIds.get(i);

            // Skip all the events belong to other DBs/tables.
            if (event.getDbName().equalsIgnoreCase(dbName)) {
              // We will encounter create_table, truncate followed by insert.
              // For the insert, set the event ID longer such that old comparator picks insert before truncate
              // Eg: Event IDs CREATE_TABLE - 5, TRUNCATE - 9, INSERT - 12 changed to
              // CREATE_TABLE - 5, TRUNCATE - 9, INSERT - 100
              // But if TRUNCATE have ID-10, then having INSERT-100 won't be sufficient to test the scenario.
              // So, we set any event comes after CREATE_TABLE starts with 20.
              // Eg: Event IDs CREATE_TABLE - 5, TRUNCATE - 10, INSERT - 12 changed to
              // CREATE_TABLE - 5, TRUNCATE - 20(20 <= Id < 100), INSERT - 100
              switch (event.getEventType()) {
                case "CREATE_TABLE": {
                  // The next ID is set to 20 or 200 or 2000 ... based on length of current event ID
                  // This is done to ensure TRUNCATE doesn't get an ID 10 or 100...
                  nextEventId = (long) Math.pow(10.0, (double) String.valueOf(event.getEventId()).length()) * 2;
                  break;
                }
                case "INSERT": {
                  // INSERT will come always after CREATE_TABLE, TRUNCATE. So, no need to validate nextEventId
                  nextEventId = (long) Math.pow(10.0, (double) String.valueOf(nextEventId).length());
                  LOG.info("Changed EventId #{} to #{}", event.getEventId(), nextEventId);
                  event.setEventId(nextEventId++);
                  break;
                }
                default: {
                  // After CREATE_TABLE all the events in this DB should get an ID >= 20 or 200 ...
                  if (nextEventId > 0) {
                    LOG.info("Changed EventId #{} to #{}", event.getEventId(), nextEventId);
                    event.setEventId(nextEventId++);
                  }
                  break;
                }
              }

              outEventIds.add(event);
            }
          }
          injectionPathCalled = true;
          if (outEventIds.isEmpty()) {
            return eventIdList; // If not even one event belongs to current DB, then return original one itself.
          } else {
            // If the new list is not empty (input list have some events from this DB), then return it
            return new NotificationEventResponse(outEventIds);
          }
        } else {
          return null;
        }
      }
    };
    InjectableBehaviourObjectStore.setGetNextNotificationBehaviour(eventIdModifier);

    // It is possible that currentNotificationEventID from metastore is less than newly set event ID by stub function.
    // In this case, REPL DUMP will skip events beyond this upper limit.
    // So, to avoid this failure, we will set the TO clause to ID 100 times of currentNotificationEventID
    String cmd = "REPL DUMP " + dbName + " FROM " + replDumpId
               + " TO " + String.valueOf(metaStoreClient.getCurrentNotificationEventId().getEventId()*100);

    advanceDumpDir();
    run(cmd, driver);
    eventIdModifier.assertInjectionsPerformed(true,false);
    InjectableBehaviourObjectStore.resetGetNextNotificationBehaviour(); // reset the behaviour

    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data, driverMirror);
  }

  @Test
  public void testDrops() throws IOException {

    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned3(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, name + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, name + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='1'", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='2'", ptn_data_2, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='1'", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='2'", ptn_data_2, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned3 PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned3 PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b=2", ptn_data_2, driver);

    // At this point, we've set up all the tables and ptns we're going to test drops across
    // Replicate it first, and then we'll drop it on the source.

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0,driver);
    String replDumpId = getResult(0,1,true,driver);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);
    verifySetup("REPL STATUS " + dbName + "_dupe", new String[]{replDumpId}, driverMirror);

    verifySetup("SELECT * from " + dbName + "_dupe.unptned", unptn_data, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned WHERE b='1'", ptn_data_1, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'", ptn_data_2, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='1'", ptn_data_1, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='2'", ptn_data_2, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned3 WHERE b=1", ptn_data_1, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned3 WHERE b=2", ptn_data_2, driverMirror);

    // All tables good on destination, drop on source.

    run("DROP TABLE " + dbName + ".unptned", driver);
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b='2')", driver);
    run("DROP TABLE " + dbName + ".ptned2", driver);
    run("ALTER TABLE " + dbName + ".ptned3 DROP PARTITION (b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='2'", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned3 WHERE b=1",empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned3", ptn_data_2, driver);

    // replicate the incremental drops

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String postDropReplDumpLocn = getResult(0,0,driver);
    String postDropReplDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'", driverMirror);

    // verify that drops were replicated. This can either be from tables or ptns
    // not existing, and thus, throwing a NoSuchObjectException, or returning nulls
    // or select * returning empty, depending on what we're testing.

    verifyIfTableNotExist(dbName + "_dupe", "unptned", metaStoreClientMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned3 WHERE b=1", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned3", ptn_data_2, driverMirror);

    verifyIfTableNotExist(dbName + "_dupe", "ptned2", metaStoreClientMirror);
  }

  @Test
  public void testDropsWithCM() throws IOException {

    String testName = "drops_with_cm";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b string) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    run("SELECT * from " + dbName + ".unptned", driver);
    verifyResults(unptn_data, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')", driver);
    run("SELECT a from " + dbName + ".ptned WHERE b='1'", driver);
    verifyResults(ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')", driver);
    run("SELECT a from " + dbName + ".ptned WHERE b='2'", driver);
    verifyResults(ptn_data_2, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')", driver);
    run("SELECT a from " + dbName + ".ptned2 WHERE b='1'", driver);
    verifyResults(ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')", driver);
    run("SELECT a from " + dbName + ".ptned2 WHERE b='2'", driver);
    verifyResults(ptn_data_2, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0,driver);
    String replDumpId = getResult(0,1,true,driver);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    run("REPL STATUS " + dbName + "_dupe", driverMirror);
    verifyResults(new String[] {replDumpId}, driverMirror);

    run("SELECT * from " + dbName + "_dupe.unptned", driverMirror);
    verifyResults(unptn_data, driverMirror);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b='1'", driverMirror);
    verifyResults(ptn_data_1, driverMirror);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'", driverMirror);
    verifyResults(ptn_data_2, driverMirror);
    run("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='1'", driverMirror);
    verifyResults(ptn_data_1, driverMirror);
    run("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='2'", driverMirror);
    verifyResults(ptn_data_2, driverMirror);

    run("CREATE TABLE " + dbName + ".unptned_copy" + " AS SELECT a FROM " + dbName + ".unptned", driver);
    run("CREATE TABLE " + dbName + ".ptned_copy" + " LIKE " + dbName + ".ptned", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_copy" + " PARTITION(b='1') SELECT a FROM " +
        dbName + ".ptned WHERE b='1'", driver);
    run("SELECT a from " + dbName + ".unptned_copy", driver);
    verifyResults(unptn_data, driver);
    run("SELECT a from " + dbName + ".ptned_copy", driver);
    verifyResults(ptn_data_1, driver);

    run("DROP TABLE " + dbName + ".unptned", driver);
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b='2')", driver);
    run("DROP TABLE " + dbName + ".ptned2", driver);
    run("SELECT a from " + dbName + ".ptned WHERE b=2", driver);
    verifyResults(empty, driver);
    run("SELECT a from " + dbName + ".ptned", driver);
    verifyResults(ptn_data_1, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String postDropReplDumpLocn = getResult(0,0,driver);
    String postDropReplDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);

    // Drop table after dump
    run("DROP TABLE " + dbName + ".unptned_copy", driver);
    // Drop partition after dump
    run("ALTER TABLE " + dbName + ".ptned_copy DROP PARTITION(b='1')", driver);

    run("REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'", driverMirror);

    Exception e = null;
    try {
      Table tbl = metaStoreClientMirror.getTable(dbName + "_dupe", "unptned");
      assertNull(tbl);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());

    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", driverMirror);
    verifyResults(empty, driverMirror);
    run("SELECT a from " + dbName + "_dupe.ptned", driverMirror);
    verifyResults(ptn_data_1, driverMirror);

    verifyIfTableNotExist(dbName +"_dupe", "ptned2", metaStoreClientMirror);

    run("SELECT a from " + dbName + "_dupe.unptned_copy", driverMirror);
    verifyResults(unptn_data, driverMirror);
    run("SELECT a from " + dbName + "_dupe.ptned_copy", driverMirror);
    verifyResults(ptn_data_1, driverMirror);
  }

  @Test
  public void testTableAlters() throws IOException {

    String testName = "TableAlters";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned2(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b string) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned2", driver);
    verifySetup("SELECT * from " + dbName + ".unptned2", unptn_data, driver);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='1'", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='2'", ptn_data_2, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='1'",ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='2'", ptn_data_2, driver);

    // base tables set up, let's replicate them over

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0,driver);
    String replDumpId = getResult(0,1,true,driver);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    run("REPL STATUS " + dbName + "_dupe", driverMirror);
    verifyResults(new String[] {replDumpId}, driverMirror);

    verifySetup("SELECT * from " + dbName + "_dupe.unptned", unptn_data, driverMirror);
    verifySetup("SELECT * from " + dbName + "_dupe.unptned2", unptn_data, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned WHERE b='1'", ptn_data_1, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'", ptn_data_2, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='1'", ptn_data_1, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='2'", ptn_data_2, driverMirror);

    // tables have been replicated over, and verified to be identical. Now, we do a couple of
    // alters on the source

    // Rename unpartitioned table
    run("ALTER TABLE " + dbName + ".unptned RENAME TO " + dbName + ".unptned_rn", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_rn", unptn_data, driver);

    // Alter unpartitioned table set table property
    String testKey = "blah";
    String testVal = "foo";
    run("ALTER TABLE " + dbName + ".unptned2 SET TBLPROPERTIES ('" + testKey + "' = '" + testVal + "')", driver);
    if (VERIFY_SETUP_STEPS){
      try {
        Table unptn2 = metaStoreClient.getTable(dbName,"unptned2");
        assertTrue(unptn2.getParameters().containsKey(testKey));
        assertEquals(testVal,unptn2.getParameters().get(testKey));
      } catch (TException e) {
        assertNull(e);
      }
    }

    // alter partitioned table, rename partition
    run("ALTER TABLE " + dbName + ".ptned PARTITION (b='2') RENAME TO PARTITION (b='22')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=22", ptn_data_2, driver);

    // alter partitioned table set table property
    run("ALTER TABLE " + dbName + ".ptned SET TBLPROPERTIES ('" + testKey + "' = '" + testVal + "')", driver);
    if (VERIFY_SETUP_STEPS){
      try {
        Table ptned = metaStoreClient.getTable(dbName,"ptned");
        assertTrue(ptned.getParameters().containsKey(testKey));
        assertEquals(testVal,ptned.getParameters().get(testKey));
      } catch (TException e) {
        assertNull(e);
      }
    }

    // alter partitioned table's partition set partition property
    // Note : No DDL way to alter a partition, so we use the MSC api directly.
    try {
      List<String> ptnVals1 = new ArrayList<String>();
      ptnVals1.add("1");
      Partition ptn1 = metaStoreClient.getPartition(dbName, "ptned", ptnVals1);
      ptn1.getParameters().put(testKey,testVal);
      metaStoreClient.alter_partition(dbName,"ptned",ptn1,null);
    } catch (TException e) {
      assertNull(e);
    }

    // rename partitioned table
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b=2", ptn_data_2, driver);
    run("ALTER TABLE " + dbName + ".ptned2 RENAME TO " + dbName + ".ptned2_rn", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2_rn WHERE b=2", ptn_data_2, driver);

    // All alters done, now we replicate them over.

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String postAlterReplDumpLocn = getResult(0,0,driver);
    String postAlterReplDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}->{}", postAlterReplDumpLocn, replDumpId, postAlterReplDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + postAlterReplDumpLocn + "'", driverMirror);

    // Replication done, we now do the following verifications:

    // verify that unpartitioned table rename succeeded.
    verifyIfTableNotExist(dbName + "_dupe", "unptned", metaStoreClientMirror);
    verifyRun("SELECT * from " + dbName + "_dupe.unptned_rn", unptn_data, driverMirror);

    // verify that partition rename succeded.
    try {
      Table unptn2 = metaStoreClientMirror.getTable(dbName + "_dupe" , "unptned2");
      assertTrue(unptn2.getParameters().containsKey(testKey));
      assertEquals(testVal,unptn2.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=22", ptn_data_2, driverMirror);

    // verify that ptned table rename succeded.
    verifyIfTableNotExist(dbName + "_dupe", "ptned2", metaStoreClientMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned2_rn WHERE b=2", ptn_data_2, driverMirror);

    // verify that ptned table property set worked
    try {
      Table ptned = metaStoreClientMirror.getTable(dbName + "_dupe" , "ptned");
      assertTrue(ptned.getParameters().containsKey(testKey));
      assertEquals(testVal, ptned.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    // verify that partitioned table partition property set worked.
    try {
      List<String> ptnVals1 = new ArrayList<String>();
      ptnVals1.add("1");
      Partition ptn1 = metaStoreClientMirror.getPartition(dbName + "_dupe", "ptned", ptnVals1);
      assertTrue(ptn1.getParameters().containsKey(testKey));
      assertEquals(testVal,ptn1.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

  }

  @Test
  public void testDatabaseAlters() throws IOException {

    String testName = "DatabaseAlters";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    String ownerName = "test";

    run("ALTER DATABASE " + dbName + " SET OWNER USER " + ownerName, driver);

    // Trigger bootstrap replication
    Tuple bootstrap = bootstrapLoadAndVerify(dbName, replDbName);

    try {
      Database replDb = metaStoreClientMirror.getDatabase(replDbName);
      assertEquals(ownerName, replDb.getOwnerName());
      assertEquals("USER", replDb.getOwnerType().toString());
    } catch (TException e) {
      assertNull(e);
    }

    // Alter database set DB property
    String testKey = "blah";
    String testVal = "foo";
    run("ALTER DATABASE " + dbName + " SET DBPROPERTIES ('" + testKey + "' = '" + testVal + "')", driver);

    // All alters done, now we replicate them over.
    Tuple incremental = incrementalLoadAndVerify(dbName, bootstrap.lastReplId, replDbName);

    // Replication done, we need to check if the new property is added
    try {
      Database replDb = metaStoreClientMirror.getDatabase(replDbName);
      assertTrue(replDb.getParameters().containsKey(testKey));
      assertEquals(testVal, replDb.getParameters().get(testKey));
    } catch (TException e) {
      assertNull(e);
    }

    String newValue = "newFoo";
    String newOwnerName = "newTest";
    run("ALTER DATABASE " + dbName + " SET DBPROPERTIES ('" + testKey + "' = '" + newValue + "')", driver);
    run("ALTER DATABASE " + dbName + " SET OWNER ROLE " + newOwnerName, driver);

    incremental = incrementalLoadAndVerify(dbName, incremental.lastReplId, replDbName);

    // Replication done, we need to check if new value is set for existing property
    try {
      Database replDb = metaStoreClientMirror.getDatabase(replDbName);
      assertTrue(replDb.getParameters().containsKey(testKey));
      assertEquals(newValue, replDb.getParameters().get(testKey));
      assertEquals(newOwnerName, replDb.getOwnerName());
      assertEquals("ROLE", replDb.getOwnerType().toString());
    } catch (TException e) {
      assertNull(e);
    }
  }

  @Test
  public void testIncrementalLoad() throws IOException {
    String testName = "incrementalLoad";
    String dbName = createDB(testName, driver);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName
        + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0,driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] unptn_data = new String[] { "eleven", "twelve" };
    String[] ptn_data_1 = new String[] { "thirteen", "fourteen", "fifteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "sixteen", "seventeen" };
    String[] empty = new String[] {};

    String unptn_locn = new Path(TEST_PATH, testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty, driverMirror);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty, driverMirror);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptn_data, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT * from " + dbName + "_dupe.unptned_late", unptn_data, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName
        + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName
        + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);

    run("CREATE TABLE " + dbName
        + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
        + ".ptned WHERE b=1", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1", ptn_data_1, driver);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
        + ".ptned WHERE b=2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptn_data_2, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=2", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", ptn_data_2, driverMirror);
  }

  @Test
  public void testIncrementalInserts() throws IOException {
    String testName = "incrementalInserts";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] unptn_data = new String[] { "eleven", "twelve" };

    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late ORDER BY a", unptn_data, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);
    verifyRun("SELECT a from " + dbName + ".unptned_late ORDER BY a", unptn_data, driver);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned_late ORDER BY a", unptn_data, driverMirror);

    String[] unptn_data_after_ins = new String[] { "eleven", "thirteen", "twelve" };
    String[] data_after_ovwrite = new String[] { "hundred" };
    run("INSERT INTO TABLE " + dbName + ".unptned_late values('" + unptn_data_after_ins[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned_late ORDER BY a", unptn_data_after_ins, driver);
    run("INSERT OVERWRITE TABLE " + dbName + ".unptned values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned", data_after_ovwrite, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.unptned_late ORDER BY a", unptn_data_after_ins, driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.unptned", data_after_ovwrite, driverMirror);
  }

  @Test
  public void testEventTypesForDynamicAddPartitionByInsert() throws IOException {
    String name = testName.getMethodName();
    final String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    Tuple bootstrap = bootstrapLoadAndVerify(dbName, replDbName);

    String[] ptn_data = new String[]{ "ten"};
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data[0] + "')", driver);

    // Inject a behaviour where it throws exception if an INSERT event is found
    // As we dynamically add a partition through INSERT INTO cmd, it should just add ADD_PARTITION
    // event not an INSERT event
    BehaviourInjection<NotificationEventResponse,NotificationEventResponse> eventTypeValidator
            = new BehaviourInjection<NotificationEventResponse,NotificationEventResponse>(){

      @Nullable
      @Override
      public NotificationEventResponse apply(@Nullable NotificationEventResponse eventsList) {
        if (null != eventsList) {
          List<NotificationEvent> events = eventsList.getEvents();
          for (int i = 0; i < events.size(); i++) {
            NotificationEvent event = events.get(i);

            // Skip all the events belong to other DBs/tables.
            if (event.getDbName().equalsIgnoreCase(dbName)) {
              if (event.getEventType().equalsIgnoreCase("INSERT")) {
                // If an insert event is found, then return null hence no event is dumped.
                LOG.error("Encountered INSERT event when it was not expected to");
                return null;
              }
            }
          }
          injectionPathCalled = true;
        }
        return eventsList;
      }
    };
    InjectableBehaviourObjectStore.setGetNextNotificationBehaviour(eventTypeValidator);

    incrementalLoadAndVerify(dbName, bootstrap.lastReplId, replDbName);

    eventTypeValidator.assertInjectionsPerformed(true,false);
    InjectableBehaviourObjectStore.resetGetNextNotificationBehaviour(); // reset the behaviour

    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1)", ptn_data, driverMirror);
  }

  @Test
  public void testIdempotentMoveTaskForInsertFiles() throws IOException {
    String name = testName.getMethodName();
    final String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    Tuple bootstrap = bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptn_data = new String[]{ "ten"};
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);

    // Inject a behaviour where it repeats the INSERT event twice with different event IDs
    BehaviourInjection<NotificationEventResponse,NotificationEventResponse> insertEventRepeater
            = new BehaviourInjection<NotificationEventResponse,NotificationEventResponse>(){

      @Nullable
      @Override
      public NotificationEventResponse apply(@Nullable NotificationEventResponse eventsList) {
        if (null != eventsList) {
          List<NotificationEvent> events = eventsList.getEvents();
          List<NotificationEvent> outEvents = new ArrayList<>();
          long insertEventId = -1;

          for (int i = 0; i < events.size(); i++) {
            NotificationEvent event = events.get(i);

            // Skip all the events belong to other DBs/tables.
            if (event.getDbName().equalsIgnoreCase(dbName)) {
              if (event.getEventType().equalsIgnoreCase("INSERT")) {
                // Add insert event twice with different event ID to allow apply of both events.
                NotificationEvent newEvent = new NotificationEvent(event);
                outEvents.add(newEvent);
                insertEventId = newEvent.getEventId();
              }
            }

            NotificationEvent newEvent = new NotificationEvent(event);
            if (insertEventId != -1) {
              insertEventId++;
              newEvent.setEventId(insertEventId);
            }
            outEvents.add(newEvent);
          }
          eventsList.setEvents(outEvents);
          injectionPathCalled = true;
        }
        return eventsList;
      }
    };
    InjectableBehaviourObjectStore.setGetNextNotificationBehaviour(insertEventRepeater);

    incrementalLoadAndVerify(dbName, bootstrap.lastReplId, replDbName);

    insertEventRepeater.assertInjectionsPerformed(true,false);
    InjectableBehaviourObjectStore.resetGetNextNotificationBehaviour(); // reset the behaviour

    verifyRun("SELECT a from " + replDbName + ".unptned", unptn_data, driverMirror);
  }

  @Test
  public void testIncrementalInsertToPartition() throws IOException {
    String testName = "incrementalInsertToPartition";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] ptn_data_1 = new String[] { "fifteen", "fourteen", "thirteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "seventeen", "sixteen" };

    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[2] + "')", driver);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=2)", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[2] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driver);
    verifyRun("SELECT a from " + dbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driver);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    String[] data_after_ovwrite = new String[] { "hundred" };
    // Insert overwrite on existing partition
    run("INSERT OVERWRITE TABLE " + dbName + ".ptned partition(b=2) values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=2)", data_after_ovwrite, driver);
    // Insert overwrite on dynamic partition
    run("INSERT OVERWRITE TABLE " + dbName + ".ptned partition(b=3) values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=3)", data_after_ovwrite, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=2)", data_after_ovwrite, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=3)", data_after_ovwrite, driverMirror);
  }

  @Test
  public void testInsertToMultiKeyPartition() throws IOException {
    String testName = "insertToMultiKeyPartition";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".namelist(name string) partitioned by (year int, month int, day int) STORED AS TEXTFILE", driver);
    run("USE " + dbName, driver);

    String[] ptn_data_1 = new String[] { "abraham", "bob", "carter" };
    String[] ptn_year_1980 = new String[] { "abraham", "bob" };
    String[] ptn_day_1 = new String[] { "abraham", "carter" };
    String[] ptn_year_1984_month_4_day_1_1 = new String[] { "carter" };
    String[] ptn_list_1 = new String[] { "year=1980/month=4/day=1", "year=1980/month=5/day=5", "year=1984/month=4/day=1" };

    run("INSERT INTO TABLE " + dbName + ".namelist partition(year=1980,month=4,day=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".namelist partition(year=1980,month=5,day=5) values('" + ptn_data_1[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".namelist partition(year=1984,month=4,day=1) values('" + ptn_data_1[2] + "')", driver);

    verifySetup("SELECT name from " + dbName + ".namelist where (year=1980) ORDER BY name", ptn_year_1980, driver);
    verifySetup("SELECT name from " + dbName + ".namelist where (day=1) ORDER BY name", ptn_day_1, driver);
    verifySetup("SELECT name from " + dbName + ".namelist where (year=1984 and month=4 and day=1) ORDER BY name",
                                                                                ptn_year_1984_month_4_day_1_1, driver);
    verifySetup("SELECT name from " + dbName + ".namelist ORDER BY name", ptn_data_1, driver);
    verifySetup("SHOW PARTITIONS " + dbName + ".namelist", ptn_list_1, driver);
    verifyRunWithPatternMatch("SHOW TABLE EXTENDED LIKE namelist PARTITION (year=1980,month=4,day=1)",
                              "location", "namelist/year=1980/month=4/day=1", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    verifyRun("SELECT name from " + dbName + "_dupe.namelist where (year=1980) ORDER BY name", ptn_year_1980, driverMirror);
    verifyRun("SELECT name from " + dbName + "_dupe.namelist where (day=1) ORDER BY name", ptn_day_1, driverMirror);
    verifyRun("SELECT name from " + dbName + "_dupe.namelist where (year=1984 and month=4 and day=1) ORDER BY name",
                                                                                   ptn_year_1984_month_4_day_1_1, driverMirror);
    verifyRun("SELECT name from " + dbName + "_dupe.namelist ORDER BY name", ptn_data_1, driverMirror);
    verifyRun("SHOW PARTITIONS " + dbName + "_dupe.namelist", ptn_list_1, driverMirror);

    run("USE " + dbName + "_dupe", driverMirror);
    verifyRunWithPatternMatch("SHOW TABLE EXTENDED LIKE namelist PARTITION (year=1980,month=4,day=1)",
            "location", "namelist/year=1980/month=4/day=1", driverMirror);
    run("USE " + dbName, driver);

    String[] ptn_data_2 = new String[] { "abraham", "bob", "carter", "david", "eugene" };
    String[] ptn_year_1984_month_4_day_1_2 = new String[] { "carter", "david" };
    String[] ptn_day_1_2 = new String[] { "abraham", "carter", "david" };
    String[] ptn_list_2 = new String[] { "year=1980/month=4/day=1", "year=1980/month=5/day=5",
                                         "year=1984/month=4/day=1", "year=1990/month=5/day=25" };

    run("INSERT INTO TABLE " + dbName + ".namelist partition(year=1984,month=4,day=1) values('" + ptn_data_2[3] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".namelist partition(year=1990,month=5,day=25) values('" + ptn_data_2[4] + "')", driver);

    verifySetup("SELECT name from " + dbName + ".namelist where (year=1980) ORDER BY name", ptn_year_1980, driver);
    verifySetup("SELECT name from " + dbName + ".namelist where (day=1) ORDER BY name", ptn_day_1_2, driver);
    verifySetup("SELECT name from " + dbName + ".namelist where (year=1984 and month=4 and day=1) ORDER BY name",
                                                                                ptn_year_1984_month_4_day_1_2, driver);
    verifySetup("SELECT name from " + dbName + ".namelist ORDER BY name", ptn_data_2, driver);
    verifyRun("SHOW PARTITIONS " + dbName + ".namelist", ptn_list_2, driver);
    verifyRunWithPatternMatch("SHOW TABLE EXTENDED LIKE namelist PARTITION (year=1990,month=5,day=25)",
            "location", "namelist/year=1990/month=5/day=25", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT name from " + dbName + "_dupe.namelist where (year=1980) ORDER BY name", ptn_year_1980, driverMirror);
    verifyRun("SELECT name from " + dbName + "_dupe.namelist where (day=1) ORDER BY name", ptn_day_1_2, driverMirror);
    verifyRun("SELECT name from " + dbName + "_dupe.namelist where (year=1984 and month=4 and day=1) ORDER BY name",
                                                                                   ptn_year_1984_month_4_day_1_2, driverMirror);
    verifyRun("SELECT name from " + dbName + "_dupe.namelist ORDER BY name", ptn_data_2, driverMirror);
    verifyRun("SHOW PARTITIONS " + dbName + "_dupe.namelist", ptn_list_2, driverMirror);
    run("USE " + dbName + "_dupe", driverMirror);
    verifyRunWithPatternMatch("SHOW TABLE EXTENDED LIKE namelist PARTITION (year=1990,month=5,day=25)",
            "location", "namelist/year=1990/month=5/day=25", driverMirror);
    run("USE " + dbName, driverMirror);

    String[] ptn_data_3 = new String[] { "abraham", "bob", "carter", "david", "fisher" };
    String[] data_after_ovwrite = new String[] { "fisher" };
    // Insert overwrite on existing partition
    run("INSERT OVERWRITE TABLE " + dbName + ".namelist partition(year=1990,month=5,day=25) values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT name from " + dbName + ".namelist where (year=1990 and month=5 and day=25)", data_after_ovwrite, driver);
    verifySetup("SELECT name from " + dbName + ".namelist ORDER BY name", ptn_data_3, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifySetup("SELECT name from " + dbName + "_dupe.namelist where (year=1990 and month=5 and day=25)", data_after_ovwrite, driverMirror);
    verifySetup("SELECT name from " + dbName + "_dupe.namelist ORDER BY name", ptn_data_3, driverMirror);
  }

  @Test
  public void testIncrementalInsertDropUnpartitionedTable() throws IOException {
    String testName = "incrementalInsertDropUnpartitionedTable";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] unptn_data = new String[] { "eleven", "twelve" };

    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    run("CREATE TABLE " + dbName + ".unptned_tmp AS SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT a from " + dbName + ".unptned_tmp ORDER BY a", unptn_data, driver);

    // Get the last repl ID corresponding to all insert/alter/create events except DROP.
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String lastDumpIdWithoutDrop = getResult(0, 1, driver);

    // Drop all the tables
    run("DROP TABLE " + dbName + ".unptned", driver);
    run("DROP TABLE " + dbName + ".unptned_tmp", driver);
    verifyFail("SELECT * FROM " + dbName + ".unptned", driver);
    verifyFail("SELECT * FROM " + dbName + ".unptned_tmp", driver);

    // Dump all the events except DROP
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + lastDumpIdWithoutDrop, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    // Need to find the tables and data as drop is not part of this dump
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned_tmp ORDER BY a", unptn_data, driverMirror);

    // Dump the drop events and check if tables are getting dropped in target as well
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyFail("SELECT * FROM " + dbName + ".unptned", driverMirror);
    verifyFail("SELECT * FROM " + dbName + ".unptned_tmp", driverMirror);
  }

  @Test
  public void testIncrementalInsertDropPartitionedTable() throws IOException {
    String testName = "incrementalInsertDropPartitionedTable";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] ptn_data_1 = new String[] { "fifteen", "fourteen", "thirteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "seventeen", "sixteen" };

    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[2] + "')", driver);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=20)", driver);
    run("ALTER TABLE " + dbName + ".ptned RENAME PARTITION (b=20) TO PARTITION (b=2", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[2] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driver);

    run("CREATE TABLE " + dbName + ".ptned_tmp AS SELECT * FROM " + dbName + ".ptned", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_tmp where (b=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_tmp where (b=2) ORDER BY a", ptn_data_2, driver);

    // Get the last repl ID corresponding to all insert/alter/create events except DROP.
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String lastDumpIdWithoutDrop = getResult(0, 1, driver);

    // Drop all the tables
    run("DROP TABLE " + dbName + ".ptned_tmp", driver);
    run("DROP TABLE " + dbName + ".ptned", driver);
    verifyFail("SELECT * FROM " + dbName + ".ptned_tmp", driver);
    verifyFail("SELECT * FROM " + dbName + ".ptned", driver);

    // Dump all the events except DROP
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + lastDumpIdWithoutDrop, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    // Need to find the tables and data as drop is not part of this dump
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_tmp where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_tmp where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    // Dump the drop events and check if tables are getting dropped in target as well
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyFail("SELECT * FROM " + dbName + ".ptned_tmp", driverMirror);
    verifyFail("SELECT * FROM " + dbName + ".ptned", driverMirror);
  }

  @Test
  public void testInsertOverwriteOnUnpartitionedTableWithCM() throws IOException {
    String testName = "insertOverwriteOnUnpartitionedTableWithCM";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    // After INSERT INTO operation, get the last Repl ID
    String[] unptn_data = new String[] { "thirteen" };
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String insertDumpId = getResult(0, 1, false, driver);

    // Insert overwrite on unpartitioned table
    String[] data_after_ovwrite = new String[] { "hundred" };
    run("INSERT OVERWRITE TABLE " + dbName + ".unptned values('" + data_after_ovwrite[0] + "')", driver);

    // Dump only one INSERT INTO operation on the table.
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + insertDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    // After Load from this dump, all target tables/partitions will have initial set of data but source will have latest data.
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data, driverMirror);

    // Dump the remaining INSERT OVERWRITE operations on the table.
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);

    // After load, shall see the overwritten data.
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", data_after_ovwrite, driverMirror);
  }

  @Test
  public void testInsertOverwriteOnPartitionedTableWithCM() throws IOException {
    String testName = "insertOverwriteOnPartitionedTableWithCM";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    // INSERT INTO 2 partitions and get the last repl ID
    String[] ptn_data_1 = new String[] { "fourteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "sixteen" };
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[1] + "')", driver);
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String insertDumpId = getResult(0, 1, false, driver);

    // Insert overwrite on one partition with multiple files
    String[] data_after_ovwrite = new String[] { "hundred" };
    run("INSERT OVERWRITE TABLE " + dbName + ".ptned partition(b=2) values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=2)", data_after_ovwrite, driver);

    // Dump only 2 INSERT INTO operations.
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + insertDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    // After Load from this dump, all target tables/partitions will have initial set of data.
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    // Dump the remaining INSERT OVERWRITE operation on the table.
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);

    // After load, shall see the overwritten data.
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=2) ORDER BY a", data_after_ovwrite, driverMirror);
  }

  @Test
  public void testDropPartitionEventWithPartitionOnTimestampColumn() throws IOException {
    String testName = "dropPartitionEventWithPartitionOnTimestampColumn";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b timestamp)", driver);
    String[] ptn_data = new String[] { "fourteen" };
    String ptnVal = "2017-10-01 01:00:10.1";
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=\"" + ptnVal +"\") values('" + ptn_data[0] + "')", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    ptn_data = new String[] { "fifteen" };
    ptnVal = "2017-10-24 00:00:00.0";
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=\"" + ptnVal +"\") values('" + ptn_data[0] + "')", driver);

    // Replicate insert event and verify
    Tuple incrDump = incrementalLoadAndVerify(dbName, bootstrapDump.lastReplId, replDbName);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=\"" + ptnVal + "\") ORDER BY a", ptn_data, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION(b=\"" + ptnVal + "\")", driver);

    // Replicate drop partition event and verify
    incrementalLoadAndVerify(dbName, incrDump.lastReplId, replDbName);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList(ptnVal)), metaStoreClientMirror);
  }

  /**
   * Verify replication when string partition column value has special chars
   * @throws IOException
   */
  @Test
  public void testWithStringPartitionSpecialChars() throws IOException {
    String testName = "testWithStringPartitionSpecialChars";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".ptned(v string) PARTITIONED BY (p string)", driver);
    String[] ptn_data = new String[] { "fourteen", "fifteen" };
    String[] ptnVal = new String [] {"has a space, /, and \t tab", "another set of '#@ chars" };
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(p=\"" + ptnVal[0] +"\") values('" + ptn_data[0] + "')", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(p=\"" + ptnVal[1] +"\") values('" + ptn_data[1] + "')", driver);
    // Replicate insert event and verify
    Tuple incrDump = incrementalLoadAndVerify(dbName, bootstrapDump.lastReplId, replDbName);
    verifyRun("SELECT p from " + replDbName + ".ptned ORDER BY p desc", ptnVal, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION(p=\"" + ptnVal[0] + "\")", driver);

    // Replicate drop partition event and verify
    incrementalLoadAndVerify(dbName, incrDump.lastReplId, replDbName);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList(ptnVal[0])), metaStoreClientMirror);
  }

  @Test
  public void testRenameTableWithCM() throws IOException {
    String testName = "renameTableWithCM";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] unptn_data = new String[] { "ten", "twenty" };
    String[] ptn_data_1 = new String[] { "fifteen", "fourteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "seventeen" };

    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);

    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[1] + "')", driver);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=2)", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[1] + "')", driver);

    // Get the last repl ID corresponding to all insert events except RENAME.
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String lastDumpIdWithoutRename = getResult(0, 1, driver);

    run("ALTER TABLE " + dbName + ".unptned RENAME TO " + dbName + ".unptned_renamed", driver);
    run("ALTER TABLE " + dbName + ".ptned RENAME TO " + dbName + ".ptned_renamed", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + lastDumpIdWithoutRename, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyFail("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", driverMirror);
    verifyFail("SELECT a from " + dbName + "_dupe.ptned where (b=1) ORDER BY a", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned_renamed ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_renamed where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_renamed where (b=2) ORDER BY a", ptn_data_2, driverMirror);
  }

  @Test
  public void testRenamePartitionWithCM() throws IOException {
    String testName = "renamePartitionWithCM";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] empty = new String[] {};
    String[] ptn_data_1 = new String[] { "fifteen", "fourteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "seventeen" };

    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[1] + "')", driver);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=2)", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[1] + "')", driver);

    // Get the last repl ID corresponding to all insert events except RENAME.
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String lastDumpIdWithoutRename = getResult(0, 1, driver);

    run("ALTER TABLE " + dbName + ".ptned PARTITION (b=2) RENAME TO PARTITION (b=10)", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + lastDumpIdWithoutRename, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=10) ORDER BY a", empty, driverMirror);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=10) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where (b=2) ORDER BY a", empty, driverMirror);
  }

  @Test
  public void testRenameTableAcrossDatabases() throws IOException {
    String testName = "renameTableAcrossDatabases";
    LOG.info("Testing " + testName);
    String dbName1 = testName + "_" + tid + "_1";
    String dbName2 = testName + "_" + tid + "_2";
    String replDbName1 = dbName1 + "_dupe";
    String replDbName2 = dbName2 + "_dupe";

    run("CREATE DATABASE " + dbName1, driver);
    run("CREATE DATABASE " + dbName2, driver);
    run("CREATE TABLE " + dbName1 + ".unptned(a string) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[] { "ten", "twenty" };
    String unptn_locn = new Path(TEST_PATH, testName + "_unptn").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName1 + ".unptned", driver);

    Tuple bootstrap1 = bootstrapLoadAndVerify(dbName1, replDbName1);
    Tuple bootstrap2 = bootstrapLoadAndVerify(dbName2, replDbName2);

    verifyRun("SELECT a from " + replDbName1 + ".unptned ORDER BY a", unptn_data, driverMirror);
    verifyIfTableNotExist(replDbName2, "unptned", metaStoreClientMirror);

    run("ALTER TABLE " + dbName1 + ".unptned RENAME TO " + dbName2 + ".unptned_renamed", driver);

    incrementalLoadAndVerify(dbName1, bootstrap1.lastReplId, replDbName1);
    incrementalLoadAndVerify(dbName2, bootstrap2.lastReplId, replDbName2);

    verifyIfTableNotExist(replDbName1, "unptned", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName1, "unptned_renamed", metaStoreClientMirror);
    verifyRun("SELECT a from " + replDbName2 + ".unptned_renamed ORDER BY a", unptn_data, driverMirror);
  }

  @Test
  public void testRenamePartitionedTableAcrossDatabases() throws IOException {
    String testName = "renamePartitionedTableAcrossDatabases";
    LOG.info("Testing " + testName);
    String dbName1 = testName + "_" + tid + "_1";
    String dbName2 = testName + "_" + tid + "_2";
    String replDbName1 = dbName1 + "_dupe";
    String replDbName2 = dbName2 + "_dupe";

    run("CREATE DATABASE " + dbName1, driver);
    run("CREATE DATABASE " + dbName2, driver);
    run("CREATE TABLE " + dbName1 + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] ptn_data = new String[] { "fifteen", "fourteen" };
    String ptn_locn = new Path(TEST_PATH, testName + "_ptn").toUri().getPath();

    createTestDataFile(ptn_locn, ptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn + "' OVERWRITE INTO TABLE " + dbName1 + ".ptned PARTITION(b=1)", driver);

    Tuple bootstrap1 = bootstrapLoadAndVerify(dbName1, replDbName1);
    Tuple bootstrap2 = bootstrapLoadAndVerify(dbName2, replDbName2);

    verifyRun("SELECT a from " + replDbName1 + ".ptned where (b=1) ORDER BY a", ptn_data, driverMirror);
    verifyIfTableNotExist(replDbName2, "ptned", metaStoreClientMirror);

    run("ALTER TABLE " + dbName1 + ".ptned RENAME TO " + dbName2 + ".ptned_renamed", driver);

    incrementalLoadAndVerify(dbName1, bootstrap1.lastReplId, replDbName1);
    incrementalLoadAndVerify(dbName2, bootstrap2.lastReplId, replDbName2);

    verifyIfTableNotExist(replDbName1, "ptned", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName1, "ptned_renamed", metaStoreClientMirror);
    verifyRun("SELECT a from " + replDbName2 + ".ptned_renamed where (b=1) ORDER BY a", ptn_data, driverMirror);
  }

  @Test
  public void testViewsReplication() throws IOException {
    String testName = "viewsReplication";
    String dbName = createDB(testName, driver);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE VIEW " + dbName + ".virtual_view AS SELECT * FROM " + dbName + ".unptned", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    verifySetup("SELECT a from " + dbName + ".ptned", empty, driver);
    verifySetup("SELECT * from " + dbName + ".unptned", empty, driver);
    verifySetup("SELECT * from " + dbName + ".virtual_view", empty, driver);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    verifySetup("SELECT * from " + dbName + ".virtual_view", unptn_data, driver);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);

    // TODO: This does not work because materialized views need the creation metadata
    // to be updated in case tables used were replicated to a different database.
    //run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view AS SELECT a FROM " + dbName + ".ptned where b=1", driver);
    //verifySetup("SELECT a from " + dbName + ".mat_view", ptn_data_1, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0,driver);
    String replDumpId = getResult(0,1,true,driver);
    LOG.info("Bootstrap-dump: Dumped to {} with id {}",replDumpLocn,replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    // view is referring to old database, so no data
    verifyRun("SELECT * from " + dbName + "_dupe.virtual_view", empty, driverMirror);
    //verifyRun("SELECT a from " + dbName + "_dupe.mat_view", ptn_data_1, driverMirror);

    run("CREATE VIEW " + dbName + ".virtual_view2 AS SELECT a FROM " + dbName + ".ptned where b=2", driver);
    verifySetup("SELECT a from " + dbName + ".virtual_view2", ptn_data_2, driver);

    // Create a view with name already exist. Just to verify if failure flow clears the added create_table event.
    run("CREATE VIEW " + dbName + ".virtual_view2 AS SELECT a FROM " + dbName + ".ptned where b=2", driver);

    //run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view2 AS SELECT * FROM " + dbName + ".unptned", driver);
    //verifySetup("SELECT * from " + dbName + ".mat_view2", unptn_data, driver);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0,0,driver);
    String incrementalDumpId = getResult(0,1,true,driver);
    LOG.info("Incremental-dump: Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'", driverMirror);

    run("REPL STATUS " + dbName + "_dupe", driverMirror);
    verifyResults(new String[] {incrementalDumpId}, driverMirror);

    verifyRun("SELECT * from " + dbName + "_dupe.unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where b=1", ptn_data_1, driverMirror);
    // view is referring to old database, so no data
    verifyRun("SELECT * from " + dbName + "_dupe.virtual_view", empty, driverMirror);
    //verifyRun("SELECT a from " + dbName + "_dupe.mat_view", ptn_data_1, driverMirror);
    // view is referring to old database, so no data
    verifyRun("SELECT * from " + dbName + "_dupe.virtual_view2", empty, driverMirror);
    //verifyRun("SELECT * from " + dbName + "_dupe.mat_view2", unptn_data, driverMirror);

    // Test "alter table" with rename
    run("ALTER VIEW " + dbName + ".virtual_view RENAME TO " + dbName + ".virtual_view_rename", driver);
    verifySetup("SELECT * from " + dbName + ".virtual_view_rename", unptn_data, driver);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + incrementalDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-dump: Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT * from " + dbName + "_dupe.virtual_view_rename", empty, driverMirror);

    // Test "alter table" with schema change
    run("ALTER VIEW " + dbName + ".virtual_view_rename AS SELECT a, concat(a, '_') as a_ FROM " + dbName + ".unptned", driver);
    verifySetup("SHOW COLUMNS FROM " + dbName + ".virtual_view_rename", new String[] {"a", "a_"}, driver);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + incrementalDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-dump: Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SHOW COLUMNS FROM " + dbName + "_dupe.virtual_view_rename", new String[] {"a", "a_"}, driverMirror);

    // Test "DROP VIEW"
    run("DROP VIEW " + dbName + ".virtual_view", driver);
    verifyIfTableNotExist(dbName, "virtual_view", metaStoreClient);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + incrementalDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-dump: Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyIfTableNotExist(dbName + "_dupe", "virtual_view", metaStoreClientMirror);
  }

  @Test
  public void testDumpLimit() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);

    String[] unptn_data = new String[] { "eleven", "thirteen", "twelve" };
    String[] unptn_data_load1 = new String[] { "eleven" };
    String[] unptn_data_load2 = new String[] { "eleven", "thirteen" };

    // x events to insert, last repl ID: replDumpId+x
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    String firstInsertLastReplId = replDumpDb(dbName, replDumpId, null, null).lastReplId;
    Integer numOfEventsIns1 = Integer.valueOf(firstInsertLastReplId) - Integer.valueOf(replDumpId);

    // x events to insert, last repl ID: replDumpId+2x
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    String secondInsertLastReplId = replDumpDb(dbName, firstInsertLastReplId, null, null).lastReplId;
    Integer numOfEventsIns2 = Integer.valueOf(secondInsertLastReplId) - Integer.valueOf(firstInsertLastReplId);

    // x events to insert, last repl ID: replDumpId+3x
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[2] + "')", driver);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " LIMIT " + numOfEventsIns1, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_load1, driverMirror);

    advanceDumpDir();
    Integer lastReplID = Integer.valueOf(replDumpId);
    lastReplID += 1000;
    String toReplID = String.valueOf(lastReplID);

    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + toReplID + " LIMIT " + numOfEventsIns2, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_load2, driverMirror);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data, driverMirror);
  }

  @Test
  public void testExchangePartition() throws IOException {
    String testName = "exchangePartition";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".ptned_src(a string) partitioned by (b int, c int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned_dest(a string) partitioned by (b int, c int) STORED AS TEXTFILE", driver);

    String[] empty = new String[] {};
    String[] ptn_data_1 = new String[] { "fifteen", "fourteen", "thirteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "seventeen", "sixteen" };

    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=1, c=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=1, c=1) values('" + ptn_data_1[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=1, c=1) values('" + ptn_data_1[2] + "')", driver);

    run("ALTER TABLE " + dbName + ".ptned_src ADD PARTITION (b=2, c=2)", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=2, c=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=2, c=2) values('" + ptn_data_2[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=2, c=2) values('" + ptn_data_2[2] + "')", driver);

    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=2, c=3) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=2, c=3) values('" + ptn_data_2[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_src partition(b=2, c=3) values('" + ptn_data_2[2] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=1 and c=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=2) ORDER BY a", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=3) ORDER BY a", ptn_data_2, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + ".ptned_src where (b=1 and c=1) ORDER BY a", ptn_data_1, driver);
    verifyRun("SELECT a from " + dbName + ".ptned_src where (b=2 and c=2) ORDER BY a", ptn_data_2, driver);
    verifyRun("SELECT a from " + dbName + ".ptned_src where (b=2 and c=3) ORDER BY a", ptn_data_2, driver);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=1 and c=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=2 and c=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=2 and c=3) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=1 and c=1)", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=2 and c=2)", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=2 and c=3)", empty, driverMirror);

    // Exchange single partitions using complete partition-spec (all partition columns)
    run("ALTER TABLE " + dbName + ".ptned_dest EXCHANGE PARTITION (b=1, c=1) WITH TABLE " + dbName + ".ptned_src", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=1 and c=1)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=2) ORDER BY a", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=3) ORDER BY a", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=1 and c=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=2 and c=2)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=2 and c=3)", empty, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=1 and c=1)", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=2 and c=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=2 and c=3) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=1 and c=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=2 and c=2)", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=2 and c=3)", empty, driverMirror);

    // Exchange multiple partitions using partial partition-spec (only one partition column)
    run("ALTER TABLE " + dbName + ".ptned_dest EXCHANGE PARTITION (b=2) WITH TABLE " + dbName + ".ptned_src", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=1 and c=1)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=2)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=3)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=1 and c=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=2 and c=2) ORDER BY a", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=2 and c=3) ORDER BY a", ptn_data_2, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=1 and c=1)", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=2 and c=2)", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_src where (b=2 and c=3)", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=1 and c=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=2 and c=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_dest where (b=2 and c=3) ORDER BY a", ptn_data_2, driverMirror);
  }

  @Test
  public void testTruncateTable() throws IOException {
    String testName = "truncateTable";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    String[] unptn_data = new String[] { "eleven", "twelve" };
    String[] empty = new String[] {};
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data, driverMirror);

    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT a from " + dbName + ".unptned", empty, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + ".unptned", empty, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned", empty, driverMirror);

    String[] unptn_data_after_ins = new String[] { "thirteen" };
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data_after_ins[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data_after_ins, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data_after_ins, driver);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_after_ins, driverMirror);
  }

  @Test
  public void testTruncatePartitionedTable() throws IOException {
    String testName = "truncatePartitionedTable";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".ptned_1(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned_2(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);

    String[] ptn_data_1 = new String[] { "fifteen", "fourteen", "thirteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "seventeen", "sixteen" };
    String[] empty = new String[] {};
    run("INSERT INTO TABLE " + dbName + ".ptned_1 PARTITION(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_1 PARTITION(b=1) values('" + ptn_data_1[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_1 PARTITION(b=1) values('" + ptn_data_1[2] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_1 PARTITION(b=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_1 PARTITION(b=2) values('" + ptn_data_2[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_1 PARTITION(b=2) values('" + ptn_data_2[2] + "')", driver);

    run("INSERT INTO TABLE " + dbName + ".ptned_2 PARTITION(b=10) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_2 PARTITION(b=10) values('" + ptn_data_1[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_2 PARTITION(b=10) values('" + ptn_data_1[2] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_2 PARTITION(b=20) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_2 PARTITION(b=20) values('" + ptn_data_2[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_2 PARTITION(b=20) values('" + ptn_data_2[2] + "')", driver);

    verifyRun("SELECT a from " + dbName + ".ptned_1 where (b=1) ORDER BY a", ptn_data_1, driver);
    verifyRun("SELECT a from " + dbName + ".ptned_1 where (b=2) ORDER BY a", ptn_data_2, driver);
    verifyRun("SELECT a from " + dbName + ".ptned_2 where (b=10) ORDER BY a", ptn_data_1, driver);
    verifyRun("SELECT a from " + dbName + ".ptned_2 where (b=20) ORDER BY a", ptn_data_2, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_1 where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_1 where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_2 where (b=10) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_2 where (b=20) ORDER BY a", ptn_data_2, driverMirror);

    run("TRUNCATE TABLE " + dbName + ".ptned_1 PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_1 where (b=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_1 where (b=2)", empty, driver);

    run("TRUNCATE TABLE " + dbName + ".ptned_2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_2 where (b=10)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_2 where (b=20)", empty, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned_1 where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned_1 where (b=2)", empty, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned_2 where (b=10)", empty, driverMirror);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned_2 where (b=20)", empty, driverMirror);
  }

  @Test
  public void testTruncateWithCM() throws IOException {
    String testName = "truncateWithCM";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);

    String[] empty = new String[] {};
    String[] unptn_data = new String[] { "eleven", "thirteen" };
    String[] unptn_data_load1 = new String[] { "eleven" };
    String[] unptn_data_load2 = new String[] { "eleven", "thirteen" };

    // x events to insert, last repl ID: replDumpId+x
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    String firstInsertLastReplId = replDumpDb(dbName, replDumpId, null, null).lastReplId;
    Integer numOfEventsIns1 = Integer.valueOf(firstInsertLastReplId) - Integer.valueOf(replDumpId);

    // x events to insert, last repl ID: replDumpId+2x
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);
    String secondInsertLastReplId = replDumpDb(dbName, firstInsertLastReplId, null, null).lastReplId;
    Integer numOfEventsIns2 = Integer.valueOf(secondInsertLastReplId) - Integer.valueOf(firstInsertLastReplId);

    // y event to truncate, last repl ID: replDumpId+2x+y
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", empty, driver);
    String thirdTruncLastReplId = replDumpDb(dbName, secondInsertLastReplId, null, null).lastReplId;
    Integer numOfEventsTrunc3 = Integer.valueOf(thirdTruncLastReplId) - Integer.valueOf(secondInsertLastReplId);

    // x events to insert, last repl ID: replDumpId+3x+y
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data_load1[0] + "')", driver);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data_load1, driver);

    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    // Dump and load only first insert (1 record)
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " LIMIT " + numOfEventsIns1, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data_load1, driver);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_load1, driverMirror);

    // Dump and load only second insert (2 records)
    advanceDumpDir();
    Integer lastReplID = Integer.valueOf(replDumpId);
    lastReplID += 1000;
    String toReplID = String.valueOf(lastReplID);

    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + toReplID + " LIMIT " + numOfEventsIns2, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_load2, driverMirror);

    // Dump and load only truncate (0 records)
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " LIMIT " + numOfEventsTrunc3, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", empty, driverMirror);

    // Dump and load insert after truncate (1 record)
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_load1, driverMirror);
  }

  @Test
  public void testIncrementalRepeatEventOnExistingObject() throws IOException {
    String testName = "incrementalRepeatEventOnExistingObject";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    // List to maintain the incremental dumps for each operation
    List<Tuple> incrementalDumpList = new ArrayList<Tuple>();

    String[] empty = new String[] {};
    String[] unptn_data = new String[] { "ten" };
    String[] ptn_data_1 = new String[] { "fifteen" };
    String[] ptn_data_2 = new String[] { "seventeen" };

    // INSERT EVENT to unpartitioned table
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    Tuple replDump = dumpDbFromLastDump(dbName, bootstrapDump);
    incrementalDumpList.add(replDump);

    // INSERT EVENT to partitioned table with dynamic ADD_PARTITION
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=1) values('" + ptn_data_1[0] + "')", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // ADD_PARTITION EVENT to partitioned table
    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=2)", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // INSERT EVENT to partitioned table on existing partition
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=2) values('" + ptn_data_2[0] + "')", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // TRUNCATE_PARTITION EVENT on partitioned table
    run("TRUNCATE TABLE " + dbName + ".ptned PARTITION (b=1)", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // TRUNCATE_TABLE EVENT on unpartitioned table
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // CREATE_TABLE EVENT with multiple partitions
    run("CREATE TABLE " + dbName + ".unptned_tmp AS SELECT * FROM " + dbName + ".ptned", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // ADD_CONSTRAINT EVENT
    run("ALTER TABLE " + dbName + ".unptned_tmp ADD CONSTRAINT uk_unptned UNIQUE(a) disable", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // Replicate all the events happened so far
    Tuple incrDump = incrementalLoadAndVerify(dbName, bootstrapDump.lastReplId, replDbName);

    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_tmp where (b=1) ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_tmp where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    // Load each incremental dump from the list. Each dump have only one operation.
    for (Tuple currDump : incrementalDumpList) {
      // Load the incremental dump and ensure it does nothing and lastReplID remains same
      loadAndVerify(replDbName, currDump.dumpLocation, incrDump.lastReplId);

      // Verify if the data are intact even after applying an applied event once again on existing objects
      verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", empty, driverMirror);
      verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", empty, driverMirror);
      verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
      verifyRun("SELECT a from " + replDbName + ".unptned_tmp where (b=1) ORDER BY a", empty, driverMirror);
      verifyRun("SELECT a from " + replDbName + ".unptned_tmp where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    }
  }

  @Test
  public void testIncrementalRepeatEventOnMissingObject() throws IOException {
    String testName = "incrementalRepeatEventOnMissingObject";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    // List to maintain the incremental dumps for each operation
    List<Tuple> incrementalDumpList = new ArrayList<Tuple>();

    String[] empty = new String[] {};
    String[] unptn_data = new String[] { "ten" };
    String[] ptn_data_1 = new String[] { "fifteen" };
    String[] ptn_data_2 = new String[] { "seventeen" };

    // INSERT EVENT to unpartitioned table
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    Tuple replDump = dumpDbFromLastDump(dbName, bootstrapDump);
    incrementalDumpList.add(replDump);

    // INSERT EVENT to partitioned table with dynamic ADD_PARTITION
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // ADD_PARTITION EVENT to partitioned table
    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=2)", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // INSERT EVENT to partitioned table on existing partition
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // TRUNCATE_PARTITION EVENT on partitioned table
    run("TRUNCATE TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // TRUNCATE_TABLE EVENT on unpartitioned table
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // CREATE_TABLE EVENT on partitioned table
    run("CREATE TABLE " + dbName + ".ptned_tmp (a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // INSERT EVENT to partitioned table with dynamic ADD_PARTITION
    run("INSERT INTO TABLE " + dbName + ".ptned_tmp partition(b=10) values('" + ptn_data_1[0] + "')", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // INSERT EVENT to partitioned table with dynamic ADD_PARTITION
    run("INSERT INTO TABLE " + dbName + ".ptned_tmp partition(b=20) values('" + ptn_data_2[0] + "')", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // DROP_PARTITION EVENT to partitioned table
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b=1)", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // RENAME_PARTITION EVENT to partitioned table
    run("ALTER TABLE " + dbName + ".ptned PARTITION (b=2) RENAME TO PARTITION (b=20)", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // RENAME_TABLE EVENT to unpartitioned table
    run("ALTER TABLE " + dbName + ".unptned RENAME TO " + dbName + ".unptned_new", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // ADD_CONSTRAINT EVENT
    run("ALTER TABLE " + dbName + ".ptned_tmp ADD CONSTRAINT uk_unptned UNIQUE(a) disable", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // DROP_TABLE EVENT to partitioned table
    run("DROP TABLE " + dbName + ".ptned_tmp", driver);
    replDump = dumpDbFromLastDump(dbName, replDump);
    incrementalDumpList.add(replDump);

    // Replicate all the events happened so far
    Tuple incrDump = incrementalLoadAndVerify(dbName, bootstrapDump.lastReplId, replDbName);

    verifyIfTableNotExist(replDbName, "unptned", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "ptned_tmp", metaStoreClientMirror);
    verifyIfTableExist(replDbName, "unptned_new", metaStoreClientMirror);
    verifyIfTableExist(replDbName, "ptned", metaStoreClientMirror);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClientMirror);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClientMirror);
    verifyIfPartitionExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("20")), metaStoreClientMirror);

    // Load each incremental dump from the list. Each dump have only one operation.
    for (Tuple currDump : incrementalDumpList) {
      // Load the current incremental dump and ensure it does nothing and lastReplID remains same
      loadAndVerify(replDbName, currDump.dumpLocation, incrDump.lastReplId);

      // Verify if the data are intact even after applying an applied event once again on missing objects
      verifyIfTableNotExist(replDbName, "unptned", metaStoreClientMirror);
      verifyIfTableNotExist(replDbName, "ptned_tmp", metaStoreClientMirror);
      verifyIfTableExist(replDbName, "unptned_new", metaStoreClientMirror);
      verifyIfTableExist(replDbName, "ptned", metaStoreClientMirror);
      verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClientMirror);
      verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClientMirror);
      verifyIfPartitionExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("20")), metaStoreClientMirror);
    }
  }

  @Test
  public void testConcatenateTable() throws IOException {
    String testName = "concatenateTable";
    String dbName = createDB(testName, driver);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS ORC", driver);

    String[] unptn_data = new String[] { "eleven", "twelve" };
    String[] empty = new String[] {};
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    run("ALTER TABLE " + dbName + ".unptned CONCATENATE", driver);

    // Replicate all the events happened after bootstrap
    Tuple incrDump = incrementalLoadAndVerify(dbName, bootstrapDump.lastReplId, replDbName);
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data, driverMirror);
  }

  @Test
  public void testConcatenatePartitionedTable() throws IOException {
    String testName = "concatenatePartitionedTable";
    String dbName = createDB(testName, driver);

    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b int) STORED AS ORC", driver);

    String[] ptn_data_1 = new String[] { "fifteen", "fourteen", "thirteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "seventeen", "sixteen" };

    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=2) values('" + ptn_data_2[0] + "')", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=1) values('" + ptn_data_1[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=1) values('" + ptn_data_1[2] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=2) values('" + ptn_data_2[1] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=2) values('" + ptn_data_2[2] + "')", driver);

    run("ALTER TABLE " + dbName + ".ptned PARTITION(b=2) CONCATENATE", driver);

    // Replicate all the events happened so far
    Tuple incrDump = incrementalLoadAndVerify(dbName, bootstrapDump.lastReplId, replDbName);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
  }

  @Test
  public void testIncrementalLoadFailAndRetry() throws IOException {
    String testName = "incrementalLoadFailAndRetry";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    // Prefixed with incrementalLoadFailAndRetry to avoid finding entry in cmpath
    String[] ptn_data_1 = new String[] { "incrementalLoadFailAndRetry_fifteen" };
    String[] empty = new String[] {};

    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("CREATE TABLE " + dbName + ".ptned_tmp AS SELECT * FROM " + dbName + ".ptned", driver);

    // Move the data files of this newly created partition to a temp location
    Partition ptn = null;
    try {
      ptn = metaStoreClient.getPartition(dbName, "ptned", new ArrayList<>(Arrays.asList("1")));
    } catch (Exception e) {
      assert(false);
    }

    Path ptnLoc = new Path(ptn.getSd().getLocation());
    Path tmpLoc = new Path(TEST_PATH + "/incrementalLoadFailAndRetry");
    FileSystem dataFs = ptnLoc.getFileSystem(hconf);
    assert(dataFs.rename(ptnLoc, tmpLoc));

    // Replicate all the events happened so far. It should fail as the data files missing in
    // original path and not available in CM as well.
    Tuple incrDump = replDumpDb(dbName, bootstrapDump.lastReplId, null, null);
    verifyFail("REPL LOAD " + replDbName + " FROM '" + incrDump.dumpLocation + "'", driverMirror);

    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", empty, driverMirror);
    verifyFail("SELECT a from " + replDbName + ".ptned_tmp where (b=1) ORDER BY a", driverMirror);

    // Move the files back to original data location
    assert(dataFs.rename(tmpLoc, ptnLoc));
    loadAndVerify(replDbName, incrDump.dumpLocation, incrDump.lastReplId);

    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_tmp where (b=1) ORDER BY a", ptn_data_1, driverMirror);
  }

  @Test
  public void testStatus() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String lastReplDumpLocn = getResult(0, 0, driver);
    String lastReplDumpId = getResult(0, 1, true, driver);
    run("REPL LOAD " + dbName + "_dupe FROM '" + lastReplDumpLocn + "'", driverMirror);

    // Bootstrap done, now on to incremental. First, we test db-level REPL LOADs.
    // Both db-level and table-level repl.last.id must be updated.

    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, "ptned", lastReplDumpId,
        "CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, "ptned", lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)");
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, "ptned", lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned PARTITION (b=1) RENAME TO PARTITION (b=11)");
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, "ptned", lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned SET TBLPROPERTIES ('blah'='foo')");
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, "ptned_rn", lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned RENAME TO  " + dbName + ".ptned_rn");
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, "ptned_rn", lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned_rn DROP PARTITION (b=11)");
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, null, lastReplDumpId,
        "DROP TABLE " + dbName + ".ptned_rn");

    // DB-level REPL LOADs testing done, now moving on to table level repl loads.
    // In each of these cases, the table-level repl.last.id must move forward, but the
    // db-level last.repl.id must not.

    String lastTblReplDumpId = lastReplDumpId;
    lastTblReplDumpId = verifyAndReturnTblReplStatus(
        dbName, "ptned2", lastReplDumpId, lastTblReplDumpId,
        "CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b int) STORED AS TEXTFILE");
    lastTblReplDumpId = verifyAndReturnTblReplStatus(
        dbName, "ptned2", lastReplDumpId, lastTblReplDumpId,
        "ALTER TABLE " + dbName + ".ptned2 ADD PARTITION (b=1)");
    lastTblReplDumpId = verifyAndReturnTblReplStatus(
        dbName, "ptned2", lastReplDumpId, lastTblReplDumpId,
        "ALTER TABLE " + dbName + ".ptned2 PARTITION (b=1) RENAME TO PARTITION (b=11)");
    lastTblReplDumpId = verifyAndReturnTblReplStatus(
        dbName, "ptned2", lastReplDumpId, lastTblReplDumpId,
        "ALTER TABLE " + dbName + ".ptned2 SET TBLPROPERTIES ('blah'='foo')");
    // Note : Not testing table rename because table rename replication is not supported for table-level repl.
    String finalTblReplDumpId = verifyAndReturnTblReplStatus(
        dbName, "ptned2", lastReplDumpId, lastTblReplDumpId,
        "ALTER TABLE " + dbName + ".ptned2 DROP PARTITION (b=11)");

    /*
    Comparisons using Strings for event Ids is wrong. This should be numbers since lexical string comparison
    and numeric comparision differ. This requires a broader change where we return the dump Id as long and not string
    fixing this here for now as it was observed in one of the builds where "1001".compareTo("998") results
    in failure of the assertion below.
     */
    assertTrue(new Long(Long.parseLong(finalTblReplDumpId)).compareTo(Long.parseLong(lastTblReplDumpId)) > 0);

    // TODO : currently not testing the following scenarios:
    //   a) Multi-db wh-level REPL LOAD - need to add that
    //   b) Insert into tables - quite a few cases need to be enumerated there, including dyn adds.

  }

  @Test
  public void testConstraints() throws IOException {
    String testName = "constraints";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName, driver);

    run("CREATE TABLE " + dbName + ".tbl1(a string, b string, primary key (a, b) disable novalidate rely)", driver);
    run("CREATE TABLE " + dbName + ".tbl2(a string, b string, foreign key (a, b) references " + dbName + ".tbl1(a, b) disable novalidate)", driver);
    run("CREATE TABLE " + dbName + ".tbl3(a string, b string not null disable, unique (a) disable)", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    try {
      List<SQLPrimaryKey> pks = metaStoreClientMirror.getPrimaryKeys(new PrimaryKeysRequest(dbName+ "_dupe" , "tbl1"));
      assertEquals(pks.size(), 2);
      List<SQLUniqueConstraint> uks = metaStoreClientMirror.getUniqueConstraints(new UniqueConstraintsRequest(dbName+ "_dupe" , "tbl3"));
      assertEquals(uks.size(), 1);
      List<SQLForeignKey> fks = metaStoreClientMirror.getForeignKeys(new ForeignKeysRequest(null, null, dbName+ "_dupe" , "tbl2"));
      assertEquals(fks.size(), 2);
      List<SQLNotNullConstraint> nns = metaStoreClientMirror.getNotNullConstraints(new NotNullConstraintsRequest(dbName+ "_dupe" , "tbl3"));
      assertEquals(nns.size(), 1);
    } catch (TException te) {
      assertNull(te);
    }

    run("CREATE TABLE " + dbName + ".tbl4(a string, b string, primary key (a, b) disable novalidate rely)", driver);
    run("CREATE TABLE " + dbName + ".tbl5(a string, b string, foreign key (a, b) references " + dbName + ".tbl4(a, b) disable novalidate)", driver);
    run("CREATE TABLE " + dbName + ".tbl6(a string, b string not null disable, unique (a) disable)", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    String pkName = null;
    String ukName = null;
    String fkName = null;
    String nnName = null;
    try {
      List<SQLPrimaryKey> pks = metaStoreClientMirror.getPrimaryKeys(new PrimaryKeysRequest(dbName+ "_dupe" , "tbl4"));
      assertEquals(pks.size(), 2);
      pkName = pks.get(0).getPk_name();
      List<SQLUniqueConstraint> uks = metaStoreClientMirror.getUniqueConstraints(new UniqueConstraintsRequest(dbName+ "_dupe" , "tbl6"));
      assertEquals(uks.size(), 1);
      ukName = uks.get(0).getUk_name();
      List<SQLForeignKey> fks = metaStoreClientMirror.getForeignKeys(new ForeignKeysRequest(null, null, dbName+ "_dupe" , "tbl5"));
      assertEquals(fks.size(), 2);
      fkName = fks.get(0).getFk_name();
      List<SQLNotNullConstraint> nns = metaStoreClientMirror.getNotNullConstraints(new NotNullConstraintsRequest(dbName+ "_dupe" , "tbl6"));
      assertEquals(nns.size(), 1);
      nnName = nns.get(0).getNn_name();

    } catch (TException te) {
      assertNull(te);
    }

    run("ALTER TABLE " + dbName + ".tbl4 DROP CONSTRAINT `" + pkName + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl4 DROP CONSTRAINT `" + ukName + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl5 DROP CONSTRAINT `" + fkName + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl6 DROP CONSTRAINT `" + nnName + "`", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + incrementalDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);

    try {
      List<SQLPrimaryKey> pks = metaStoreClientMirror.getPrimaryKeys(new PrimaryKeysRequest(dbName+ "_dupe" , "tbl4"));
      assertTrue(pks.isEmpty());
      List<SQLUniqueConstraint> uks = metaStoreClientMirror.getUniqueConstraints(new UniqueConstraintsRequest(dbName+ "_dupe" , "tbl4"));
      assertTrue(uks.isEmpty());
      List<SQLForeignKey> fks = metaStoreClientMirror.getForeignKeys(new ForeignKeysRequest(null, null, dbName+ "_dupe" , "tbl5"));
      assertTrue(fks.isEmpty());
      List<SQLNotNullConstraint> nns = metaStoreClientMirror.getNotNullConstraints(new NotNullConstraintsRequest(dbName+ "_dupe" , "tbl6"));
      assertTrue(nns.isEmpty());
    } catch (TException te) {
      assertNull(te);
    }
  }

  @Test
  public void testRemoveStats() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);

    String[] unptn_data = new String[]{ "1" , "2" };
    String[] ptn_data_1 = new String[]{ "5", "7", "8"};
    String[] ptn_data_2 = new String[]{ "3", "2", "9"};

    String unptn_locn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, name + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, name + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    run("CREATE TABLE " + dbName + ".unptned(a int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    run("CREATE TABLE " + dbName + ".ptned(a int) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    run("ANALYZE TABLE " + dbName + ".unptned COMPUTE STATISTICS FOR COLUMNS", driver);
    run("ANALYZE TABLE " + dbName + ".unptned COMPUTE STATISTICS", driver);
    run("ANALYZE TABLE " + dbName + ".ptned partition(b) COMPUTE STATISTICS FOR COLUMNS", driver);
    run("ANALYZE TABLE " + dbName + ".ptned partition(b) COMPUTE STATISTICS", driver);

    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    verifySetup("SELECT count(*) from " + dbName + ".unptned", new String[]{"2"}, driver);
    verifySetup("SELECT count(*) from " + dbName + ".ptned", new String[]{"3"}, driver);
    verifySetup("SELECT max(a) from " + dbName + ".unptned", new String[]{"2"}, driver);
    verifySetup("SELECT max(a) from " + dbName + ".ptned where b=1", new String[]{"8"}, driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0,driver);
    String replDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}",replDumpLocn,replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    verifyRun("SELECT count(*) from " + dbName + "_dupe.unptned", new String[]{"2"}, driverMirror);
    verifyRun("SELECT count(*) from " + dbName + "_dupe.ptned", new String[]{"3"}, driverMirror);
    verifyRun("SELECT max(a) from " + dbName + "_dupe.unptned", new String[]{"2"}, driverMirror);
    verifyRun("SELECT max(a) from " + dbName + "_dupe.ptned where b=1", new String[]{"8"}, driverMirror);

    run("CREATE TABLE " + dbName + ".unptned2(a int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned2", driver);
    run("CREATE TABLE " + dbName + ".ptned2(a int) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b=1)", driver);
    run("ANALYZE TABLE " + dbName + ".unptned2 COMPUTE STATISTICS FOR COLUMNS", driver);
    run("ANALYZE TABLE " + dbName + ".unptned2 COMPUTE STATISTICS", driver);
    run("ANALYZE TABLE " + dbName + ".ptned2 partition(b) COMPUTE STATISTICS FOR COLUMNS", driver);
    run("ANALYZE TABLE " + dbName + ".ptned2 partition(b) COMPUTE STATISTICS", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0,0,driver);
    String incrementalDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'", driverMirror);

    verifyRun("SELECT count(*) from " + dbName + "_dupe.unptned2", new String[]{"2"}, driverMirror);
    verifyRun("SELECT count(*) from " + dbName + "_dupe.ptned2", new String[]{"3"}, driverMirror);
    verifyRun("SELECT max(a) from " + dbName + "_dupe.unptned2", new String[]{"2"}, driverMirror);
    verifyRun("SELECT max(a) from " + dbName + "_dupe.ptned2 where b=1", new String[]{"8"}, driverMirror);
  }

  @Test
  public void testSkipTables() throws IOException {
    String testName = "skipTables";
    String dbName = createDB(testName, driver);

    // Create table
    run("CREATE TABLE " + dbName + ".acid_table (key int, value int) PARTITIONED BY (load_date date) " +
        "CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    run("CREATE TABLE " + dbName + ".mm_table (key int, value int) PARTITIONED BY (load_date date) " +
        "CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true'," +
        " 'transactional_properties'='insert_only')", driver);
    verifyIfTableExist(dbName, "acid_table", metaStoreClient);
    verifyIfTableExist(dbName, "mm_table", metaStoreClient);

    // Bootstrap test
    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0,driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);
    verifyIfTableNotExist(dbName + "_dupe", "acid_table", metaStoreClientMirror);
    verifyIfTableNotExist(dbName + "_dupe", "mm_table", metaStoreClientMirror);

    // Test alter table
    run("ALTER TABLE " + dbName + ".acid_table RENAME TO " + dbName + ".acid_table_rename", driver);
    verifyIfTableExist(dbName, "acid_table_rename", metaStoreClient);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId, driver);
    String incrementalDumpLocn = getResult(0, 0, driver);
    String incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-dump: Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'", driverMirror);
    verifyIfTableNotExist(dbName + "_dupe", "acid_table_rename", metaStoreClientMirror);

    // Create another table for incremental repl verification
    run("CREATE TABLE " + dbName + ".acid_table_incremental (key int, value int) PARTITIONED BY (load_date date) " +
        "CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    run("CREATE TABLE " + dbName + ".mm_table_incremental (key int, value int) PARTITIONED BY (load_date date) " +
        "CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true'," +
        " 'transactional_properties'='insert_only')", driver);
    verifyIfTableExist(dbName, "acid_table_incremental", metaStoreClient);
    verifyIfTableExist(dbName, "mm_table_incremental", metaStoreClient);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + incrementalDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-dump: Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    printOutput(driverMirror);
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'", driverMirror);
    verifyIfTableNotExist(dbName + "_dupe", "acid_table_incremental", metaStoreClientMirror);
    verifyIfTableNotExist(dbName + "_dupe", "mm_table_incremental", metaStoreClientMirror);

    // Test adding a constraint
    run("ALTER TABLE " + dbName + ".acid_table_incremental ADD CONSTRAINT key_pk PRIMARY KEY (key) DISABLE NOVALIDATE", driver);
    try {
      List<SQLPrimaryKey> pks = metaStoreClient.getPrimaryKeys(new PrimaryKeysRequest(dbName, "acid_table_incremental"));
      assertEquals(pks.size(), 1);
    } catch (TException te) {
      assertNull(te);
    }

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + incrementalDumpId, driver);
    incrementalDumpLocn = getResult(0, 0, driver);
    incrementalDumpId = getResult(0, 1, true, driver);
    LOG.info("Incremental-dump: Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'", driverMirror);
    printOutput(driverMirror);
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'", driverMirror);
    verifyIfTableNotExist(dbName + "_dupe", "acid_table_incremental", metaStoreClientMirror);
  }

  @Test
  public void testDeleteStagingDir() throws IOException {
	String testName = "deleteStagingDir";
	String dbName = createDB(testName, driver);
	String tableName = "unptned";
    run("CREATE TABLE " + StatsUtils.getFullyQualifiedTableName(dbName, tableName) + "(a string) STORED AS TEXTFILE",
        driver);

    String[] unptn_data = new String[] {"one", "two"};
    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    createTestDataFile(unptn_locn, unptn_data);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);

    // Perform repl
    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0,driver);
    // Reset the driver
    driverMirror.close();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);
    // Calling close() explicitly to clean up the staging dirs
    driverMirror.close();
    // Check result
    Path warehouse = new Path(System.getProperty("test.warehouse.dir", "/tmp"));
    FileSystem fs = FileSystem.get(warehouse.toUri(), hconf);
    try {
      Path path = new Path(warehouse, dbName + "_dupe.db" + Path.SEPARATOR + tableName);
      // First check if the table dir exists (could have been deleted for some reason in pre-commit tests)
      if (!fs.exists(path))
      {
        return;
      }
      PathFilter filter = new PathFilter()
      {
        @Override
        public boolean accept(Path path)
        {
          return path.getName().startsWith(HiveConf.getVar(hconf, HiveConf.ConfVars.STAGINGDIR));
        }
      };
      FileStatus[] statuses = fs.listStatus(path, filter);
      assertEquals(0, statuses.length);
    } catch (IOException e) {
      LOG.error("Failed to list files in: " + warehouse, e);
      assert(false);
    }
  }

  @Test
  public void testCMConflict() throws IOException {
    String testName = "cmConflict";
    String dbName = createDB(testName, driver);

    // Create table and insert two file of the same content
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('ten')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('ten')", driver);

    // Bootstrap test
    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0, 0,driver);
    String replDumpId = getResult(0, 1, true, driver);

    // Drop two files so they are moved to CM
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);

    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'", driverMirror);

    verifyRun("SELECT count(*) from " + dbName + "_dupe.unptned", new String[]{"2"}, driverMirror);
  }

  private static String createDB(String name, IDriver myDriver) {
    LOG.info("Testing " + name);
    String dbName = name + "_" + tid;
    run("CREATE DATABASE " + dbName, myDriver);
    return dbName;
  }

  @Test
  public void testEventFilters(){
    // Test testing that the filters introduced by EventUtils are working correctly.

    // The current filters we use in ReplicationSemanticAnalyzer is as follows:
    //    IMetaStoreClient.NotificationFilter evFilter = EventUtils.andFilter(
    //        EventUtils.getDbTblNotificationFilter(dbNameOrPattern, tblNameOrPattern),
    //        EventUtils.getEventBoundaryFilter(eventFrom, eventTo),
    //        EventUtils.restrictByMessageFormat(MessageFactory.getInstance().getMessageFormat()));
    // So, we test each of those three filters, and then test andFilter itself.


    String dbname = "testfilter_db";
    String tblname = "testfilter_tbl";

    // Test EventUtils.getDbTblNotificationFilter - this is supposed to restrict
    // events to those that match the dbname and tblname provided to the filter.
    // If the tblname passed in to the filter is null, then it restricts itself
    // to dbname-matching alone.
    IMetaStoreClient.NotificationFilter dbTblFilter = new DatabaseAndTableFilter(dbname,tblname);
    IMetaStoreClient.NotificationFilter dbFilter = new DatabaseAndTableFilter(dbname,null);

    assertFalse(dbTblFilter.accept(null));
    assertTrue(dbTblFilter.accept(createDummyEvent(dbname, tblname, 0)));
    assertFalse(dbTblFilter.accept(createDummyEvent(dbname, tblname + "extra",0)));
    assertFalse(dbTblFilter.accept(createDummyEvent(dbname + "extra", tblname,0)));

    assertFalse(dbFilter.accept(null));
    assertTrue(dbFilter.accept(createDummyEvent(dbname, tblname,0)));
    assertTrue(dbFilter.accept(createDummyEvent(dbname, tblname + "extra", 0)));
    assertFalse(dbFilter.accept(createDummyEvent(dbname + "extra", tblname,0)));


    // Test EventUtils.getEventBoundaryFilter - this is supposed to only allow events
    // within a range specified.
    long evBegin = 50;
    long evEnd = 75;
    IMetaStoreClient.NotificationFilter evRangeFilter = new EventBoundaryFilter(evBegin,evEnd);

    assertTrue(evBegin < evEnd);
    assertFalse(evRangeFilter.accept(null));
    assertFalse(evRangeFilter.accept(createDummyEvent(dbname, tblname, evBegin - 1)));
    assertTrue(evRangeFilter.accept(createDummyEvent(dbname, tblname, evBegin)));
    assertTrue(evRangeFilter.accept(createDummyEvent(dbname, tblname, evBegin + 1)));
    assertTrue(evRangeFilter.accept(createDummyEvent(dbname, tblname, evEnd - 1)));
    assertTrue(evRangeFilter.accept(createDummyEvent(dbname, tblname, evEnd)));
    assertFalse(evRangeFilter.accept(createDummyEvent(dbname, tblname, evEnd + 1)));


    // Test EventUtils.restrictByMessageFormat - this restricts events generated to those
    // that match a provided message format

    IMetaStoreClient.NotificationFilter restrictByDefaultMessageFormat =
        new MessageFormatFilter(MessageFactory.getInstance().getMessageFormat());
    IMetaStoreClient.NotificationFilter restrictByArbitraryMessageFormat =
        new MessageFormatFilter(MessageFactory.getInstance().getMessageFormat() + "_bogus");
    NotificationEvent dummyEvent = createDummyEvent(dbname,tblname,0);

    assertEquals(MessageFactory.getInstance().getMessageFormat(),dummyEvent.getMessageFormat());

    assertFalse(restrictByDefaultMessageFormat.accept(null));
    assertTrue(restrictByDefaultMessageFormat.accept(dummyEvent));
    assertFalse(restrictByArbitraryMessageFormat.accept(dummyEvent));

    // Test andFilter operation.

    IMetaStoreClient.NotificationFilter yes = new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent notificationEvent) {
        return true;
      }
    };

    IMetaStoreClient.NotificationFilter no = new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent notificationEvent) {
        return false;
      }
    };

    assertTrue(new AndFilter(yes, yes).accept(dummyEvent));
    assertFalse(new AndFilter(yes, no).accept(dummyEvent));
    assertFalse(new AndFilter(no, yes).accept(dummyEvent));
    assertFalse(new AndFilter(no, no).accept(dummyEvent));

    assertTrue(new AndFilter(yes, yes, yes).accept(dummyEvent));
    assertFalse(new AndFilter(yes, yes, no).accept(dummyEvent));
    assertFalse(new AndFilter(yes, no, yes).accept(dummyEvent));
    assertFalse(new AndFilter(yes, no, no).accept(dummyEvent));
    assertFalse(new AndFilter(no, yes, yes).accept(dummyEvent));
    assertFalse(new AndFilter(no, yes, no).accept(dummyEvent));
    assertFalse(new AndFilter(no, no, yes).accept(dummyEvent));
    assertFalse(new AndFilter(no, no, no).accept(dummyEvent));
  }

  @Test
  public void testAuthForNotificationAPIs() throws Exception {
    // Setup
    long firstEventId = metaStoreClient.getCurrentNotificationEventId().getEventId();
    String dbName = "testAuthForNotificationAPIs";
    driver.run("create database " + dbName);
    NotificationEventResponse rsp = metaStoreClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
    // Test various scenarios
    // Remove the proxy privilege and the auth should fail (in reality the proxy setting should not be changed on the fly)
    hconf.unset(proxySettingName);
    // Need to explicitly update ProxyUsers
    ProxyUsers.refreshSuperUserGroupsConfiguration(hconf);
    // Verify if the auth should fail
    Exception ex = null;
    try {
      rsp = metaStoreClient.getNextNotification(firstEventId, 0, null);
    } catch (TException e) {
      ex = e;
    }
    assertNotNull(ex);
    // Disable auth so the call should succeed
    hconf.setBoolVar(HiveConf.ConfVars.METASTORE_EVENT_DB_NOTIFICATION_API_AUTH, false);
    try {
      rsp = metaStoreClient.getNextNotification(firstEventId, 0, null);
      assertEquals(1, rsp.getEventsSize());
    } finally {
      // Restore the settings
      hconf.setBoolVar(HiveConf.ConfVars.METASTORE_EVENT_DB_NOTIFICATION_API_AUTH, true);
      hconf.set(proxySettingName, "*");
      ProxyUsers.refreshSuperUserGroupsConfiguration(hconf);
    }
  }

  @Test
  public void testRecycleFileDropTempTable() throws IOException {
    String dbName = createDB(testName.getMethodName(), driver);

    run("CREATE TABLE " + dbName + ".normal(a int)", driver);
    run("INSERT INTO " + dbName + ".normal values (1)", driver);
    run("DROP TABLE " + dbName + ".normal", driver);

    String cmDir = hconf.getVar(HiveConf.ConfVars.REPLCMDIR);
    Path path = new Path(cmDir);
    FileSystem fs = path.getFileSystem(hconf);
    ContentSummary cs = fs.getContentSummary(path);
    long fileCount = cs.getFileCount();

    assertTrue(fileCount != 0);

    run("CREATE TABLE " + dbName + ".normal(a int)", driver);
    run("INSERT INTO " + dbName + ".normal values (1)", driver);

    run("CREATE TEMPORARY TABLE " + dbName + ".temp(a int)", driver);
    run("INSERT INTO " + dbName + ".temp values (2)", driver);
    run("INSERT OVERWRITE TABLE " + dbName + ".temp select * from " + dbName + ".normal", driver);

    cs = fs.getContentSummary(path);
    long fileCountAfter = cs.getFileCount();

    assertTrue(fileCount == fileCountAfter);

    run("INSERT INTO " + dbName + ".temp values (3)", driver);
    run("TRUNCATE TABLE " + dbName + ".temp", driver);

    cs = fs.getContentSummary(path);
    fileCountAfter = cs.getFileCount();
    assertTrue(fileCount == fileCountAfter);

    run("INSERT INTO " + dbName + ".temp values (4)", driver);
    run("ALTER TABLE " + dbName + ".temp RENAME to " + dbName + ".temp1", driver);
    verifyRun("SELECT count(*) from " + dbName + ".temp1", new String[]{"1"}, driver);

    cs = fs.getContentSummary(path);
    fileCountAfter = cs.getFileCount();
    assertTrue(fileCount == fileCountAfter);

    run("INSERT INTO " + dbName + ".temp1 values (5)", driver);
    run("DROP TABLE " + dbName + ".temp1", driver);

    cs = fs.getContentSummary(path);
    fileCountAfter = cs.getFileCount();
    assertTrue(fileCount == fileCountAfter);
  }

  private NotificationEvent createDummyEvent(String dbname, String tblname, long evid) {
    MessageFactory msgFactory = MessageFactory.getInstance();
    Table t = new Table();
    t.setDbName(dbname);
    t.setTableName(tblname);
    NotificationEvent event = new NotificationEvent(
        evid,
        (int)System.currentTimeMillis(),
        MessageFactory.CREATE_TABLE_EVENT,
        msgFactory.buildCreateTableMessage(t, Arrays.asList("/tmp/").iterator()).toString()
    );
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    event.setMessageFormat(msgFactory.getMessageFormat());
    return event;
  }

  private String verifyAndReturnDbReplStatus(String dbName, String tblName, String prevReplDumpId, String cmd) throws IOException {
    run(cmd, driver);
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + prevReplDumpId, driver);
    String lastDumpLocn = getResult(0, 0, driver);
    String lastReplDumpId = getResult(0, 1, true, driver);
    run("REPL LOAD " + dbName + "_dupe FROM '" + lastDumpLocn + "'", driverMirror);
    verifyRun("REPL STATUS " + dbName + "_dupe", lastReplDumpId, driverMirror);
    if (tblName != null){
      verifyRun("REPL STATUS " + dbName + "_dupe." + tblName, lastReplDumpId, driverMirror);
    }
    assertTrue(Long.parseLong(lastReplDumpId) > Long.parseLong(prevReplDumpId));
    return lastReplDumpId;
  }

  // Tests that doing a table-level REPL LOAD updates table repl.last.id, but not db-level repl.last.id
  private String verifyAndReturnTblReplStatus(
      String dbName, String tblName, String lastDbReplDumpId, String prevReplDumpId, String cmd) throws IOException {
    run(cmd, driver);
    advanceDumpDir();
    run("REPL DUMP " + dbName + "."+ tblName + " FROM " + prevReplDumpId, driver);
    String lastDumpLocn = getResult(0, 0, driver);
    String lastReplDumpId = getResult(0, 1, true, driver);
    run("REPL LOAD " + dbName + "_dupe." + tblName + " FROM '" + lastDumpLocn + "'", driverMirror);
    verifyRun("REPL STATUS " + dbName + "_dupe", lastDbReplDumpId, driverMirror);
    verifyRun("REPL STATUS " + dbName + "_dupe." + tblName, lastReplDumpId, driverMirror);
    assertTrue(Long.parseLong(lastReplDumpId) > Long.parseLong(prevReplDumpId));
    return lastReplDumpId;
  }


  private String getResult(int rowNum, int colNum, IDriver myDriver) throws IOException {
    return getResult(rowNum,colNum,false, myDriver);
  }
  private String getResult(int rowNum, int colNum, boolean reuse, IDriver myDriver) throws IOException {
    if (!reuse) {
      lastResults = new ArrayList<String>();
      myDriver.getResults(lastResults);
    }
    // Split around the 'tab' character
    return (lastResults.get(rowNum).split("\\t"))[colNum];
  }

  /**
   * All the results that are read from the hive output will not preserve
   * case sensitivity and will all be in lower case, hence we will check against
   * only lower case data values.
   * Unless for Null Values it actually returns in UpperCase and hence explicitly lowering case
   * before assert.
   */
  private void verifyResults(String[] data, IDriver myDriver) throws IOException {
    List<String> results = getOutput(myDriver);
    LOG.info("Expecting {}", data);
    LOG.info("Got {}", results);
    assertEquals(data.length, results.size());
    for (int i = 0; i < data.length; i++) {
      assertEquals(data[i].toLowerCase().trim(), results.get(i).toLowerCase().trim());
    }
  }

  private List<String> getOutput(IDriver myDriver) throws IOException {
    List<String> results = new ArrayList<>();
    myDriver.getResults(results);
    return results;
  }

  private void printOutput(IDriver myDriver) throws IOException {
    for (String s : getOutput(myDriver)){
      LOG.info(s);
    }
  }

  private void verifyIfTableNotExist(String dbName, String tableName, HiveMetaStoreClient myClient){
    Exception e = null;
    try {
      Table tbl = myClient.getTable(dbName, tableName);
      assertNull(tbl);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());
  }

  private void verifyIfTableExist(String dbName, String tableName, HiveMetaStoreClient myClient){
    Exception e = null;
    try {
      Table tbl = myClient.getTable(dbName, tableName);
      assertNotNull(tbl);
    } catch (TException te) {
      assert(false);
    }
  }

  private void verifyIfPartitionNotExist(String dbName, String tableName, List<String> partValues,
      HiveMetaStoreClient myClient){
    Exception e = null;
    try {
      Partition ptn = myClient.getPartition(dbName, tableName, partValues);
      assertNull(ptn);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());
  }

  private void verifyIfPartitionExist(String dbName, String tableName, List<String> partValues,
      HiveMetaStoreClient myClient){
    Exception e = null;
    try {
      Partition ptn = myClient.getPartition(dbName, tableName, partValues);
      assertNotNull(ptn);
    } catch (TException te) {
      assert(false);
    }
  }

  private void verifyIfDirNotExist(FileSystem fs, Path path, PathFilter filter){
    try {
      FileStatus[] statuses = fs.listStatus(path, filter);
      assertEquals(0, statuses.length);
    } catch (IOException e) {
      assert(false);
    }
  }

  private void verifySetup(String cmd, String[] data, IDriver myDriver) throws  IOException {
    if (VERIFY_SETUP_STEPS){
      run(cmd, myDriver);
      verifyResults(data, myDriver);
    }
  }

  private void verifyRun(String cmd, String data, IDriver myDriver) throws IOException {
    verifyRun(cmd, new String[] { data }, myDriver);
  }

  private void verifyRun(String cmd, String[] data, IDriver myDriver) throws IOException {
    run(cmd, myDriver);
    verifyResults(data, myDriver);
  }

  private void verifyFail(String cmd, IDriver myDriver) throws RuntimeException {
    boolean success = false;
    try {
      success = run(cmd, false, myDriver);
    } catch (AssertionError ae){
      LOG.warn("AssertionError:",ae);
      throw new RuntimeException(ae);
    } catch (Exception e) {
      success = false;
    }
    assertFalse(success);
  }

  private void verifyRunWithPatternMatch(String cmd, String key, String pattern, IDriver myDriver) throws IOException {
    run(cmd, myDriver);
    List<String> results = getOutput(myDriver);
    assertTrue(results.size() > 0);
    boolean success = false;
    for (int i = 0; i < results.size(); i++) {
      if (results.get(i).contains(key) && results.get(i).contains(pattern)) {
         success = true;
         break;
      }
    }

    assertTrue(success);
  }

  private static void run(String cmd, IDriver myDriver) throws RuntimeException {
    try {
    run(cmd,false, myDriver); // default arg-less run simply runs, and does not care about failure
    } catch (AssertionError ae){
      // Hive code has AssertionErrors in some cases - we want to record what happens
      LOG.warn("AssertionError:",ae);
      throw new RuntimeException(ae);
    }
  }

  private static boolean run(String cmd, boolean errorOnFail, IDriver myDriver) throws RuntimeException {
    boolean success = false;
    CommandProcessorResponse ret = myDriver.run(cmd);
    success = ((ret.getException() == null) && (ret.getErrorMessage() == null));
    if (!success) {
      LOG.warn("Error {} : {} running [{}].", ret.getErrorCode(), ret.getErrorMessage(), cmd);
    }
    return success;
  }

  private static void createTestDataFile(String filename, String[] lines) throws IOException {
    FileWriter writer = null;
    try {
      File file = new File(filename);
      file.deleteOnExit();
      writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line + "\n");
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }
}
