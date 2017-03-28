/**
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.EventUtils;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec.ReplStateMap;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.Shell;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestReplicationScenarios {

  final static String DBNOTIF_LISTENER_CLASSNAME = "org.apache.hive.hcatalog.listener.DbNotificationListener";
      // FIXME : replace with hive copy once that is copied
  final static String tid =
      TestReplicationScenarios.class.getCanonicalName().replace('.','_') + "_" + System.currentTimeMillis();
  final static String TEST_PATH = System.getProperty("test.warehouse.dir","/tmp") + Path.SEPARATOR + tid;

  static HiveConf hconf;
  static boolean useExternalMS = false;
  static int msPort;
  static Driver driver;
  static HiveMetaStoreClient metaStoreClient;

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
      useExternalMS = true;
      return;
    }

    hconf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS,
        DBNOTIF_LISTENER_CLASSNAME); // turn on db notification listener on metastore
    hconf.setBoolVar(HiveConf.ConfVars.REPLCMENABLED, true);
    hconf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    hconf.setVar(HiveConf.ConfVars.REPLCMDIR, TEST_PATH + "/cmroot/");
    msPort = MetaStoreUtils.startMetaStore(hconf);
    hconf.setVar(HiveConf.ConfVars.REPLDIR,TEST_PATH + "/hrepl/");
    hconf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
    hconf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hconf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
        "false");
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

    Path testPath = new Path(TEST_PATH);
    FileSystem fs = FileSystem.get(testPath.toUri(),hconf);
    fs.mkdirs(testPath);

    driver = new Driver(hconf);
    SessionState.start(new CliSessionState(hconf));
    metaStoreClient = new HiveMetaStoreClient(hconf);
  }

  @AfterClass
  public static void tearDownAfterClass(){
    // FIXME : should clean up TEST_PATH, but not doing it now, for debugging's sake
  }

  @Before
  public void setUp(){
    // before each test
  }

  @After
  public void tearDown(){
    // after each test
  }

  private static  int next = 0;
  private synchronized void advanceDumpDir() {
    next++;
    ReplicationSemanticAnalyzer.injectNextDumpDirForTest(String.valueOf(next));
  }

  /**
   * Tests basic operation - creates a db, with 4 tables, 2 ptned and 2 unptned.
   * Inserts data into one of the ptned tables, and one of the unptned tables,
   * and verifies that a REPL DUMP followed by a REPL LOAD is able to load it
   * appropriately. This tests bootstrap behaviour primarily.
   */
  @Test
  public void testBasic() throws IOException {

    String testName = "basic";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE");

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

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2);
    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty);

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    verifyRun("REPL STATUS " + dbName + "_dupe", replDumpId);

    verifyRun("SELECT * from " + dbName + "_dupe.unptned", unptn_data);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", ptn_data_1);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", ptn_data_2);
    verifyRun("SELECT a from " + dbName + ".ptned_empty", empty);
    verifyRun("SELECT * from " + dbName + ".unptned_empty", empty);
  }

  @Test
  public void testBasicWithCM() throws Exception {

    String testName = "basic_with_cm";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE");

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] ptn_data_2_later = new String[]{ "eighteen", "nineteen", "twenty"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();
    String ptn_locn_2_later = new Path(TEST_PATH , testName + "_ptn2_later").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);
    createTestDataFile(ptn_locn_2_later, ptn_data_2_later);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned");
    verifyResults(unptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)");
    run("SELECT a from " + dbName + ".ptned WHERE b=1");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)");
    run("SELECT a from " + dbName + ".ptned WHERE b=2");
    verifyResults(ptn_data_2);
    run("SELECT a from " + dbName + ".ptned_empty");
    verifyResults(empty);
    run("SELECT * from " + dbName + ".unptned_empty");
    verifyResults(empty);

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);

    // Table dropped after "repl dump"
    run("DROP TABLE " + dbName + ".unptned");
    // Partition droppped after "repl dump"
    run("ALTER TABLE " + dbName + ".ptned " + "DROP PARTITION(b=1)");
    // File changed after "repl dump"
    Partition p = metaStoreClient.getPartition(dbName, "ptned", "b=2");
    Path loc = new Path(p.getSd().getLocation());
    FileSystem fs = loc.getFileSystem(hconf);
    Path file = fs.listStatus(loc)[0].getPath();
    fs.delete(file, false);
    fs.copyFromLocalFile(new Path(ptn_locn_2_later), file);

    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    run("REPL STATUS " + dbName + "_dupe");
    verifyResults(new String[] {replDumpId});

    run("SELECT * from " + dbName + "_dupe.unptned");
    verifyResults(unptn_data);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=1");
    verifyResults(ptn_data_1);
    // Since partition(b=2) changed manually, Hive cannot find
    // it in original location and cmroot, thus empty
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=2");
    verifyResults(empty);
    run("SELECT a from " + dbName + ".ptned_empty");
    verifyResults(empty);
    run("SELECT * from " + dbName + ".unptned_empty");
    verifyResults(empty);
  }

  @Test
  public void testIncrementalAdds() throws IOException {
    String testName = "incrementalAdds";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE");

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}",replDumpLocn,replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

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

    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty);

    // Now, we load data into the tables, and see if an incremental
    // repl drop/load can duplicate it.

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data);
    run("CREATE TABLE " + dbName + ".unptned_late AS SELECT * from " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptn_data);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2);

    run("CREATE TABLE " + dbName + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE");
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=1)");
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1",ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=2)");
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptn_data_2);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId );
    String incrementalDumpLocn = getResult(0,0);
    String incrementalDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'");

    run("REPL STATUS " + dbName + "_dupe");
    verifyResults(new String[] {incrementalDumpId});

    // VERIFY tables and partitions on destination for equivalence.

    verifyRun("SELECT * from " + dbName + "_dupe.unptned_empty", empty);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_empty", empty);

//    verifyRun("SELECT * from " + dbName + "_dupe.unptned", unptn_data);
    // TODO :this does not work because LOAD DATA LOCAL INPATH into an unptned table seems
    // to use ALTER_TABLE only - it does not emit an INSERT or CREATE - re-enable after
    // fixing that.
    verifyRun("SELECT * from " + dbName + "_dupe.unptned_late", unptn_data);

    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=1", ptn_data_1);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", ptn_data_2);

    verifyRun("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=1", ptn_data_1);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=2", ptn_data_2);
  }

  @Test
  public void testDrops() throws IOException {

    String testName = "drops";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned3(a string) partitioned by (b int) STORED AS TEXTFILE");

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

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='1'", ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='2'", ptn_data_2);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')");
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='1'", ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')");
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='2'", ptn_data_2);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned3 PARTITION(b=1)");
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b=1", ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned3 PARTITION(b=2)");
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b=2", ptn_data_2);

    // At this point, we've set up all the tables and ptns we're going to test drops across
    // Replicate it first, and then we'll drop it on the source.

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    verifySetup("REPL STATUS " + dbName + "_dupe", new String[]{replDumpId});

    verifySetup("SELECT * from " + dbName + "_dupe.unptned", unptn_data);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned WHERE b='1'", ptn_data_1);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'", ptn_data_2);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='1'", ptn_data_1);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='2'", ptn_data_2);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned3 WHERE b=1", ptn_data_1);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned3 WHERE b=2", ptn_data_2);

    // All tables good on destination, drop on source.

    run("DROP TABLE " + dbName + ".unptned");
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b='2')");
    run("DROP TABLE " + dbName + ".ptned2");
    run("ALTER TABLE " + dbName + ".ptned3 DROP PARTITION (b=1)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='2'", empty);
    verifySetup("SELECT a from " + dbName + ".ptned", ptn_data_1);
    verifySetup("SELECT a from " + dbName + ".ptned3 WHERE b=1",empty);
    verifySetup("SELECT a from " + dbName + ".ptned3", ptn_data_2);

    // replicate the incremental drops

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    String postDropReplDumpLocn = getResult(0,0);
    String postDropReplDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'");

    // verify that drops were replicated. This can either be from tables or ptns
    // not existing, and thus, throwing a NoSuchObjectException, or returning nulls
    // or select * returning empty, depending on what we're testing.

    Exception e = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName + "_dupe", "unptned");
      assertNull(tbl);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());

    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'", empty);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned", ptn_data_1);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned3 WHERE b=1", empty);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned3", ptn_data_2);

    Exception e2 = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName+"_dupe","ptned2");
      assertNull(tbl);
    } catch (TException te) {
      e2 = te;
    }
    assertNotNull(e2);
    assertEquals(NoSuchObjectException.class, e.getClass());
  }

  @Test
  public void testDropsWithCM() throws IOException {

    String testName = "drops_with_cm";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b string) STORED AS TEXTFILE");

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

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    run("SELECT * from " + dbName + ".unptned");
    verifyResults(unptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')");
    run("SELECT a from " + dbName + ".ptned WHERE b='1'");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')");
    run("SELECT a from " + dbName + ".ptned WHERE b='2'");
    verifyResults(ptn_data_2);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')");
    run("SELECT a from " + dbName + ".ptned2 WHERE b='1'");
    verifyResults(ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')");
    run("SELECT a from " + dbName + ".ptned2 WHERE b='2'");
    verifyResults(ptn_data_2);

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    run("REPL STATUS " + dbName + "_dupe");
    verifyResults(new String[] {replDumpId});

    run("SELECT * from " + dbName + "_dupe.unptned");
    verifyResults(unptn_data);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b='1'");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'");
    verifyResults(ptn_data_2);
    run("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='1'");
    verifyResults(ptn_data_1);
    run("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='2'");
    verifyResults(ptn_data_2);

    run("CREATE TABLE " + dbName + ".unptned_copy" + " AS SELECT a FROM " + dbName + ".unptned");
    run("CREATE TABLE " + dbName + ".ptned_copy" + " LIKE " + dbName + ".ptned");
    run("INSERT INTO TABLE " + dbName + ".ptned_copy" + " PARTITION(b='1') SELECT a FROM " +
        dbName + ".ptned WHERE b='1'");
    run("SELECT a from " + dbName + ".unptned_copy");
    verifyResults(unptn_data);
    run("SELECT a from " + dbName + ".ptned_copy");
    verifyResults(ptn_data_1);

    run("DROP TABLE " + dbName + ".unptned");
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b='2')");
    run("DROP TABLE " + dbName + ".ptned2");
    run("SELECT a from " + dbName + ".ptned WHERE b=2");
    verifyResults(empty);
    run("SELECT a from " + dbName + ".ptned");
    verifyResults(ptn_data_1);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    String postDropReplDumpLocn = getResult(0,0);
    String postDropReplDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);

    // Drop table after dump
    run("DROP TABLE " + dbName + ".unptned_copy");
    // Drop partition after dump
    run("ALTER TABLE " + dbName + ".ptned_copy DROP PARTITION(b='1')");

    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + postDropReplDumpLocn + "'");

    Exception e = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName + "_dupe", "unptned");
      assertNull(tbl);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());

    run("SELECT a from " + dbName + "_dupe.ptned WHERE b=2");
    verifyResults(empty);
    run("SELECT a from " + dbName + "_dupe.ptned");
    verifyResults(ptn_data_1);

    Exception e2 = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName+"_dupe","ptned2");
      assertNull(tbl);
    } catch (TException te) {
      e2 = te;
    }
    assertNotNull(e2);
    assertEquals(NoSuchObjectException.class, e.getClass());

    run("SELECT a from " + dbName + "_dupe.unptned_copy");
    verifyResults(unptn_data);
    run("SELECT a from " + dbName + "_dupe.ptned_copy");
    verifyResults(ptn_data_1);
  }

  @Test
  public void testAlters() throws IOException {

    String testName = "alters";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned2(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned2(a string) partitioned by (b string) STORED AS TEXTFILE");

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

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned2");
    verifySetup("SELECT * from " + dbName + ".unptned2", unptn_data);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='1'", ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='2'", ptn_data_2);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')");
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='1'",ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')");
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='2'", ptn_data_2);

    // base tables set up, let's replicate them over

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    run("REPL STATUS " + dbName + "_dupe");
    verifyResults(new String[] {replDumpId});

    verifySetup("SELECT * from " + dbName + "_dupe.unptned", unptn_data);
    verifySetup("SELECT * from " + dbName + "_dupe.unptned2", unptn_data);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned WHERE b='1'", ptn_data_1);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned WHERE b='2'", ptn_data_2);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='1'", ptn_data_1);
    verifySetup("SELECT a from " + dbName + "_dupe.ptned2 WHERE b='2'", ptn_data_2);

    // tables have been replicated over, and verified to be identical. Now, we do a couple of
    // alters on the source

    // Rename unpartitioned table
    run("ALTER TABLE " + dbName + ".unptned RENAME TO " + dbName + ".unptned_rn");
    verifySetup("SELECT * from " + dbName + ".unptned_rn", unptn_data);

    // Alter unpartitioned table set table property
    String testKey = "blah";
    String testVal = "foo";
    run("ALTER TABLE " + dbName + ".unptned2 SET TBLPROPERTIES ('" + testKey + "' = '" + testVal + "')");
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
    run("ALTER TABLE " + dbName + ".ptned PARTITION (b='2') RENAME TO PARTITION (b='22')");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", empty);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=22", ptn_data_2);

    // alter partitioned table set table property
    run("ALTER TABLE " + dbName + ".ptned SET TBLPROPERTIES ('" + testKey + "' = '" + testVal + "')");
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
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b=2", ptn_data_2);
    run("ALTER TABLE " + dbName + ".ptned2 RENAME TO " + dbName + ".ptned2_rn");
    verifySetup("SELECT a from " + dbName + ".ptned2_rn WHERE b=2", ptn_data_2);

    // All alters done, now we replicate them over.

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    String postAlterReplDumpLocn = getResult(0,0);
    String postAlterReplDumpId = getResult(0,1,true);
    LOG.info("Dumped to {} with id {}->{}", postAlterReplDumpLocn, replDumpId, postAlterReplDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + postAlterReplDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + postAlterReplDumpLocn + "'");

    // Replication done, we now do the following verifications:

    // verify that unpartitioned table rename succeeded.
    Exception e = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName + "_dupe" , "unptned");
      assertNull(tbl);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());
    verifyRun("SELECT * from " + dbName + "_dupe.unptned_rn", unptn_data);

    // verify that partition rename succeded.
    try {
      Table unptn2 = metaStoreClient.getTable(dbName + "_dupe" , "unptned2");
      assertTrue(unptn2.getParameters().containsKey(testKey));
      assertEquals(testVal,unptn2.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=2", empty);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned WHERE b=22", ptn_data_2);

    // verify that ptned table rename succeded.
    Exception e2 = null;
    try {
      Table tbl = metaStoreClient.getTable(dbName + "_dupe" , "ptned2");
      assertNull(tbl);
    } catch (TException te) {
      e2 = te;
    }
    assertNotNull(e2);
    assertEquals(NoSuchObjectException.class, e.getClass());
    verifyRun("SELECT a from " + dbName + "_dupe.ptned2_rn WHERE b=2", ptn_data_2);

    // verify that ptned table property set worked
    try {
      Table ptned = metaStoreClient.getTable(dbName + "_dupe" , "ptned");
      assertTrue(ptned.getParameters().containsKey(testKey));
      assertEquals(testVal, ptned.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    // verify that partitioned table partition property set worked.
    try {
      List<String> ptnVals1 = new ArrayList<String>();
      ptnVals1.add("1");
      Partition ptn1 = metaStoreClient.getPartition(dbName + "_dupe", "ptned", ptnVals1);
      assertTrue(ptn1.getParameters().containsKey(testKey));
      assertEquals(testVal,ptn1.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

  }

  @Test
  public void testIncrementalLoad() throws IOException {
    String testName = "incrementalLoad";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName
        + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE");

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0, 0);
    String replDumpId = getResult(0, 1, true);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

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

    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned");
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptn_data);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    String incrementalDumpLocn = getResult(0, 0);
    String incrementalDumpId = getResult(0, 1, true);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    verifyRun("SELECT * from " + dbName + "_dupe.unptned_late", unptn_data);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName
        + ".ptned PARTITION(b=1)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName
        + ".ptned PARTITION(b=2)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2);

    run("CREATE TABLE " + dbName
        + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE");
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
        + ".ptned WHERE b=1");
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1", ptn_data_1);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
        + ".ptned WHERE b=2");
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptn_data_2);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    incrementalDumpLocn = getResult(0, 0);
    incrementalDumpId = getResult(0, 1, true);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");

    verifyRun("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=1", ptn_data_1);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned_late WHERE b=2", ptn_data_2);
  }

  @Test
  public void testIncrementalInserts() throws IOException {
    String testName = "incrementalInserts";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0, 0);
    String replDumpId = getResult(0, 1, true);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    String[] unptn_data = new String[] { "eleven", "twelve" };
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')");
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')");
    verifyRun("SELECT a from " + dbName + ".unptned", unptn_data);

    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned");
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned");
    verifyRun("SELECT * from " + dbName + ".unptned_late", unptn_data);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    String incrementalDumpLocn = getResult(0, 0);
    String incrementalDumpId = getResult(0, 1, true);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    verifyRun("SELECT a from " + dbName + ".unptned", unptn_data);
    verifyRun("SELECT a from " + dbName + ".unptned_late", unptn_data);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned", unptn_data);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned_late", unptn_data);

    String[] unptn_data_after_ins = new String[] { "eleven", "twelve", "thirteen" };
    run("INSERT INTO TABLE " + dbName + ".unptned_late values('" + unptn_data_after_ins[2] + "')");
    verifySetup("SELECT a from " + dbName + ".unptned_late", unptn_data_after_ins);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    incrementalDumpLocn = getResult(0, 0);
    incrementalDumpId = getResult(0, 1, true);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");

    verifyRun("SELECT a from " + dbName + "_dupe.unptned_late", unptn_data_after_ins);
  }

  @Test
  public void testViewsReplication() throws IOException {
    String testName = "viewsReplication";
    LOG.info("Testing "+testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE");
    run("CREATE VIEW " + dbName + ".virtual_view AS SELECT * FROM " + dbName + ".unptned");

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

    verifySetup("SELECT a from " + dbName + ".ptned", empty);
    verifySetup("SELECT * from " + dbName + ".unptned", empty);
    verifySetup("SELECT * from " + dbName + ".virtual_view", empty);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data);
    verifySetup("SELECT * from " + dbName + ".virtual_view", unptn_data);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)");
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2);

    run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view AS SELECT a FROM " + dbName + ".ptned where b=1");
    verifySetup("SELECT a from " + dbName + ".mat_view", ptn_data_1);

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0,0);
    String replDumpId = getResult(0,1,true);
    LOG.info("Bootstrap-dump: Dumped to {} with id {}",replDumpLocn,replDumpId);
    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    verifyRun("SELECT * from " + dbName + "_dupe.virtual_view", unptn_data);
    verifyRun("SELECT a from " + dbName + "_dupe.mat_view", ptn_data_1);

    run("CREATE VIEW " + dbName + ".virtual_view2 AS SELECT a FROM " + dbName + ".ptned where b=2");
    verifySetup("SELECT a from " + dbName + ".virtual_view2", ptn_data_2);

    // Create a view with name already exist. Just to verify if failure flow clears the added create_table event.
    run("CREATE VIEW " + dbName + ".virtual_view2 AS SELECT a FROM " + dbName + ".ptned where b=2");

    run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view2 AS SELECT * FROM " + dbName + ".unptned");
    verifySetup("SELECT * from " + dbName + ".mat_view2", unptn_data);

    // Perform REPL-DUMP/LOAD
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId );
    String incrementalDumpLocn = getResult(0,0);
    String incrementalDumpId = getResult(0,1,true);
    LOG.info("Incremental-dump: Dumped to {} with id {}", incrementalDumpLocn, incrementalDumpId);
    run("EXPLAIN REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    printOutput();
    run("REPL LOAD " + dbName + "_dupe FROM '"+incrementalDumpLocn+"'");

    run("REPL STATUS " + dbName + "_dupe");
    verifyResults(new String[] {incrementalDumpId});

    verifyRun("SELECT * from " + dbName + "_dupe.unptned", unptn_data);
    verifyRun("SELECT a from " + dbName + "_dupe.ptned where b=1", ptn_data_1);
    verifyRun("SELECT * from " + dbName + "_dupe.virtual_view", unptn_data);
    verifyRun("SELECT a from " + dbName + "_dupe.mat_view", ptn_data_1);
    verifyRun("SELECT * from " + dbName + "_dupe.virtual_view2", ptn_data_2);
    verifyRun("SELECT * from " + dbName + "_dupe.mat_view2", unptn_data);
  }

  @Test
  public void testDumpLimit() throws IOException {
    String testName = "dumpLimit";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE");

    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String replDumpLocn = getResult(0, 0);
    String replDumpId = getResult(0, 1, true);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);

    String[] unptn_data = new String[] { "eleven", "thirteen", "twelve" };
    String[] unptn_data_load1 = new String[] { "eleven" };
    String[] unptn_data_load2 = new String[] { "eleven", "thirteen" };

    // 3 events to insert, last repl ID: replDumpId+3
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')");
    // 3 events to insert, last repl ID: replDumpId+6
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')");
    // 3 events to insert, last repl ID: replDumpId+9
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[2] + "')");
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data);

    run("REPL LOAD " + dbName + "_dupe FROM '" + replDumpLocn + "'");

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId + " LIMIT 3");
    String incrementalDumpLocn = getResult(0, 0);
    String incrementalDumpId = getResult(0, 1, true);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;

    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data);
    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_load1);

    advanceDumpDir();
    Integer lastReplID = Integer.valueOf(replDumpId);
    lastReplID += 1000;
    String toReplID = String.valueOf(lastReplID);

    run("REPL DUMP " + dbName + " FROM " + replDumpId + " TO " + toReplID + " LIMIT 3");
    incrementalDumpLocn = getResult(0, 0);
    incrementalDumpId = getResult(0, 1, true);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");

    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_load2);

    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + replDumpId);
    incrementalDumpLocn = getResult(0, 0);
    incrementalDumpId = getResult(0, 1, true);
    LOG.info("Incremental-Dump: Dumped to {} with id {} from {}", incrementalDumpLocn, incrementalDumpId, replDumpId);
    replDumpId = incrementalDumpId;
    run("REPL LOAD " + dbName + "_dupe FROM '" + incrementalDumpLocn + "'");

    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data);
  }

  @Test
  public void testStatus() throws IOException {
    // first test ReplStateMap functionality
    Map<String,Long> cmap = new ReplStateMap<String,Long>();

    Long oldV;
    oldV = cmap.put("a",1L);
    assertEquals(1L,cmap.get("a").longValue());
    assertEquals(null,oldV);

    cmap.put("b",2L);
    oldV = cmap.put("b",-2L);
    assertEquals(2L, cmap.get("b").longValue());
    assertEquals(2L, oldV.longValue());

    cmap.put("c",3L);
    oldV = cmap.put("c",33L);
    assertEquals(33L, cmap.get("c").longValue());
    assertEquals(3L, oldV.longValue());

    // Now, to actually testing status - first, we bootstrap.

    String testName = "incrementalStatus";
    LOG.info("Testing " + testName);
    String dbName = testName + "_" + tid;

    run("CREATE DATABASE " + dbName);
    advanceDumpDir();
    run("REPL DUMP " + dbName);
    String lastReplDumpLocn = getResult(0, 0);
    String lastReplDumpId = getResult(0, 1, true);
    run("REPL LOAD " + dbName + "_dupe FROM '" + lastReplDumpLocn + "'");

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

    assertTrue(finalTblReplDumpId.compareTo(lastTblReplDumpId) > 0);

    // TODO : currently not testing the following scenarios:
    //   a) Multi-db wh-level REPL LOAD - need to add that
    //   b) Insert into tables - quite a few cases need to be enumerated there, including dyn adds.

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
    IMetaStoreClient.NotificationFilter dbTblFilter = EventUtils.getDbTblNotificationFilter(dbname,tblname);
    IMetaStoreClient.NotificationFilter dbFilter = EventUtils.getDbTblNotificationFilter(dbname,null);

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
    IMetaStoreClient.NotificationFilter evRangeFilter = EventUtils.getEventBoundaryFilter(evBegin,evEnd);

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
        EventUtils.restrictByMessageFormat(MessageFactory.getInstance().getMessageFormat());
    IMetaStoreClient.NotificationFilter restrictByArbitraryMessageFormat =
        EventUtils.restrictByMessageFormat(MessageFactory.getInstance().getMessageFormat() + "_bogus");
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

    assertTrue(EventUtils.andFilter(yes, yes).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(yes, no).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(no, yes).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(no, no).accept(dummyEvent));

    assertTrue(EventUtils.andFilter(yes, yes, yes).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(yes, yes, no).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(yes, no, yes).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(yes, no, no).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(no, yes, yes).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(no, yes, no).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(no, no, yes).accept(dummyEvent));
    assertFalse(EventUtils.andFilter(no, no, no).accept(dummyEvent));


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
    run(cmd);
    advanceDumpDir();
    run("REPL DUMP " + dbName + " FROM " + prevReplDumpId);
    String lastDumpLocn = getResult(0, 0);
    String lastReplDumpId = getResult(0, 1, true);
    run("REPL LOAD " + dbName + "_dupe FROM '" + lastDumpLocn + "'");
    verifyRun("REPL STATUS " + dbName + "_dupe", lastReplDumpId);
    if (tblName != null){
      verifyRun("REPL STATUS " + dbName + "_dupe." + tblName, lastReplDumpId);
    }
    assertTrue(lastReplDumpId.compareTo(prevReplDumpId) > 0);
    return lastReplDumpId;
  }

  // Tests that doing a table-level REPL LOAD updates table repl.last.id, but not db-level repl.last.id
  private String verifyAndReturnTblReplStatus(
      String dbName, String tblName, String lastDbReplDumpId, String prevReplDumpId, String cmd) throws IOException {
    run(cmd);
    advanceDumpDir();
    run("REPL DUMP " + dbName + "."+ tblName + " FROM " + prevReplDumpId);
    String lastDumpLocn = getResult(0, 0);
    String lastReplDumpId = getResult(0, 1, true);
    run("REPL LOAD " + dbName + "_dupe." + tblName + " FROM '" + lastDumpLocn + "'");
    verifyRun("REPL STATUS " + dbName + "_dupe", lastDbReplDumpId);
    verifyRun("REPL STATUS " + dbName + "_dupe." + tblName, lastReplDumpId);
    assertTrue(lastReplDumpId.compareTo(prevReplDumpId) > 0);
    return lastReplDumpId;
  }


  private String getResult(int rowNum, int colNum) throws IOException {
    return getResult(rowNum,colNum,false);
  }
  private String getResult(int rowNum, int colNum, boolean reuse) throws IOException {
    if (!reuse) {
      lastResults = new ArrayList<String>();
      try {
        driver.getResults(lastResults);
      } catch (CommandNeedRetryException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    // Split around the 'tab' character
    return (lastResults.get(rowNum).split("\\t"))[colNum];
  }

  private void verifyResults(String[] data) throws IOException {
    List<String> results = getOutput();
    LOG.info("Expecting {}",data);
    LOG.info("Got {}",results);
    assertEquals(data.length,results.size());
    for (int i = 0; i < data.length; i++){
      assertEquals(data[i],results.get(i));
    }
  }

  private List<String> getOutput() throws IOException {
    List<String> results = new ArrayList<String>();
    try {
      driver.getResults(results);
    } catch (CommandNeedRetryException e) {
      LOG.warn(e.getMessage(),e);
      throw new RuntimeException(e);
    }
    return results;
  }

  private void printOutput() throws IOException {
    for (String s : getOutput()){
      LOG.info(s);
    }
  }

  private void verifySetup(String cmd, String[] data) throws  IOException {
    if (VERIFY_SETUP_STEPS){
      run(cmd);
      verifyResults(data);
    }
  }

  private void verifyRun(String cmd, String data) throws IOException {
    verifyRun(cmd, new String[] { data });
  }

  private void verifyRun(String cmd, String[] data) throws IOException {
    run(cmd);
    verifyResults(data);
  }

  private static void run(String cmd) throws RuntimeException {
    try {
    run(cmd,false); // default arg-less run simply runs, and does not care about failure
    } catch (AssertionError ae){
      // Hive code has AssertionErrors in some cases - we want to record what happens
      LOG.warn("AssertionError:",ae);
      throw new RuntimeException(ae);
    }
  }

  private static boolean run(String cmd, boolean errorOnFail) throws RuntimeException {
    boolean success = false;
    try {
      CommandProcessorResponse ret = driver.run(cmd);
      success = (ret.getException() == null);
      if (!success){
        LOG.warn("Error {} : {} running [{}].", ret.getErrorCode(), ret.getErrorMessage(), cmd);
      }
    } catch (CommandNeedRetryException e) {
      if (errorOnFail){
        throw new RuntimeException(e);
      } else {
        LOG.warn(e.getMessage(),e);
        // do nothing else
      }
    }
    return success;
  }

  public static void createTestDataFile(String filename, String[] lines) throws IOException {
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
