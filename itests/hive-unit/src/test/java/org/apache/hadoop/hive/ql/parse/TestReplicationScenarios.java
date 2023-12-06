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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.hive.ql.parse.repl.metric.MetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.PersistenceManagerProvider;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.MessageFormatFilter;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.ddl.DDLTask;
import org.apache.hadoop.hive.ql.ddl.table.partition.add.AlterTableAddPartitionDesc;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplLoadWork;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.EventDumpDirComparator;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.BootstrapLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.IncrementalLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.REPL_EVENT_DB_LISTENER_TTL;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.DUMP_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.NON_RECOVERABLE_MARKER;
import static org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData.DUMP_METADATA;
import static org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector.isMetricsEnabledForTests;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

  static HiveConf hconf;
  static HiveMetaStoreClient metaStoreClient;
  private static IDriver driver;
  private static String proxySettingName;
  private static HiveConf hconfMirror;
  private static IDriver driverMirror;
  private static HiveMetaStoreClient metaStoreClientMirror;

  // Make sure we skip backward-compat checking for those tests that don't generate events

  protected static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private ArrayList<String> lastResults;

  private boolean verifySetupSteps = false;
  // if verifySetup is set to true, all the test setup we do will perform additional
  // verifications as well, which is useful to verify that our setup occurred
  // correctly when developing and debugging tests. These verifications, however
  // do not test any new functionality for replication, and thus, are not relevant
  // for testing replication itself. For steady state, we want this to be false.

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HashMap<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    internalBeforeClassSetup(overrideProperties);
  }

  static void internalBeforeClassSetup(Map<String, String> additionalProperties)
      throws Exception {
    hconf = new HiveConf(TestReplicationScenarios.class);
    String metastoreUri = System.getProperty("test."+MetastoreConf.ConfVars.THRIFT_URIS.getHiveName());
    if (metastoreUri != null) {
      hconf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreUri);
      return;
    }
    // Disable auth so the call should succeed
    MetastoreConf.setBoolVar(hconf, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, false);
    hconf.set(MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS.getHiveName(),
        DBNOTIF_LISTENER_CLASSNAME); // turn on db notification listener on metastore
    hconf.setBoolVar(HiveConf.ConfVars.REPL_CM_ENABLED, true);
    hconf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    hconf.setVar(HiveConf.ConfVars.REPL_CM_DIR, TEST_PATH + "/cmroot/");
    proxySettingName = "hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts";
    hconf.set(proxySettingName, "*");
    MetastoreConf.setBoolVar(hconf, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, false);
    hconf.setVar(HiveConf.ConfVars.REPL_DIR,TEST_PATH + "/hrepl/");
    hconf.set(MetastoreConf.ConfVars.THRIFT_CONNECTION_RETRIES.getHiveName(), "3");
    hconf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, "true");
    hconf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
    hconf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hconf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname,
        "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
    hconf.set(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname,
        "org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore");
    hconf.set(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL.varname, "/tmp/warehouse/external");
    hconf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_METADATA_QUERIES, true);
    hconf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, true);
    hconf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE, true);
    hconf.setBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET, false);
    hconf.setBoolVar(HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS, false);
    System.setProperty(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, " ");

    additionalProperties.forEach((key, value) -> {
      hconf.set(key, value);
    });

    MetaStoreTestUtils.startMetaStoreWithRetry(hconf, true);
    // re set the WAREHOUSE property to the test dir, as the previous command added a random port to it
    hconf.set(MetastoreConf.ConfVars.WAREHOUSE.getVarname(), System.getProperty("test.warehouse.dir", "/tmp/warehouse/managed"));
    hconf.set(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname(), System.getProperty("test.warehouse.external.dir", "/tmp/warehouse/external"));

    Path testPath = new Path(TEST_PATH);
    FileSystem fs = FileSystem.get(testPath.toUri(),hconf);
    fs.mkdirs(testPath);
    driver = DriverFactory.newDriver(hconf);
    SessionState.start(new CliSessionState(hconf));
    metaStoreClient = new HiveMetaStoreClient(hconf);

    FileUtils.deleteDirectory(new File("metastore_db2"));
    HiveConf hconfMirrorServer = new HiveConf();
    hconfMirrorServer.set(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY.varname, "jdbc:derby:;databaseName=metastore_db2;create=true");
    MetaStoreTestUtils.startMetaStoreWithRetry(hconfMirrorServer, true);
    hconfMirror = new HiveConf(hconf);
    MetastoreConf.setBoolVar(hconfMirror, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, false);
    hconfMirrorServer.set(proxySettingName, "*");
    String thriftUri = MetastoreConf.getVar(hconfMirrorServer, MetastoreConf.ConfVars.THRIFT_URIS);
    MetastoreConf.setVar(hconfMirror, MetastoreConf.ConfVars.THRIFT_URIS, thriftUri);
    driverMirror = DriverFactory.newDriver(hconfMirror);
    metaStoreClientMirror = new HiveMetaStoreClient(hconfMirror);

    PersistenceManagerProvider.setTwoMetastoreTesting(true);
    MetastoreConf.setTimeVar(hconf, EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL, 0, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(hconfMirror, EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL, 0, TimeUnit.SECONDS);
  }

  @AfterClass
  public static void tearDownAfterClass(){
    // FIXME : should clean up TEST_PATH, but not doing it now, for debugging's sake
    //Clean up the warehouse after test run as we are restoring the warehouse path for other metastore creation
    Path warehousePath = new Path(MetastoreConf.getVar(hconf, MetastoreConf.ConfVars.WAREHOUSE));
    Path warehousePathReplica = new Path(MetastoreConf.getVar(hconfMirror, MetastoreConf.ConfVars.WAREHOUSE));
    try {
      warehousePath.getFileSystem(hconf).delete(warehousePath, true);
      warehousePathReplica.getFileSystem(hconfMirror).delete(warehousePathReplica, true);
    } catch (IOException e) {

    }
    Hive.closeCurrent();
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
    if (metaStoreClientMirror != null) {
      metaStoreClientMirror.close();
    }
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
    return incrementalLoadAndVerify(dbName, replDbName);
  }

  private Tuple bootstrapLoadAndVerify(String dbName, String replDbName, List<String> withClauseOptions)
          throws IOException {
    return incrementalLoadAndVerify(dbName, replDbName, withClauseOptions);
  }

  private Tuple incrementalLoadAndVerify(String dbName, String replDbName) throws IOException {
    Tuple dump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, dump.lastReplId);
    return dump;
  }

  private Tuple incrementalLoadAndVerify(String dbName, String replDbName, List<String> withClauseOptions)
          throws IOException {
    Tuple dump = replDumpDb(dbName, withClauseOptions);
    loadAndVerify(replDbName, dbName, dump.lastReplId, withClauseOptions);
    return dump;
  }

  private Tuple replDumpDb(String dbName) throws IOException {
    return replDumpDb(dbName, null);
  }

  private Tuple replDumpDb(String dbName, List<String> withClauseOptions) throws IOException {
    String withClause = getWithClause(withClauseOptions);
    advanceDumpDir();
    String dumpCmd = "REPL DUMP " + dbName + withClause;
    run(dumpCmd, driver);
    String dumpLocation = getResult(0, 0, driver);
    String lastReplId = getResult(0, 1, true, driver);
    LOG.info("Dumped to {} with id {} for command: {}", dumpLocation, lastReplId, dumpCmd);
    return new Tuple(dumpLocation, lastReplId);
  }

  private Tuple replDumpAllDbs(List<String> withClauseOptions) throws IOException {
    String withClause = getWithClause(withClauseOptions);
    advanceDumpDir();
    String dumpCmd = "REPL DUMP `*` " + withClause;
    run(dumpCmd, driver);
    String dumpLocation = getResult(0, 0, driver);
    String lastReplId = getResult(0, 1, true, driver);
    LOG.info("Dumped to {} with id {} for command: {}", dumpLocation, lastReplId, dumpCmd);
    return new Tuple(dumpLocation, lastReplId);
  }

  private String getWithClause(List<String> withClauseOptions) {
    if (withClauseOptions != null && !withClauseOptions.isEmpty()) {
      return  " with (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return "";
  }

  private void loadAndVerify(String replDbName, String sourceDbNameOrPattern, String lastReplId) throws IOException {
    loadAndVerify(replDbName, sourceDbNameOrPattern, lastReplId, null);
  }

  private void loadAndVerify(String replDbName, String sourceDbNameOrPattern, String lastReplId,
                             List<String> withClauseOptions) throws IOException {
    String withClause = getWithClause(withClauseOptions);
    run("REPL LOAD " + sourceDbNameOrPattern + " INTO " + replDbName + withClause, driverMirror);
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
  public void testBasic() throws IOException, SemanticException {
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
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replicatedDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(hconf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    verifyRun("SELECT * from " + replicatedDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned WHERE b=2", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned_empty", empty, driverMirror);
    verifyRun("SELECT * from " + replicatedDbName + ".unptned_empty", empty, driverMirror);
  }

  @Test
  public void testBootstrapFailedDump() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] unptnData = new String[]{"eleven", "twelve"};
    String[] ptnData1 = new String[]{"thirteen", "fourteen", "fifteen"};
    String[] ptnData2 = new String[]{"fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptnLocn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, name + "_ptn1").toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, name + "_ptn2").toUri().getPath();

    createTestDataFile(unptnLocn, unptnData);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);

    run("LOAD DATA LOCAL INPATH '" + unptnLocn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptnData, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptnData1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptnData2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty, driver);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty, driver);

    String replicatedDbName = dbName + "_dupe";


    EximUtil.DataCopyPath.setNullSrcPath(hconf, true);
    verifyFail("REPL DUMP " + dbName, driver);
    advanceDumpDir();
    EximUtil.DataCopyPath.setNullSrcPath(hconf, false);
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replicatedDbName);
    advanceDumpDir();
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(hconf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    verifyRun("SELECT * from " + replicatedDbName + ".unptned", unptnData, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned WHERE b=2", ptnData2, driverMirror);
    verifyRun("SELECT a from " + replicatedDbName + ".ptned_empty", empty, driverMirror);
    verifyRun("SELECT * from " + replicatedDbName + ".unptned_empty", empty, driverMirror);
  }

  private abstract class checkTaskPresent {
    public boolean hasTask(Task rootTask) {
      if (rootTask == null) {
        return false;
      }
      if (validate(rootTask)) {
        return true;
      }
      List<Task<?>> childTasks = rootTask.getChildTasks();
      if (childTasks == null) {
        return false;
      }
      for (Task<?> childTask : childTasks) {
        if (hasTask(childTask)) {
          return true;
        }
      }
      return false;
    }

    public abstract boolean validate(Task task);
  }

  private boolean hasMoveTask(Task rootTask) {
    checkTaskPresent validator =  new checkTaskPresent() {
      public boolean validate(Task task) {
        return  (task instanceof MoveTask);
      }
    };
    return validator.hasTask(rootTask);
  }

  private boolean hasPartitionTask(Task rootTask) {
    checkTaskPresent validator =  new checkTaskPresent() {
      public boolean validate(Task task) {
        if (task instanceof DDLTask) {
          DDLTask ddlTask = (DDLTask)task;
          return ddlTask.getWork().getDDLDesc() instanceof AlterTableAddPartitionDesc;
        }
        return false;
      }
    };
    return validator.hasTask(rootTask);
  }

  private Task getReplLoadRootTask(String sourceDb, String replicadb, boolean isIncrementalDump,
                                   Tuple tuple) throws Throwable {
    HiveConf confTemp = driverMirror.getConf();
    Path loadPath = new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    ReplicationMetricCollector metricCollector;
    if (isIncrementalDump) {
      metricCollector = new IncrementalLoadMetricCollector(replicadb, tuple.dumpLocation, 0,
        confTemp);
    } else {
      metricCollector = new BootstrapLoadMetricCollector(replicadb, tuple.dumpLocation, 0,
        confTemp);
    }
    /* When 'hive.repl.retain.custom.db.locations.on.target' is enabled, the first iteration of repl load would
       run only database creation task, and only in next iteration of Repl Load Task execution, remaining tasks will be
       executed. Hence disabling this to perform the test on task optimization.  */
    confTemp.setBoolVar(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET, false);
    ReplLoadWork replLoadWork = new ReplLoadWork(confTemp, loadPath.toString(), sourceDb, replicadb,
            null, null, isIncrementalDump, Long.valueOf(tuple.lastReplId),
        0L, metricCollector, false);
    Task replLoadTask = TaskFactory.get(replLoadWork, confTemp);
    replLoadTask.initialize(null, null, new TaskQueue(driver.getContext()), driver.getContext());
    replLoadTask.executeTask(null);
    Hive.closeCurrent();
    return replLoadWork.getRootTask();
  }

  @Test
  public void testTaskCreationOptimization() throws Throwable {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String dbNameReplica = dbName + "_replica";
    run("create table " + dbName + ".t2 (place string) partitioned by (country string)", driver);
    run("insert into table " + dbName + ".t2 partition(country='india') values ('bangalore')", driver);

    Tuple dump = replDumpDb(dbName);

    //bootstrap load should not have move task
    Task task = getReplLoadRootTask(dbName, dbNameReplica, false, dump);
    assertEquals(false, hasMoveTask(task));
    assertEquals(true, hasPartitionTask(task));

    Path loadPath = new Path(dump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    //delete load ack to reload the same dump
    loadPath.getFileSystem(hconf).delete(new Path(loadPath, LOAD_ACKNOWLEDGEMENT.toString()), true);
    loadAndVerify(dbNameReplica, dbName, dump.lastReplId);

    run("insert into table " + dbName + ".t2 partition(country='india') values ('delhi')", driver);
    dump = replDumpDb(dbName);

    // Partition level statistics gets updated as part of the INSERT above. So we see a partition
    // task corresponding to an ALTER_PARTITION event.
    task = getReplLoadRootTask(dbName, dbNameReplica, true, dump);
    assertEquals(true, hasMoveTask(task));
    assertEquals(true, hasPartitionTask(task));

    loadPath = new Path(dump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    //delete load ack to reload the same dump
    loadPath.getFileSystem(hconf).delete(new Path(loadPath, LOAD_ACKNOWLEDGEMENT.toString()), true);
    loadAndVerify(dbNameReplica, dbName, dump.lastReplId);

    run("insert into table " + dbName + ".t2 partition(country='us') values ('sf')", driver);
    dump = replDumpDb(dbName);

    //no move task should be added as the operation is adding a dynamic partition
    task = getReplLoadRootTask(dbName, dbNameReplica, true, dump);
    assertEquals(false, hasMoveTask(task));
    assertEquals(true, hasPartitionTask(task));
  }

  @Test
  public void testBasicWithCM() throws Exception {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
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

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = getResult(0,0, driver);
    String replDumpId = getResult(0,1,true, driver);

    // Table dropped after "repl dump"
    run("DROP TABLE " + dbName + ".unptned", driver);

    // Partition droppped after "repl dump"
    run("ALTER TABLE " + dbName + ".ptned " + "DROP PARTITION(b=1)", driver);

    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);
    verifyRun("REPL STATUS " + replDbName, new String[] {replDumpId}, driverMirror);

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_empty", empty, driverMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned_empty", empty, driverMirror);
  }

  @Test
  public void testBasicWithCMLazyCopy() throws Exception {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
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

    String lazyCopyClause = " with ('" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname  + "'='true')";

    advanceDumpDir();
    run("REPL DUMP " + dbName + lazyCopyClause, driver);
    String replDumpLocn = getResult(0,0, driver);
    String replDumpId = getResult(0,1,true, driver);

    // Table dropped after "repl dump"
    run("DROP TABLE " + dbName + ".unptned", driver);

    // Partition droppped after "repl dump"
    run("ALTER TABLE " + dbName + ".ptned " + "DROP PARTITION(b=1)", driver);

    run("REPL LOAD " + dbName + " INTO " + replDbName + lazyCopyClause, driverMirror);
    verifyRun("REPL STATUS " + replDbName, new String[] {replDumpId}, driverMirror);

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_empty", empty, driverMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned_empty", empty, driverMirror);
  }

  @Test
  public void testBootstrapLoadOnExistingDb() throws IOException {
    String testName = "bootstrapLoadOnExistingDb";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    createTestDataFile(unptn_locn, unptn_data);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    // Create an empty database to load
    createDB(dbName + "_empty", driverMirror);

    // Load to an empty database
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, dbName + "_empty");

    verifyRun("SELECT * from " + dbName + "_empty.unptned", unptn_data, driverMirror);

    String[] nullReplId = new String[]{ "NULL" };

    // Create a database with a table
    createDB(dbName + "_withtable", driverMirror);
    run("CREATE TABLE " + dbName + "_withtable.unptned(a string) STORED AS TEXTFILE", driverMirror);
    // Load using same dump to a DB with table will not do anything. Just print a log saying its already loaded
    run("REPL LOAD " + dbName + " INTO " + dbName + "_withtable ", driverMirror);

    // REPL STATUS should return NULL
    verifyRun("REPL STATUS " + dbName + "_withtable", nullReplId, driverMirror);

    // Create a database with a view
    createDB(dbName + "_withview", driverMirror);
    run("CREATE TABLE " + dbName + "_withview.unptned(a string) STORED AS TEXTFILE", driverMirror);
    run("CREATE VIEW " + dbName + "_withview.view AS SELECT * FROM " + dbName + "_withview.unptned", driverMirror);
    // Load using same dump to a DB with table will not do anything. Just print a log saying its already loaded
    run("REPL LOAD " + dbName + " INTO " + dbName + "_withview", driverMirror);

    // REPL STATUS should return NULL
    verifyRun("REPL STATUS " + dbName + "_withview", nullReplId, driverMirror);
  }

  @Test
  public void testBootstrapWithConcurrentDropTable() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};

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
        LOG.info("Performing injection on table " + table.getTableName());
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
    try {
      // The ptned table will not be dumped as getTable will return null
      run("REPL DUMP " + dbName, driver);
      ptnedTableNuller.assertInjectionsPerformed(true,true);
    } finally {
      InjectableBehaviourObjectStore.resetGetTableBehaviour(); // reset the behaviour
    }

    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);

    // The ptned table should miss in target as the table was marked virtually as dropped
    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyFail("SELECT a from " + replDbName + ".ptned WHERE b=1", driverMirror);
    verifyIfTableNotExist(replDbName + "", "ptned", metaStoreClient);

    // Verify if Drop table on a non-existing table is idempotent
    run("DROP TABLE " + dbName + ".ptned", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String postDropReplDumpLocn = getResult(0,0, driver);
    String postDropReplDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);
    assert(run("REPL LOAD " + dbName + " INTO " + replDbName, true, driverMirror));

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyIfTableNotExist(replDbName, "ptned", metaStoreClientMirror);
    verifyFail("SELECT a from " + replDbName + ".ptned WHERE b=1", driverMirror);
  }

  @Test
  public void testBootstrapWithConcurrentDropPartition() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
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
    try {
      // None of the partitions will be dumped as the partitions list was empty
      run("REPL DUMP " + dbName, driver);
      listPartitionNamesNuller.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetListPartitionNamesBehaviour(); // reset the behaviour
    }

    String replDumpLocn = getResult(0, 0, driver);
    String replDumpId = getResult(0, 1, true, driver);
    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);

    // All partitions should miss in target as it was marked virtually as dropped
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", empty, driverMirror);
    verifyIfPartitionNotExist(replDbName , "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClientMirror);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClientMirror);

    // Verify if drop partition on a non-existing partition is idempotent and just a noop.
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b=1)", driver);
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b=2)", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String postDropReplDumpLocn = getResult(0,0,driver);
    String postDropReplDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);
    assert(run("REPL LOAD " + dbName + " INTO " + replDbName, true, driverMirror));

    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClientMirror);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClientMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", empty, driverMirror);
  }

  @Test
  public void testBootstrapWithConcurrentRename() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] ptn_data = new String[]{ "eleven" , "twelve" };
    String ptn_locn = new Path(TEST_PATH, name + "_ptn").toUri().getPath();

    createTestDataFile(ptn_locn, ptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);

    BehaviourInjection<Table,Table> ptnedTableRenamer = new BehaviourInjection<Table,Table>(){
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
              try {
                driver2.run("ALTER TABLE " + dbName + ".ptned PARTITION (b=1) RENAME TO PARTITION (b=10)");
                driver2.run("ALTER TABLE " + dbName + ".ptned RENAME TO " + dbName + ".ptned_renamed");
              } catch (CommandProcessorException e) {
                throw new RuntimeException(e);
              }
              LOG.info("Exit new thread success");
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
    try {
      // The intermediate rename would've failed as bootstrap dump in progress
      bootstrapLoadAndVerify(dbName, replDbName);
      ptnedTableRenamer.assertInjectionsPerformed(true,true);
    } finally {
      InjectableBehaviourObjectStore.resetGetTableBehaviour(); // reset the behaviour
    }

    // The ptned table should be there in both source and target as rename was not successful
    verifyRun("SELECT a from " + dbName + ".ptned WHERE (b=1) ORDER BY a", ptn_data, driver);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE (b=1) ORDER BY a", ptn_data, driverMirror);

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
    String ptn_locn = new Path(TEST_PATH, name + "_ptn").toUri().getPath();

    createTestDataFile(ptn_locn, ptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);

    BehaviourInjection<Table,Table> ptnedTableRenamer = new BehaviourInjection<Table,Table>(){
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
              try {
                driver2.run("DROP TABLE " + dbName + ".ptned");
              } catch (CommandProcessorException e) {
                throw new RuntimeException(e);
              }
              LOG.info("Exit new thread success");
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
    Tuple bootstrap = null;
    try {
      bootstrap = bootstrapLoadAndVerify(dbName, replDbName);
      ptnedTableRenamer.assertInjectionsPerformed(true,true);
    } finally {
      InjectableBehaviourObjectStore.resetGetTableBehaviour(); // reset the behaviour
    }

    incrementalLoadAndVerify(dbName, replDbName);
    verifyIfTableNotExist(replDbName, "ptned", metaStoreClientMirror);
  }

  @Test
  public void testIncrementalAdds() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

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

    verifyRun("SELECT a from " + replDbName + ".ptned_empty", empty, driverMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned_empty", empty, driverMirror);

    // Now, we load data into the tables, and see if an incremental
    // repl drop/load can duplicate it.

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", true, driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    run("CREATE TABLE " + dbName + ".unptned_late AS SELECT * from " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptn_data, driver);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", true, driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", true, driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);

    run("CREATE TABLE " + dbName + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=1)", true, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1",ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=2)", true, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptn_data_2, driver);

    // Perform REPL-DUMP/LOAD
    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(hconf);
    Path dumpPath = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    // VERIFY tables and partitions on destination for equivalence.
    verifyRun("SELECT * from " + replDbName + ".unptned_empty", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_empty", empty, driverMirror);

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned_late", unptn_data, driverMirror);

    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptn_data_2, driverMirror);

    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=2", ptn_data_2, driverMirror);
  }

  @Test
  public void testMultipleDbMetadataOnlyDump() throws IOException {
    verifySetupSteps = true;
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    //create one extra db for bootstrap
    String bootstrapDb = dbName + "_boot";

    //insert data in the additional db
    String[] unptn_data = new String[]{"eleven", "twelve"};
    String unptn_locn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    createTestDataFile(unptn_locn, unptn_data);
    run("CREATE DATABASE " + bootstrapDb, driver);
    run("CREATE TABLE " + bootstrapDb + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + bootstrapDb + ".unptned", true, driver);
    verifySetup("SELECT * from " + bootstrapDb + ".unptned", unptn_data, driver);
    List<String> metadataOnlyClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname + "'='true'");

    //dump all dbs and create load-marker
    Tuple bootstrapDump = replDumpAllDbs(metadataOnlyClause);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(hconf);
    Path hiveDumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(hiveDumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    fs.create(new Path(hiveDumpPath, LOAD_ACKNOWLEDGEMENT.toString()));
    assertTrue(fs.exists(new Path(hiveDumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    //create new database and dump all databases again
    String incDbName1 = dbName + "_inc1";
    run("CREATE DATABASE " + incDbName1, driver);
    run("CREATE TABLE " + incDbName1 + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + incDbName1 + ".unptned", true, driver);
    verifySetup("SELECT * from " + incDbName1 + ".unptned", unptn_data, driver);
    Tuple incrementalDump = replDumpAllDbs(metadataOnlyClause);

    //create load-marker
    hiveDumpPath = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(hiveDumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    fs.create(new Path(hiveDumpPath, LOAD_ACKNOWLEDGEMENT.toString()));
    assertTrue(fs.exists(new Path(hiveDumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    //create new database and dump all databases again
    String incDbName2 = dbName + "_inc2";
    run("CREATE DATABASE " + incDbName2, driver);
    run("CREATE TABLE " + incDbName2 + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + incDbName2 + ".unptned", true, driver);
    verifySetup("SELECT * from " + incDbName2 + ".unptned", unptn_data, driver);
    replDumpAllDbs(metadataOnlyClause);
  }

  @Test
  public void testIncrementalLogs() throws IOException {
    verifySetupSteps = true;
    String name = testName.getMethodName();
    org.apache.logging.log4j.Logger logger = LogManager.getLogger("hive.ql.metadata.HIVE");
    StringAppender appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(logger.getName(), Level.INFO);
    appender.start();

    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

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


    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", true, driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", true, driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", true, driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);

    run("CREATE TABLE " + dbName + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=1)", true, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1",ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned_late PARTITION(b=2)", true, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptn_data_2, driver);

    // Perform REPL-DUMP/LOAD
    // Set approx load tasks to a low value to trigger REPL_LOAD execution multiple times
    List<String> replApproxTasksClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS.varname + "'='1'");
    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName, replApproxTasksClause);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(hconf);
    Path dumpPath = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
    verifyIncrementalLogs(appender);

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=2", ptn_data_2, driverMirror);
    appender.removeFromLogger(logger.getName());
    verifySetupSteps = false;
  }

  @Test
  public void testIncrementalLoadWithVariableLengthEventId() throws IOException, TException {
    String testName = "incrementalLoadWithVariableLengthEventId";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('ten')", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    String replDumpId = bootstrapDump.lastReplId;

    // CREATE_TABLE - TRUNCATE - INSERT - The result is just one record.
    // Creating dummy table to control the event ID of TRUNCATE not to be 10 or 100 or 1000...
    String[] unptn_data = new String[]{ "eleven" };
    run("CREATE TABLE " + dbName + ".dummy(a string) STORED AS TEXTFILE", driver);
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);

    Tuple incrementalDump = replDumpDb(dbName);
    String incrementalDumpLocn = incrementalDump.dumpLocation;

    // Rename the event directories such a way that the length varies.
    // We will encounter create_table, truncate followed by insert.
    // For the insert, set the event ID longer such that old comparator picks insert before truncate
    // Eg: Event IDs CREATE_TABLE - 5, TRUNCATE - 9, INSERT - 12 changed to
    // CREATE_TABLE - 5, TRUNCATE - 9, INSERT - 100
    // But if TRUNCATE have ID-10, then having INSERT-100 won't be sufficient to test the scenario.
    // So, we set any event comes after CREATE_TABLE starts with 20.
    // Eg: Event IDs CREATE_TABLE - 5, TRUNCATE - 10, INSERT - 12 changed to
    // CREATE_TABLE - 5, TRUNCATE - 20(20 <= Id < 100), INSERT - 100
    Path dumpPath = new Path(incrementalDumpLocn);
    FileSystem fs = dumpPath.getFileSystem(hconf);
    FileStatus[] dirsInLoadPath = fs.listStatus(dumpPath, EximUtil.getDirectoryFilter(fs));
    Arrays.sort(dirsInLoadPath, new EventDumpDirComparator());
    long nextEventId = 0;
    for (FileStatus dir : dirsInLoadPath) {
      Path srcPath = dir.getPath();
      if (nextEventId == 0) {
        nextEventId = (long) Math.pow(10.0, (double) srcPath.getName().length()) * 2;
        continue;
      }
      Path destPath = new Path(srcPath.getParent(), String.valueOf(nextEventId));
      fs.rename(srcPath, destPath);
      LOG.info("Renamed eventDir {} to {}", srcPath.getName(), destPath.getName());
      // Once the eventId reaches 5-20-100, then just increment it sequentially. This is to avoid longer values.
      if (String.valueOf(nextEventId).length() - srcPath.getName().length() >= 2) {
        nextEventId++;
      } else {
        nextEventId = (long) Math.pow(10.0, (double) String.valueOf(nextEventId).length());
      }
    }

    // Load from modified dump event directories.
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data, driverMirror);
  }

  @Test
  public void testIncrementalReplWithEventsMissing() throws IOException, TException {
    String testName = "incrementalReplWithEventsMissing";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    String replDumpId = bootstrapDump.lastReplId;

    // CREATE_TABLE - INSERT - TRUNCATE - INSERT - The result is just one record.
    String[] unptn_data = new String[]{ "eleven" };
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('ten')", driver);
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);

    // Inject a behaviour where some events missing from notification_log table.
    // This ensures the incremental dump doesn't get all events for replication.
    BehaviourInjection<NotificationEventResponse,NotificationEventResponse> eventIdSkipper
            = new BehaviourInjection<NotificationEventResponse,NotificationEventResponse>(){

      @Nullable
      @Override
      public NotificationEventResponse apply(@Nullable NotificationEventResponse eventIdList) {
        if (null != eventIdList) {
          List<NotificationEvent> eventIds = eventIdList.getEvents();
          List<NotificationEvent> outEventIds = new ArrayList<NotificationEvent>();
          for (int i = 0; i < eventIds.size(); i++) {
            NotificationEvent event = eventIds.get(i);

            // Skip all the INSERT events
            if (event.getDbName().equalsIgnoreCase(dbName) && event.getEventType().equalsIgnoreCase("INSERT")) {
              injectionPathCalled = true;
              continue;
            }
            outEventIds.add(event);
          }

          // Return the new list
          return new NotificationEventResponse(outEventIds);
        } else {
          return null;
        }
      }
    };
    InjectableBehaviourObjectStore.setGetNextNotificationBehaviour(eventIdSkipper);
    try {
      advanceDumpDir();
      try {
        driver.run("REPL DUMP " + dbName);
        assert false;
      } catch (CommandProcessorException e) {
        assertTrue(e.getCauseMessage() == ErrorMsg.REPL_EVENTS_MISSING_IN_METASTORE.getMsg());
      }
      eventIdSkipper.assertInjectionsPerformed(true,false);
    } finally {
      InjectableBehaviourObjectStore.resetGetNextNotificationBehaviour(); // reset the behaviour
    }
  }

  @Test
  public void testDrops() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
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

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b='1'", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b='2'", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned2 WHERE b='1'", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned2 WHERE b='2'", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned3 WHERE b=1", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned3 WHERE b=2", ptn_data_2, driverMirror);

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
    incrementalLoadAndVerify(dbName, replDbName);

    // verify that drops were replicated. This can either be from tables or ptns
    // not existing, and thus, throwing a NoSuchObjectException, or returning nulls
    // or select * returning empty, depending on what we're testing.

    verifyIfTableNotExist(replDbName, "unptned", metaStoreClientMirror);

    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b='2'", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned3 WHERE b=1", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned3", ptn_data_2, driverMirror);

    verifyIfTableNotExist(replDbName, "ptned2", metaStoreClientMirror);
  }

  @Test
  public void testDropsWithCM() throws IOException {
    String testName = "drops_with_cm";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
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
    verifySetup("SELECT * from " + dbName + ".unptned",unptn_data,  driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='1')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='1'", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b='2')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b='2'", ptn_data_2, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='1')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='1'", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b='2')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned2 WHERE b='2'", ptn_data_2, driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    String replDumpId = bootstrapDump.lastReplId;

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b='1'", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b='2'", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned2 WHERE b='1'", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned2 WHERE b='2'", ptn_data_2, driverMirror);

    run("CREATE TABLE " + dbName + ".unptned_copy" + " AS SELECT a FROM " + dbName + ".unptned", driver);
    run("CREATE TABLE " + dbName + ".ptned_copy" + " LIKE " + dbName + ".ptned", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_copy" + " PARTITION(b='1') SELECT a FROM " +
        dbName + ".ptned WHERE b='1'", driver);
    verifySetup("SELECT a from " + dbName + ".unptned_copy", unptn_data, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_copy", ptn_data_1, driver);

    run("DROP TABLE " + dbName + ".unptned", driver);
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b='2')", driver);
    run("DROP TABLE " + dbName + ".ptned2", driver);

    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String postDropReplDumpLocn = getResult(0,0,driver);
    String postDropReplDumpId = getResult(0,1,true,driver);
    LOG.info("Dumped to {} with id {}->{}", postDropReplDumpLocn, replDumpId, postDropReplDumpId);

    // Drop table after dump
    run("DROP TABLE " + dbName + ".unptned_copy", driver);
    // Drop partition after dump
    run("ALTER TABLE " + dbName + ".ptned_copy DROP PARTITION(b='1')", driver);

    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);

    Exception e = null;
    try {
      Table tbl = metaStoreClientMirror.getTable(replDbName, "unptned");
      assertNull(tbl);
    } catch (TException te) {
      e = te;
    }
    assertNotNull(e);
    assertEquals(NoSuchObjectException.class, e.getClass());

    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned", ptn_data_1, driverMirror);

    verifyIfTableNotExist(replDbName, "ptned2", metaStoreClientMirror);

    verifyRun("SELECT a from " + replDbName + ".unptned_copy", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_copy", ptn_data_1, driverMirror);
  }

  @Test
  public void testTableAlters() throws IOException {
    String testName = "TableAlters";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
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
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned2", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b='1'", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b='2'", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned2 WHERE b='1'", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned2 WHERE b='2'", ptn_data_2, driverMirror);

    // tables have been replicated over, and verified to be identical. Now, we do a couple of
    // alters on the source

    // Rename unpartitioned table
    run("ALTER TABLE " + dbName + ".unptned RENAME TO " + dbName + ".unptned_rn", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_rn", unptn_data, driver);

    // Alter unpartitioned table set table property
    String testKey = "blah";
    String testVal = "foo";
    run("ALTER TABLE " + dbName + ".unptned2 SET TBLPROPERTIES ('" + testKey + "' = '" + testVal + "')", driver);
    if (verifySetupSteps){
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
    if (verifySetupSteps){
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
    incrementalLoadAndVerify(dbName, replDbName);

    // Replication done, we now do the following verifications:

    // verify that unpartitioned table rename succeeded.
    verifyIfTableNotExist(replDbName, "unptned", metaStoreClientMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned_rn", unptn_data, driverMirror);

    // verify that partition rename succeded.
    try {
      Table unptn2 = metaStoreClientMirror.getTable(replDbName, "unptned2");
      assertTrue(unptn2.getParameters().containsKey(testKey));
      assertEquals(testVal,unptn2.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=22", ptn_data_2, driverMirror);

    // verify that ptned table rename succeded.
    verifyIfTableNotExist(replDbName, "ptned2", metaStoreClientMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned2_rn WHERE b=2", ptn_data_2, driverMirror);

    // verify that ptned table property set worked
    try {
      Table ptned = metaStoreClientMirror.getTable(replDbName, "ptned");
      assertTrue(ptned.getParameters().containsKey(testKey));
      assertEquals(testVal, ptned.getParameters().get(testKey));
    } catch (TException te) {
      assertNull(te);
    }

    // verify that partitioned table partition property set worked.
    try {
      List<String> ptnVals1 = new ArrayList<String>();
      ptnVals1.add("1");
      Partition ptn1 = metaStoreClientMirror.getPartition(replDbName, "ptned", ptnVals1);
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
    Tuple incremental = incrementalLoadAndVerify(dbName, replDbName);

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

    incremental = incrementalLoadAndVerify(dbName, replDbName);

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
  public void testBootstrapWithDataInDumpDir() throws IOException {
    String nameOfTest = "testBootstrapWithDataInDumpDir";
    String dbName = createDB(nameOfTest, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    String[] unptnData1 = new String[] {"eleven", "twelve"};
    String[] unptnData2 = new String[] {"thirteen", "fourteen", "fifteen"};
    String[] unptnAllData = new String[] {"eleven", "twelve", "thirteen", "fourteen", "fifteen"};
    String[] ptnData1 = new String[] {"one", "two", "three"};
    String[] ptnData2 = new String[] {"four", "five"};
    String[] empty = new String[] {};
    String unptnedFileName1 = nameOfTest + "_unptn_1";
    String unptnedFileName2 = nameOfTest + "_unptn_2";
    String ptnedFileName1 = nameOfTest + "_ptn_1";
    String ptnedFileName2 = nameOfTest + "_ptn_2";

    String unptnLocn1= new Path(TEST_PATH, unptnedFileName1).toUri().getPath();
    String unptnLocn2 = new Path(TEST_PATH, unptnedFileName2).toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, ptnedFileName1).toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, ptnedFileName2).toUri().getPath();
    createTestDataFile(unptnLocn1, unptnData1);
    createTestDataFile(unptnLocn2, unptnData2);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);
    verifySetup("SELECT * from " + dbName + ".unptned", empty, driverMirror);
    verifySetup("SELECT * from " + dbName + ".ptned", empty, driverMirror);
    run("LOAD DATA LOCAL INPATH '" + unptnLocn1 + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    run("LOAD DATA LOCAL INPATH '" + unptnLocn2 + "' INTO TABLE " + dbName + ".unptned", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' INTO TABLE " + dbName
            + ".ptned PARTITION(b=2)", driver);
    Tuple dump = replDumpDb(dbName);
    Path path = new Path(System.getProperty("test.warehouse.dir", ""));
    String tableRelativeSrcPath = dbName.toLowerCase()+".db" + File.separator + "unptned";
    Path srcFileLocation = new Path(path, tableRelativeSrcPath + File.separator + unptnedFileName1);
    String tgtFileRelativePath = ReplUtils.REPL_HIVE_BASE_DIR + File.separator + EximUtil.DATA_PATH_NAME
            + File.separator + dbName.toLowerCase() + File.separator + "unptned" +File.separator + unptnedFileName1;
    Path tgtFileLocation = new Path(dump.dumpLocation, tgtFileRelativePath);
    //A file in table at src location should be copied to $dumplocation/hive/<db>/<table>/data/<unptned_fileName>
    verifyChecksum(srcFileLocation, tgtFileLocation, true);
    srcFileLocation = new Path(path, tableRelativeSrcPath + File.separator + unptnedFileName2);
    verifyChecksum(srcFileLocation, tgtFileLocation, false);

    String partitionRelativeSrcPath = dbName.toLowerCase()+".db" + File.separator + "ptned" + File.separator + "b=1";
    srcFileLocation = new Path(path, partitionRelativeSrcPath + File.separator + ptnedFileName1);
    tgtFileRelativePath = ReplUtils.REPL_HIVE_BASE_DIR + File.separator + EximUtil.DATA_PATH_NAME
            + File.separator + dbName.toLowerCase()
            + File.separator + "ptned" + File.separator + "b=1" + File.separator
            + ptnedFileName1;
    tgtFileLocation = new Path(dump.dumpLocation, tgtFileRelativePath);
    //A partitioned file in table at src location should be copied to
    // $dumplocation/hive/<db>/<table>/<partition>/data/<unptned_fileName>
    verifyChecksum(srcFileLocation, tgtFileLocation, true);
    partitionRelativeSrcPath = dbName.toLowerCase()+".db" + File.separator + "ptned" + File.separator + "b=2";
    srcFileLocation = new Path(path, partitionRelativeSrcPath + File.separator + ptnedFileName2);
    loadAndVerify(replDbName, dbName, dump.lastReplId);
    verifyChecksum(srcFileLocation, tgtFileLocation, false);
    verifySetup("SELECT * from " + replDbName + ".unptned", unptnAllData, driver);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptnData2, driverMirror);
  }

  @Test
  public void testIncrementalLoad() throws IOException {
    String testName = "incrementalLoad";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName
        + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptnData = new String[] {"eleven", "twelve"};
    String[] ptnData1 = new String[] {"thirteen", "fourteen", "fifteen"};
    String[] ptnData2 = new String[] {"fifteen", "sixteen", "seventeen"};
    String[] empty = new String[] {};

    String unptnLocn = new Path(TEST_PATH, testName + "_unptn").toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, testName + "_ptn1").toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptnLocn, unptnData);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);

    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty, driverMirror);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty, driverMirror);

    run("LOAD DATA LOCAL INPATH '" + unptnLocn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptnData, driver);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptnData, driver);

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    //Verify dump data structure
    Path hiveDumpDir = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), hconf);
    verifyDataFileExist(fs, hiveDumpDir, null, new Path(unptnLocn).getName());
    verifyDataListFileDoesNotExist(fs, hiveDumpDir, null);


    verifyRun("SELECT * from " + replDbName + ".unptned_late", unptnData, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptnData1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptnData2, driver);

    run("CREATE TABLE " + dbName
            + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
            + ".ptned WHERE b=1", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1", ptnData1, driver);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
            + ".ptned WHERE b=2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptnData2, driver);

    incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    hiveDumpDir = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    verifyDataFileExist(fs, hiveDumpDir, "b=1", new Path(ptnLocn1).getName());
    verifyDataFileExist(fs, hiveDumpDir, "b=2", new Path(ptnLocn2).getName());
    verifyDataListFileDoesNotExist(fs, hiveDumpDir, "b=1");
    verifyDataListFileDoesNotExist(fs, hiveDumpDir, "b=2");

    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=2", ptnData2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptnData2, driverMirror);
  }

  @Test
  public void testIncrementalLoadLazyCopy() throws IOException {
    String testName = "testIncrementalLoadLazyCopy";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName
            + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    List<String> lazyCopyClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName, lazyCopyClause);

    String[] unptnData = new String[] {"eleven", "twelve"};
    String[] ptnData1 = new String[] {"thirteen", "fourteen", "fifteen"};
    String[] ptnData2 = new String[] {"fifteen", "sixteen", "seventeen"};
    String[] empty = new String[] {};

    String unptnLocn = new Path(TEST_PATH, testName + "_unptn").toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, testName + "_ptn1").toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptnLocn, unptnData);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);

    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty, driverMirror);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty, driverMirror);

    run("LOAD DATA LOCAL INPATH '" + unptnLocn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptnData, driver);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptnData, driver);

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName, lazyCopyClause);

    Path hiveDumpDir = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), hconf);
    verifyRun("SELECT * from " + replDbName + ".unptned_late", unptnData, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptnData1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptnData2, driver);

    run("CREATE TABLE " + dbName
            + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
            + ".ptned WHERE b=1", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1", ptnData1, driver);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
            + ".ptned WHERE b=2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptnData2, driver);

    incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    hiveDumpDir = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);

    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=2", ptnData2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptnData2, driverMirror);
  }

  @Test
  public void testIncrementalInserts() throws IOException {
    String testName = "incrementalInserts";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptn_data = new String[] { "eleven", "twelve" };

    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late ORDER BY a", unptn_data, driver);

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_late ORDER BY a", unptn_data, driverMirror);

    String[] unptn_data_after_ins = new String[] { "eleven", "thirteen", "twelve" };
    String[] data_after_ovwrite = new String[] { "hundred" };
    run("INSERT INTO TABLE " + dbName + ".unptned_late values('" + unptn_data_after_ins[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned_late ORDER BY a", unptn_data_after_ins, driver);
    run("INSERT OVERWRITE TABLE " + dbName + ".unptned values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned", data_after_ovwrite, driver);

    incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".unptned_late ORDER BY a", unptn_data_after_ins, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned", data_after_ovwrite, driverMirror);
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
    try {
      incrementalLoadAndVerify(dbName, replDbName);
      eventTypeValidator.assertInjectionsPerformed(true,false);
    } finally {
      InjectableBehaviourObjectStore.resetGetNextNotificationBehaviour(); // reset the behaviour
    }

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
    try {
      incrementalLoadAndVerify(dbName, replDbName);
      insertEventRepeater.assertInjectionsPerformed(true,false);
    } finally {
      InjectableBehaviourObjectStore.resetGetNextNotificationBehaviour(); // reset the behaviour
    }

    verifyRun("SELECT a from " + replDbName + ".unptned", unptn_data[0], driverMirror);
  }

  @Test
  public void testIncrementalLoadWithOneFailedDump() throws IOException {
    String nameOfTest = "testIncrementalLoadWithOneFailedDump";
    String dbName = createDB(nameOfTest, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName
            + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptnData = new String[] {"eleven", "twelve"};
    String[] ptnData1 = new String[] {"thirteen", "fourteen", "fifteen"};
    String[] ptnData2 = new String[] {"fifteen", "sixteen", "seventeen"};
    String[] empty = new String[] {};

    String unptnLocn = new Path(TEST_PATH, nameOfTest + "_unptn").toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, nameOfTest + "_ptn1").toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, nameOfTest + "_ptn2").toUri().getPath();

    createTestDataFile(unptnLocn, unptnData);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);

    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty, driverMirror);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty, driverMirror);

    run("LOAD DATA LOCAL INPATH '" + unptnLocn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptnData, driver);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptnData, driver);

    Tuple incrementalDump = replDumpDb(dbName);

    //Remove the dump ack file, so that dump is treated as an invalid dump.
    String ackFileRelativePath = ReplUtils.REPL_HIVE_BASE_DIR + File.separator
            + DUMP_ACKNOWLEDGEMENT.toString();
    Path dumpFinishedAckFilePath = new Path(incrementalDump.dumpLocation, ackFileRelativePath);
    Path tmpDumpFinishedAckFilePath = new Path(dumpFinishedAckFilePath.getParent(),
            "old_" + dumpFinishedAckFilePath.getName());
    FileSystem fs  = FileSystem.get(new Path(incrementalDump.dumpLocation).toUri(), hconf);
    fs.rename(dumpFinishedAckFilePath, tmpDumpFinishedAckFilePath);
    loadAndVerify(replDbName, dbName, bootstrapDump.lastReplId);

    fs.rename(tmpDumpFinishedAckFilePath, dumpFinishedAckFilePath);
    //Repl Load should recover when it finds valid load
    loadAndVerify(replDbName, dbName, incrementalDump.lastReplId);

    verifyRun("SELECT * from " + replDbName + ".unptned_late", unptnData, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptnData1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptnData2, driver);

    run("CREATE TABLE " + dbName
            + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
            + ".ptned WHERE b=1", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1", ptnData1, driver);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
            + ".ptned WHERE b=2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptnData2, driver);

    incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=2", ptnData2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptnData2, driverMirror);
  }

  @Test
  public void testIncrementalLoadWithPreviousDumpDeleteFailed() throws IOException {
    String nameOfTest = "testIncrementalLoadWithPreviousDumpDeleteFailed";
    String dbName = createDB(nameOfTest, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".unptned_empty(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName
            + ".ptned_empty(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptnData = new String[] {"eleven", "twelve"};
    String[] ptnData1 = new String[] {"thirteen", "fourteen", "fifteen"};
    String[] ptnData2 = new String[] {"fifteen", "sixteen", "seventeen"};
    String[] empty = new String[] {};

    String unptnLocn = new Path(TEST_PATH, nameOfTest + "_unptn").toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, nameOfTest + "_ptn1").toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, nameOfTest + "_ptn2").toUri().getPath();

    createTestDataFile(unptnLocn, unptnData);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);

    verifySetup("SELECT a from " + dbName + ".ptned_empty", empty, driverMirror);
    verifySetup("SELECT * from " + dbName + ".unptned_empty", empty, driverMirror);

    run("LOAD DATA LOCAL INPATH '" + unptnLocn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptnData, driver);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptnData, driver);

    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    Tuple incrDump = replDumpDb(dbName);

    // Delete some file except ack.
    Path bootstrapDumpDir = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    String tablePath = dbName + File.separator + "unptned";
    Path fileToDelete = new Path(bootstrapDumpDir, tablePath);
    FileSystem fs = FileSystem.get(fileToDelete.toUri(), hconf);
    fs.delete(fileToDelete, true);
    assertTrue(fs.exists(bootstrapDumpDir));
    assertTrue(fs.exists(new Path(bootstrapDumpDir, DUMP_ACKNOWLEDGEMENT.toString())));

    loadAndVerify(replDbName, dbName, incrDump.lastReplId);

    verifyRun("SELECT * from " + replDbName + ".unptned_late", unptnData, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptnData1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptnData2, driver);

    run("CREATE TABLE " + dbName
            + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
            + ".ptned WHERE b=1", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1", ptnData1, driver);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
            + ".ptned WHERE b=2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptnData2, driver);

    ReplDumpWork.testDeletePreviousDumpMetaPath(false);

    Path incrHiveDumpDir = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    incrDump = replDumpDb(dbName);
    //This time delete previous dump dir should work fine.
    assertFalse(FileSystem.get(fileToDelete.toUri(), hconf).exists(incrHiveDumpDir));
    assertFalse(fs.exists(bootstrapDumpDir));
    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=2", ptnData2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptnData2, driverMirror);
  }

  @Test
  public void testConfiguredDeleteOfPrevDumpDir() throws IOException {
    boolean verifySetupOriginal = verifySetupSteps;
    verifySetupSteps = true;
    String nameOfTest = testName.getMethodName();
    String dbName = createDB(nameOfTest, driver);
    String replDbName = dbName + "_dupe";
    List<String> withConfigDeletePrevDump = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'= 'false' ");
    List<String> withConfigRetainPrevDump = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'= 'true' ",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT + "'= '2' ");

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptnData = new String[] {"eleven", "twelve"};
    String[] ptnData1 = new String[] {"thirteen", "fourteen", "fifteen"};
    String[] ptnData2 = new String[] {"fifteen", "sixteen", "seventeen"};
    String[] empty = new String[] {};

    String unptnLocn = new Path(TEST_PATH, nameOfTest + "_unptn").toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, nameOfTest + "_ptn1").toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, nameOfTest + "_ptn2").toUri().getPath();

    createTestDataFile(unptnLocn, unptnData);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);

    run("LOAD DATA LOCAL INPATH '" + unptnLocn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptnData, driver);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptnData, driver);

    //perform first incremental with default option and check that bootstrap-dump-dir gets deleted
    Path bootstrapDumpDir = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    FileSystem fs = FileSystem.get(bootstrapDumpDir.toUri(), hconf);
    assertTrue(fs.exists(bootstrapDumpDir));
    Tuple incrDump = replDumpDb(dbName);
    assertFalse(fs.exists(bootstrapDumpDir));


    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT * from " + replDbName + ".unptned", unptnData, driverMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned_late", unptnData, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptnData1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptnData2, driver);


    //Perform 2nd incremental with retain option.
    //Check 1st incremental dump-dir is present even after 2nd incr dump.
    Path incrDumpDir1 = new Path(incrDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    incrDump = replDumpDb(dbName, withConfigRetainPrevDump);
    assertTrue(fs.exists(incrDumpDir1));

    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptnData2, driverMirror);

    run("CREATE TABLE " + dbName
            + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
            + ".ptned WHERE b=1", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1", ptnData1, driver);


    //Perform 3rd incremental with retain option, retaining last 2 consumed dump-dirs.
    //Verify 1st and 2nd incr-dump-dirs are present after 3rd incr-dump
    Path incrDumpDir2 = new Path(incrDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    incrDump = replDumpDb(dbName, withConfigRetainPrevDump);
    assertTrue(fs.exists(incrDumpDir1));
    assertTrue(fs.exists(incrDumpDir2));

    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=1", ptnData1, driverMirror);


    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
            + ".ptned WHERE b=2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptnData2, driver);


    //perform 4'th incr-dump with retain option in policy, retaining only last 2 dump-dirs
    //verify incr-1 dumpdir gets deleted, incr-2 and incr-3 remain
    Path incrDumpDir3 = new Path(incrDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    incrDump = replDumpDb(dbName, withConfigRetainPrevDump);
    assertFalse(fs.exists(incrDumpDir1));
    assertTrue(fs.exists(incrDumpDir2));
    assertTrue(fs.exists(incrDumpDir3));

    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=2", ptnData2, driverMirror);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=3) SELECT a FROM " + dbName
            + ".ptned WHERE b=2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=3", ptnData2, driver);


    //ensure 4'th incr-dump dir is present
    //perform 5'th incr-dump with retain option to be false
    //verify all prev dump-dirs get deleted
    Path incrDumpDir4 = new Path(incrDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(incrDumpDir4));
    incrDump = replDumpDb(dbName, withConfigDeletePrevDump);
    assertFalse(fs.exists(incrDumpDir2));
    assertFalse(fs.exists(incrDumpDir3));
    assertFalse(fs.exists(incrDumpDir4));

    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=3", ptnData2, driverMirror);

    verifySetupSteps = verifySetupOriginal;
  }

  @Test
  public void testDumpMetadataBackwardCompatibility() throws IOException, SemanticException {
    boolean verifySetupOriginal = verifySetupSteps;
    verifySetupSteps = true;
    String nameOfTest = testName.getMethodName();
    String dbName = createDB(nameOfTest, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    //ensure bootstrap load runs with earlier format of dumpmetadata
    Tuple bootstrapDump = replDumpDb(dbName);
    deleteNewMetadataFields(bootstrapDump);
    loadAndVerify(replDbName, dbName, bootstrapDump.lastReplId);

    String[] unptnData = new String[] {"eleven", "twelve"};
    String[] ptnData1 = new String[] {"thirteen", "fourteen", "fifteen"};
    String[] ptnData2 = new String[] {"fifteen", "sixteen", "seventeen"};
    String[] empty = new String[] {};

    String unptnLocn = new Path(TEST_PATH, nameOfTest + "_unptn").toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, nameOfTest + "_ptn1").toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, nameOfTest + "_ptn2").toUri().getPath();

    createTestDataFile(unptnLocn, unptnData);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);

    run("LOAD DATA LOCAL INPATH '" + unptnLocn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptnData, driver);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptnData, driver);

    //ensure first incremental load runs with earlier format of dumpmetadata
    Tuple incDump = replDumpDb(dbName);
    deleteNewMetadataFields(incDump);
    loadAndVerify(replDbName, dbName, incDump.lastReplId);
    verifyRun("SELECT * from " + replDbName + ".unptned", unptnData, driverMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned_late", unptnData, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptnData1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptnData2, driver);

    //verify 2nd incremental load
    incDump = replDumpDb(dbName);
    deleteNewMetadataFields(incDump);
    loadAndVerify(replDbName, dbName, incDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", ptnData1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", ptnData2, driverMirror);
    verifySetupSteps = verifySetupOriginal;
  }

  @Test
  public void testReplConfiguredCleanupOfNotificationEvents() throws Exception {

    boolean verifySetupOriginal = verifySetupSteps;
    verifySetupSteps = true;
    final int cleanerTtlSeconds = 1;
    final int cleanerIntervalSeconds = 1;
    String nameOfTest = testName.getMethodName();
    String dbName = createDB(nameOfTest, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    //bootstrap
    bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptnData = new String[] {"eleven", "twelve"};
    String[] ptnData1 = new String[] {"thirteen", "fourteen", "fifteen"};
    String[] ptnData2 = new String[] {"fifteen", "sixteen", "seventeen"};
    String[] empty = new String[] {};

    String unptnLocn = new Path(TEST_PATH, nameOfTest + "_unptn").toUri().getPath();
    String ptnLocn1 = new Path(TEST_PATH, nameOfTest + "_ptn1").toUri().getPath();
    String ptnLocn2 = new Path(TEST_PATH, nameOfTest + "_ptn2").toUri().getPath();

    createTestDataFile(unptnLocn, unptnData);
    createTestDataFile(ptnLocn1, ptnData1);
    createTestDataFile(ptnLocn2, ptnData2);

    run("LOAD DATA LOCAL INPATH '" + unptnLocn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptnData, driver);
    run("CREATE TABLE " + dbName + ".unptned_late LIKE " + dbName + ".unptned", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned_late SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late", unptnData, driver);

    // CM was enabled during setup, REPL_EVENT_DB_LISTENER_TTL should be used, set the other one to a low value
    MetastoreConf.setTimeVar(hconf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, cleanerTtlSeconds, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(hconf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, cleanerIntervalSeconds, TimeUnit.SECONDS);
    DbNotificationListener.resetCleaner(hconf);

    //sleep to ensure correct conf(REPL_EVENT_DB_LISTENER_TTL) is used
    try {
      Thread.sleep(cleanerIntervalSeconds * 1000 * 10);
    } catch (InterruptedException e) {
      LOG.warn("Sleep unsuccesful", e);
    }

    //verify events get replicated
    Tuple incrDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT * from " + replDbName + ".unptned", unptnData, driverMirror);
    verifyRun("SELECT * from " + replDbName + ".unptned_late", unptnData, driverMirror);


    // For next run, CM is enabled, set REPL_EVENT_DB_LISTENER_TTL to low value for events to get deleted
    MetastoreConf.setTimeVar(hconf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, cleanerTtlSeconds * 60 * 60, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(hconf, REPL_EVENT_DB_LISTENER_TTL, cleanerTtlSeconds , TimeUnit.SECONDS);
    DbNotificationListener.resetCleaner(hconf);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn1 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptnData1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptnLocn2 + "' OVERWRITE INTO TABLE " + dbName
            + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptnData2, driver);

    try {
      Thread.sleep(cleanerIntervalSeconds * 1000 * 10);
    } catch (InterruptedException e) {
      LOG.warn("Sleep unsuccesful", e);
    }

    incrDump = replDumpDb(dbName);

    // expected empty data because REPL_EVENT_DB_LISTENER_TTL should have been exceeded before dump
    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=1", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned WHERE b=2", empty, driverMirror);

    // With CM disabled, EVENT_DB_LISTENER_TTL should be used.
    // First check with high ttl
    MetastoreConf.setBoolVar(hconf, MetastoreConf.ConfVars.REPLCMENABLED, false);
    MetastoreConf.setTimeVar(hconf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, cleanerTtlSeconds  * 60 * 60, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(hconf, REPL_EVENT_DB_LISTENER_TTL, cleanerTtlSeconds, TimeUnit.SECONDS);
    DbNotificationListener.resetCleaner(hconf);

    run("CREATE TABLE " + dbName
            + ".ptned_late(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=1) SELECT a FROM " + dbName
            + ".ptned WHERE b=1", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=1", ptnData1, driver);


    //sleep to ensure correct conf(EVENT_DB_LISTENER_TTL) is used
    try {
      Thread.sleep(cleanerIntervalSeconds * 1000 * 10);
    } catch (InterruptedException e) {
      LOG.warn("Sleep unsuccesful", e);
    }

    //check replication success
    incrDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=1", ptnData1, driverMirror);


    //With CM disabled, set a low ttl for events to get deleted
    MetastoreConf.setBoolVar(hconf, MetastoreConf.ConfVars.REPLCMENABLED, false);
    MetastoreConf.setTimeVar(hconf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, cleanerTtlSeconds, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(hconf, REPL_EVENT_DB_LISTENER_TTL, cleanerTtlSeconds   * 60 * 60, TimeUnit.SECONDS);
    DbNotificationListener.resetCleaner(hconf);

    run("INSERT INTO TABLE " + dbName + ".ptned_late PARTITION(b=2) SELECT a FROM " + dbName
            + ".ptned WHERE b=2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_late WHERE b=2", ptnData2, driver);


    try {
      Thread.sleep(cleanerIntervalSeconds * 1000 * 10);
    } catch (InterruptedException e) {
      LOG.warn("Sleep unsuccesful", e);
    }

    //events should be deleted before dump
    incrDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, incrDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned_late WHERE b=2", empty, driverMirror);


    //restore original values
    MetastoreConf.setBoolVar(hconf, MetastoreConf.ConfVars.REPLCMENABLED, true);
    MetastoreConf.setTimeVar(hconf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_TTL, 86400, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(hconf, REPL_EVENT_DB_LISTENER_TTL, 864000, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(hconf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, 7200, TimeUnit.SECONDS);
    DbNotificationListener.resetCleaner(hconf);
    verifySetupSteps = verifySetupOriginal;
  }

  @Test
  public void testCleanerThreadStartupWait() throws Exception {
    int eventsTtl = 20;
    HiveConf newConf = new HiveConf(hconf);

    // Set TTL short enough for testing.
    MetastoreConf.setTimeVar(newConf, REPL_EVENT_DB_LISTENER_TTL, eventsTtl, TimeUnit.SECONDS);

    // Set startup wait interval.
    MetastoreConf.setTimeVar(newConf, EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL, eventsTtl * 5, TimeUnit.SECONDS);

    // Set cleaner wait interval.
    MetastoreConf
        .setTimeVar(newConf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, 10, TimeUnit.MILLISECONDS);
    newConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL, true);
    // Reset Cleaner to have a initial wait time.
    DbNotificationListener.resetCleaner(newConf);

    IMetaStoreClient msClient = metaStoreClient;
    run("create database referenceDb", driver);

    long firstEventId = msClient.getCurrentNotificationEventId().getEventId();;

    run("create database cleanupStartup", driver);
    run("drop database cleanupStartup", driver);

    LOG.info("Done with creating events.");

    // Check events are pushed into notification logs.
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    // Reset Cleaner to have a initial wait time.
    DbNotificationListener.resetCleaner(newConf);

    // Sleep for eventsTtl time and see if events are there.
    Thread.sleep(eventsTtl * 1000);

    // Check events are there in notification logs.
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    // Sleep for some more time and see if events are there.
    Thread.sleep(eventsTtl * 1000);

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    // Sleep more than the initial wait time and see if Events get cleaned up post that
    Thread.sleep(eventsTtl * 4000);

    // Events should have cleaned up.
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(0, rsp.getEventsSize());

    // Reset with original configuration.
    DbNotificationListener.resetCleaner(hconf);
    run("drop database referenceDb", driver);
  }

  @Test
  public void testIncrementalInsertToPartition() throws IOException {
    String testName = "incrementalInsertToPartition";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

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

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    String[] data_after_ovwrite = new String[] { "hundred" };
    // Insert overwrite on existing partition
    run("INSERT OVERWRITE TABLE " + dbName + ".ptned partition(b=2) values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=2)", data_after_ovwrite, driver);
    // Insert overwrite on dynamic partition
    run("INSERT OVERWRITE TABLE " + dbName + ".ptned partition(b=3) values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=3)", data_after_ovwrite, driver);

    incrementalLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2)", data_after_ovwrite, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=3)", data_after_ovwrite, driverMirror);
  }

  @Test
  public void testInsertToMultiKeyPartition() throws IOException {
    String testName = "insertToMultiKeyPartition";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

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

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT name from " + replDbName + ".namelist where (year=1980) ORDER BY name", ptn_year_1980, driverMirror);
    verifyRun("SELECT name from " + replDbName + ".namelist where (day=1) ORDER BY name", ptn_day_1, driverMirror);
    verifyRun("SELECT name from " + replDbName + ".namelist where (year=1984 and month=4 and day=1) ORDER BY name",
                                                                                   ptn_year_1984_month_4_day_1_1, driverMirror);
    verifyRun("SELECT name from " + replDbName + ".namelist ORDER BY name", ptn_data_1, driverMirror);
    verifyRun("SHOW PARTITIONS " + replDbName + ".namelist", ptn_list_1, driverMirror);

    run("USE " + replDbName, driverMirror);
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

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT name from " + replDbName + ".namelist where (year=1980) ORDER BY name", ptn_year_1980, driverMirror);
    verifyRun("SELECT name from " + replDbName + ".namelist where (day=1) ORDER BY name", ptn_day_1_2, driverMirror);
    verifyRun("SELECT name from " + replDbName + ".namelist where (year=1984 and month=4 and day=1) ORDER BY name",
                                                                                   ptn_year_1984_month_4_day_1_2, driverMirror);
    verifyRun("SELECT name from " + replDbName + ".namelist ORDER BY name", ptn_data_2, driverMirror);
    verifyRun("SHOW PARTITIONS " + replDbName + ".namelist", ptn_list_2, driverMirror);
    run("USE " + replDbName, driverMirror);
    verifyRunWithPatternMatch("SHOW TABLE EXTENDED LIKE namelist PARTITION (year=1990,month=5,day=25)",
            "location", "namelist/year=1990/month=5/day=25", driverMirror);
    run("USE " + dbName, driver);

    String[] ptn_data_3 = new String[] { "abraham", "bob", "carter", "david", "fisher" };
    String[] data_after_ovwrite = new String[] { "fisher" };
    // Insert overwrite on existing partition
    run("INSERT OVERWRITE TABLE " + dbName + ".namelist partition(year=1990,month=5,day=25) values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT name from " + dbName + ".namelist where (year=1990 and month=5 and day=25)", data_after_ovwrite, driver);
    verifySetup("SELECT name from " + dbName + ".namelist ORDER BY name", ptn_data_3, driver);

    incrementalLoadAndVerify(dbName, replDbName);
    verifySetup("SELECT name from " + replDbName + ".namelist where (year=1990 and month=5 and day=25)", data_after_ovwrite, driverMirror);
    verifySetup("SELECT name from " + replDbName + ".namelist ORDER BY name", ptn_data_3, driverMirror);
  }

  @Test
  public void testIncrementalInsertDropUnpartitionedTable() throws IOException {
    String testName = "incrementalInsertDropUnpartitionedTable";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptn_data = new String[] { "eleven", "twelve" };

    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    run("CREATE TABLE " + dbName + ".unptned_tmp AS SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT a from " + dbName + ".unptned_tmp ORDER BY a", unptn_data, driver);

    // Get the last repl ID corresponding to all insert/alter/create events except DROP.
    Tuple incrementalDump = replDumpDb(dbName);


    // Drop all the tables
    run("DROP TABLE " + dbName + ".unptned", driver);
    run("DROP TABLE " + dbName + ".unptned_tmp", driver);
    verifyFail("SELECT * FROM " + dbName + ".unptned", driver);
    verifyFail("SELECT * FROM " + dbName + ".unptned_tmp", driver);

    // Dump all the events except DROP
    loadAndVerify(replDbName, dbName, incrementalDump.lastReplId);

    // Need to find the tables and data as drop is not part of this dump
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_tmp ORDER BY a", unptn_data, driverMirror);

    // Dump the drop events and check if tables are getting dropped in target as well
    incrementalLoadAndVerify(dbName, replDbName);
    verifyFail("SELECT * FROM " + replDbName + ".unptned", driverMirror);
    verifyFail("SELECT * FROM " + replDbName + ".unptned_tmp", driverMirror);
  }

  @Test
  public void testIncrementalInsertDropPartitionedTable() throws IOException {
    String testName = "incrementalInsertDropPartitionedTable";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

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
    Tuple incrementalDump = replDumpDb(dbName);

    // Drop all the tables
    run("DROP TABLE " + dbName + ".ptned_tmp", driver);
    run("DROP TABLE " + dbName + ".ptned", driver);
    verifyFail("SELECT * FROM " + dbName + ".ptned_tmp", driver);
    verifyFail("SELECT * FROM " + dbName + ".ptned", driver);

    // Replicate all the events except DROP
    loadAndVerify(replDbName, dbName, incrementalDump.lastReplId);

    // Need to find the tables and data as drop is not part of this dump
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_tmp where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_tmp where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    // Replicate the drop events and check if tables are getting dropped in target as well
    incrementalLoadAndVerify(dbName, replDbName);
    verifyFail("SELECT * FROM " + replDbName + ".ptned_tmp", driverMirror);
    verifyFail("SELECT * FROM " + replDbName + ".ptned", driverMirror);
  }

  @Test
  public void testInsertOverwriteOnUnpartitionedTableWithCM() throws IOException {
    String testName = "insertOverwriteOnUnpartitionedTableWithCM";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    // After INSERT INTO operation, get the last Repl ID
    String[] unptn_data = new String[] { "thirteen" };
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    Tuple incrementalDump = replDumpDb(dbName);

    // Insert overwrite on unpartitioned table
    String[] data_after_ovwrite = new String[] { "hundred" };
    run("INSERT OVERWRITE TABLE " + dbName + ".unptned values('" + data_after_ovwrite[0] + "')", driver);

    // Replicate only one INSERT INTO operation on the table.
    loadAndVerify(replDbName, dbName, incrementalDump.lastReplId);

    // After Load from this dump, all target tables/partitions will have initial set of data but source will have latest data.
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data, driverMirror);

    // Replicate the remaining INSERT OVERWRITE operations on the table.
    incrementalLoadAndVerify(dbName, replDbName);

    // After load, shall see the overwritten data.
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", data_after_ovwrite, driverMirror);
  }

  @Test
  public void testInsertOverwriteOnPartitionedTableWithCM() throws IOException {
    String testName = "insertOverwriteOnPartitionedTableWithCM";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    // INSERT INTO 2 partitions and get the last repl ID
    String[] ptn_data_1 = new String[] { "fourteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "sixteen" };
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[1] + "')", driver);

    Tuple incrementalDump = replDumpDb(dbName);

    // Insert overwrite on one partition with multiple files
    String[] data_after_ovwrite = new String[] { "hundred" };
    run("INSERT OVERWRITE TABLE " + dbName + ".ptned partition(b=2) values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".ptned where (b=2)", data_after_ovwrite, driver);

    // Replicate only 2 INSERT INTO operations.
    loadAndVerify(replDbName, dbName, incrementalDump.lastReplId);
    incrementalDump = replDumpDb(dbName);

    // After Load from this dump, all target tables/partitions will have initial set of data.
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    // Replicate the remaining INSERT OVERWRITE operation on the table.
    loadAndVerify(replDbName, dbName, incrementalDump.lastReplId);

    // After load, shall see the overwritten data.
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", data_after_ovwrite, driverMirror);
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
    Tuple incrDump = incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=\"" + ptnVal + "\") ORDER BY a", ptn_data, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION(b=\"" + ptnVal + "\")", driver);

    // Replicate drop partition event and verify
    incrementalLoadAndVerify(dbName, replDbName);
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
    Tuple incrDump = incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT p from " + replDbName + ".ptned ORDER BY p desc", ptnVal, driverMirror);

    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION(p=\"" + ptnVal[0] + "\")", driver);

    // Replicate drop partition event and verify
    incrementalLoadAndVerify(dbName, replDbName);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList(ptnVal[0])), metaStoreClientMirror);
  }

  @Test
  public void testRenameTableWithCM() throws IOException {
    String testName = "renameTableWithCM";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

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
    Tuple incrementalDump = replDumpDb(dbName);

    run("ALTER TABLE " + dbName + ".unptned RENAME TO " + dbName + ".unptned_renamed", driver);
    run("ALTER TABLE " + dbName + ".ptned RENAME TO " + dbName + ".ptned_renamed", driver);

    loadAndVerify(replDbName, dbName, incrementalDump.lastReplId);

    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    incrementalLoadAndVerify(dbName, replDbName);
    verifyFail("SELECT a from " + replDbName + ".unptned ORDER BY a", driverMirror);
    verifyFail("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_renamed ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_renamed where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_renamed where (b=2) ORDER BY a", ptn_data_2, driverMirror);
  }

  @Test
  public void testRenamePartitionWithCM() throws IOException {
    String testName = "renamePartitionWithCM";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    String[] empty = new String[] {};
    String[] ptn_data_1 = new String[] { "fifteen", "fourteen" };
    String[] ptn_data_2 = new String[] { "fifteen", "seventeen" };

    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[1] + "')", driver);

    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=2)", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[1] + "')", driver);

    // Get the last repl ID corresponding to all insert events except RENAME.
    Tuple incrementalDump = replDumpDb(dbName);

    run("ALTER TABLE " + dbName + ".ptned PARTITION (b=2) RENAME TO PARTITION (b=10)", driver);

    loadAndVerify(replDbName, dbName, incrementalDump.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=10) ORDER BY a", empty, driverMirror);

    incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=10) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", empty, driverMirror);
  }

  @Test
  public void testRenameTableAcrossDatabases() throws IOException {
    String testName = "renameTableAcrossDatabases";
    LOG.info("Testing " + testName);
    String dbName1 = testName + "_" + tid + "_1";
    String dbName2 = testName + "_" + tid + "_2";
    String replDbName1 = dbName1 + "_dupe";
    String replDbName2 = dbName2 + "_dupe";

    createDB(dbName1, driver);
    createDB(dbName2, driver);
    run("CREATE TABLE " + dbName1 + ".unptned(a string) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[] { "ten", "twenty" };
    String unptn_locn = new Path(TEST_PATH, testName + "_unptn").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName1 + ".unptned", driver);

    Tuple bootstrap1 = bootstrapLoadAndVerify(dbName1, replDbName1);
    Tuple bootstrap2 = bootstrapLoadAndVerify(dbName2, replDbName2);

    verifyRun("SELECT a from " + replDbName1 + ".unptned ORDER BY a", unptn_data, driverMirror);
    verifyIfTableNotExist(replDbName2, "unptned", metaStoreClientMirror);

    verifyFail("ALTER TABLE " + dbName1 + ".unptned RENAME TO " + dbName2 + ".unptned_renamed", driver);

    incrementalLoadAndVerify(dbName1, replDbName1);
    incrementalLoadAndVerify(dbName2, replDbName2);

    verifyIfTableNotExist(replDbName1, "unptned_renamed", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName2, "unptned_renamed", metaStoreClientMirror);
    verifyRun("SELECT a from " + replDbName1 + ".unptned ORDER BY a", unptn_data, driverMirror);
  }

  @Test
  public void testRenamePartitionedTableAcrossDatabases() throws IOException {
    String testName = "renamePartitionedTableAcrossDatabases";
    LOG.info("Testing " + testName);
    String dbName1 = testName + "_" + tid + "_1";
    String dbName2 = testName + "_" + tid + "_2";
    String replDbName1 = dbName1 + "_dupe";
    String replDbName2 = dbName2 + "_dupe";

    createDB(dbName1, driver);
    createDB(dbName2, driver);
    run("CREATE TABLE " + dbName1 + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] ptn_data = new String[] { "fifteen", "fourteen" };
    String ptn_locn = new Path(TEST_PATH, testName + "_ptn").toUri().getPath();

    createTestDataFile(ptn_locn, ptn_data);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn + "' OVERWRITE INTO TABLE " + dbName1 + ".ptned PARTITION(b=1)", driver);

    Tuple bootstrap1 = bootstrapLoadAndVerify(dbName1, replDbName1);
    Tuple bootstrap2 = bootstrapLoadAndVerify(dbName2, replDbName2);

    verifyRun("SELECT a from " + replDbName1 + ".ptned where (b=1) ORDER BY a", ptn_data, driverMirror);
    verifyIfTableNotExist(replDbName2, "ptned", metaStoreClientMirror);

    verifyFail("ALTER TABLE " + dbName1 + ".ptned RENAME TO " + dbName2 + ".ptned_renamed", driver);

    incrementalLoadAndVerify(dbName1, replDbName1);
    incrementalLoadAndVerify(dbName2, replDbName2);

    verifyIfTableNotExist(replDbName1, "ptned_renamed", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName2, "ptned_renamed", metaStoreClientMirror);
    verifyRun("SELECT a from " + replDbName1 + ".ptned where (b=1) ORDER BY a", ptn_data, driverMirror);
  }

  @Test
  public void testViewsReplication() throws IOException {
    String testName = "viewsReplication";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ext_ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("CREATE VIEW " + dbName + ".virtual_view AS SELECT * FROM " + dbName + ".unptned", driver);
    run("CREATE VIEW " + dbName + ".virtual_view_with_partition PARTITIONED ON (b) AS SELECT * FROM " + dbName + ".ext_ptned", driver);

    String[] unptn_data = new String[]{ "eleven" , "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH , testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH , testName + "_ptn2").toUri().getPath();
    String ext_ptned_locn = new Path(TEST_PATH , testName + "_ext_ptned").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ext_ptned_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    verifySetup("SELECT a from " + dbName + ".ptned", empty, driver);
    verifySetup("SELECT * from " + dbName + ".unptned", empty, driver);
    verifySetup("SELECT * from " + dbName + ".virtual_view", empty, driver);
    verifySetup("SELECT * from " + dbName + ".virtual_view_with_partition", empty, driver);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    verifySetup("SELECT * from " + dbName + ".virtual_view", unptn_data, driver);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);

    run("LOAD DATA LOCAL INPATH '" + ext_ptned_locn + "' OVERWRITE INTO TABLE " + dbName + ".ext_ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ext_ptned WHERE b=2", ptn_data_2, driver);

    // TODO: This does not work because materialized views need the creation metadata
    // to be updated in case tables used were replicated to a different database.
    //run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view AS SELECT a FROM " + dbName + ".ptned where b=1", driver);
    //verifySetup("SELECT a from " + dbName + ".mat_view", ptn_data_1, driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    // view is referring to old database, so no data
    verifyRun("SELECT * from " + replDbName + ".virtual_view", empty, driverMirror);
    //verifyRun("SELECT a from " + replDbName + ".mat_view", ptn_data_1, driverMirror);

    verifySetup("SELECT * from " + replDbName + ".virtual_view_with_partition", empty, driver);

    run("CREATE VIEW " + dbName + ".virtual_view2 AS SELECT a FROM " + dbName + ".ptned where b=2", driver);
    verifySetup("SELECT a from " + dbName + ".virtual_view2", ptn_data_2, driver);

    // Create a view with name already exist. Just to verify if failure flow clears the added create_table event.
    run("CREATE VIEW " + dbName + ".virtual_view2 AS SELECT a FROM " + dbName + ".ptned where b=2", driver);

    // Create view with partition
    run("CREATE VIEW " + dbName + ".virtual_view_with_partition_2 PARTITIONED ON (b) AS SELECT * FROM " + dbName + ".ext_ptned", driver);

    //run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view2 AS SELECT * FROM " + dbName + ".unptned", driver);
    //verifySetup("SELECT * from " + dbName + ".mat_view2", unptn_data, driver);

    // Perform REPL-DUMP/LOAD
    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where b=1", ptn_data_1, driverMirror);
    // view is referring to old database, so no data
    verifyRun("SELECT * from " + replDbName + ".virtual_view", empty, driverMirror);
    //verifyRun("SELECT a from " + replDbName + ".mat_view", ptn_data_1, driverMirror);
    // view is referring to old database, so no data
    verifyRun("SELECT * from " + replDbName + ".virtual_view2", empty, driverMirror);
    //verifyRun("SELECT * from " + replDbName + ".mat_view2", unptn_data, driverMirror);
    verifySetup("SELECT * from " + dbName + ".virtual_view_with_partition_2", empty, driver);

    // Test "alter table" with rename
    run("ALTER VIEW " + dbName + ".virtual_view RENAME TO " + dbName + ".virtual_view_rename", driver);
    verifySetup("SELECT * from " + dbName + ".virtual_view_rename", unptn_data, driver);

    // Perform REPL-DUMP/LOAD
    incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT * from " + replDbName + ".virtual_view_rename", empty, driverMirror);

    // Test "alter table" with schema change
    run("ALTER VIEW " + dbName + ".virtual_view_rename AS SELECT a, concat(a, '_') as a_ FROM " + dbName + ".unptned", driver);
    verifySetup("SHOW COLUMNS FROM " + dbName + ".virtual_view_rename", new String[] {"a", "a_"}, driver);

    // Perform REPL-DUMP/LOAD
    incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SHOW COLUMNS FROM " + replDbName + ".virtual_view_rename", new String[] {"a", "a_"}, driverMirror);

    // Test "DROP VIEW"
    run("DROP VIEW " + dbName + ".virtual_view", driver);
    verifyIfTableNotExist(dbName, "virtual_view", metaStoreClient);

    // Perform REPL-DUMP/LOAD
    incrementalLoadAndVerify(dbName, replDbName);
    verifyIfTableNotExist(replDbName, "virtual_view", metaStoreClientMirror);
  }

  @Test
  public void testMaterializedViewsReplication() throws Exception {
    boolean verifySetupOriginal = verifySetupSteps;
    verifySetupSteps = true;
    String testName = "materializedviewsreplication";
    String testName2 = testName + "2";
    String dbName = createDB(testName, driver);
    String dbName2 = createDB(testName2, driver); //for creating multi-db materialized view
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName2 + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);

    String[] unptn_data = new String[]{ "eleven", "twelve" };
    String[] ptn_data_1 = new String[]{ "thirteen", "fourteen", "fifteen"};
    String[] ptn_data_2 = new String[]{ "fifteen", "sixteen", "seventeen"};
    String[] empty = new String[]{};

    String unptn_locn = new Path(TEST_PATH, testName + "_unptn").toUri().getPath();
    String ptn_locn_1 = new Path(TEST_PATH, testName + "_ptn1").toUri().getPath();
    String ptn_locn_2 = new Path(TEST_PATH, testName + "_ptn2").toUri().getPath();

    createTestDataFile(unptn_locn, unptn_data);
    createTestDataFile(ptn_locn_1, ptn_data_1);
    createTestDataFile(ptn_locn_2, ptn_data_2);

    verifySetup("SELECT a from " + dbName + ".ptned", empty, driver);
    verifySetup("SELECT * from " + dbName + ".unptned", empty, driver);
    verifySetup("SELECT * from " + dbName2 + ".unptned", empty, driver);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName2 + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);
    verifySetup("SELECT * from " + dbName2 + ".unptned", unptn_data, driver);

    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=1", ptn_data_1, driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_2 + "' OVERWRITE INTO TABLE " + dbName + ".ptned PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned WHERE b=2", ptn_data_2, driver);


    run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view_boot disable rewrite  stored as textfile AS SELECT a FROM " + dbName + ".ptned where b=1", driver);
    verifySetup("SELECT a from " + dbName + ".mat_view_boot", ptn_data_1, driver);

    run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view_boot2 disable rewrite  stored as textfile AS SELECT t1.a FROM " + dbName + ".unptned as t1 join " + dbName2 + ".unptned as t2 on t1.a = t2.a", driver);
    verifySetup("SELECT a from " + dbName + ".mat_view_boot2", unptn_data, driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT * from " + replDbName + ".unptned", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where b=1", ptn_data_1, driverMirror);

    //verify source MVs are not on replica
    verifyIfTableNotExist(replDbName, "mat_view_boot", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "mat_view_boot2", metaStoreClientMirror);

    //test alter materialized view with rename
    run("ALTER TABLE " + dbName + ".mat_view_boot RENAME TO " + dbName + ".mat_view_rename", driver);

    //verify rename, i.e. new MV exists and old MV does not exist
    verifyIfTableNotExist(dbName, "mat_view_boot", metaStoreClient);
    verifyIfTableExist(dbName, "mat_view_rename", metaStoreClient);

    // Perform REPL-DUMP/LOAD
    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    //verify source MVs are not on replica
    verifyIfTableNotExist(replDbName, "mat_view_rename", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "mat_view_boot2", metaStoreClientMirror);

    //test alter materialized view rebuild
    run("ALTER MATERIALIZED VIEW " + dbName + ".mat_view_boot2 REBUILD" , driver);
    verifyRun("SELECT a from " + dbName + ".mat_view_boot2", unptn_data, driver);

    // Perform REPL-DUMP/LOAD
    incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    //verify source MVs are not on replica
    verifyIfTableNotExist(replDbName, "mat_view_rename", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "mat_view_boot2", metaStoreClientMirror);

    run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view_inc disable rewrite  stored as textfile AS SELECT a FROM " + dbName + ".ptned where b=2", driver);
    verifySetup("SELECT a from " + dbName + ".mat_view_inc", ptn_data_2, driver);

    run("CREATE MATERIALIZED VIEW " + dbName + ".mat_view_inc2 disable rewrite  stored as textfile AS SELECT t1.a FROM " + dbName + ".unptned as t1 join " + dbName2 + ".unptned as t2 on t1.a = t2.a", driver);
    verifySetup("SELECT a from " + dbName + ".mat_view_inc2", unptn_data, driver);

    // Perform REPL-DUMP/LOAD
    incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    //verify source MVs are not on replica
    verifyIfTableNotExist(replDbName, "mat_view_rename", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "mat_view_boot2", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "mat_view_inc", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "mat_view_inc2", metaStoreClientMirror);

    verifySetupSteps = verifySetupOriginal;
  }



  @Test
  public void testExchangePartition() throws IOException {
    String testName = "exchangePartition";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

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

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=1 and c=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=2 and c=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=2 and c=3) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=1 and c=1)", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=2 and c=2)", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=2 and c=3)", empty, driverMirror);

    // Exchange single partitions using complete partition-spec (all partition columns)
    run("ALTER TABLE " + dbName + ".ptned_dest EXCHANGE PARTITION (b=1, c=1) WITH TABLE " + dbName + ".ptned_src", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=1 and c=1)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=2) ORDER BY a", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=3) ORDER BY a", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=1 and c=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=2 and c=2)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=2 and c=3)", empty, driver);

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=1 and c=1)", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=2 and c=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=2 and c=3) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=1 and c=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=2 and c=2)", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=2 and c=3)", empty, driverMirror);

    // Exchange multiple partitions using partial partition-spec (only one partition column)
    run("ALTER TABLE " + dbName + ".ptned_dest EXCHANGE PARTITION (b=2) WITH TABLE " + dbName + ".ptned_src", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=1 and c=1)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=2)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_src where (b=2 and c=3)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=1 and c=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=2 and c=2) ORDER BY a", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_dest where (b=2 and c=3) ORDER BY a", ptn_data_2, driver);

    incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=1 and c=1)", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=2 and c=2)", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_src where (b=2 and c=3)", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=1 and c=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=2 and c=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_dest where (b=2 and c=3) ORDER BY a", ptn_data_2, driverMirror);
  }

  @Test
  public void testTruncateTable() throws IOException {
    String testName = "truncateTable";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptn_data = new String[] { "eleven", "twelve" };
    String[] empty = new String[] {};
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data, driverMirror);

    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT a from " + dbName + ".unptned", empty, driver);

    incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".unptned", empty, driverMirror);

    String[] unptn_data_after_ins = new String[] { "thirteen" };
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data_after_ins[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data_after_ins, driver);

    incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data_after_ins, driverMirror);
  }

  @Test
  public void testTruncatePartitionedTable() throws IOException {
    String testName = "truncatePartitionedTable";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

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

    verifySetup("SELECT a from " + dbName + ".ptned_1 where (b=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_1 where (b=2) ORDER BY a", ptn_data_2, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_2 where (b=10) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_2 where (b=20) ORDER BY a", ptn_data_2, driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    String replDumpId = bootstrapDump.lastReplId;
    verifyRun("SELECT a from " + replDbName + ".ptned_1 where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_1 where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_2 where (b=10) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_2 where (b=20) ORDER BY a", ptn_data_2, driverMirror);

    run("TRUNCATE TABLE " + dbName + ".ptned_1 PARTITION(b=2)", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_1 where (b=1) ORDER BY a", ptn_data_1, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_1 where (b=2)", empty, driver);

    run("TRUNCATE TABLE " + dbName + ".ptned_2", driver);
    verifySetup("SELECT a from " + dbName + ".ptned_2 where (b=10)", empty, driver);
    verifySetup("SELECT a from " + dbName + ".ptned_2 where (b=20)", empty, driver);

    incrementalLoadAndVerify(dbName, replDbName);
    verifySetup("SELECT a from " + replDbName + ".ptned_1 where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifySetup("SELECT a from " + replDbName + ".ptned_1 where (b=2)", empty, driverMirror);
    verifySetup("SELECT a from " + replDbName + ".ptned_2 where (b=10)", empty, driverMirror);
    verifySetup("SELECT a from " + replDbName + ".ptned_2 where (b=20)", empty, driverMirror);
  }

  @Test
  public void testTruncateWithCM() throws IOException {
    String testName = "truncateWithCM";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);

    Tuple bootstrapDump = replDumpDb(dbName);
    String replDumpId = bootstrapDump.lastReplId;
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);

    String[] empty = new String[] {};
    String[] unptn_data = new String[] { "eleven", "thirteen" };
    String[] unptn_data_load1 = new String[] { "eleven" };
    String[] unptn_data_load2 = new String[] { "eleven", "thirteen" };

    // x events to insert, last repl ID: replDumpId+x
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    Tuple firstInsert = replDumpDb(dbName);
    Integer numOfEventsIns1 = Integer.valueOf(firstInsert.lastReplId) - Integer.valueOf(replDumpId);
    // load only first insert (1 record)
    loadAndVerify(replDbName, dbName, firstInsert.lastReplId);

    verifyRun("SELECT a from " + dbName + "_dupe.unptned ORDER BY a", unptn_data_load1, driverMirror);

    // x events to insert, last repl ID: replDumpId+2x
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);
    Tuple secondInsert = replDumpDb(dbName);
    Integer numOfEventsIns2 = Integer.valueOf(secondInsert.lastReplId) - Integer.valueOf(firstInsert.lastReplId);
    // load only second insert (2 records)

    loadAndVerify(replDbName, dbName, secondInsert.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data_load2, driverMirror);

    // y event to truncate, last repl ID: replDumpId+2x+y
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", empty, driver);
    Tuple thirdTrunc = replDumpDb(dbName);
    Integer numOfEventsTrunc3 = Integer.valueOf(thirdTrunc.lastReplId) - Integer.valueOf(secondInsert.lastReplId);
    // load only truncate (0 records)
    loadAndVerify(replDbName, dbName, thirdTrunc.lastReplId);
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", empty, driverMirror);

    // x events to insert, last repl ID: replDumpId+3x+y
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data_load1[0] + "')", driver);
    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data_load1, driver);
    // Dump and load insert after truncate (1 record)
    incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data_load1, driverMirror);
  }

  @Test
  public void testIncrementalRepeatEventOnExistingObject() throws IOException, InterruptedException {
    String testName = "incrementalRepeatEventOnExistingObject";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    bootstrapLoadAndVerify(dbName, replDbName);

    String[] empty = new String[] {};
    String[] unptn_data = new String[] { "ten" };
    String[] ptn_data_1 = new String[] { "fifteen" };
    String[] ptn_data_2 = new String[] { "seventeen" };

    // INSERT EVENT to unpartitioned table
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    Tuple replDump = replDumpDb(dbName);
    Thread.sleep(1000);
    // INSERT EVENT to partitioned table with dynamic ADD_PARTITION
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=1) values('" + ptn_data_1[0] + "')", driver);
    //Second dump without load will print a warning
    run("REPL DUMP " + dbName, driverMirror);
    //Load the previous dump first
    loadAndVerify(replDbName, dbName, replDump.lastReplId);

    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // ADD_PARTITION EVENT to partitioned table
    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=2)", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // INSERT EVENT to partitioned table on existing partition
    run("INSERT INTO TABLE " + dbName + ".ptned PARTITION(b=2) values('" + ptn_data_2[0] + "')", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // TRUNCATE_PARTITION EVENT on partitioned table
    run("TRUNCATE TABLE " + dbName + ".ptned PARTITION (b=1)", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // TRUNCATE_TABLE EVENT on unpartitioned table
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // CREATE_TABLE EVENT with multiple partitions
    run("CREATE TABLE " + dbName + ".unptned_tmp AS SELECT * FROM " + dbName + ".ptned", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // ADD_CONSTRAINT EVENT
    run("ALTER TABLE " + dbName + ".unptned_tmp ADD CONSTRAINT uk_unptned UNIQUE(a) disable", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);

    Tuple incrDump = incrementalLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_tmp where (b=1) ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_tmp where (b=2) ORDER BY a", ptn_data_2, driverMirror);

    // Load the incremental dump and ensure it does nothing and lastReplID remains same
    loadAndVerify(replDbName, dbName, incrDump.lastReplId);

    // Verify if the data are intact even after applying an applied event once again on existing objects
    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned where (b=2) ORDER BY a", ptn_data_2, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_tmp where (b=1) ORDER BY a", empty, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_tmp where (b=2) ORDER BY a", ptn_data_2, driverMirror);
  }

  @Test
  public void testIncrementalRepeatEventOnMissingObject() throws Exception {
    String testName = "incrementalRepeatEventOnMissingObject";
    String dbName = createDB(testName, driver);
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + dbName + ".ptned(a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptn_data = new String[] { "ten" };
    String[] ptn_data_1 = new String[] { "fifteen" };
    String[] ptn_data_2 = new String[] { "seventeen" };

    // INSERT EVENT to unpartitioned table
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    Tuple replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // INSERT EVENT to partitioned table with dynamic ADD_PARTITION
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=1) values('" + ptn_data_1[0] + "')", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // ADD_PARTITION EVENT to partitioned table
    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=2)", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // INSERT EVENT to partitioned table on existing partition
    run("INSERT INTO TABLE " + dbName + ".ptned partition(b=2) values('" + ptn_data_2[0] + "')", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // TRUNCATE_PARTITION EVENT on partitioned table
    run("TRUNCATE TABLE " + dbName + ".ptned PARTITION(b=1)", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // TRUNCATE_TABLE EVENT on unpartitioned table
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // CREATE_TABLE EVENT on partitioned table
    run("CREATE TABLE " + dbName + ".ptned_tmp (a string) PARTITIONED BY (b int) STORED AS TEXTFILE", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // INSERT EVENT to partitioned table with dynamic ADD_PARTITION
    run("INSERT INTO TABLE " + dbName + ".ptned_tmp partition(b=10) values('" + ptn_data_1[0] + "')", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // INSERT EVENT to partitioned table with dynamic ADD_PARTITION
    run("INSERT INTO TABLE " + dbName + ".ptned_tmp partition(b=20) values('" + ptn_data_2[0] + "')", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // DROP_PARTITION EVENT to partitioned table
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION (b=1)", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // RENAME_PARTITION EVENT to partitioned table
    run("ALTER TABLE " + dbName + ".ptned PARTITION (b=2) RENAME TO PARTITION (b=20)", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // RENAME_TABLE EVENT to unpartitioned table
    run("ALTER TABLE " + dbName + ".unptned RENAME TO " + dbName + ".unptned_new", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // ADD_CONSTRAINT EVENT
    run("ALTER TABLE " + dbName + ".ptned_tmp ADD CONSTRAINT uk_unptned UNIQUE(a) disable", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);
    // DROP_TABLE EVENT to partitioned table
    run("DROP TABLE " + dbName + ".ptned_tmp", driver);
    replDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, replDump.lastReplId);
    Thread.sleep(1000);

    // Replicate all the events happened so far
    Tuple incrDump = incrementalLoadAndVerify(dbName, replDbName);
    // Verify if the data are intact even after applying an applied event once again on missing objects
    verifyIfTableNotExist(replDbName, "unptned", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "ptned_tmp", metaStoreClientMirror);
    verifyIfTableExist(replDbName, "unptned_new", metaStoreClientMirror);
    verifyIfTableExist(replDbName, "ptned", metaStoreClientMirror);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClientMirror);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClientMirror);
    verifyIfPartitionExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("20")), metaStoreClientMirror);

    // Load each incremental dump from the list. Each dump have only one operation.
    // Load the current incremental dump and ensure it does nothing and lastReplID remains same
    loadAndVerify(replDbName, dbName, incrDump.lastReplId);

    // Verify if the data are intact even after applying an applied event once again on missing objects
    verifyIfTableNotExist(replDbName, "unptned", metaStoreClientMirror);
    verifyIfTableNotExist(replDbName, "ptned_tmp", metaStoreClientMirror);
    verifyIfTableExist(replDbName, "unptned_new", metaStoreClientMirror);
    verifyIfTableExist(replDbName, "ptned", metaStoreClientMirror);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("1")), metaStoreClientMirror);
    verifyIfPartitionNotExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("2")), metaStoreClientMirror);
    verifyIfPartitionExist(replDbName, "ptned", new ArrayList<>(Arrays.asList("20")), metaStoreClientMirror);
  }

  @Test
  public void testConcatenateTable() throws IOException {
    String testName = "concatenateTable";
    String dbName = createDB(testName, driver);

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS ORC", driver);

    String[] unptn_data = new String[] { "eleven", "twelve" };
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);

    // Bootstrap dump/load
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    run("ALTER TABLE " + dbName + ".unptned CONCATENATE", driver);

    verifyRun("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    // Replicate all the events happened after bootstrap
    incrementalLoadAndVerify(dbName, replDbName);

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
    incrementalLoadAndVerify(dbName, replDbName);

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

    // Replicate all the events happened so far. It should fail during dump as the data files missing in
    // original path and not available in CM as well.
    verifyFail("REPL DUMP " + dbName, driverMirror);

    // Move the files back to original data location
    assert(dataFs.rename(tmpLoc, ptnLoc));
    Tuple incrDump = replDumpDb(dbName);
    loadAndVerify(replDbName, dbName, incrDump.lastReplId);

    verifyRun("SELECT a from " + replDbName + ".ptned where (b=1) ORDER BY a", ptn_data_1, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".ptned_tmp where (b=1) ORDER BY a", ptn_data_1, driverMirror);
  }

  @Test
  public void testStatus() throws Throwable {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    String lastReplDumpId = bootstrapDump.lastReplId;

    // Bootstrap done, now on to incremental. First, we test db-level REPL LOADs.
    // Both db-level and table-level repl.last.id must be updated.

    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, lastReplDumpId,
        "CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE",
            replDbName);
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)",
            replDbName);
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned PARTITION (b=1) RENAME TO PARTITION (b=11)",
            replDbName);
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned SET TBLPROPERTIES ('blah'='foo')",
            replDbName);
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned RENAME TO  " + dbName + ".ptned_rn",
            replDbName);
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, lastReplDumpId,
        "ALTER TABLE " + dbName + ".ptned_rn DROP PARTITION (b=11)",
            replDbName);
    lastReplDumpId = verifyAndReturnDbReplStatus(dbName, lastReplDumpId,
        "DROP TABLE " + dbName + ".ptned_rn",
            replDbName);

  }

  @Test
  public void testConstraints() throws IOException {
    String testName = "constraints";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".tbl1(a string, b string, primary key (a, b) disable novalidate rely)", driver);
    run("CREATE TABLE " + dbName + ".tbl2(a string, b string, foreign key (a, b) references " + dbName + ".tbl1(a, b) disable novalidate)", driver);
    run("CREATE TABLE " + dbName + ".tbl3(a string, b string not null disable, unique (a) disable)", driver);
    run("CREATE TABLE " + dbName + ".tbl7(a string CHECK (a like 'a%'), price double CHECK (price > 0 AND price <= 1000))", driver);
    run("CREATE TABLE " + dbName + ".tbl8(a string, b int DEFAULT 0)", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    String replDumpId = bootstrapDump.lastReplId;

    try {
      List<SQLPrimaryKey> pks = metaStoreClientMirror.getPrimaryKeys(new PrimaryKeysRequest(replDbName, "tbl1"));
      assertEquals(pks.size(), 2);
      List<SQLUniqueConstraint> uks = metaStoreClientMirror.getUniqueConstraints(new UniqueConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl3"));
      assertEquals(uks.size(), 1);
      List<SQLForeignKey> fks = metaStoreClientMirror.getForeignKeys(new ForeignKeysRequest(null, null, replDbName, "tbl2"));
      assertEquals(fks.size(), 2);
      List<SQLNotNullConstraint> nns = metaStoreClientMirror.getNotNullConstraints(new NotNullConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl3"));
      assertEquals(nns.size(), 1);
      List<SQLCheckConstraint> cks = metaStoreClientMirror.getCheckConstraints(new CheckConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl7"));
      assertEquals(cks.size(), 2);
      List<SQLDefaultConstraint> dks = metaStoreClientMirror.getDefaultConstraints(new DefaultConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl8"));
      assertEquals(dks.size(), 1);
    } catch (TException te) {
      assertNull(te);
    }

    run("CREATE TABLE " + dbName + ".tbl4(a string, b string, primary key (a, b) disable novalidate rely)", driver);
    run("CREATE TABLE " + dbName + ".tbl5(a string, b string, foreign key (a, b) references " + dbName + ".tbl4(a, b) disable novalidate)", driver);
    run("CREATE TABLE " + dbName + ".tbl6(a string, b string not null disable, unique (a) disable)", driver);
    run("CREATE TABLE " + dbName + ".tbl9(a string CHECK (a like 'a%'), price double CHECK (price > 0 AND price <= 1000))", driver);
    run("CREATE TABLE " + dbName + ".tbl10(a string, b int DEFAULT 0)", driver);

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    replDumpId = incrementalDump.lastReplId;

    String pkName = null;
    String ukName = null;
    String fkName = null;
    String nnName = null;
    String dkName1 = null;
    String ckName1 = null;
    String ckName2 = null;
    try {
      List<SQLPrimaryKey> pks = metaStoreClientMirror.getPrimaryKeys(new PrimaryKeysRequest(replDbName, "tbl4"));
      assertEquals(pks.size(), 2);
      pkName = pks.get(0).getPk_name();
      List<SQLUniqueConstraint> uks = metaStoreClientMirror.getUniqueConstraints(new UniqueConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl6"));
      assertEquals(uks.size(), 1);
      ukName = uks.get(0).getUk_name();
      List<SQLForeignKey> fks = metaStoreClientMirror.getForeignKeys(new ForeignKeysRequest(null, null, replDbName, "tbl5"));
      assertEquals(fks.size(), 2);
      fkName = fks.get(0).getFk_name();
      List<SQLNotNullConstraint> nns = metaStoreClientMirror.getNotNullConstraints(new NotNullConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl6"));
      assertEquals(nns.size(), 1);
      nnName = nns.get(0).getNn_name();
      List<SQLCheckConstraint> cks = metaStoreClientMirror.getCheckConstraints(new CheckConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl9"));
      assertEquals(cks.size(), 2);
      ckName1 = cks.get(0).getDc_name();
      ckName2 = cks.get(1).getDc_name();
      List<SQLDefaultConstraint> dks = metaStoreClientMirror.getDefaultConstraints(new DefaultConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl10"));
      assertEquals(dks.size(), 1);
      dkName1 = dks.get(0).getDc_name();
    } catch (TException te) {
      assertNull(te);
    }

    String dkName2 = "custom_dk_name";
    String ckName3 = "customer_ck_name";
    run("ALTER TABLE " + dbName + ".tbl10 CHANGE COLUMN a a string CONSTRAINT " + ckName3 + " CHECK (a like 'a%')", driver);
    run("ALTER TABLE " + dbName + ".tbl10 CHANGE COLUMN b b int CONSTRAINT " + dkName2 + " DEFAULT 1 ENABLE", driver);
    incrementalLoadAndVerify(dbName, replDbName);
    try {
      List<SQLDefaultConstraint> dks = metaStoreClientMirror.getDefaultConstraints(new DefaultConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl10"));
      assertEquals(dks.size(), 2);
      assertEquals(dks.get(1).getDefault_value(), "1");
      List<SQLCheckConstraint> cks = metaStoreClientMirror.getCheckConstraints(new CheckConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl10"));
      assertEquals(cks.size(), 1);
      assertEquals(cks.get(0).getDc_name(), ckName3);
    } catch (TException te) {
      assertNull(te);
    }

    run("ALTER TABLE " + dbName + ".tbl4 DROP CONSTRAINT `" + pkName + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl4 DROP CONSTRAINT `" + ukName + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl5 DROP CONSTRAINT `" + fkName + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl6 DROP CONSTRAINT `" + nnName + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl9 DROP CONSTRAINT `" + ckName1 + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl9 DROP CONSTRAINT `" + ckName2 + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl10 DROP CONSTRAINT `" + ckName3 + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl10 DROP CONSTRAINT `" + dkName1 + "`", driver);
    run("ALTER TABLE " + dbName + ".tbl10 DROP CONSTRAINT `" + dkName2 + "`", driver);

    incrementalLoadAndVerify(dbName, replDbName);
    try {
      List<SQLPrimaryKey> pks = metaStoreClientMirror.getPrimaryKeys(new PrimaryKeysRequest(replDbName, "tbl4"));
      assertTrue(pks.isEmpty());
      List<SQLUniqueConstraint> uks = metaStoreClientMirror.getUniqueConstraints(new UniqueConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl4"));
      assertTrue(uks.isEmpty());
      List<SQLForeignKey> fks = metaStoreClientMirror.getForeignKeys(new ForeignKeysRequest(null, null, replDbName, "tbl5"));
      assertTrue(fks.isEmpty());
      List<SQLNotNullConstraint> nns = metaStoreClientMirror.getNotNullConstraints(new NotNullConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl6"));
      assertTrue(nns.isEmpty());
      List<SQLDefaultConstraint> dks = metaStoreClientMirror.getDefaultConstraints(new DefaultConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl10"));
      assertTrue(dks.isEmpty());
      List<SQLCheckConstraint> cks = metaStoreClientMirror.getCheckConstraints(new CheckConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl9"));
      assertTrue(cks.isEmpty());
      cks = metaStoreClientMirror.getCheckConstraints(new CheckConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl10"));
      assertTrue(cks.isEmpty());
      dks = metaStoreClientMirror.getDefaultConstraints(new DefaultConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl12"));
      assertTrue(dks.isEmpty());
      cks = metaStoreClientMirror.getCheckConstraints(new CheckConstraintsRequest(DEFAULT_CATALOG_NAME, replDbName, "tbl12"));
      assertTrue(cks.isEmpty());

    } catch (TException te) {
      assertNull(te);
    }
  }

  @Test
  public void testRemoveStats() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String replDbName = dbName + "_dupe";

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

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    verifyRun("SELECT count(*) from " + replDbName + ".unptned", new String[]{"2"}, driverMirror);
    verifyRun("SELECT count(*) from " + replDbName + ".ptned", new String[]{"3"}, driverMirror);
    verifyRun("SELECT max(a) from " + replDbName + ".unptned", new String[]{"2"}, driverMirror);
    verifyRun("SELECT max(a) from " + replDbName + ".ptned where b=1", new String[]{"8"}, driverMirror);

    run("CREATE TABLE " + dbName + ".unptned2(a int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned2", driver);
    run("CREATE TABLE " + dbName + ".ptned2(a int) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("LOAD DATA LOCAL INPATH '" + ptn_locn_1 + "' OVERWRITE INTO TABLE " + dbName + ".ptned2 PARTITION(b=1)", driver);
    run("ANALYZE TABLE " + dbName + ".unptned2 COMPUTE STATISTICS FOR COLUMNS", driver);
    run("ANALYZE TABLE " + dbName + ".unptned2 COMPUTE STATISTICS", driver);
    run("ANALYZE TABLE " + dbName + ".ptned2 partition(b) COMPUTE STATISTICS FOR COLUMNS", driver);
    run("ANALYZE TABLE " + dbName + ".ptned2 partition(b) COMPUTE STATISTICS", driver);

    incrementalLoadAndVerify(dbName, replDbName);
    verifyRun("SELECT count(*) from " + replDbName + ".unptned2", new String[]{"2"}, driverMirror);
    verifyRun("SELECT count(*) from " + replDbName + ".ptned2", new String[]{"3"}, driverMirror);
    verifyRun("SELECT max(a) from " + replDbName + ".unptned2", new String[]{"2"}, driverMirror);
    verifyRun("SELECT max(a) from " + replDbName + ".ptned2 where b=1", new String[]{"8"}, driverMirror);
  }

  @Test
  public void testDeleteStagingDir() throws IOException {
    String testName = "deleteStagingDir";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    String tableName = "unptned";
    run("CREATE TABLE " + StatsUtils.getFullyQualifiedTableName(dbName, tableName) + "(a string) STORED AS TEXTFILE",
        driver);

    String[] unptn_data = new String[] {"one", "two"};
    String unptn_locn = new Path(TEST_PATH , testName + "_unptn").toUri().getPath();
    createTestDataFile(unptn_locn, unptn_data);
    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned", unptn_data, driver);

    // Perform repl
    String replDumpLocn = replDumpDb(dbName).dumpLocation;
    // Reset the driver
    driverMirror.close();
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);
    // Calling close() explicitly to clean up the staging dirs
    driverMirror.close();
    // Check result
    Path warehouse = new Path(System.getProperty("test.warehouse.dir", "/tmp"));
    FileSystem fs = FileSystem.get(warehouse.toUri(), hconf);
    try {
      Path path = new Path(warehouse, replDbName + ".db" + Path.SEPARATOR + tableName);
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
          return path.getName().startsWith(HiveConf.getVar(hconf, HiveConf.ConfVars.STAGING_DIR));
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
    String replDbName = dbName + "_dupe";

    // Create table and insert two file of the same content
    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('ten')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('ten')", driver);

    // Bootstrap test
    Tuple bootstrapDump = replDumpDb(dbName);
    advanceDumpDir();
    run("REPL DUMP " + dbName, driver);
    String replDumpLocn = bootstrapDump.dumpLocation;
    String replDumpId = bootstrapDump.lastReplId;

    // Drop two files so they are moved to CM
    run("TRUNCATE TABLE " + dbName + ".unptned", driver);

    LOG.info("Bootstrap-Dump: Dumped to {} with id {}", replDumpLocn, replDumpId);
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);

    verifyRun("SELECT count(*) from " + replDbName + ".unptned", new String[]{"2"}, driverMirror);
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
        new MessageFormatFilter(JSONMessageEncoder.FORMAT);
    IMetaStoreClient.NotificationFilter restrictByArbitraryMessageFormat =
        new MessageFormatFilter(JSONMessageEncoder.FORMAT + "_bogus");
    NotificationEvent dummyEvent = createDummyEvent(dbname,tblname,0);

    assertEquals(JSONMessageEncoder.FORMAT,dummyEvent.getMessageFormat());

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
    createDB(dbName, driver);
    NotificationEventResponse rsp = metaStoreClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
    // Test various scenarios
    // Remove the proxy privilege by reseting proxy configuration to default value.
    // The auth should fail (in reality the proxy setting should not be changed on the fly)
    // Pretty hacky: Affects both instances of HMS
    ProxyUsers.refreshSuperUserGroupsConfiguration();
    try {
      hconf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, false);
      MetastoreConf.setBoolVar(hconf, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, true);
      rsp = metaStoreClient.getNextNotification(firstEventId, 0, null);
      Assert.fail("Get Next Nofitication should have failed due to no proxy auth");
    } catch (TException e) {
      // Expected to throw an Exception - keep going
    }
    // Disable auth so the call should succeed
    MetastoreConf.setBoolVar(hconf, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, false);
    MetastoreConf.setBoolVar(hconfMirror, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, false);
    try {
      rsp = metaStoreClient.getNextNotification(firstEventId, 0, null);
      assertEquals(1, rsp.getEventsSize());
    } finally {
      // Restore the settings
      MetastoreConf.setBoolVar(hconf, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, false);
      hconf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
      hconf.set(proxySettingName, "*");

      // Restore Proxy configurations to test values
      // Pretty hacky: Applies one setting to both instances of HMS
      ProxyUsers.refreshSuperUserGroupsConfiguration(hconf);
    }
  }

  @Test
  public void testRecycleFileDropTempTable() throws IOException {
    String dbName = createDB(testName.getMethodName(), driver);

    run("CREATE TABLE " + dbName + ".normal(a int)", driver);
    run("INSERT INTO " + dbName + ".normal values (1)", driver);
    run("DROP TABLE " + dbName + ".normal", driver);

    String cmDir = hconf.getVar(HiveConf.ConfVars.REPL_CM_DIR);
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

  @Test
  public void testLoadCmPathMissing() throws Exception {
    String dbName = createDB(testName.getMethodName(), driver);
    run("CREATE TABLE " + dbName + ".normal(a int)", driver);
    run("INSERT INTO " + dbName + ".normal values (1)", driver);

    advanceDumpDir();
    run("repl dump " + dbName, true, driver);
    String dumpLocation = getResult(0, 0, driver);

    run("DROP TABLE " + dbName + ".normal", driver);

    String cmDir = hconf.getVar(HiveConf.ConfVars.REPL_CM_DIR);
    Path path = new Path(cmDir);
    FileSystem fs = path.getFileSystem(hconf);
    ContentSummary cs = fs.getContentSummary(path);
    long fileCount = cs.getFileCount();
    assertTrue(fileCount != 0);
    fs.delete(path);
    driverMirror.run("REPL LOAD " + dbName + " INTO " + dbName);
    run("drop database " + dbName, true, driver);
  }

  @Test
  public void testDumpWithTableDirMissing() throws IOException {
    String dbName = createDB(testName.getMethodName(), driver);
    run("CREATE TABLE " + dbName + ".normal(a int)", driver);
    run("INSERT INTO " + dbName + ".normal values (1)", driver);

    Database db = null;
    Path path = null;

    try {
      metaStoreClient.getDatabase(dbName);
      path = new Path(db.getManagedLocationUri());
    } catch (Exception e) {
      path = new Path(System.getProperty("test.warehouse.dir", "/tmp/warehouse/managed"));
      path = new Path(path, dbName.toLowerCase()+".db");
    }
    path = new Path(path, "normal");
    FileSystem fs = path.getFileSystem(hconf);
    fs.delete(path);

    advanceDumpDir();
    try {
      driver.run("REPL DUMP " + dbName);
      assert false;
    } catch (CommandProcessorException e) {
      Assert.assertEquals(e.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());
    }

    run("DROP TABLE " + dbName + ".normal", driver);
    run("drop database " + dbName, true, driver);
  }

  @Test
  public void testDumpWithPartitionDirMissing() throws IOException {
    String dbName = createDB(testName.getMethodName(), driver);
    run("CREATE TABLE " + dbName + ".normal(a int) PARTITIONED BY (part int)", driver);
    run("INSERT INTO " + dbName + ".normal partition (part= 124) values (1)", driver);

    Database db = null;
    Path path = null;

    try {
      metaStoreClient.getDatabase(dbName);
      path = new Path(db.getManagedLocationUri());
    } catch (Exception e) {
      path = new Path(System.getProperty("test.warehouse.dir", "/tmp/warehouse/managed"));
      path = new Path(path, dbName.toLowerCase()+".db");
    }
    path = new Path(path, "normal");
    path = new Path(path, "part=124");
    FileSystem fs = path.getFileSystem(hconf);
    fs.delete(path);

    advanceDumpDir();
    try {
      driver.run("REPL DUMP " + dbName);
      assert false;
    } catch (CommandProcessorException e) {
      Assert.assertEquals(e.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());
    }

    run("DROP TABLE " + dbName + ".normal", driver);
    run("drop database " + dbName, true, driver);
  }


  @Test
  public void testDDLTasksInParallel() throws Throwable{
    Logger logger = null;
    LoggerContext ctx = null;
    Level oldLevel = null;
    StringAppender appender = null;
    LoggerConfig loggerConfig = null;
    try {
      driverMirror.getConf().set(HiveConf.ConfVars.EXEC_PARALLEL.varname, "true");
      logger = LogManager.getLogger("hive.ql.metadata.Hive");
      oldLevel = logger.getLevel();
      ctx = (LoggerContext) LogManager.getContext(false);
      Configuration config = ctx.getConfiguration();
      loggerConfig = config.getLoggerConfig(logger.getName());
      loggerConfig.setLevel(Level.INFO);
      ctx.updateLoggers();
      // Create a String Appender to capture log output
      appender = StringAppender.createStringAppender("%m");
      appender.addToLogger(logger.getName(), Level.DEBUG);
      appender.start();
      String testName = "testDDLTasksInParallel";
      String dbName = createDB(testName, driver);
      String replDbName = dbName + "_dupe";

      run("CREATE TABLE " + dbName + ".t1 (id int)", driver);
      run("insert into table " + dbName + ".t1 values ('1')", driver);
      run("CREATE TABLE " + dbName + ".t2 (id int)", driver);
      run("insert into table " + dbName + ".t2 values ('2')", driver);

      Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

      String logText = appender.getOutput();
      Pattern pattern = Pattern.compile("Starting task \\[Stage-[0-9]:DDL\\] in parallel");
      Matcher matcher = pattern.matcher(logText);
      int count = 0;
      while(matcher.find()){
        count++;
      }
      assertEquals(count, 2);
      appender.reset();
    } finally {
      driverMirror.getConf().set(HiveConf.ConfVars.EXEC_PARALLEL.varname, "false");
      loggerConfig.setLevel(oldLevel);
      ctx.updateLoggers();
      appender.removeFromLogger(logger.getName());
    }
  }

  @Test
  public void testRecycleFileNonReplDatabase() throws IOException {
    String dbName = createDBNonRepl(testName.getMethodName(), driver);

    String cmDir = hconf.getVar(HiveConf.ConfVars.REPL_CM_DIR);
    Path path = new Path(cmDir);
    FileSystem fs = path.getFileSystem(hconf);
    ContentSummary cs = fs.getContentSummary(path);
    long fileCount = cs.getFileCount();

    run("CREATE TABLE " + dbName + ".normal(a int)", driver);
    run("INSERT INTO " + dbName + ".normal values (1)", driver);

    cs = fs.getContentSummary(path);
    long fileCountAfter = cs.getFileCount();
    assertTrue(fileCount == fileCountAfter);

    run("INSERT INTO " + dbName + ".normal values (3)", driver);
    run("TRUNCATE TABLE " + dbName + ".normal", driver);

    cs = fs.getContentSummary(path);
    fileCountAfter = cs.getFileCount();
    assertTrue(fileCount == fileCountAfter);

    run("INSERT INTO " + dbName + ".normal values (4)", driver);
    run("ALTER TABLE " + dbName + ".normal RENAME to " + dbName + ".normal1", driver);
    verifyRun("SELECT count(*) from " + dbName + ".normal1", new String[]{"1"}, driver);

    cs = fs.getContentSummary(path);
    fileCountAfter = cs.getFileCount();
    assertTrue(fileCount == fileCountAfter);

    run("INSERT INTO " + dbName + ".normal1 values (5)", driver);
    run("DROP TABLE " + dbName + ".normal1", driver);

    cs = fs.getContentSummary(path);
    fileCountAfter = cs.getFileCount();
    assertTrue(fileCount == fileCountAfter);
  }

  @Test
  public void testMoveOptimizationBootstrap() throws IOException {
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);
    String tableNameNoPart = dbName + "_no_part";
    String tableNamePart = dbName + "_part";

    run(" use " + dbName, driver);
    run("CREATE TABLE " + tableNameNoPart + " (fld int) STORED AS TEXTFILE", driver);
    run("CREATE TABLE " + tableNamePart + " (fld int) partitioned by (part int) STORED AS TEXTFILE", driver);

    run("insert into " + tableNameNoPart + " values (1) ", driver);
    run("insert into " + tableNameNoPart + " values (2) ", driver);
    verifyRun("SELECT fld from " + tableNameNoPart , new String[]{ "1" , "2" }, driver);

    run("insert into " + tableNamePart + " partition (part=10) values (1) ", driver);
    run("insert into " + tableNamePart + " partition (part=10) values (2) ", driver);
    run("insert into " + tableNamePart + " partition (part=11) values (3) ", driver);
    verifyRun("SELECT fld from " + tableNamePart , new String[]{ "1" , "2" , "3"}, driver);
    verifyRun("SELECT fld from " + tableNamePart + " where part = 10" , new String[]{ "1" , "2"}, driver);
    verifyRun("SELECT fld from " + tableNamePart + " where part = 11" , new String[]{ "3" }, driver);

    String replDbName = dbName + "_replica";
    Tuple dump = replDumpDb(dbName);
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);
    verifyRun("REPL STATUS " + replDbName, dump.lastReplId, driverMirror);

    run(" use " + replDbName, driverMirror);
    verifyRun("SELECT fld from " + tableNamePart , new String[]{ "1" , "2" , "3"}, driverMirror);
    verifyRun("SELECT fld from " + tableNamePart + " where part = 10" , new String[]{ "1" , "2"}, driverMirror);
    verifyRun("SELECT fld from " + tableNamePart + " where part = 11" , new String[]{ "3" }, driverMirror);
    verifyRun("SELECT fld from " + tableNameNoPart , new String[]{ "1" , "2" }, driverMirror);
    verifyRun("SELECT count(*) from " + tableNamePart , new String[]{ "3"}, driverMirror);
    verifyRun("SELECT count(*) from " + tableNamePart + " where part = 10" , new String[]{ "2"}, driverMirror);
    verifyRun("SELECT count(*) from " + tableNamePart + " where part = 11" , new String[]{ "1" }, driverMirror);
    verifyRun("SELECT count(*) from " + tableNameNoPart , new String[]{ "2" }, driverMirror);
  }

  @Test
  public void testMoveOptimizationIncremental() throws IOException {
    String testName = "testMoveOptimizationIncremental";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_replica";

    bootstrapLoadAndVerify(dbName, replDbName);

    String[] unptn_data = new String[] { "eleven", "twelve" };

    run("CREATE TABLE " + dbName + ".unptned(a string) STORED AS TEXTFILE", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[0] + "')", driver);
    run("INSERT INTO TABLE " + dbName + ".unptned values('" + unptn_data[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned ORDER BY a", unptn_data, driver);

    run("CREATE TABLE " + dbName + ".unptned_late AS SELECT * FROM " + dbName + ".unptned", driver);
    verifySetup("SELECT * from " + dbName + ".unptned_late ORDER BY a", unptn_data, driver);

    Tuple incrementalDump = replDumpDb(dbName);
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);
    verifyRun("REPL STATUS " + replDbName, incrementalDump.lastReplId, driverMirror);

    verifyRun("SELECT a from " + replDbName + ".unptned ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned_late ORDER BY a", unptn_data, driverMirror);
    verifyRun("SELECT count(*) from " + replDbName + ".unptned ", "2", driverMirror);
    verifyRun("SELECT count(*) from " + replDbName + ".unptned_late", "2", driverMirror);

    String[] unptn_data_after_ins = new String[] { "eleven", "thirteen", "twelve" };
    String[] data_after_ovwrite = new String[] { "hundred" };
    run("INSERT INTO TABLE " + dbName + ".unptned_late values('" + unptn_data_after_ins[1] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned_late ORDER BY a", unptn_data_after_ins, driver);
    run("INSERT OVERWRITE TABLE " + dbName + ".unptned values('" + data_after_ovwrite[0] + "')", driver);
    verifySetup("SELECT a from " + dbName + ".unptned", data_after_ovwrite, driver);

    incrementalDump = replDumpDb(dbName);
    run("REPL LOAD " + dbName + " INTO " + replDbName, driverMirror);
    verifyRun("REPL STATUS " + replDbName, incrementalDump.lastReplId, driverMirror);

    verifyRun("SELECT a from " + replDbName + ".unptned_late ORDER BY a", unptn_data_after_ins, driverMirror);
    verifyRun("SELECT a from " + replDbName + ".unptned", data_after_ovwrite, driverMirror);
    verifyRun("SELECT count(*) from " + replDbName + ".unptned", "1", driverMirror);
    verifyRun("SELECT count(*) from " + replDbName + ".unptned_late ", "3", driverMirror);
  }

  @Test
  public void testDatabaseInJobName() throws Throwable {
    // Clean up configurations
    driver.getConf().set(JobContext.JOB_NAME, "");
    driverMirror.getConf().set(JobContext.JOB_NAME, "");
    // Get the logger at the root level.
    Logger logger = LogManager.getLogger("hive.ql.metadata.Hive");
    Level oldLevel = logger.getLevel();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();
    // Create a String Appender to capture log output

    StringAppender appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(logger.getName(), Level.DEBUG);
    appender.start();
    String testName = "testDatabaseInJobName";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".emp (id int)", driver);
    run("insert into table " + dbName + ".emp values ('1')", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    assertTrue(appender.getOutput().contains("Using Repl#testDatabaseInJobName as job name for map-reduce jobs."));
    assertTrue(appender.getOutput().contains("Using Repl#testDatabaseInJobName_dupe as job name for map-reduce jobs."));
    loggerConfig.setLevel(oldLevel);
    ctx.updateLoggers();
    appender.removeFromLogger(logger.getName());
  }

  @Test
  public void testPolicyIdImplicitly() throws Exception {
    // Create a database.
    String name = testName.getMethodName();
    String dbName = createDB(name, driver);

    // Remove SOURCE_OF_REPLICATION property.
    run("ALTER DATABASE " + name + " Set DBPROPERTIES ( '"
        + SOURCE_OF_REPLICATION + "' = '')", driver);

    // Create a table with some data.
    run("CREATE TABLE " + dbName + ".dataTable(a string) STORED AS TEXTFILE",
        driver);

    String[] unptn_data = new String[] {"eleven", "twelve"};
    String unptn_locn = new Path(TEST_PATH, name + "_unptn").toUri().getPath();
    createTestDataFile(unptn_locn, unptn_data);

    run("LOAD DATA LOCAL INPATH '" + unptn_locn + "' OVERWRITE INTO TABLE "
        + dbName + ".dataTable", driver);

    // Perform Dump & Load and verify the data is replicated properly.
    String replicatedDbName = dbName + "_dupe";

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replicatedDbName);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(hconf);
    Path dumpPath =
        new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
    verifyRun("SELECT * from " + replicatedDbName + ".dataTable", unptn_data,
        driverMirror);

    // Check the value of SOURCE_OF_REPLICATION in the database, it should
    // get set automatically.
    run("DESCRIBE DATABASE EXTENDED " + dbName, driver);
    List<String> result = getOutput(driver);
    System.out.print(result);
    assertTrue(result.get(0),
        result.get(0).contains("repl.source.for=default_REPL DUMP " + dbName));

    // Remove SOURCE_OF_REPLICATION property after bootstrap dump.
    run("ALTER DATABASE " + name + " Set DBPROPERTIES ( '"
            + SOURCE_OF_REPLICATION + "' = '')", driver);
    run("INSERT INTO TABLE " + dbName + ".dataTable values('a', 'b', 'c')", driver);

    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replicatedDbName);
    fs = new Path(incrementalDump.dumpLocation).getFileSystem(hconf);
    dumpPath = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    // Check the value of SOURCE_OF_REPLICATION in the database, it should
    // get set automatically.
    run("DESCRIBE DATABASE EXTENDED " + dbName, driver);
    result = getOutput(driver);
    assertTrue(result.get(0),
            result.get(0).contains("repl.source.for=default_REPL DUMP " + dbName));
  }

  @Test
  public void testReplicationMetricForSkippedIteration() throws Throwable {
    String name = testName.getMethodName();
    String primaryDbName = createDB(name, driver);
    String replicaDbName = "replicaDb";
    try {
      isMetricsEnabledForTests(true);
      MetricCollector collector = MetricCollector.getInstance();
      run("create table " + primaryDbName + ".t1 (id int) STORED AS TEXTFILE", driver);
      run("insert into " + primaryDbName + ".t1 values(1)", driver);
      run("repl dump " + primaryDbName, driver);

      ReplicationMetric metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SUCCESS);

      run("repl dump " + primaryDbName, driver);

      metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SKIPPED);

      run("repl load " + primaryDbName + " into " + replicaDbName, driverMirror);

      metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SUCCESS);

      run("repl load " + primaryDbName + " into " + replicaDbName, driverMirror);

      metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SKIPPED);
    } finally {
      isMetricsEnabledForTests(false);
    }
  }

  @Test
  public void testReplicationMetricForFailedIteration() throws Throwable {
    String name = testName.getMethodName();
    String primaryDbName = createDB(name, driver);
    String replicaDbName = "tgtDb";
    try {
      isMetricsEnabledForTests(true);
      MetricCollector collector = MetricCollector.getInstance();
      run("create table " + primaryDbName + ".t1 (id int) STORED AS TEXTFILE", driver);
      run("insert into " + primaryDbName + ".t1 values(1)", driver);
      Tuple dumpData = replDumpDb(primaryDbName);

      ReplicationMetric metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SUCCESS);

      run("repl load " + primaryDbName + " into " + replicaDbName, driverMirror);

      Path nonRecoverableFile = new Path(new Path(dumpData.dumpLocation), ReplAck.NON_RECOVERABLE_MARKER.toString());
      FileSystem fs = new Path(dumpData.dumpLocation).getFileSystem(hconf);
      fs.create(nonRecoverableFile);

      verifyFail("REPL DUMP " + primaryDbName, driver);

      metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SKIPPED);
      assertEquals(metric.getProgress().getStages().get(0).getErrorLogPath(), nonRecoverableFile.toString());

      verifyFail("REPL DUMP " + primaryDbName, driver);
      metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SKIPPED);
      assertEquals(metric.getProgress().getStages().get(0).getErrorLogPath(), nonRecoverableFile.toString());

      fs.delete(nonRecoverableFile, true);
      dumpData = replDumpDb(primaryDbName);

      metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SUCCESS);

      run("ALTER DATABASE " + replicaDbName + " SET DBPROPERTIES('" + ReplConst.REPL_INCOMPATIBLE + "'='true')", driverMirror);

      verifyFail("REPL LOAD " + primaryDbName + " INTO " + replicaDbName, driverMirror);

      nonRecoverableFile = new Path(new Path(dumpData.dumpLocation), ReplAck.NON_RECOVERABLE_MARKER.toString());
      assertTrue(fs.exists(nonRecoverableFile));

      metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.FAILED_ADMIN);
      assertEquals(metric.getProgress().getStages().get(0).getErrorLogPath(), nonRecoverableFile.toString());

      verifyFail("REPL LOAD " + primaryDbName + " INTO " + replicaDbName, driverMirror);

      metric = collector.getMetrics().getLast();
      assertEquals(metric.getProgress().getStatus(), Status.SKIPPED);
      assertEquals(metric.getProgress().getStages().get(0).getErrorLogPath(), nonRecoverableFile.toString());
    } finally {
      isMetricsEnabledForTests(false);
    }
  }

  @Test
  public void testAddPartition() throws Throwable{
    // Get the logger at the root level.
    Logger logger = LogManager.getLogger("hive.ql.metadata.Hive");
    Level oldLevel = logger.getLevel();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();
    // Create a String Appender to capture log output

    StringAppender appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(logger.getName(), Level.DEBUG);
    appender.start();
    String testName = "testAddPartition";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    run("insert into table " + dbName + ".ptned partition(b='2') values "
        + "('delhi')", driver);

    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);


    run("ALTER TABLE " + dbName + ".ptned ADD PARTITION (b=1)", driver);
    appender.reset();
    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    // Check the add partition calls get triggered.
    assertTrue(appender.getOutput().contains("Calling AddPartition for [Partition(values:[1]"));
    // Check the alter partition call doesn't get triggered
    assertFalse(appender.getOutput().contains("Calling AlterPartition for "));
    loggerConfig.setLevel(oldLevel);
    ctx.updateLoggers();
    appender.removeFromLogger(logger.getName());
  }

  @Test
  public void testDropPartitionSingleEvent() throws IOException {
    Logger logger = LogManager.getLogger("hive.ql.metadata.Hive");
    Level oldLevel = logger.getLevel();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();

    StringAppender appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(logger.getName(), Level.DEBUG);
    appender.start();

    String testName = "testDropPartitionSingleEvent";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";

    // Create a partitioned table.
    run("CREATE TABLE " + dbName + ".ptned(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    ArrayList<String> partitions = new ArrayList<>();

    // Create around 10 partitoins.
    for (int i = 0; i < 10; i++) {
      run("ALTER TABLE " + dbName + ".ptned ADD PARTITION(b=" + i + ")", driver);
      partitions.add("b=" + i);
    }

    // Verify that the partitions got created, then do a dump & load cycle.
    verifyRun("SHOW PARTITIONS " + dbName + ".ptned", partitions.toArray(new String[] {}), driver);
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);

    verifyRun("SHOW PARTITIONS " + replDbName + ".ptned", partitions.toArray(new String[] {}), driverMirror);

    // Drop 3 partitions in one go, at source.
    run("ALTER TABLE " + dbName + ".ptned DROP PARTITION(b<3)", driver);

    partitions.remove("b=0");
    partitions.remove("b=1");
    partitions.remove("b=2");

    appender.reset();

    // Do an incremntal load and see the partitions got deleted and the normal drop partition flow was used.
    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);
    assertTrue(appender.getOutput().contains("Replication calling normal drop partitions for regular partition drops"));
    assertTrue(appender.getOutput().contains("Dropped 3 partitions for replication."));
    verifyRun("SHOW PARTITIONS " + replDbName + ".ptned", partitions.toArray(new String[] {}), driverMirror);

    // Clean up
    loggerConfig.setLevel(oldLevel);
    ctx.updateLoggers();
    appender.removeFromLogger(logger.getName());
  }

  @org.junit.Ignore("HIVE-26073")
  @Test
  public void testIncrementalStatisticsMetrics() throws Throwable {
    isMetricsEnabledForTests(true);
    ReplLoadWork.setMbeansParamsForTesting(true, false);
    MetricCollector collector = MetricCollector.getInstance();
    String testName = "testIncrementalStatisticsMetrics";
    String dbName = createDB(testName, driver);
    String replDbName = dbName + "_dupe";
    String nameStri = "Hadoop:" + "service=HiveServer2" + "," + "name=" + "Database-" + replDbName + " Policy-pol";

    // Do a bootstrap dump & load
    Tuple bootstrapDump = bootstrapLoadAndVerify(dbName, replDbName);
    collector.getMetrics();
    ReplLoadWork.setMbeansParamsForTesting(true,true);

    // Do some operations at the source side so that the count & metrics can be counted at the load side.

    // 10 create table
    for (int i = 0; i < 10; i++) {
      run("CREATE TABLE " + dbName + ".ptned" + i + "(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
      for (int j = 0; j < 5; j++) {
        // Create 5 partitoins per table.
        run("ALTER TABLE " + dbName + ".ptned" + i + " ADD PARTITION(b=" + j + ")", driver);
      }
    }
    verifyRun("SHOW PARTITIONS " + dbName + ".ptned1", new String[] {"b=0","b=1","b=2","b=3","b=4"}, driver);

    // Do an incremental load & verify the metrics.
    Tuple incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    String events[] = new String[] { "[[Event Name: EVENT_CREATE_TABLE; " + "Total Number: 10;",
        "[[Event Name: EVENT_ADD_PARTITION; Total Number: 50;" };

    Iterator<ReplicationMetric> itr = collector.getMetrics().iterator();
    while (itr.hasNext()) {
      ReplicationMetric elem = itr.next();
      assertEquals(Metadata.ReplicationType.INCREMENTAL, elem.getMetadata().getReplicationType());
      List<Stage> stages = elem.getProgress().getStages();
      assertTrue(stages.size() != 0);
      for (Stage stage : stages) {
        if (stage.getReplStats() == null) {
          continue;
        }
        for (String event : events) {
          assertTrue(stage.getReplStats(), stage.getReplStats().contains(event));
        }
      }
    }

    verifyMBeanStatistics(testName, replDbName, nameStri, events, incrementalDump);

    // Do some drop table/drop partition & rename table operations.
    for (int i = 0; i < 3; i++) {
      // Drop 3 tables
      run("DROP TABLE " + dbName + ".ptned" + i, driver);
    }

    for (int i = 3; i < 6; i++) {
      // Rename 3 tables
      run("ALTER TABLE " + dbName + ".ptned" + i + " RENAME TO " + dbName + ".ptned" + i + "_renamed", driver);
    }

    for (int i = 6; i < 10; i++) {
      // Drop partitions from 4 tables
      run("ALTER TABLE " + dbName + ".ptned" + i + " DROP PARTITION(b=1)", driver);
    }

    for (int i = 10; i < 12; i++) {
      // Create 2 tables
      run("CREATE TABLE " + dbName + ".ptned" + i + "(a string) partitioned by (b int) STORED AS TEXTFILE", driver);
    }

    incrementalDump = incrementalLoadAndVerify(dbName, replDbName);

    events = new String[] { "[[Event Name: EVENT_CREATE_TABLE; " + "Total Number: 2;",
        "[[Event Name: EVENT_DROP_TABLE; " + "Total Number: 3;",
        "[[Event Name: EVENT_RENAME_TABLE; " + "Total Number: 3;",
        "[[Event Name: EVENT_DROP_PARTITION; Total Number: 4;" };

      itr = collector.getMetrics().iterator();
    while (itr.hasNext()) {
      ReplicationMetric elem = itr.next();
      assertEquals(Metadata.ReplicationType.INCREMENTAL, elem.getMetadata().getReplicationType());
      List<Stage> stages = elem.getProgress().getStages();
      assertTrue(stages.size() != 0);
      for (Stage stage : stages) {
        if (stage.getReplStats() == null) {
          continue;
        }
        for (String event : events) {
          assertTrue(stage.getReplStats(), stage.getReplStats().contains(event));
        }
      }
    }
    verifyMBeanStatistics(testName, replDbName, nameStri, events, incrementalDump);
    // Clean up the test setup.
    ReplLoadWork.setMbeansParamsForTesting(false,false);
    MBeans.unregister(ObjectName.getInstance(nameStri));
  }

  private void verifyMBeanStatistics(String testName, String replDbName, String nameStri, String[] events,
      Tuple incrementalDump)
      throws MalformedObjectNameException, MBeanException, AttributeNotFoundException, InstanceNotFoundException,
      ReflectionException {
    ObjectName name = ObjectName.getInstance(nameStri);

    assertTrue(ManagementFactory.getPlatformMBeanServer().getAttribute(name, "ReplicationType").toString()
        .contains("INCREMENTAL"));
    // Check the dump location is set correctly.
    assertTrue(ManagementFactory.getPlatformMBeanServer().getAttribute(name, "DumpDirectory").toString()
        .startsWith(incrementalDump.dumpLocation));
    // The CurrentEventId should be the last dumped repl id, once the load is complete.
    assertEquals(incrementalDump.lastReplId,
        ManagementFactory.getPlatformMBeanServer().getAttribute(name, "CurrentEventId"));
    assertEquals(Long.parseLong(incrementalDump.lastReplId),
        ManagementFactory.getPlatformMBeanServer().getAttribute(name, "LastEventId"));
    assertTrue(
        ManagementFactory.getPlatformMBeanServer().getAttribute(name, "SourceDatabase").toString().contains(testName));
    assertTrue(ManagementFactory.getPlatformMBeanServer().getAttribute(name, "TargetDatabase").toString()
        .contains(replDbName));
    for (String event : events) {
      assertTrue(ManagementFactory.getPlatformMBeanServer().getAttribute(name, "ReplStats").toString().contains(event));
    }
  }

  private static String createDB(String name, IDriver myDriver) {
    LOG.info("Testing " + name);
    String mgdLocation = System.getProperty("test.warehouse.dir", "file:/tmp/warehouse/managed");
    String extLocation = System.getProperty("test.warehouse.external.dir", "/tmp/warehouse/external");
    run("CREATE DATABASE " + name + " LOCATION '" + extLocation + "/" + name.toLowerCase() + ".db' MANAGEDLOCATION '" + mgdLocation + "/" + name.toLowerCase() + ".db' WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')", myDriver);
    return name;
  }

  private static String createDBNonRepl(String name, IDriver myDriver) {
    LOG.info("Testing " + name);
    String dbName = name + "_" + tid;
    run("CREATE DATABASE " + dbName, myDriver);
    return dbName;
  }

  private NotificationEvent createDummyEvent(String dbname, String tblname, long evid) {
    MessageEncoder msgEncoder = null;
    try {
      msgEncoder = MessageFactory.getInstance(JSONMessageEncoder.FORMAT);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Table t = new Table();
    t.setDbName(dbname);
    t.setTableName(tblname);
    NotificationEvent event = new NotificationEvent(
        evid,
        (int)System.currentTimeMillis(),
        MessageBuilder.CREATE_TABLE_EVENT,
        MessageBuilder.getInstance().buildCreateTableMessage(t, Arrays.asList("/tmp/").iterator())
            .toString()
    );
    event.setDbName(t.getDbName());
    event.setTableName(t.getTableName());
    event.setMessageFormat(msgEncoder.getMessageFormat());
    return event;
  }

  private String verifyAndReturnDbReplStatus(String dbName,
                                             String prevReplDumpId, String cmd,
                                             String replDbName) throws IOException {
    run(cmd, driver);
    String lastReplDumpId = incrementalLoadAndVerify(dbName, replDbName).lastReplId;
    assertTrue(Long.parseLong(lastReplDumpId) > Long.parseLong(prevReplDumpId));
    return lastReplDumpId;
  }

  // Tests that verify table's last repl ID
  private String verifyAndReturnTblReplStatus(
      String dbName, String tblName, String lastDbReplDumpId, String cmd,
      String replDbName) throws IOException, TException {
    run(cmd, driver);
    String lastReplDumpId
            = incrementalLoadAndVerify(dbName, replDbName).lastReplId;
    verifyRun("REPL STATUS " + replDbName, lastReplDumpId, driverMirror);
    assertTrue(Long.parseLong(lastReplDumpId) > Long.parseLong(lastDbReplDumpId));

    Table tbl = metaStoreClientMirror.getTable(replDbName, tblName);
    String tblLastReplId = tbl.getParameters().get(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString());
    assertTrue(Long.parseLong(tblLastReplId) > Long.parseLong(lastDbReplDumpId));
    assertTrue(Long.parseLong(tblLastReplId) <= Long.parseLong(lastReplDumpId));
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

  private void verifyChecksum(Path sourceFilePath, Path targetFilePath, boolean shouldMatch) throws IOException {
    FileSystem srcFS = sourceFilePath.getFileSystem(hconf);
    FileSystem tgtFS = targetFilePath.getFileSystem(hconf);
    if (shouldMatch) {
      assertTrue(srcFS.getFileChecksum(sourceFilePath).equals(tgtFS.getFileChecksum(targetFilePath)));
    } else {
      assertFalse(srcFS.getFileChecksum(sourceFilePath).equals(tgtFS.getFileChecksum(targetFilePath)));
    }
  }
  private List<String> getOutput(IDriver myDriver) throws IOException {
    List<String> results = new ArrayList<>();
    myDriver.getResults(results);
    return results;
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

  private void verifyIfTableExist(
      String dbName, String tableName, HiveMetaStoreClient myClient) throws Exception {
    Table tbl = myClient.getTable(dbName, tableName);
    assertNotNull(tbl);
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
    try {
      Partition ptn = myClient.getPartition(dbName, tableName, partValues);
      assertNotNull(ptn);
    } catch (TException te) {
      assert(false);
    }
  }

  private void verifySetup(String cmd, String[] data, IDriver myDriver) throws  IOException {
    if (verifySetupSteps){
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

  private void verifyDataFileExist(FileSystem fs, Path hiveDumpDir, String part, String dataFile) throws IOException {
    FileStatus[] eventFileStatuses = fs.listStatus(hiveDumpDir);
    boolean dataFileFound = false;
    for (FileStatus eventFileStatus: eventFileStatuses) {
      String dataRelativePath = null;
      if (part == null) {
        dataRelativePath = EximUtil.DATA_PATH_NAME + File.separator + dataFile;
      } else {
        dataRelativePath = EximUtil.DATA_PATH_NAME + File.separator + part + File.separator + dataFile;
      }
      if (fs.exists(new Path(eventFileStatus.getPath(), dataRelativePath))) {
        dataFileFound = true;
        break;
      }
    }
    assertTrue(dataFileFound);
  }

  private void verifyDataListFileDoesNotExist(FileSystem fs, Path hiveDumpDir, String part)
          throws IOException {
    FileStatus[] eventFileStatuses = fs.listStatus(hiveDumpDir);
    boolean dataListFileFound = false;
    for (FileStatus eventFileStatus: eventFileStatuses) {
      String dataRelativePath = null;
      if (part == null) {
        dataRelativePath = EximUtil.DATA_PATH_NAME + File.separator + EximUtil.FILES_NAME;
      } else {
        dataRelativePath = part + File.separator + EximUtil.FILES_NAME;
      }
      if (fs.exists(new Path(eventFileStatus.getPath(), dataRelativePath))) {
        dataListFileFound = true;
        break;
      }
    }
    assertFalse(dataListFileFound);
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
    try {
      myDriver.run(cmd);
      success = true;
    } catch (CommandProcessorException e) {
      LOG.warn("Error {} : {} running [{}].", e.getErrorCode(), e.getMessage(), cmd);
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

  public static Path getNonRecoverablePath(Path dumpDir, String dbName, HiveConf conf) throws IOException {
    Path dumpPath = new Path(dumpDir,
            Base64.getEncoder().encodeToString(dbName.toLowerCase()
                    .getBytes(StandardCharsets.UTF_8.name())));
    FileSystem fs = dumpPath.getFileSystem(conf);
    if (fs.exists(dumpPath)) {
      FileStatus[] statuses = fs.listStatus(dumpPath);
      if (statuses.length > 0) {
        return new Path(statuses[statuses.length -1].getPath(), NON_RECOVERABLE_MARKER.toString());
      }
    }
    return null;
  }

  private void verifyIncrementalLogs(StringAppender appender) {
    String logStr = appender.getOutput();
    String eventStr = "REPL::EVENT_LOAD:";
    String eventDurationStr = "eventDuration";
    String incLoadStageStr = "REPL_INCREMENTAL_LOAD";
    String incLoadTaskDurationStr = "REPL_INCREMENTAL_LOAD stage duration";
    String incTaskBuilderDurationStr = "REPL_INCREMENTAL_LOAD task-builder";
    assertTrue(logStr, logStr.contains(eventStr));
    //verify for each loaded event, there is the event duration log
    assertEquals(StringUtils.countMatches(logStr, eventStr), StringUtils.countMatches(logStr, eventDurationStr));
    //verify for each repl-load stage, there is one log for entire stage-duration and one log for DAG-duration(builder)
    assertTrue(StringUtils.countMatches(logStr, incLoadStageStr) > 3);
    assertEquals(StringUtils.countMatches(logStr, incLoadStageStr) / 3, StringUtils.countMatches(logStr, incTaskBuilderDurationStr));
    assertEquals(StringUtils.countMatches(logStr, incLoadStageStr) / 3, StringUtils.countMatches(logStr, incLoadTaskDurationStr));
  }

  private void deleteNewMetadataFields(Tuple dump) throws SemanticException {
    Path dumpHiveDir = new Path(dump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    DumpMetaData dmd = new DumpMetaData(dumpHiveDir, hconf);
    Path dumpMetaPath = new Path(dumpHiveDir, DUMP_METADATA);

    List<List<String>> listValues = new ArrayList<>();
    DumpType dumpType = dmd.getDumpType();
    Long eventFrom = dmd.getEventFrom();
    Long eventTo = dmd.getEventTo();
    String cmRoot = "testCmRoot";
    String payload = dmd.getPayload();
    Long dumpExecutionId = dmd.getDumpExecutionId();
    ReplScope replScope = dmd.getReplScope();
    listValues.add(
            Arrays.asList(
                    dumpType.toString(),
                    eventFrom.toString(),
                    eventTo.toString(),
                    cmRoot,
                    dumpExecutionId.toString(),
                    payload)
    );
    if (replScope != null) {
      listValues.add(dmd.prepareReplScopeValues());
    }
    org.apache.hadoop.hive.ql.parse.repl.dump
            .Utils.writeOutput(listValues, dumpMetaPath, hconf, true);
  }
}
