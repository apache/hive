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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.common.DataCopyStatistics;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.incremental.IncrementalLoadEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.api.repl.ReplicationV1CompatRule;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.codehaus.plexus.util.ExceptionUtils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WarehouseInstance implements Closeable {
  final String functionsRoot, repldDir;
  private Logger logger;
  private IDriver driver;
  HiveConf hiveConf;
  MiniDFSCluster miniDFSCluster;
  private HiveMetaStoreClient client;
  final Path warehouseRoot;
  final Path externalTableWarehouseRoot;

  private static int uniqueIdentifier = 0;

  private final static String LISTENER_CLASS = DbNotificationListener.class.getCanonicalName();

  WarehouseInstance(Logger logger, MiniDFSCluster cluster, Map<String, String> overridesForHiveConf,
      String keyNameForEncryptedZone) throws Exception {
    this.logger = logger;
    this.miniDFSCluster = cluster;
    assert miniDFSCluster.isClusterUp();
    assert miniDFSCluster.isDataNodeUp();
    DistributedFileSystem fs = miniDFSCluster.getFileSystem();

    warehouseRoot = mkDir(fs, "/warehouse" + uniqueIdentifier);
    externalTableWarehouseRoot = mkDir(fs, "/external" + uniqueIdentifier);
    if (StringUtils.isNotEmpty(keyNameForEncryptedZone)) {
      fs.createEncryptionZone(warehouseRoot, keyNameForEncryptedZone);
      fs.createEncryptionZone(externalTableWarehouseRoot, keyNameForEncryptedZone);
    }
    Path cmRootPath = mkDir(fs, "/cmroot" + uniqueIdentifier);
    this.functionsRoot = mkDir(fs, "/functions" + uniqueIdentifier).toString();
    String tmpDir = "/tmp/"
        + TestReplicationScenarios.class.getCanonicalName().replace('.', '_')
        + "_"
        + System.nanoTime();

    this.repldDir = mkDir(fs, tmpDir + "/hrepl" + uniqueIdentifier + "/").toString();
    initialize(cmRootPath.toString(), externalTableWarehouseRoot.toString(),
        warehouseRoot.toString(), overridesForHiveConf);
  }

  WarehouseInstance(Logger logger, MiniDFSCluster cluster,
      Map<String, String> overridesForHiveConf) throws Exception {
    this(logger, cluster, overridesForHiveConf, null);
  }

  private void initialize(String cmRoot, String externalTableWarehouseRoot, String warehouseRoot,
      Map<String, String> overridesForHiveConf) throws Exception {
    hiveConf = new HiveConf(miniDFSCluster.getConfiguration(0), TestReplicationScenarios.class);

    String metaStoreUri = System.getProperty("test." + HiveConf.ConfVars.METASTORE_URIS.varname);
    if (metaStoreUri != null) {
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_URIS, metaStoreUri);
      return;
    }

    //    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, hiveInTest);
    // turn on db notification listener on meta store
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_WAREHOUSE, warehouseRoot);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL, externalTableWarehouseRoot);
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS, LISTENER_CLASS);
    hiveConf.setBoolVar(HiveConf.ConfVars.REPL_CM_ENABLED, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    hiveConf.setVar(HiveConf.ConfVars.REPL_CM_DIR, cmRoot);
    hiveConf.setVar(HiveConf.ConfVars.REPL_FUNCTIONS_ROOT_DIR, functionsRoot);
    hiveConf.setBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE, false);
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY,
        "jdbc:derby:memory:${test.tmp.dir}/APP;create=true");
    hiveConf.setVar(HiveConf.ConfVars.REPL_DIR, this.repldDir);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");
    if (!hiveConf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER).equals("org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")) {
      hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    }
    hiveConf.set(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname,
            "org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore");
    System.setProperty(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, " ");

    for (Map.Entry<String, String> entry : overridesForHiveConf.entrySet()) {
      hiveConf.set(entry.getKey(), entry.getValue());
    }

    MetaStoreTestUtils.startMetaStoreWithRetry(hiveConf, true, true);

    // Add the below mentioned dependency in metastore/pom.xml file. For postgres need to copy postgresql-42.2.1.jar to
    // .m2//repository/postgresql/postgresql/9.3-1102.jdbc41/postgresql-9.3-1102.jdbc41.jar.
    /*
    <dependency>
      <groupId>mysql</groupId>
      <artifactId>mysql-connector-java</artifactId>
      <version>8.0.15</version>
    </dependency>

    <dependency>
      <groupId>postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <version>9.3-1102.jdbc41</version>
    </dependency>
    */

    /*hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY, "jdbc:mysql://localhost:3306/APP");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "com.mysql.jdbc.Driver");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_PWD, "hivepassword");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "hiveuser");*/

    /*hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY,"jdbc:postgresql://localhost/app");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "org.postgresql.Driver");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_PWD, "password");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "postgres");*/

    driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    client = new HiveMetaStoreClient(hiveConf);

    TestTxnDbUtil.cleanDb(hiveConf);
    TestTxnDbUtil.prepDb(hiveConf);

    // change the value for the next instance.
    ++uniqueIdentifier;
  }

  private Path mkDir(DistributedFileSystem fs, String pathString)
      throws IOException, SemanticException {
    Path path = new Path(pathString);
    fs.mkdirs(path, new FsPermission("777"));
    return PathBuilder.fullyQualifiedHDFSUri(path, fs);
  }

  public HiveConf getConf() {
    return hiveConf;
  }

  private int next = 0;

  private void advanceDumpDir() {
    next++;
    ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next));
  }

  private ArrayList<String> lastResults;

  private String row0Result(int colNum, boolean reuse) throws IOException {
    if (!reuse) {
      lastResults = new ArrayList<>();
      driver.getResults(lastResults);
    }
    // Split around the 'tab' character
    return !lastResults.isEmpty() ? (lastResults.get(0).split("\\t"))[colNum] : "";
  }

  public WarehouseInstance run(String command) throws Throwable {
    try {
      driver.run(command);
      return this;
    } catch (CommandProcessorException e) {
      if (e.getCause() != null) {
        throw  e.getCause();
      }
      throw e;
    }
  }

  public CommandProcessorResponse runCommand(String command) throws Throwable {
    return driver.run(command);
  }

  WarehouseInstance runFailure(String command) throws Throwable {
    try {
      driver.run(command);
      throw new RuntimeException("command execution passed for a invalid command" + command);
    } catch (CommandProcessorException e) {
      return this;
    }
  }

  WarehouseInstance runFailure(String command, int errorCode) throws Throwable {
    try {
      driver.run(command);
      throw new RuntimeException("command execution passed for a invalid command" + command);
    } catch (CommandProcessorException e) {
      if (e.getResponseCode() != errorCode) {
        throw new RuntimeException("Command: " + command + " returned incorrect error code: " +
            e.getResponseCode() + " instead of " + errorCode);
      }
      return this;
    }
  }

  Tuple dump(String dbName)
          throws Throwable {
    return dump(dbName, Collections.emptyList());
  }

  Tuple dump(String dumpExpression, List<String> withClauseOptions)
      throws Throwable {
    String dumpCommand =
        "REPL DUMP " + dumpExpression;
    if (withClauseOptions != null && !withClauseOptions.isEmpty()) {
      dumpCommand += " with (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return dumpWithCommand(dumpCommand);
  }

  Tuple dumpWithCommand(String dumpCommand) throws Throwable {
    advanceDumpDir();
    run(dumpCommand);
    String dumpLocation = row0Result(0, false);
    String lastDumpId = row0Result(1, true);
    return new Tuple(dumpLocation, lastDumpId);
  }

  WarehouseInstance dumpFailure(String dbName) throws Throwable {
    String dumpCommand =
            "REPL DUMP " + dbName;
    advanceDumpDir();
    runFailure(dumpCommand);
    return this;
  }

  WarehouseInstance load(String replicatedDbName, String primaryDbName) throws Throwable {
    StringBuilder replCommand = new StringBuilder("REPL LOAD " + primaryDbName);
    if (!StringUtils.isEmpty(replicatedDbName)) {
      replCommand.append(" INTO " + replicatedDbName);
    }
    run("EXPLAIN " + replCommand.toString());
    printOutput();
    run(replCommand.toString());
    return this;
  }

  WarehouseInstance loadWithoutExplain(String replicatedDbName, String primaryDbName) throws Throwable {
    StringBuilder replCommand = new StringBuilder("REPL LOAD " + primaryDbName);
    if (!StringUtils.isEmpty(replicatedDbName)) {
      replCommand.append(" INTO " + replicatedDbName);
    }
    run(replCommand.toString() + " with ('hive.exec.parallel'='true')");
    return this;
  }

  WarehouseInstance load(String replicatedDbName, String primaryDbName, List<String> withClauseOptions)
          throws Throwable {
    String replLoadCmd = "REPL LOAD " + primaryDbName + " INTO " + replicatedDbName;
    if ((withClauseOptions != null) && !withClauseOptions.isEmpty()) {
      replLoadCmd += " WITH (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return run(replLoadCmd);
  }

  WarehouseInstance status(String replicatedDbName) throws Throwable {
    String replStatusCmd = "REPL STATUS " + replicatedDbName;
    return run(replStatusCmd);
  }

  WarehouseInstance status(String replicatedDbName, List<String> withClauseOptions) throws Throwable {
    String replStatusCmd = "REPL STATUS " + replicatedDbName;
    if ((withClauseOptions != null) && !withClauseOptions.isEmpty()) {
      replStatusCmd += " WITH (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return run(replStatusCmd);
  }

  WarehouseInstance loadFailure(String replicatedDbName, String primaryDbName) throws Throwable {
    loadFailure(replicatedDbName, primaryDbName, null);
    return this;
  }

  WarehouseInstance loadFailure(String replicatedDbName, String primaryDbName, List<String> withClauseOptions)
          throws Throwable {
    String replLoadCmd = "REPL LOAD " + primaryDbName + " INTO " + replicatedDbName;
    if ((withClauseOptions != null) && !withClauseOptions.isEmpty()) {
      replLoadCmd += " WITH (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return runFailure(replLoadCmd);
  }

  WarehouseInstance loadFailure(String replicatedDbName, String primaryDbName, List<String> withClauseOptions,
                                int errorCode) throws Throwable {
    String replLoadCmd = "REPL LOAD " + primaryDbName + " INTO " + replicatedDbName;
    if ((withClauseOptions != null) && !withClauseOptions.isEmpty()) {
      replLoadCmd += " WITH (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return runFailure(replLoadCmd, errorCode);
  }

  WarehouseInstance verifyResult(String data) throws IOException {
    verifyResults(data == null ? new String[] {} : new String[] { data });
    return this;
  }

  /**
   * All the results that are read from the hive output will not preserve
   * case sensitivity and will all be in lower case, hence we will check against
   * only lower case data values.
   * Unless for Null Values it actually returns in UpperCase and hence explicitly lowering case
   * before assert.
   */
  WarehouseInstance verifyResults(String[] data) throws IOException {
    List<String> results = getOutput();
    logger.info("Expecting {}", StringUtils.join(data, ","));
    logger.info("Got {}", results);
    List<String> filteredResults = results.stream().filter(
        x -> !x.toLowerCase()
            .contains(SemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX.toLowerCase()))
        .map(String::toLowerCase)
        .collect(Collectors.toList());
    List<String> lowerCaseData =
        Arrays.stream(data).map(String::toLowerCase).collect(Collectors.toList());
    assertEquals(data.length, filteredResults.size());
    assertTrue(StringUtils.join(filteredResults, ",") + " does not contain all expected " + StringUtils
            .join(lowerCaseData, ","), filteredResults.containsAll(lowerCaseData));
    return this;
  }

  WarehouseInstance verifyFailure(String[] data) throws IOException {
    List<String> results = getOutput();
    logger.info("Expecting {}", StringUtils.join(data, ","));
    logger.info("Got {}", results);
    boolean dataMatched = (data.length == results.size());
    if (dataMatched) {
      for (int i = 0; i < data.length; i++) {
        dataMatched &= data[i].toLowerCase().equals(results.get(i).toLowerCase());
      }
    }
    assertFalse(dataMatched);
    return this;
  }

  /**
   * verify's result without regard for ordering.
   */
  WarehouseInstance verifyResults(List data) throws IOException {
    List<String> results = getOutput();
    logger.info("Expecting {}", StringUtils.join(data, ","));
    logger.info("Got {}", results);
    assertEquals(data.size(), results.size());
    assertTrue(results.containsAll(data));
    return this;
  }

  public List<String> getOutput() throws IOException {
    List<String> results = new ArrayList<>();
    driver.getResults(results);
    return results;
  }

  private void printOutput() throws IOException {
    for (String s : getOutput()) {
      logger.info(s);
    }
  }

  private void verifyIfCkptSet(Map<String, String> props, String dumpDir) {
    assertTrue(props.containsKey(ReplConst.REPL_TARGET_DB_PROPERTY));
    assertTrue(props.get(ReplConst.REPL_TARGET_DB_PROPERTY).equals(dumpDir));
  }

  public void verifyIfCkptSet(String dbName, String dumpDir) throws Exception {
    Database db = getDatabase(dbName);
    verifyIfCkptSet(db.getParameters(), dumpDir);

    List<String> tblNames = getAllTables(dbName);
    verifyIfCkptSetForTables(dbName, tblNames, dumpDir);
  }

  public void verifyIfCkptSetForTables(String dbName, List<String> tblNames, String dumpDir) throws Exception {
    for (String tblName : tblNames) {
      Table tbl = getTable(dbName, tblName);
      verifyIfCkptSet(tbl.getParameters(), dumpDir);
      if (tbl.getPartitionKeysSize() != 0) {
        List<Partition> partitions = getAllPartitions(dbName, tblName);
        for (Partition ptn : partitions) {
          verifyIfCkptSet(ptn.getParameters(), dumpDir);
        }
      }
    }
  }

  // Make sure that every table in the target database is marked as target of the replication.
  // Stats updater task and partition management task skip processing tables being replicated into.
  private void verifyReplTargetProperty(Map<String, String> props) {
    assertTrue(props.containsKey(ReplConst.REPL_TARGET_TABLE_PROPERTY));
  }

  public WarehouseInstance verifyReplTargetProperty(String dbName) throws Exception {
    verifyReplTargetProperty(getDatabase(dbName).getParameters());
    return this;
  }

  public Database getDatabase(String dbName) throws Exception {
    try {
      return client.getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return null;
    }
  }

  public int getNoOfEventsDumped(String dumpLocation, HiveConf conf) throws Throwable {
    IncrementalLoadEventsIterator itr = new IncrementalLoadEventsIterator(
            dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR, conf);
    return itr.getTotalEventsCount();
  }

  public List<String> getAllTables(String dbName) throws Exception {
    return client.getAllTables(dbName);
  }

  public Table getTable(String dbName, String tableName) throws Exception {
    try {
      return client.getTable(dbName, tableName);
    } catch (NoSuchObjectException e) {
      return null;
    }
  }

  /**
   * Get statistics for given set of columns of a given table in the given database.
   * @param dbName - the database where the table resides
   * @param tableName - tablename whose statistics are to be retrieved
   * @return - list of ColumnStatisticsObj objects in the order of the specified columns
   */
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName) throws Exception {
    return client.getTableColumnStatistics(dbName, tableName, getTableColNames(dbName, tableName), Constants.HIVE_ENGINE);
  }

  /**
   * @param dbName, database name
   * @param tableName, table name
   * @return - list of columns of given table in the given database.
   * @throws Exception
   */
  public List<String> getTableColNames(String dbName, String tableName) throws Exception {
    List<String> colNames = new ArrayList();
    client.getSchema(dbName, tableName).forEach(fs -> colNames.add(fs.getName()));
    return colNames;
  }

  /**
   * Get statistics for given set of columns of a given table in the given database
   * @param dbName - the database where the table resides
   * @param tableName - tablename whose statistics are to be retrieved
   * @return - list of ColumnStatisticsObj objects in the order of the specified columns
   */
  public Map<String, List<ColumnStatisticsObj>> getAllPartitionColumnStatistics(String dbName,
                                                                    String tableName) throws Exception {
    List<String> colNames = new ArrayList();
    client.getFields(dbName, tableName).forEach(fs -> colNames.add(fs.getName()));
    return client.getPartitionColumnStatistics(dbName, tableName,
            client.listPartitionNames(dbName, tableName, (short) -1), colNames, Constants.HIVE_ENGINE);
  }

  /**
   * Get statistics for a given partition of the given table in the given database.
   * @param dbName - the database where the table resides
   * @param tableName - name of the partitioned table in the database
   * @param colNames - columns whose statistics is to be retrieved
   * @return Map of partition name and list of ColumnStatisticsObj. The objects in the list are
   * ordered according to the given list of columns.
   * @throws Exception
   */
  List<ColumnStatisticsObj> getPartitionColumnStatistics(String dbName, String tableName,
                                                         String partName, List<String> colNames)
          throws Exception {
    return client.getPartitionColumnStatistics(dbName, tableName,
                                              Collections.singletonList(partName), colNames, Constants.HIVE_ENGINE).get(0);
  }

  public List<Partition> getAllPartitions(String dbName, String tableName) throws Exception {
    try {
      return client.listPartitions(dbName, tableName, Short.MAX_VALUE);
    } catch (NoSuchObjectException e) {
      return null;
    }
  }

  public Partition getPartition(String dbName, String tableName, List<String> partValues) throws Exception {
    try {
      return client.getPartition(dbName, tableName, partValues);
    } catch (NoSuchObjectException e) {
      return null;
    }
  }

  public List<SQLPrimaryKey> getPrimaryKeyList(String dbName, String tblName) throws Exception {
    return client.getPrimaryKeys(new PrimaryKeysRequest(dbName, tblName));
  }

  public List<SQLForeignKey> getForeignKeyList(String dbName, String tblName) throws Exception {
    return client.getForeignKeys(new ForeignKeysRequest(null, null, dbName, tblName));
  }

  public List<SQLUniqueConstraint> getUniqueConstraintList(String dbName, String tblName) throws Exception {
    return client.getUniqueConstraints(new UniqueConstraintsRequest(Warehouse.DEFAULT_CATALOG_NAME, dbName, tblName));
  }

  public List<SQLNotNullConstraint> getNotNullConstraintList(String dbName, String tblName) throws Exception {
    return client.getNotNullConstraints(
            new NotNullConstraintsRequest(Warehouse.DEFAULT_CATALOG_NAME, dbName, tblName));
  }

  ReplicationV1CompatRule getReplivationV1CompatRule(List<String> testsToSkip) {
    return new ReplicationV1CompatRule(client, hiveConf, testsToSkip);
  }

  // Test if the number of events between the given event ids and with the given database name are
  // same as expected. toEventId = 0 is treated as unbounded. Same is the case with limit 0.
  public void testEventCounts(String dbName, long fromEventId, Long toEventId, Integer limit,
                               long expectedCount) throws Exception {
    NotificationEventsCountRequest rqst = new NotificationEventsCountRequest(fromEventId, dbName);

    if (toEventId != null) {
      rqst.setToEventId(toEventId);
    }
    if (limit != null) {
      rqst.setLimit(limit);
    }

    assertEquals(expectedCount, client.getNotificationEventsCount(rqst).getEventsCount());
  }

  public boolean isAcidEnabled() {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY) &&
        hiveConf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER).equals("org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")) {
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    if (miniDFSCluster != null && miniDFSCluster.isClusterUp()) {
      miniDFSCluster.shutdown();
    }
    if (client != null) {
      client.close();
    }
  }

  CurrentNotificationEventId getCurrentNotificationEventId() throws Exception {
    return client.getCurrentNotificationEventId();
  }

  List<Path> copyToHDFS(List<URI> localUris) throws IOException, SemanticException {
    DistributedFileSystem fs = miniDFSCluster.getFileSystem();
    Path destinationBasePath = new Path("/", String.valueOf(System.nanoTime()));
    mkDir(fs, destinationBasePath.toString());
    localUris.forEach(uri -> {
      Path localPath = new Path(uri);
      try {
        FileSystem localFs = localPath.getFileSystem(hiveConf);
        DataCopyStatistics copyStatistics = new DataCopyStatistics();
        boolean success = FileUtils
            .copy(localFs, localPath, fs, destinationBasePath, false, false, hiveConf, copyStatistics);
        if (!success) {
          fail("FileUtils could not copy local uri " + localPath.toString() + " to hdfs");
        }
      } catch (IOException e) {
        String message = "error on copy of local uri " + localPath.toString() + " to hdfs";
        logger.error(message, e);
        fail(message + ExceptionUtils.getFullStackTrace(e));
      }
    });

    List<FileStatus> fileStatuses =
        Arrays.asList(fs.globStatus(new Path(destinationBasePath, "*")));
    return fileStatuses.stream().map(FileStatus::getPath).collect(Collectors.toList());
  }

  static class Tuple {
    final String dumpLocation;
    final String lastReplicationId;

    Tuple(String dumpLocation, String lastReplicationId) {
      this.dumpLocation = dumpLocation;
      this.lastReplicationId = lastReplicationId;
    }
  }
}
