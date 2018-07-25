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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.api.repl.ReplicationV1CompatRule;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.codehaus.plexus.util.ExceptionUtils;
import org.slf4j.Logger;

import java.io.Closeable;
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
  final String functionsRoot;
  private Logger logger;
  private IDriver driver;
  HiveConf hiveConf;
  MiniDFSCluster miniDFSCluster;
  private HiveMetaStoreClient client;

  private static int uniqueIdentifier = 0;

  private final static String LISTENER_CLASS = DbNotificationListener.class.getCanonicalName();

  WarehouseInstance(Logger logger, MiniDFSCluster cluster, Map<String, String> overridesForHiveConf,
      String keyNameForEncryptedZone) throws Exception {
    this.logger = logger;
    this.miniDFSCluster = cluster;
    assert miniDFSCluster.isClusterUp();
    assert miniDFSCluster.isDataNodeUp();
    DistributedFileSystem fs = miniDFSCluster.getFileSystem();

    Path warehouseRoot = mkDir(fs, "/warehouse" + uniqueIdentifier);
    if (StringUtils.isNotEmpty(keyNameForEncryptedZone)) {
      fs.createEncryptionZone(warehouseRoot, keyNameForEncryptedZone);
    }
    Path cmRootPath = mkDir(fs, "/cmroot" + uniqueIdentifier);
    this.functionsRoot = mkDir(fs, "/functions" + uniqueIdentifier).toString();
    initialize(cmRootPath.toString(), warehouseRoot.toString(), overridesForHiveConf);
  }

  public WarehouseInstance(Logger logger, MiniDFSCluster cluster,
      Map<String, String> overridesForHiveConf) throws Exception {
    this(logger, cluster, overridesForHiveConf, null);
  }

  private void initialize(String cmRoot, String warehouseRoot,
      Map<String, String> overridesForHiveConf) throws Exception {
    hiveConf = new HiveConf(miniDFSCluster.getConfiguration(0), TestReplicationScenarios.class);
    for (Map.Entry<String, String> entry : overridesForHiveConf.entrySet()) {
      hiveConf.set(entry.getKey(), entry.getValue());
    }
    String metaStoreUri = System.getProperty("test." + HiveConf.ConfVars.METASTOREURIS.varname);
    String hiveWarehouseLocation = System.getProperty("test.warehouse.dir", "/tmp")
        + Path.SEPARATOR
        + TestReplicationScenarios.class.getCanonicalName().replace('.', '_')
        + "_"
        + System.nanoTime();
    if (metaStoreUri != null) {
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
      return;
    }

    //    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, hiveInTest);
    // turn on db notification listener on meta store
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseRoot);
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS, LISTENER_CLASS);
    hiveConf.setBoolVar(HiveConf.ConfVars.REPLCMENABLED, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    hiveConf.setVar(HiveConf.ConfVars.REPLCMDIR, cmRoot);
    hiveConf.setVar(HiveConf.ConfVars.REPL_FUNCTIONS_ROOT_DIR, functionsRoot);
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
        "jdbc:derby:memory:${test.tmp.dir}/APP;create=true");
    hiveConf.setVar(HiveConf.ConfVars.REPLDIR,
        hiveWarehouseLocation + "/hrepl" + uniqueIdentifier + "/");
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    if (!hiveConf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER).equals("org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")) {
      hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    }
    hiveConf.set(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname,
            "org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore");
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

    MetaStoreTestUtils.startMetaStoreWithRetry(hiveConf, true);

    Path testPath = new Path(hiveWarehouseLocation);
    FileSystem testPathFileSystem = FileSystem.get(testPath.toUri(), hiveConf);
    testPathFileSystem.mkdirs(testPath);

    driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    client = new HiveMetaStoreClient(hiveConf);

    TxnDbUtil.cleanDb(hiveConf);
    TxnDbUtil.prepDb(hiveConf);

    // change the value for the next instance.
    ++uniqueIdentifier;
  }

  private Path mkDir(DistributedFileSystem fs, String pathString)
      throws IOException, SemanticException {
    Path path = new Path(pathString);
    fs.mkdir(path, new FsPermission("777"));
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
    return (lastResults.get(0).split("\\t"))[colNum];
  }

  public WarehouseInstance run(String command) throws Throwable {
    CommandProcessorResponse ret = driver.run(command);
    if (ret.getException() != null) {
      throw ret.getException();
    }
    return this;
  }

  WarehouseInstance runFailure(String command) throws Throwable {
    CommandProcessorResponse ret = driver.run(command);
    if (ret.getException() == null) {
      throw new RuntimeException("command execution passed for a invalid command" + command);
    }
    return this;
  }

  Tuple dump(String dbName, String lastReplicationId, List<String> withClauseOptions)
      throws Throwable {
    String dumpCommand =
        "REPL DUMP " + dbName + (lastReplicationId == null ? "" : " FROM " + lastReplicationId);
    if (!withClauseOptions.isEmpty()) {
      dumpCommand += " with (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return dump(dumpCommand);
  }

  Tuple dump(String dumpCommand) throws Throwable {
    advanceDumpDir();
    run(dumpCommand);
    String dumpLocation = row0Result(0, false);
    String lastDumpId = row0Result(1, true);
    return new Tuple(dumpLocation, lastDumpId);
  }

  Tuple dump(String dbName, String lastReplicationId) throws Throwable {
    return dump(dbName, lastReplicationId, Collections.emptyList());
  }

  WarehouseInstance dumpFailure(String dbName, String lastReplicationId) throws Throwable {
    String dumpCommand =
            "REPL DUMP " + dbName + (lastReplicationId == null ? "" : " FROM " + lastReplicationId);
    advanceDumpDir();
    runFailure(dumpCommand);
    return this;
  }

  WarehouseInstance load(String replicatedDbName, String dumpLocation) throws Throwable {
    run("EXPLAIN REPL LOAD " + replicatedDbName + " FROM '" + dumpLocation + "'");
    printOutput();
    run("REPL LOAD " + replicatedDbName + " FROM '" + dumpLocation + "'");
    return this;
  }

  WarehouseInstance load(String replicatedDbName, String dumpLocation, List<String> withClauseOptions)
          throws Throwable {
    String replLoadCmd = "REPL LOAD " + replicatedDbName + " FROM '" + dumpLocation + "'";
    if (!withClauseOptions.isEmpty()) {
      replLoadCmd += " WITH (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    run("EXPLAIN " + replLoadCmd);
    printOutput();
    return run(replLoadCmd);
  }

  WarehouseInstance status(String replicatedDbName) throws Throwable {
    String replStatusCmd = "REPL STATUS " + replicatedDbName;
    return run(replStatusCmd);
  }

  WarehouseInstance status(String replicatedDbName, List<String> withClauseOptions) throws Throwable {
    String replStatusCmd = "REPL STATUS " + replicatedDbName;
    if (!withClauseOptions.isEmpty()) {
      replStatusCmd += " WITH (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return run(replStatusCmd);
  }

  WarehouseInstance loadFailure(String replicatedDbName, String dumpLocation) throws Throwable {
    runFailure("REPL LOAD " + replicatedDbName + " FROM '" + dumpLocation + "'");
    return this;
  }

  WarehouseInstance loadFailure(String replicatedDbName, String dumpLocation, List<String> withClauseOptions)
          throws Throwable {
    String replLoadCmd = "REPL LOAD " + replicatedDbName + " FROM '" + dumpLocation + "'";
    if (!withClauseOptions.isEmpty()) {
      replLoadCmd += " WITH (" + StringUtils.join(withClauseOptions, ",") + ")";
    }
    return runFailure(replLoadCmd);
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
    assertTrue(StringUtils.join(filteredResults, ",") + " does not contain all expected" + StringUtils
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

  public Database getDatabase(String dbName) throws Exception {
    try {
      return client.getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return null;
    }
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

  @Override
  public void close() throws IOException {
    if (miniDFSCluster != null && miniDFSCluster.isClusterUp()) {
      miniDFSCluster.shutdown();
    }
  }

  List<Path> copyToHDFS(List<URI> localUris) throws IOException, SemanticException {
    DistributedFileSystem fs = miniDFSCluster.getFileSystem();
    Path destinationBasePath = new Path("/", String.valueOf(System.nanoTime()));
    mkDir(fs, destinationBasePath.toString());
    localUris.forEach(uri -> {
      Path localPath = new Path(uri);
      try {
        FileSystem localFs = localPath.getFileSystem(hiveConf);
        boolean success = FileUtils
            .copy(localFs, localPath, fs, destinationBasePath, false, false, hiveConf);
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
