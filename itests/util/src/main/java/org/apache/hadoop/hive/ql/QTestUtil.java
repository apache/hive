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

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.control.AbstractCliConfig;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.io.SessionStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientWithLocalCache;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.QTestMiniClusters.FsType;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.dataset.QTestDatasetHandler;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.events.NotificationEventPoll;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSources;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.hadoop.hive.ql.qoption.QTestAuthorizerHandler;
import org.apache.hadoop.hive.ql.qoption.QTestDisabledHandler;
import org.apache.hadoop.hive.ql.qoption.QTestDatabaseHandler;
import org.apache.hadoop.hive.ql.qoption.QTestOptionDispatcher;
import org.apache.hadoop.hive.ql.qoption.QTestReplaceHandler;
import org.apache.hadoop.hive.ql.qoption.QTestSysDbHandler;
import org.apache.hadoop.hive.ql.qoption.QTestTimezoneHandler;
import org.apache.hadoop.hive.ql.qoption.QTestTransactional;
import org.apache.hadoop.hive.ql.scheduled.QTestScheduledQueryCleaner;
import org.apache.hadoop.hive.ql.scheduled.QTestScheduledQueryServiceProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.ProcessUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * QTestUtil.
 */
public class QTestUtil {
  private static final Logger LOG = LoggerFactory.getLogger("QTestUtil");

  public static final String QTEST_LEAVE_FILES = "QTEST_LEAVE_FILES";
  private final String[] testOnlyCommands = new String[]{ "crypto", "erasure" };
  public static String DEBUG_HINT =
      "\nSee ./ql/target/tmp/log/hive.log or ./itests/qtest/target/tmp/log/hive.log, "
          + "or check ./ql/target/surefire-reports or ./itests/qtest/target/surefire-reports/ for specific test cases logs.";

  private String testWarehouse;
  @Deprecated private final String testFiles;
  private final String outDir;
  protected final String logDir;
  private File inputFile;
  private String inputContent;
  private final Set<String> srcUDFs;
  private final FsType fsType;
  private ParseDriver pd;
  protected Hive db;
  protected HiveConf conf;
  protected HiveConf savedConf;
  private BaseSemanticAnalyzer sem;
  private CliDriver cliDriver;
  private final QTestMiniClusters miniClusters = new QTestMiniClusters();
  private final QOutProcessor qOutProcessor;
  private static QTestResultProcessor qTestResultProcessor = new QTestResultProcessor();
  protected QTestDatasetHandler datasetHandler;
  protected QTestReplaceHandler replaceHandler;
  private final String initScript;
  private final String cleanupScript;
  QTestOptionDispatcher dispatcher = new QTestOptionDispatcher();

  private boolean isSessionStateStarted = false;

  public CliDriver getCliDriver() {
    if (cliDriver == null) {
      throw new RuntimeException("no clidriver");
    }
    return cliDriver;
  }

  /**
   * Returns the default UDF names which should not be removed when resetting the test database
   *
   * @return The list of the UDF names not to remove
   */
  private Set<String> getSrcUDFs() {
    HashSet<String> srcUDFs = new HashSet<String>();
    // FIXME: moved default value to here...for now
    // i think this features is never really used from the command line
    String defaultTestSrcUDFs = "qtest_get_java_boolean";
    for (String srcUDF : QTestSystemProperties.getSourceUdfs(defaultTestSrcUDFs)) {
      srcUDF = srcUDF.trim();
      if (!srcUDF.isEmpty()) {
        srcUDFs.add(srcUDF);
      }
    }
    if (srcUDFs.isEmpty()) {
      throw new RuntimeException("Source UDFs cannot be empty");
    }
    return srcUDFs;
  }

  public HiveConf getConf() {
    return conf;
  }

  public void initConf() throws Exception {
    if (QTestSystemProperties.isVectorizationEnabled()) {
      conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    }

    // Plug verifying metastore in for testing DirectSQL.
    conf.setVar(ConfVars.METASTORE_RAW_STORE_IMPL, "org.apache.hadoop.hive.metastore.VerifyingObjectStore");

    miniClusters.initConf(conf);
  }

  public QTestUtil(QTestArguments testArgs) throws Exception {
    LOG.info("Setting up QTestUtil with outDir={}, logDir={}, clusterType={}, confDir={},"
            + " initScript={}, cleanupScript={}, withLlapIo={}, fsType={}",
        testArgs.getOutDir(),
        testArgs.getLogDir(),
        testArgs.getClusterType(),
        testArgs.getConfDir(),
        testArgs.getInitScript(),
        testArgs.getCleanupScript(),
        testArgs.isWithLlapIo(),
        testArgs.getFsType());

    logClassPath();

    Preconditions.checkNotNull(testArgs.getClusterType(), "ClusterType cannot be null");

    this.fsType = testArgs.getFsType();
    this.outDir = testArgs.getOutDir();
    this.logDir = testArgs.getLogDir();
    this.srcUDFs = getSrcUDFs();
    this.replaceHandler = new QTestReplaceHandler();
    this.qOutProcessor = new QOutProcessor(fsType, replaceHandler);

    // HIVE-14443 move this fall-back logic to CliConfigs
    if (testArgs.getConfDir() != null && !testArgs.getConfDir().isEmpty()) {
      HiveConf.setHiveSiteLocation(new URL("file://"
          + new File(testArgs.getConfDir()).toURI().getPath()
          + "/hive-site.xml"));
      MetastoreConf.setHiveSiteLocation(HiveConf.getHiveSiteLocation());
      System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    }

    // For testing configurations set by System.setProperties
    System.setProperty("hive.query.max.length", "100Mb");

    conf = new HiveConf(IDriver.class);
    setCustomConfs(conf, testArgs.getCustomConfs());
    setMetaStoreProperties();

    final String scriptsDir = getScriptsDir(conf);
    this.miniClusters.setup(testArgs, conf, scriptsDir, logDir);

    initConf();

    datasetHandler = new QTestDatasetHandler(conf);
    testFiles = datasetHandler.getDataDir(conf);
    conf.set("test.data.dir", datasetHandler.getDataDir(conf));
    conf.setVar(ConfVars.HIVE_QUERY_RESULTS_CACHE_DIRECTORY, "/tmp/hive/_resultscache_" + ProcessUtils.getPid());
    dispatcher.register("dataset", datasetHandler);
    dispatcher.register("replace", replaceHandler);
    dispatcher.register("sysdb", new QTestSysDbHandler());
    dispatcher.register("transactional", new QTestTransactional());
    dispatcher.register("scheduledqueryservice", new QTestScheduledQueryServiceProvider(conf));
    dispatcher.register("scheduledquerycleaner", new QTestScheduledQueryCleaner());
    dispatcher.register("timezone", new QTestTimezoneHandler());
    dispatcher.register("authorizer", new QTestAuthorizerHandler());
    dispatcher.register("disabled", new QTestDisabledHandler());
    dispatcher.register("database", new QTestDatabaseHandler());

    this.initScript = scriptsDir + File.separator + testArgs.getInitScript();
    this.cleanupScript = scriptsDir + File.separator + testArgs.getCleanupScript();

    savedConf = new HiveConf(conf);

  }

  private void setCustomConfs(HiveConf conf, Map<ConfVars,String> customConfigValueMap) {
    customConfigValueMap.entrySet().forEach(item-> conf.set(item.getKey().varname, item.getValue()));
  }

  private void logClassPath() {
    String classpath = System.getProperty("java.class.path");
    String[] classpathEntries = classpath.split(File.pathSeparator);
    LOG.info("QTestUtil classpath: " + String.join("\n", Arrays.asList(classpathEntries)));
  }

  private void setMetaStoreProperties() {
    setMetastoreConfPropertyFromSystemProperty(MetastoreConf.ConfVars.CONNECT_URL_KEY);
    setMetastoreConfPropertyFromSystemProperty(MetastoreConf.ConfVars.CONNECTION_DRIVER);
    setMetastoreConfPropertyFromSystemProperty(MetastoreConf.ConfVars.CONNECTION_USER_NAME);
    setMetastoreConfPropertyFromSystemProperty(MetastoreConf.ConfVars.PWD);
    setMetastoreConfPropertyFromSystemProperty(MetastoreConf.ConfVars.AUTO_CREATE_ALL);
  }

  private void setMetastoreConfPropertyFromSystemProperty(MetastoreConf.ConfVars var) {
    if (System.getProperty(var.getVarname()) != null) {
      if (var.getDefaultVal().getClass() == Boolean.class) {
        MetastoreConf.setBoolVar(conf, var, Boolean.getBoolean(System.getProperty(var.getVarname())));
      } else {
        MetastoreConf.setVar(conf, var, System.getProperty(var.getVarname()));
      }
    }
  }

  public static String getScriptsDir(HiveConf conf) {
    // Use the current directory if it is not specified
    String scriptsDir = conf.get("test.data.scripts");
    if (scriptsDir == null) {
      scriptsDir = new File(".").getAbsolutePath() + "/data/scripts";
    }
    return scriptsDir;
  }

  public void shutdown() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) == null) {
      cleanUp();
    }

    miniClusters.shutDown();

    Hive.closeCurrent();
  }

  public void setInputFile(String queryFile) throws IOException {
    setInputFile(new File(queryFile));
  }

  public void setInputFile(File qf) throws IOException {
    String query = FileUtils.readFileToString(qf);
    inputFile = qf;
    inputContent = query;
    qTestResultProcessor.init(query);
    qOutProcessor.initMasks(query);
  }

  public final File getInputFile() {
    return inputFile;
  }

  /**
   * Clear out any side effects of running tests
   */
  public void clearPostTestEffects() throws Exception {
    dispatcher.afterTest(this);
    miniClusters.postTest(conf);
  }

  public void clearKeysCreatedInTests() {
    if (miniClusters.getHdfsEncryptionShim() == null) {
      return;
    }
    try {
      for (String keyAlias : miniClusters.getHdfsEncryptionShim().getKeys()) {
        miniClusters.getHdfsEncryptionShim().deleteKey(keyAlias);
      }
    } catch (IOException e) {
      LOG.error("Fail to clean the keys created in test due to the error", e);
    }
  }

  public void clearUDFsCreatedDuringTests() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }
    // Delete functions created by the tests
    // It is enough to remove functions from the default database, other databases are dropped
    for (String udfName : db.getFunctions(DEFAULT_DATABASE_NAME, ".*")) {
      if (!srcUDFs.contains(udfName)) {
        db.dropFunction(DEFAULT_DATABASE_NAME, udfName);
      }
    }
  }

  /**
   * Clear out any side effects of running tests
   */
  public void clearTablesCreatedDuringTests() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }

    conf.set("hive.metastore.filter.hook", "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl");
    db = Hive.get(conf);

    // First delete any MVs to avoid race conditions
    for (String dbName : db.getAllDatabases()) {
      SessionState.get().setCurrentDatabase(dbName);
      for (String tblName : db.getAllTables()) {
        Table tblObj = null;
        try {
          tblObj = db.getTable(tblName);
        } catch (InvalidTableException e) {
          LOG.warn("Trying to drop table " + e.getTableName() + ". But it does not exist.");
          continue;
        }
        // only remove MVs first
        if (!tblObj.isMaterializedView()) {
          continue;
        }
        db.dropTable(dbName, tblName, true, true, fsType == FsType.ENCRYPTED_HDFS);
        HiveMaterializedViewsRegistry.get().dropMaterializedView(tblObj.getDbName(), tblObj.getTableName());
      }
    }

    // Delete any tables other than the source tables
    // and any databases other than the default database.
    for (String dbName : db.getAllDatabases()) {
      SessionState.get().setCurrentDatabase(dbName);
      // FIXME: HIVE-24130 should remove this
      if (dbName.equalsIgnoreCase("tpch_0_001")) {
        continue;
      }
      for (String tblName : db.getAllTables()) {
        if (!DEFAULT_DATABASE_NAME.equals(dbName) || !QTestDatasetHandler.isSourceTable(tblName)) {
          try {
            db.getTable(tblName);
          } catch (InvalidTableException e) {
            LOG.warn("Trying to drop table " + e.getTableName() + ". But it does not exist.");
            continue;
          }
          db.dropTable(dbName, tblName, true, true, miniClusters.fsNeedsPurge(fsType));
        }
      }
      if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
        // Drop cascade, functions dropped by cascade
        db.dropDatabase(dbName, true, true, true);
      }
    }

    // delete remaining directories for external tables (can affect stats for following tests)
    try {
      Path p = new Path(testWarehouse);
      FileSystem fileSystem = p.getFileSystem(conf);
      if (fileSystem.exists(p)) {
        for (FileStatus status : fileSystem.listStatus(p)) {
          if (status.isDirectory() && !QTestDatasetHandler.isSourceTable(status.getPath().getName())) {
            fileSystem.delete(status.getPath(), true);
          }
        }
      }
    } catch (IllegalArgumentException e) {
      // ignore.. provides invalid url sometimes intentionally
    }
    SessionState.get().setCurrentDatabase(DEFAULT_DATABASE_NAME);

    List<String> roleNames = db.getAllRoleNames();
    for (String roleName : roleNames) {
      if (!"PUBLIC".equalsIgnoreCase(roleName) && !"ADMIN".equalsIgnoreCase(roleName)) {
        db.dropRole(roleName);
      }
    }
  }

  public void newSession() throws Exception {
    newSession(true);
  }

  public void newSession(boolean canReuseSession) throws Exception {
    // allocate and initialize a new conf since a test can
    // modify conf by using 'set' commands
    conf = new HiveConf(savedConf);
    initConf();
    initConfFromSetup();

    // renew the metastore since the cluster type is unencrypted
    db = Hive.get(conf); // propagate new conf to meta store

    HiveConf.setVar(conf,
        HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        "org.apache.hadoop.hive.ql.security.DummyAuthenticator");
    CliSessionState ss = new CliSessionState(conf);
    ss.in = System.in;

    SessionState oldSs = SessionState.get();
    miniClusters.restartSessions(canReuseSession, ss, oldSs);
    closeSession(oldSs);

    SessionState.start(ss);

    cliDriver = new CliDriver();

    File outf = new File(logDir, "initialize.log");
    setSessionOutputs(ss, outf);

  }

  /**
   * Clear out any side effects of running tests
   */
  public void clearTestSideEffects() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }
    // the test might have configured security/etc; open a new session to get rid of that
    newSession();

    // Remove any cached results from the previous test.
    Utilities.clearWorkMap(conf);
    NotificationEventPoll.shutdown();
    QueryResultsCache.cleanupInstance();
    clearTablesCreatedDuringTests();
    clearUDFsCreatedDuringTests();
    clearKeysCreatedInTests();
    StatsSources.clearGlobalStats();
    dispatcher.afterTest(this);
  }

  protected void initConfFromSetup() throws Exception {
    miniClusters.preTest(conf);
  }

  public void cleanUp() throws Exception {
    if (!isSessionStateStarted) {
      startSessionState(qTestResultProcessor.canReuseSession());
    }
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }
    conf.setBoolean("hive.test.shutdown.phase", true);

    clearKeysCreatedInTests();

    String metastoreDb = QTestSystemProperties.getMetaStoreDb();
    if (metastoreDb == null || "derby".equalsIgnoreCase(metastoreDb)) {
      // otherwise, the docker container is already destroyed by this time
      cleanupFromFile();
    }

    // delete any contents in the warehouse dir
    Path p = new Path(testWarehouse);
    FileSystem fs = p.getFileSystem(conf);

    try {
      FileStatus[] ls = fs.listStatus(p);
      for (int i = 0; (ls != null) && (i < ls.length); i++) {
        fs.delete(ls[i].getPath(), true);
      }
    } catch (FileNotFoundException e) {
      // Best effort
    }

    // TODO: Clean up all the other paths that are created.

    FunctionRegistry.unregisterTemporaryUDF("test_udaf");
    FunctionRegistry.unregisterTemporaryUDF("test_error");
  }

  private void cleanupFromFile() throws IOException {
    File cleanupFile = new File(cleanupScript);
    if (cleanupFile.isFile()) {
      String cleanupCommands = FileUtils.readFileToString(cleanupFile);
      LOG.info("Cleanup (" + cleanupScript + "):\n" + cleanupCommands);

      try {
        getCliDriver().processLine(cleanupCommands);
      } catch (CommandProcessorException e) {
        LOG.error("Failed during cleanup processLine with code={}. Ignoring", e.getResponseCode());
        // TODO Convert this to an Assert.fail once HIVE-14682 is fixed
      }
    } else {
      LOG.info("No cleanup script detected. Skipping.");
    }
  }

  public void createSources() throws Exception {
    if (!isSessionStateStarted) {
      startSessionState(qTestResultProcessor.canReuseSession());
    }

    getCliDriver().processLine("set test.data.dir=" + testFiles + ";");

    conf.setBoolean("hive.test.init.phase", true);

    initFromScript();

    conf.setBoolean("hive.test.init.phase", false);
  }

  private void initFromScript() throws IOException {
    File scriptFile = new File(this.initScript);
    if (!scriptFile.isFile()) {
      LOG.info("No init script detected. Skipping");
      return;
    }

    String initCommands = FileUtils.readFileToString(scriptFile);
    LOG.info("Initial setup (" + initScript + "):\n" + initCommands);

    try {
      cliDriver.processLine(initCommands);
      LOG.info("Result from cliDrriver.processLine in createSources=0");
    } catch (CommandProcessorException e) {
      Assert.fail("Failed during createSources processLine with code=" + e.getResponseCode());
    }
  }

  public void postInit() throws Exception {
    miniClusters.postInit(conf);

    sem = new SemanticAnalyzer(new QueryState.Builder().withHiveConf(conf).build());

    testWarehouse = conf.getVar(HiveConf.ConfVars.METASTORE_WAREHOUSE);

    db = Hive.get(conf);
    pd = new ParseDriver();

    initMaterializedViews(); // Create views registry
    firstStartSessionState();

    // setup metastore client cache
    if (conf.getBoolVar(ConfVars.MSC_CACHE_ENABLED)) {
      HiveMetaStoreClientWithLocalCache.init(conf);
    }
  }

  private void initMaterializedViews() {
    String registryImpl = db.getConf().get("hive.server2.materializedviews.registry.impl");
    db.getConf().set("hive.server2.materializedviews.registry.impl", "DUMMY");
    HiveMaterializedViewsRegistry.get().init(db);
    db.getConf().set("hive.server2.materializedviews.registry.impl", registryImpl);
  }

  //FIXME: check why mr is needed for starting a session state from conf
  private void firstStartSessionState() {
    String execEngine = conf.get("hive.execution.engine");
    conf.set("hive.execution.engine", "mr");
    SessionState.start(conf);
    conf.set("hive.execution.engine", execEngine);
  }

  public String cliInit() throws Exception {
    File file = Objects.requireNonNull(inputFile);
    String fileName = inputFile.getName();

    dispatcher.process(file);
    dispatcher.beforeTest(this);

    if (!qTestResultProcessor.canReuseSession()) {
      newSession(false);
    }

    CliSessionState ss = (CliSessionState) SessionState.get();

    String outFileExtension = getOutFileExtension();
    String stdoutName = null;

    if (outDir != null) {
      // TODO: why is this needed?
      File qf = new File(outDir, fileName);
      stdoutName = qf.getName().concat(outFileExtension);
    } else {
      stdoutName = fileName + outFileExtension;
    }
    File outf = new File(logDir, stdoutName);
    setSessionOutputs(ss, outf);
    ss.setIsQtestLogging(true);

    if (fileName.equals("init_file.q")) {
      ss.initFiles.add(AbstractCliConfig.HIVE_ROOT + "/data/scripts/test_init_file.sql");
    }
    cliDriver.processInitFiles(ss);

    return outf.getAbsolutePath();
  }

  private void setSessionOutputs(CliSessionState ss, File outf) throws Exception {
    OutputStream fo = new BufferedOutputStream(new FileOutputStream(outf));
    if (ss.out != null) {
      ss.out.flush();
    }
    if (ss.err != null) {
      ss.err.flush();
    }

    qTestResultProcessor.setOutputs(ss, fo);

    ss.err = new CachingPrintStream(fo, true, "UTF-8");
    ss.setIsSilent(true);
    ss.setIsQtestLogging(true);
  }

  public CliSessionState startSessionState(boolean canReuseSession) throws IOException {

    HiveConf.setVar(conf,
        HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        "org.apache.hadoop.hive.ql.security.DummyAuthenticator");

    //FIXME: check why mr is needed for starting a session state from conf
    String execEngine = conf.get("hive.execution.engine");
    conf.set("hive.execution.engine", "mr");

    CliSessionState ss = new CliSessionState(conf);
    ss.in = System.in;
    ss.out = new SessionStream(System.out);
    ss.err = new SessionStream(System.out);

    SessionState oldSs = SessionState.get();

    miniClusters.restartSessions(canReuseSession, ss, oldSs);

    closeSession(oldSs);
    SessionState.start(ss);

    isSessionStateStarted = true;

    conf.set("hive.execution.engine", execEngine);
    return ss;
  }

  private void closeSession(SessionState oldSs) throws IOException {
    if (oldSs != null && oldSs.out != null && oldSs.out != System.out) {
      oldSs.out.close();
    }
    if (oldSs != null) {
      oldSs.close();
    }
  }

  public int executeAdHocCommand(String q) throws CommandProcessorException {
    if (!q.contains(";")) {
      return -1;
    }

    String q1 = q.split(";")[0] + ";";

    LOG.debug("Executing " + q1);
    cliDriver.processLine(q1);
    return 0;
  }

  public CommandProcessorResponse executeClient() throws CommandProcessorException {
    return executeClientInternal(getCommand());
  }

  private CommandProcessorResponse executeClientInternal(String commands) throws CommandProcessorException {
    List<String> cmds = CliDriver.splitSemiColon(commands);
    CommandProcessorResponse response = new CommandProcessorResponse();

    StringBuilder command = new StringBuilder();
    QTestSyntaxUtil qtsu = new QTestSyntaxUtil(this, conf, pd);
    qtsu.checkQFileSyntax(cmds);

    for (String oneCmd : cmds) {
      if (StringUtils.endsWith(oneCmd, "\\")) {
        command.append(StringUtils.chop(oneCmd) + "\\;");
        continue;
      } else {
        if (isHiveCommand(oneCmd)) {
          command.setLength(0);
        }
        command.append(oneCmd);
      }
      if (StringUtils.isBlank(command.toString())) {
        continue;
      }

      String strCommand = command.toString();
      try {
        if (isCommandUsedForTesting(strCommand)) {
          response = executeTestCommand(strCommand);
        } else {
          response = cliDriver.processLine(strCommand);
        }
      } catch (CommandProcessorException e) {
        if (!ignoreErrors()) {
          throw e;
        }
      }
      command.setLength(0);
    }
    if (SessionState.get() != null) {
      SessionState.get().setLastCommand(null);  // reset
    }
    return response;
  }

/**
   * This allows a .q file to continue executing after a statement runs into an error which is convenient
   * if you want to use another hive cmd after the failure to sanity check the state of the system.
   */
  private boolean ignoreErrors() {
    return conf.getBoolVar(HiveConf.ConfVars.CLI_IGNORE_ERRORS);
  }

  boolean isHiveCommand(String command) {
    String[] cmd = command.trim().split("\\s+");
    if (HiveCommand.find(cmd) != null) {
      return true;
    } else if (HiveCommand.find(cmd, HiveCommand.ONLY_FOR_TESTING) != null) {
      return true;
    } else {
      return false;
    }
  }

  private CommandProcessorResponse executeTestCommand(String command) throws CommandProcessorException {
    String commandName = command.trim().split("\\s+")[0];
    String commandArgs = command.trim().substring(commandName.length());

    if (commandArgs.endsWith(";")) {
      commandArgs = StringUtils.chop(commandArgs);
    }

    //replace ${hiveconf:hive.metastore.warehouse.dir} with actual dir if existed.
    //we only want the absolute path, so remove the header, such as hdfs://localhost:57145
    String wareHouseDir =
        SessionState.get().getConf().getVar(ConfVars.METASTORE_WAREHOUSE).replaceAll("^[a-zA-Z]+://.*?:\\d+", "");
    commandArgs = commandArgs.replaceAll("\\$\\{hiveconf:hive\\.metastore\\.warehouse\\.dir\\}", wareHouseDir);

    if (SessionState.get() != null) {
      SessionState.get().setLastCommand(commandName + " " + commandArgs.trim());
    }

    enableTestOnlyCmd(SessionState.get().getConf());

    try {
      CommandProcessor proc = getTestCommand(commandName);
      if (proc != null) {
        try {
          CommandProcessorResponse response = proc.run(commandArgs.trim());
          return response;
        } catch (CommandProcessorException e) {
          SessionState.getConsole().printError(e.toString(),
                  e.getCause() != null ? Throwables.getStackTraceAsString(e.getCause()) : "");
          throw e;
        }
      } else {
        throw new RuntimeException("Could not get CommandProcessor for command: " + commandName);
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not execute test command", e);
    }
  }

  private CommandProcessor getTestCommand(final String commandName) throws SQLException {
    HiveCommand testCommand = HiveCommand.find(new String[]{ commandName }, HiveCommand.ONLY_FOR_TESTING);

    if (testCommand == null) {
      return null;
    }

    return CommandProcessorFactory.getForHiveCommandInternal(new String[]{ commandName },
        SessionState.get().getConf(),
        testCommand.isOnlyForTesting());
  }

  private void enableTestOnlyCmd(HiveConf conf) {
    StringBuilder securityCMDs = new StringBuilder(conf.getVar(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST));
    for (String c : testOnlyCommands) {
      securityCMDs.append(",");
      securityCMDs.append(c);
    }
    conf.set(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST.toString(), securityCMDs.toString());
  }

  private boolean isCommandUsedForTesting(final String command) {
    String commandName = command.trim().split("\\s+")[0];
    HiveCommand testCommand = HiveCommand.find(new String[]{ commandName }, HiveCommand.ONLY_FOR_TESTING);
    return testCommand != null;
  }

  private String getCommand() {
    String commands = inputContent;
    StringBuilder newCommands = new StringBuilder(commands.length());
    int lastMatchEnd = 0;
    Matcher commentMatcher = Pattern.compile("^--.*$", Pattern.MULTILINE).matcher(commands);
    // remove the comments
    while (commentMatcher.find()) {
      newCommands.append(commands.substring(lastMatchEnd, commentMatcher.start()));
      lastMatchEnd = commentMatcher.end();
    }
    newCommands.append(commands.substring(lastMatchEnd, commands.length()));
    commands = newCommands.toString();
    return commands;
  }

  private String getOutFileExtension() {
    return ".out";
  }

  public QTestProcessExecResult checkNegativeResults(String tname, Exception e) throws Exception {

    String outFileExtension = getOutFileExtension();

    File qf = new File(outDir, tname);
    String expf = outPath(outDir.toString(), tname.concat(outFileExtension));

    File outf = null;
    outf = new File(logDir);
    outf = new File(outf, qf.getName().concat(outFileExtension));

    FileWriter outfd = new FileWriter(outf);
    if (e instanceof ParseException) {
      outfd.write("Parse Error: ");
    } else if (e instanceof SemanticException) {
      outfd.write("Semantic Exception: \n");
    } else {
      outfd.close();
      throw e;
    }

    outfd.write(e.getMessage());
    outfd.close();

    QTestProcessExecResult result = qTestResultProcessor.executeDiffCommand(outf.getPath(), expf, false);
    if (QTestSystemProperties.shouldOverwriteResults()) {
      qTestResultProcessor.overwriteResults(outf.getPath(), expf);
      return QTestProcessExecResult.createWithoutOutput(0);
    }

    return result;
  }

  public QTestProcessExecResult checkNegativeResults(String tname, Error e) throws Exception {

    String outFileExtension = getOutFileExtension();

    File qf = new File(outDir, tname);
    String expf = outPath(outDir.toString(), tname.concat(outFileExtension));

    File outf = null;
    outf = new File(logDir);
    outf = new File(outf, qf.getName().concat(outFileExtension));

    FileWriter outfd = new FileWriter(outf, true);

    outfd.write("FAILED: "
        + e.getClass().getSimpleName()
        + " "
        + e.getClass().getName()
        + ": "
        + e.getMessage()
        + "\n");
    outfd.close();

    QTestProcessExecResult result = qTestResultProcessor.executeDiffCommand(outf.getPath(), expf, false);
    if (QTestSystemProperties.shouldOverwriteResults()) {
      qTestResultProcessor.overwriteResults(outf.getPath(), expf);
      return QTestProcessExecResult.createWithoutOutput(0);
    }

    return result;
  }

  /**
   * Given the current configurations (e.g., hadoop version and execution mode), return
   * the correct file name to compare with the current test run output.
   *
   * @param outDir   The directory where the reference log files are stored.
   * @param testName The test file name (terminated by ".out").
   * @return The file name appended with the configuration values if it exists.
   */
  public String outPath(String outDir, String testName) {
    String ret = (new File(outDir, testName)).getPath();
    // List of configurations. Currently the list consists of hadoop version and execution mode only
    List<String> configs = new ArrayList<String>();
    configs.add(miniClusters.getClusterType().getQOutFileExtensionPostfix());

    Deque<String> stack = new LinkedList<String>();
    StringBuilder sb = new StringBuilder();
    sb.append(testName);
    stack.push(sb.toString());

    // example file names are input1.q.out_mr_0.17 or input2.q.out_0.17
    for (String s : configs) {
      sb.append('_');
      sb.append(s);
      stack.push(sb.toString());
    }
    while (stack.size() > 0) {
      String fileName = stack.pop();
      File f = new File(outDir, fileName);
      if (f.exists()) {
        ret = f.getPath();
        break;
      }
    }
    return ret;
  }

  public QTestProcessExecResult checkCliDriverResults() throws Exception {
    String tname = inputFile.getName(); 

    String outFileExtension = getOutFileExtension();
    String outFileName = outPath(outDir, tname + outFileExtension);

    File f = new File(logDir, tname + outFileExtension);
    qOutProcessor.maskPatterns(f.getPath());

    if (QTestSystemProperties.shouldOverwriteResults()) {
      qTestResultProcessor.overwriteResults(f.getPath(), outFileName);
      return QTestProcessExecResult.createWithoutOutput(0);
    } else {
      return qTestResultProcessor.executeDiffCommand(f.getPath(), outFileName, false);
    }
  }

  public ASTNode parseQuery() throws Exception {
    return pd.parse(inputContent).getTree();
  }

  public List<Task<?>> analyzeAST(ASTNode ast) throws Exception {

    // Do semantic analysis and plan generation
    Context ctx = new Context(conf);
    while ((ast.getToken() == null) && (ast.getChildCount() > 0)) {
      ast = (ASTNode) ast.getChild(0);
    }
    sem.getOutputs().clear();
    sem.getInputs().clear();
    sem.analyze(ast, ctx);
    ctx.clear();
    return sem.getRootTasks();
  }

  // for negative tests, which is succeeded.. no need to print the query string
  public void failed(String fname, String debugHint) {
    Assert.fail("Client Execution was expected to fail, but succeeded with error code 0 for fname=" + fname + (debugHint
        != null ? (" " + debugHint) : ""));
  }

  public void failedDiff(int ecode, String fname, String debugHint) {
    String
        message =
        "Client Execution succeeded but contained differences "
            + "(error code = "
            + ecode
            + ") after executing "
            + fname
            + (debugHint != null ? (" " + debugHint) : "");
    LOG.error(message);
    Assert.fail(message);
  }

  public void failedQuery(Throwable e, int ecode, String fname, String debugHint) {
    String command = SessionState.get() != null ? SessionState.get().getLastCommand() : null;

    String message = String.format(
        "Client execution failed with error code = %d %nrunning %s %nfname=%s%n%s%n %s", ecode,
        command != null ? command : "", fname, debugHint != null ? debugHint : "",
        e == null ? "" : org.apache.hadoop.util.StringUtils.stringifyException(e));
    LOG.error(message);
    Assert.fail(message);
  }

  public void failedWithException(Exception e, String fname, String debugHint) {
    String command = SessionState.get() != null ? SessionState.get().getLastCommand() : null;
    System.err.println("Failed query: " + fname);
    System.err.flush();
    Assert.fail("Unexpected exception " + org.apache.hadoop.util.StringUtils.stringifyException(e) + "\n" + (command
        != null ? " running " + command : "") + (debugHint != null ? debugHint : ""));
  }

  public QOutProcessor getQOutProcessor() {
    return qOutProcessor;
  }

  public static void initEventNotificationPoll() throws Exception {
    NotificationEventPoll.initialize(SessionState.get().getConf());
  }
}
