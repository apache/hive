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

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.RuntimeException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;
import junit.framework.TestSuite;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.io.DigestPrintStream;
import org.apache.hadoop.hive.common.io.SortAndDigestPrintStream;
import org.apache.hadoop.hive.common.io.SortPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.Shell;
import org.apache.hive.common.util.StreamPrinter;
import org.apache.tools.ant.BuildException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.collect.ImmutableList;

/**
 * QTestUtil.
 *
 */
public class QTestUtil {

  public static final String UTF_8 = "UTF-8";

  // security property names
  private static final String SECURITY_KEY_PROVIDER_URI_NAME = "dfs.encryption.key.provider.uri";
  private static final String CRLF = System.getProperty("line.separator");

  private static final Log LOG = LogFactory.getLog("QTestUtil");
  private static final String QTEST_LEAVE_FILES = "QTEST_LEAVE_FILES";
  private final String defaultInitScript = "q_test_init.sql";
  private final String defaultCleanupScript = "q_test_cleanup.sql";
  private final String[] testOnlyCommands = new String[]{"crypto"};

  private String testWarehouse;
  private final String testFiles;
  protected final String outDir;
  protected final String logDir;
  private final TreeMap<String, String> qMap;
  private final Set<String> qSkipSet;
  private final Set<String> qSortSet;
  private final Set<String> qSortQuerySet;
  private final Set<String> qHashQuerySet;
  private final Set<String> qSortNHashQuerySet;
  private final Set<String> qJavaVersionSpecificOutput;
  private static final String SORT_SUFFIX = ".sorted";
  public static final HashSet<String> srcTables = new HashSet<String>();
  private static MiniClusterType clusterType = MiniClusterType.none;
  private ParseDriver pd;
  protected Hive db;
  protected HiveConf conf;
  private Driver drv;
  private BaseSemanticAnalyzer sem;
  protected final boolean overWrite;
  private CliDriver cliDriver;
  private HadoopShims.MiniMrShim mr = null;
  private HadoopShims.MiniDFSShim dfs = null;
  private HadoopShims.HdfsEncryptionShim hes = null;
  private boolean miniMr = false;
  private String hadoopVer = null;
  private QTestSetup setup = null;
  private SparkSession sparkSession = null;
  private boolean isSessionStateStarted = false;
  private static final String javaVersion = getJavaVersion();

  private final String initScript;
  private final String cleanupScript;

  public interface SuiteAddTestFunctor {
    public void addTestToSuite(TestSuite suite, Object setup, String tName);
  }

  static {
    for (String srcTable : System.getProperty("test.src.tables", "").trim().split(",")) {
      srcTable = srcTable.trim();
      if (!srcTable.isEmpty()) {
        srcTables.add(srcTable);
      }
    }
    if (srcTables.isEmpty()) {
      throw new AssertionError("Source tables cannot be empty");
    }
  }

  public HiveConf getConf() {
    return conf;
  }

  public boolean deleteDirectory(File path) {
    if (path.exists()) {
      File[] files = path.listFiles();
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    return (path.delete());
  }

  public void copyDirectoryToLocal(Path src, Path dest) throws Exception {

    FileSystem srcFs = src.getFileSystem(conf);
    FileSystem destFs = dest.getFileSystem(conf);
    if (srcFs.exists(src)) {
      FileStatus[] files = srcFs.listStatus(src);
      for (FileStatus file : files) {
        String name = file.getPath().getName();
        Path dfs_path = file.getPath();
        Path local_path = new Path(dest, name);

        // If this is a source table we do not copy it out
        if (srcTables.contains(name)) {
          continue;
        }

        if (file.isDir()) {
          if (!destFs.exists(local_path)) {
            destFs.mkdirs(local_path);
          }
          copyDirectoryToLocal(dfs_path, local_path);
        } else {
          srcFs.copyToLocalFile(dfs_path, local_path);
        }
      }
    }
  }

  static Pattern mapTok = Pattern.compile("(\\.?)(.*)_map_(.*)");
  static Pattern reduceTok = Pattern.compile("(.*)(reduce_[^\\.]*)((\\..*)?)");

  public void normalizeNames(File path) throws Exception {
    if (path.isDirectory()) {
      File[] files = path.listFiles();
      for (File file : files) {
        normalizeNames(file);
      }
    } else {
      Matcher m = reduceTok.matcher(path.getName());
      if (m.matches()) {
        String name = m.group(1) + "reduce" + m.group(3);
        path.renameTo(new File(path.getParent(), name));
      } else {
        m = mapTok.matcher(path.getName());
        if (m.matches()) {
          String name = m.group(1) + "map_" + m.group(3);
          path.renameTo(new File(path.getParent(), name));
        }
      }
    }
  }

  public QTestUtil(String outDir, String logDir, String initScript, String cleanupScript) throws
      Exception {
    this(outDir, logDir, MiniClusterType.none, null, "0.20", initScript, cleanupScript);
  }

  public String getOutputDirectory() {
    return outDir;
  }

  public String getLogDirectory() {
    return logDir;
  }

  private String getHadoopMainVersion(String input) {
    if (input == null) {
      return null;
    }
    Pattern p = Pattern.compile("^(\\d+\\.\\d+).*");
    Matcher m = p.matcher(input);
    if (m.matches()) {
      return m.group(1);
    }
    return null;
  }

  public void initConf() throws Exception {

    String vectorizationEnabled = System.getProperty("test.vectorization.enabled");
    if(vectorizationEnabled != null && vectorizationEnabled.equalsIgnoreCase("true")) {
      conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    }

    // Plug verifying metastore in for testing.
    conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
      "org.apache.hadoop.hive.metastore.VerifyingObjectStore");

    if (mr != null) {
      assert dfs != null;

      mr.setupConfiguration(conf);

      // set fs.default.name to the uri of mini-dfs
      String dfsUriString = WindowsPathUtil.getHdfsUriString(dfs.getFileSystem().getUri().toString());
      conf.setVar(HiveConf.ConfVars.HADOOPFS, dfsUriString);
      // hive.metastore.warehouse.dir needs to be set relative to the mini-dfs
      conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,
                  (new Path(dfsUriString,
                            "/build/ql/test/data/warehouse/")).toString());
    }

    // Windows paths should be converted after MiniMrShim.setupConfiguration()
    // since setupConfiguration may overwrite configuration values.
    if (Shell.WINDOWS) {
      WindowsPathUtil.convertPathsFromWindowsToHdfs(conf);
    }
  }

  public enum MiniClusterType {
    mr,
    tez,
    spark,
    encrypted,
    miniSparkOnYarn,
    none;

    public static MiniClusterType valueForString(String type) {
      if (type.equals("miniMR")) {
        return mr;
      } else if (type.equals("tez")) {
        return tez;
      } else if (type.equals("spark")) {
        return spark;
      } else if (type.equals("encrypted")) {
        return encrypted;
      } else if (type.equals("miniSparkOnYarn")) {
        return miniSparkOnYarn;
      } else {
        return none;
      }
    }
  }

  public QTestUtil(String outDir, String logDir, MiniClusterType clusterType, String hadoopVer,
                   String initScript, String cleanupScript)
    throws Exception {
    this(outDir, logDir, clusterType, null, hadoopVer, initScript, cleanupScript);
  }

  private String getKeyProviderURI() {
    // Use the target directory if it is not specified
    String HIVE_ROOT = QTestUtil.ensurePathEndsInSlash(System.getProperty("hive.root"));
    String keyDir = HIVE_ROOT + "ql/target/";

    // put the jks file in the current test path only for test purpose
    return "jceks://file" + new Path(keyDir, "test.jks").toUri();
  }

  public QTestUtil(String outDir, String logDir, MiniClusterType clusterType,
      String confDir, String hadoopVer, String initScript, String cleanupScript)
    throws Exception {
    this.outDir = outDir;
    this.logDir = logDir;
    if (confDir != null && !confDir.isEmpty()) {
      HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
      System.out.println("Setting hive-site: "+HiveConf.getHiveSiteLocation());
    }
    conf = new HiveConf(Driver.class);
    this.hadoopVer = getHadoopMainVersion(hadoopVer);
    qMap = new TreeMap<String, String>();
    qSkipSet = new HashSet<String>();
    qSortSet = new HashSet<String>();
    qSortQuerySet = new HashSet<String>();
    qHashQuerySet = new HashSet<String>();
    qSortNHashQuerySet = new HashSet<String>();
    qJavaVersionSpecificOutput = new HashSet<String>();
    this.clusterType = clusterType;

    HadoopShims shims = ShimLoader.getHadoopShims();
    int numberOfDataNodes = 4;

    if (clusterType != MiniClusterType.none && clusterType != MiniClusterType.spark) {
      FileSystem fs = null;

      if (clusterType == MiniClusterType.encrypted) {
        // Set the security key provider so that the MiniDFS cluster is initialized
        // with encryption
        conf.set(SECURITY_KEY_PROVIDER_URI_NAME, getKeyProviderURI());
        conf.setInt("fs.trash.interval", 50);

        dfs = shims.getMiniDfs(conf, numberOfDataNodes, true, null);
        fs = dfs.getFileSystem();

        // set up the java key provider for encrypted hdfs cluster
        hes = shims.createHdfsEncryptionShim(fs, conf);

        LOG.info("key provider is initialized");
      } else {
        dfs = shims.getMiniDfs(conf, numberOfDataNodes, true, null);
        fs = dfs.getFileSystem();
      }

      String uriString = WindowsPathUtil.getHdfsUriString(fs.getUri().toString());
      if (clusterType == MiniClusterType.tez) {
        mr = shims.getMiniTezCluster(conf, 4, uriString, 1);
      } else if (clusterType == MiniClusterType.miniSparkOnYarn) {
        mr = shims.getMiniSparkCluster(conf, 4, uriString, 1);
      } else {
        mr = shims.getMiniMrCluster(conf, 4, uriString, 1);
      }
    }

    initConf();

    // Use the current directory if it is not specified
    String dataDir = conf.get("test.data.files");
    if (dataDir == null) {
      dataDir = new File(".").getAbsolutePath() + "/data/files";
    }
    testFiles = dataDir;

    // Use the current directory if it is not specified
    String scriptsDir = conf.get("test.data.scripts");
    if (scriptsDir == null) {
      scriptsDir = new File(".").getAbsolutePath() + "/data/scripts";
    }
    if (initScript.isEmpty()) {
      initScript = defaultInitScript;
    }
    if (cleanupScript.isEmpty()) {
      cleanupScript = defaultCleanupScript;
    }
    this.initScript = scriptsDir + "/" + initScript;
    this.cleanupScript = scriptsDir + "/" + cleanupScript;

    overWrite = "true".equalsIgnoreCase(System.getProperty("test.output.overwrite"));

    setup = new QTestSetup();
    setup.preTest(conf);
    init();
  }

  public void shutdown() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) == null) {
      cleanUp();
    }

    setup.tearDown();
    if (sparkSession != null) {
      try {
        SparkSessionManagerImpl.getInstance().closeSession(sparkSession);
      } catch (Exception ex) {
        LOG.error("Error closing spark session.", ex);
      } finally {
        sparkSession = null;
      }
    }
    if (mr != null) {
      mr.shutdown();
      mr = null;
    }
    FileSystem.closeAll();
    if (dfs != null) {
      dfs.shutdown();
      dfs = null;
    }
  }

  public String readEntireFileIntoString(File queryFile) throws IOException {
    InputStreamReader isr = new InputStreamReader(
        new BufferedInputStream(new FileInputStream(queryFile)), QTestUtil.UTF_8);
    StringWriter sw = new StringWriter();
    try {
      IOUtils.copy(isr, sw);
    } finally {
      if (isr != null) {
        isr.close();
      }
    }
    return sw.toString();
  }

  public void addFile(String queryFile) throws IOException {
    addFile(queryFile, false);
  }

  public void addFile(String queryFile, boolean partial) throws IOException {
    addFile(new File(queryFile));
  }

  public void addFile(File qf) throws IOException {
    addFile(qf, false);
  }

  public void addFile(File qf, boolean partial) throws IOException  {
    String query = readEntireFileIntoString(qf);
    qMap.put(qf.getName(), query);
    if (partial) return;

    if(checkHadoopVersionExclude(qf.getName(), query)
      || checkOSExclude(qf.getName(), query)) {
      qSkipSet.add(qf.getName());
    }

    if (checkNeedJavaSpecificOutput(qf.getName(), query)) {
      qJavaVersionSpecificOutput.add(qf.getName());
    }

    if (matches(SORT_BEFORE_DIFF, query)) {
      qSortSet.add(qf.getName());
    } else if (matches(SORT_QUERY_RESULTS, query)) {
      qSortQuerySet.add(qf.getName());
    } else if (matches(HASH_QUERY_RESULTS, query)) {
      qHashQuerySet.add(qf.getName());
    } else if (matches(SORT_AND_HASH_QUERY_RESULTS, query)) {
      qSortNHashQuerySet.add(qf.getName());
    }
  }

  private static final Pattern SORT_BEFORE_DIFF = Pattern.compile("-- SORT_BEFORE_DIFF");
  private static final Pattern SORT_QUERY_RESULTS = Pattern.compile("-- SORT_QUERY_RESULTS");
  private static final Pattern HASH_QUERY_RESULTS = Pattern.compile("-- HASH_QUERY_RESULTS");
  private static final Pattern SORT_AND_HASH_QUERY_RESULTS = Pattern.compile("-- SORT_AND_HASH_QUERY_RESULTS");

  private boolean matches(Pattern pattern, String query) {
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) {
      return true;
    }
    return false;
  }

  private boolean checkHadoopVersionExclude(String fileName, String query){

    // Look for a hint to not run a test on some Hadoop versions
    Pattern pattern = Pattern.compile("-- (EX|IN)CLUDE_HADOOP_MAJOR_VERSIONS\\((.*)\\)");

    boolean excludeQuery = false;
    boolean includeQuery = false;
    Set<String> versionSet = new HashSet<String>();
    String hadoopVer = ShimLoader.getMajorVersion();

    Matcher matcher = pattern.matcher(query);

    // Each qfile may include at most one INCLUDE or EXCLUDE directive.
    //
    // If a qfile contains an INCLUDE directive, and hadoopVer does
    // not appear in the list of versions to include, then the qfile
    // is skipped.
    //
    // If a qfile contains an EXCLUDE directive, and hadoopVer is
    // listed in the list of versions to EXCLUDE, then the qfile is
    // skipped.
    //
    // Otherwise, the qfile is included.

    if (matcher.find()) {

      String prefix = matcher.group(1);
      if ("EX".equals(prefix)) {
        excludeQuery = true;
      } else {
        includeQuery = true;
      }

      String versions = matcher.group(2);
      for (String s : versions.split("\\,")) {
        s = s.trim();
        versionSet.add(s);
      }
    }

    if (matcher.find()) {
      //2nd match is not supposed to be there
      String message = "QTestUtil: qfile " + fileName
        + " contains more than one reference to (EX|IN)CLUDE_HADOOP_MAJOR_VERSIONS";
      throw new UnsupportedOperationException(message);
    }

    if (excludeQuery && versionSet.contains(hadoopVer)) {
      System.out.println("QTestUtil: " + fileName
        + " EXCLUDE list contains Hadoop Version " + hadoopVer + ". Skipping...");
      return true;
    } else if (includeQuery && !versionSet.contains(hadoopVer)) {
      System.out.println("QTestUtil: " + fileName
        + " INCLUDE list does not contain Hadoop Version " + hadoopVer + ". Skipping...");
      return true;
    }
    return false;
  }

  private boolean checkOSExclude(String fileName, String query){
    // Look for a hint to not run a test on some Hadoop versions
    Pattern pattern = Pattern.compile("-- (EX|IN)CLUDE_OS_WINDOWS");

    // detect whether this query wants to be excluded or included
    // on windows
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) {
      String prefix = matcher.group(1);
      if ("EX".equals(prefix)) {
        //windows is to be exluded
        if(Shell.WINDOWS){
          System.out.println("Due to the OS being windows " +
                             "adding the  query " + fileName +
                             " to the set of tests to skip");
          return true;
        }
      }
      else  if(!Shell.WINDOWS){
        //non windows to be exluded
        System.out.println("Due to the OS not being windows " +
                           "adding the  query " + fileName +
                           " to the set of tests to skip");
        return true;
      }
    }
    return false;
  }

  private boolean checkNeedJavaSpecificOutput(String fileName, String query) {
    Pattern pattern = Pattern.compile("-- JAVA_VERSION_SPECIFIC_OUTPUT");
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) {
      System.out.println("Test is flagged to generate Java version specific " +
          "output. Since we are using Java version " + javaVersion +
          ", we will generated Java " + javaVersion + " specific " +
          "output file for query file " + fileName);
      return true;
    }

    return false;
  }

  /**
   * Get formatted Java version to include minor version, but
   * exclude patch level.
   *
   * @return Java version formatted as major_version.minor_version
   */
  private static String getJavaVersion() {
    String version = System.getProperty("java.version");
    if (version == null) {
      throw new NullPointerException("No java version could be determined " +
          "from system properties");
    }

    // "java version" system property is formatted
    // major_version.minor_version.patch_level.
    // Find second dot, instead of last dot, to be safe
    int pos = version.indexOf('.');
    pos = version.indexOf('.', pos + 1);
    return version.substring(0, pos);
  }

  /**
   * Clear out any side effects of running tests
   */
  public void clearPostTestEffects() throws Exception {
    setup.postTest(conf);
  }

  public void clearKeysCreatedInTests() {
    if (hes == null) {
      return;
    }
    try {
      for (String keyAlias : hes.getKeys()) {
        hes.deleteKey(keyAlias);
      }
    } catch (IOException e) {
      LOG.error("Fail to clean the keys created in test due to the error", e);
    }
  }

  /**
   * Clear out any side effects of running tests
   */
  public void clearTablesCreatedDuringTests() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }

    // Delete any tables other than the source tables
    // and any databases other than the default database.
    for (String dbName : db.getAllDatabases()) {
      SessionState.get().setCurrentDatabase(dbName);
      for (String tblName : db.getAllTables()) {
        if (!DEFAULT_DATABASE_NAME.equals(dbName) || !srcTables.contains(tblName)) {
          Table tblObj = db.getTable(tblName);
          // dropping index table can not be dropped directly. Dropping the base
          // table will automatically drop all its index table
          if(tblObj.isIndexTable()) {
            continue;
          }
          db.dropTable(dbName, tblName, true, true, clusterType == MiniClusterType.encrypted);
        } else {
          // this table is defined in srcTables, drop all indexes on it
         List<Index> indexes = db.getIndexes(dbName, tblName, (short)-1);
          if (indexes != null && indexes.size() > 0) {
            for (Index index : indexes) {
              db.dropIndex(dbName, tblName, index.getIndexName(), true, true);
            }
          }
        }
      }
      if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
        // Drop cascade, may need to drop functions
        db.dropDatabase(dbName, true, true, true);
      }
    }

    // delete remaining directories for external tables (can affect stats for following tests)
    try {
      Path p = new Path(testWarehouse);
      FileSystem fileSystem = p.getFileSystem(conf);
      if (fileSystem.exists(p)) {
        for (FileStatus status : fileSystem.listStatus(p)) {
          if (status.isDir() && !srcTables.contains(status.getPath().getName())) {
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

  /**
   * Clear out any side effects of running tests
   */
  public void clearTestSideEffects() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }

    clearTablesCreatedDuringTests();
    clearKeysCreatedInTests();

    // allocate and initialize a new conf since a test can
    // modify conf by using 'set' commands
    conf = new HiveConf(Driver.class);
    initConf();
    // renew the metastore since the cluster type is unencrypted
    db = Hive.get(conf);  // propagate new conf to meta store

    setup.preTest(conf);
  }

  public void cleanUp() throws Exception {
    if(!isSessionStateStarted) {
      startSessionState();
    }
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }

    clearTablesCreatedDuringTests();
    clearKeysCreatedInTests();

    SessionState.get().getConf().setBoolean("hive.test.shutdown.phase", true);

    String cleanupCommands = readEntireFileIntoString(new File(cleanupScript));
    LOG.info("Cleanup (" + cleanupScript + "):\n" + cleanupCommands);
    if(cliDriver == null) {
      cliDriver = new CliDriver();
    }
    cliDriver.processLine(cleanupCommands);

    SessionState.get().getConf().setBoolean("hive.test.shutdown.phase", false);

    // delete any contents in the warehouse dir
    Path p = new Path(testWarehouse);
    FileSystem fs = p.getFileSystem(conf);

    try {
      FileStatus [] ls = fs.listStatus(p);
      for (int i=0; (ls != null) && (i<ls.length); i++) {
        fs.delete(ls[i].getPath(), true);
      }
    } catch (FileNotFoundException e) {
      // Best effort
    }

    FunctionRegistry.unregisterTemporaryUDF("test_udaf");
    FunctionRegistry.unregisterTemporaryUDF("test_error");
  }

  protected void runCreateTableCmd(String createTableCmd) throws Exception {
    int ecode = 0;
    ecode = drv.run(createTableCmd).getResponseCode();
    if (ecode != 0) {
      throw new Exception("create table command: " + createTableCmd
          + " failed with exit code= " + ecode);
    }

    return;
  }

  protected void runCmd(String cmd) throws Exception {
    int ecode = 0;
    ecode = drv.run(cmd).getResponseCode();
    drv.close();
    if (ecode != 0) {
      throw new Exception("command: " + cmd
          + " failed with exit code= " + ecode);
    }
    return;
  }

  public void createSources() throws Exception {
    if(!isSessionStateStarted) {
      startSessionState();
    }
    conf.setBoolean("hive.test.init.phase", true);

    String initCommands = readEntireFileIntoString(new File(this.initScript));
    LOG.info("Initial setup (" + initScript + "):\n" + initCommands);
    if(cliDriver == null) {
      cliDriver = new CliDriver();
    }
    cliDriver.processLine("set test.data.dir=" + testFiles + ";");
    cliDriver.processLine(initCommands);

    conf.setBoolean("hive.test.init.phase", false);
  }

  public void init() throws Exception {
    // System.out.println(conf.toString());
    testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    // conf.logVars(System.out);
    // System.out.flush();

    String execEngine = conf.get("hive.execution.engine");
    conf.set("hive.execution.engine", "mr");
    SessionState.start(conf);
    conf.set("hive.execution.engine", execEngine);
    db = Hive.get(conf);
    drv = new Driver(conf);
    drv.init();
    pd = new ParseDriver();
    sem = new SemanticAnalyzer(conf);
  }

  public void init(String tname) throws Exception {
    cleanUp();
    createSources();
    cliDriver.processCmd("set hive.cli.print.header=true;");
  }

  public void cliInit(String tname) throws Exception {
    cliInit(tname, true);
  }

  public String cliInit(String tname, boolean recreate) throws Exception {
    if (recreate) {
      cleanUp();
      createSources();
    }

    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
    "org.apache.hadoop.hive.ql.security.DummyAuthenticator");
    Utilities.clearWorkMap();
    CliSessionState ss = createSessionState();
    assert ss != null;
    ss.in = System.in;

    String outFileExtension = getOutFileExtension(tname);
    String stdoutName = null;
    if (outDir != null) {
      // TODO: why is this needed?
      File qf = new File(outDir, tname);
      stdoutName = qf.getName().concat(outFileExtension);
    } else {
      stdoutName = tname + outFileExtension;
    }

    File outf = new File(logDir, stdoutName);
    OutputStream fo = new BufferedOutputStream(new FileOutputStream(outf));
    if (qSortQuerySet.contains(tname)) {
      ss.out = new SortPrintStream(fo, "UTF-8");
    } else if (qHashQuerySet.contains(tname)) {
      ss.out = new DigestPrintStream(fo, "UTF-8");
    } else if (qSortNHashQuerySet.contains(tname)) {
      ss.out = new SortAndDigestPrintStream(fo, "UTF-8");
    } else {
      ss.out = new PrintStream(fo, true, "UTF-8");
    }
    ss.err = new CachingPrintStream(fo, true, "UTF-8");
    ss.setIsSilent(true);
    SessionState oldSs = SessionState.get();

    if (oldSs != null && (clusterType == MiniClusterType.tez || clusterType == MiniClusterType.spark
        || clusterType == MiniClusterType.miniSparkOnYarn)) {
      sparkSession = oldSs.getSparkSession();
      ss.setSparkSession(sparkSession);
      oldSs.setSparkSession(null);
      oldSs.close();
    }

    if (oldSs != null && oldSs.out != null && oldSs.out != System.out) {
      oldSs.out.close();
    }
    SessionState.start(ss);

    cliDriver = new CliDriver();

    if (tname.equals("init_file.q")) {
      ss.initFiles.add("../../data/scripts/test_init_file.sql");
    }
    cliDriver.processInitFiles(ss);

    return outf.getAbsolutePath();
  }

  private CliSessionState createSessionState() {
   return new CliSessionState(conf) {
      public void setSparkSession(SparkSession sparkSession) {
        super.setSparkSession(sparkSession);
        if (sparkSession != null) {
          try {
            // Wait a little for cluster to init, at most 4 minutes
            long endTime = System.currentTimeMillis() + 240000;
            while (sparkSession.getMemoryAndCores().getSecond() <= 1) {
              if (System.currentTimeMillis() >= endTime) {
                String msg = "Timed out waiting for Spark cluster to init";
                throw new IllegalStateException(msg);
              }
              Thread.sleep(100);
            }
          } catch (Exception e) {
            String msg = "Error trying to obtain executor info: " + e;
            LOG.error(msg, e);
            throw new IllegalStateException(msg, e);
          }
        }
      }
    };
  }

  private CliSessionState startSessionState()
      throws IOException {

    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        "org.apache.hadoop.hive.ql.security.DummyAuthenticator");

    String execEngine = conf.get("hive.execution.engine");
    conf.set("hive.execution.engine", "mr");
    CliSessionState ss = createSessionState();
    assert ss != null;
    ss.in = System.in;
    ss.out = System.out;
    ss.err = System.out;

    SessionState oldSs = SessionState.get();
    if (oldSs != null && (clusterType == MiniClusterType.tez || clusterType == MiniClusterType.spark
        || clusterType == MiniClusterType.miniSparkOnYarn)) {
      sparkSession = oldSs.getSparkSession();
      ss.setSparkSession(sparkSession);
      oldSs.setSparkSession(null);
      oldSs.close();
    }
    if (oldSs != null && oldSs.out != null && oldSs.out != System.out) {
      oldSs.out.close();
    }
    SessionState.start(ss);

    isSessionStateStarted = true;

    conf.set("hive.execution.engine", execEngine);
    return ss;
  }

  public int executeOne(String tname) {
    String q = qMap.get(tname);

    if (q.indexOf(";") == -1) {
      return -1;
    }

    String q1 = q.substring(0, q.indexOf(";") + 1);
    String qrest = q.substring(q.indexOf(";") + 1);
    qMap.put(tname, qrest);

    System.out.println("Executing " + q1);
    return cliDriver.processLine(q1);
  }

  public int execute(String tname) {
    try {
      return drv.run(qMap.get(tname)).getResponseCode();
    } catch (CommandNeedRetryException e) {
      LOG.error("driver failed to run the command: " + tname + " due to the exception: ", e);
      e.printStackTrace();
      return -1;
    }
  }

  public int executeClient(String tname1, String tname2) {
    String commands = getCommand(tname1) + CRLF + getCommand(tname2);
    return executeClientInternal(commands);
  }

  public int executeClient(String tname) {
    return executeClientInternal(getCommand(tname));
  }

  private int executeClientInternal(String commands) {
    String [] cmds = commands.split(";");
    int rc = 0;

    String command = "";
    for (String oneCmd : cmds) {
      if (StringUtils.endsWith(oneCmd, "\\")) {
        command += StringUtils.chop(oneCmd) + "\\;";
        continue;
      } else {
        if (isHiveCommand(oneCmd)) {
          command = oneCmd;
        } else {
          command += oneCmd;
        }
      }
      if (StringUtils.isBlank(command)) {
        continue;
      }

      if (isCommandUsedForTesting(command)) {
        rc = executeTestCommand(command);
      } else {
        rc = cliDriver.processLine(command);
      }

      if (rc != 0 && !ignoreErrors()) {
        break;
      }
      command = "";
    }
    if (rc == 0 && SessionState.get() != null) {
      SessionState.get().setLastCommand(null);  // reset
    }
    return rc;
  }

  /**
   * This allows a .q file to continue executing after a statement runs into an error which is convenient
   * if you want to use another hive cmd after the failure to sanity check the state of the system.
   */
  private boolean ignoreErrors() {
    return conf.getBoolVar(HiveConf.ConfVars.CLIIGNOREERRORS);
  }

  private boolean isHiveCommand(String command) {
    String[] cmd = command.trim().split("\\s+");
    if (HiveCommand.find(cmd) != null) {
      return true;
    } else if (HiveCommand.find(cmd, HiveCommand.ONLY_FOR_TESTING) != null) {
      return true;
    } else {
      return false;
    }
  }

  private int executeTestCommand(final String command) {
    String commandName = command.trim().split("\\s+")[0];
    String commandArgs = command.trim().substring(commandName.length());

    if (commandArgs.endsWith(";")) {
      commandArgs = StringUtils.chop(commandArgs);
    }

    //replace ${hiveconf:hive.metastore.warehouse.dir} with actual dir if existed.
    //we only want the absolute path, so remove the header, such as hdfs://localhost:57145
    String wareHouseDir = SessionState.get().getConf().getVar(ConfVars.METASTOREWAREHOUSE)
        .replaceAll("^[a-zA-Z]+://.*?:\\d+", "");
    commandArgs = commandArgs.replaceAll("\\$\\{hiveconf:hive\\.metastore\\.warehouse\\.dir\\}",
      wareHouseDir);

    enableTestOnlyCmd(SessionState.get().getConf());

    try {
      CommandProcessor proc = getTestCommand(commandName);
      if (proc != null) {
        CommandProcessorResponse response = proc.run(commandArgs.trim());

        int rc = response.getResponseCode();
        if (rc != 0) {
          SessionState.get().out.println(response);
        }

        return rc;
      } else {
        throw new RuntimeException("Could not get CommandProcessor for command: " + commandName);
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not execute test command: " + e.getMessage());
    }
  }

  private CommandProcessor getTestCommand(final String commandName) throws SQLException {
    HiveCommand testCommand = HiveCommand.find(new String[]{commandName}, HiveCommand.ONLY_FOR_TESTING);

    if (testCommand == null) {
      return null;
    }

    return CommandProcessorFactory
      .getForHiveCommandInternal(new String[]{commandName}, SessionState.get().getConf(),
        testCommand.isOnlyForTesting());
  }

  private void enableTestOnlyCmd(HiveConf conf){
    StringBuilder securityCMDs = new StringBuilder(conf.getVar(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST));
    for(String c : testOnlyCommands){
      securityCMDs.append(",");
      securityCMDs.append(c);
    }
    conf.set(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST.toString(), securityCMDs.toString());
  }

  private boolean isCommandUsedForTesting(final String command) {
    String commandName = command.trim().split("\\s+")[0];
    HiveCommand testCommand = HiveCommand.find(new String[]{commandName}, HiveCommand.ONLY_FOR_TESTING);
    return testCommand != null;
  }

  private String getCommand(String tname) {
    String commands = qMap.get(tname);
    StringBuilder newCommands = new StringBuilder(commands.length());
    int lastMatchEnd = 0;
    Matcher commentMatcher = Pattern.compile("^--.*$", Pattern.MULTILINE).matcher(commands);
    while (commentMatcher.find()) {
      newCommands.append(commands.substring(lastMatchEnd, commentMatcher.start()));
      newCommands.append(commentMatcher.group().replaceAll("(?<!\\\\);", "\\\\;"));
      lastMatchEnd = commentMatcher.end();
    }
    newCommands.append(commands.substring(lastMatchEnd, commands.length()));
    commands = newCommands.toString();
    return commands;
  }

  private boolean isComment(final String line) {
    String lineTrimmed = line.trim();
    return lineTrimmed.startsWith("#") || lineTrimmed.startsWith("--");
  }

  public boolean shouldBeSkipped(String tname) {
    return qSkipSet.contains(tname);
  }

  private String getOutFileExtension(String fname) {
    String outFileExtension = ".out";
    if (qJavaVersionSpecificOutput.contains(fname)) {
      outFileExtension = ".java" + javaVersion + ".out";
    }

    return outFileExtension;
  }

  public void convertSequenceFileToTextFile() throws Exception {
    // Create an instance of hive in order to create the tables
    testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    db = Hive.get(conf);
    // Create dest4 to replace dest4_sequencefile
    LinkedList<String> cols = new LinkedList<String>();
    cols.add("key");
    cols.add("value");

    // Move all data from dest4_sequencefile to dest4
    drv
      .run("FROM dest4_sequencefile INSERT OVERWRITE TABLE dest4 SELECT dest4_sequencefile.*");

    // Drop dest4_sequencefile
    db.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "dest4_sequencefile",
        true, true);
  }

  public int checkNegativeResults(String tname, Exception e) throws Exception {

    String outFileExtension = getOutFileExtension(tname);

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
      throw e;
    }

    outfd.write(e.getMessage());
    outfd.close();

    int exitVal = executeDiffCommand(outf.getPath(), expf, false,
                                     qSortSet.contains(qf.getName()));
    if (exitVal != 0 && overWrite) {
      exitVal = overwriteResults(outf.getPath(), expf);
    }

    return exitVal;
  }

  public int checkParseResults(String tname, ASTNode tree) throws Exception {

    if (tree != null) {
      String outFileExtension = getOutFileExtension(tname);

      File parseDir = new File(outDir, "parse");
      String expf = outPath(parseDir.toString(), tname.concat(outFileExtension));

      File outf = null;
      outf = new File(logDir);
      outf = new File(outf, tname.concat(outFileExtension));

      FileWriter outfd = new FileWriter(outf);
      outfd.write(tree.toStringTree());
      outfd.close();

      int exitVal = executeDiffCommand(outf.getPath(), expf, false, false);

      if (exitVal != 0 && overWrite) {
        exitVal = overwriteResults(outf.getPath(), expf);
      }

      return exitVal;
    } else {
      throw new Exception("Parse tree is null");
    }
  }

  private final Pattern[] xmlPlanMask = toPattern(new String[] {
      "<java version=\".*\" class=\"java.beans.XMLDecoder\">",
      "<string>.*/tmp/.*</string>",
      "<string>file:.*</string>",
      "<string>pfile:.*</string>",
      "<string>[0-9]{10}</string>",
      "<string>/.*/warehouse/.*</string>"
  });

  public int checkPlan(String tname, List<Task<? extends Serializable>> tasks) throws Exception {

    if (tasks == null) {
      throw new Exception("Plan is null");
    }
    File planDir = new File(outDir, "plan");
    String planFile = outPath(planDir.toString(), tname + ".xml");

    File outf = null;
    outf = new File(logDir);
    outf = new File(outf, tname.concat(".xml"));

    FileOutputStream ofs = new FileOutputStream(outf);
    try {
      conf.set(HiveConf.ConfVars.PLAN_SERIALIZATION.varname, "javaXML");
      for (Task<? extends Serializable> plan : tasks) {
        Utilities.serializePlan(plan, ofs, conf);
      }
      ofs.close();
      fixXml4JDK7(outf.getPath());
      maskPatterns(xmlPlanMask, outf.getPath());

      int exitVal = executeDiffCommand(outf.getPath(), planFile, true, false);

      if (exitVal != 0 && overWrite) {
        exitVal = overwriteResults(outf.getPath(), planFile);
      }
      return exitVal;
    } finally {
      conf.set(HiveConf.ConfVars.PLAN_SERIALIZATION.varname, "kryo");
      IOUtils.closeQuietly(ofs);
    }
  }

  /**
   * Given the current configurations (e.g., hadoop version and execution mode), return
   * the correct file name to compare with the current test run output.
   * @param outDir The directory where the reference log files are stored.
   * @param testName The test file name (terminated by ".out").
   * @return The file name appended with the configuration values if it exists.
   */
  public String outPath(String outDir, String testName) {
    String ret = (new File(outDir, testName)).getPath();
    // List of configurations. Currently the list consists of hadoop version and execution mode only
    List<String> configs = new ArrayList<String>();
    configs.add(this.hadoopVer);

    Deque<String> stack = new LinkedList<String>();
    StringBuilder sb = new StringBuilder();
    sb.append(testName);
    stack.push(sb.toString());

    // example file names are input1.q.out_0.20.0_minimr or input2.q.out_0.17
    for (String s: configs) {
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

  /**
   * Fix the XML generated by JDK7 which is slightly different from what's generated by JDK6,
   * causing 40+ test failures. There are mainly two problems:
   *
   * 1. object element's properties, id and class, are in reverse order, i.e.
   *    <object class="org.apache.hadoop.hive.ql.exec.MapRedTask" id="MapRedTask0">
   *    which needs to be fixed to
   *    <object id="MapRedTask0" class="org.apache.hadoop.hive.ql.exec.MapRedTask">
   * 2. JDK introduces Enum as class, i.e.
   *    <object id="GenericUDAFEvaluator$Mode0" class="java.lang.Enum">
   *      <class>org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator$Mode</class>
   *    which needs to be fixed to
   *    <object id="GenericUDAFEvaluator$Mode0" class="org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator$Mode"
   *     method="valueOf">
   *
   * Though not elegant, this allows these test cases to pass until we have a better serialization mechanism.
   *
   * Did I mention this is test code?
   *
   * @param fname the name of the file to fix
   * @throws Exception in case of IO error
   */
  private static void fixXml4JDK7(String fname) throws Exception {
    String version = System.getProperty("java.version");
    if (!version.startsWith("1.7")) {
      return;
    }

    BufferedReader in = new BufferedReader(new FileReader(fname));
    BufferedWriter out = new BufferedWriter(new FileWriter(fname + ".orig"));
    String line = null;
    while (null != (line = in.readLine())) {
      out.write(line);
      out.write('\n');
    }
    in.close();
    out.close();

    in = new BufferedReader(new FileReader(fname + ".orig"));
    out = new BufferedWriter(new FileWriter(fname));

    while (null != (line = in.readLine())) {
      if (line.indexOf("<object ") == -1 || line.indexOf("class=") == -1) {
        out.write(line);
      } else {
        StringBuilder sb = new StringBuilder();
        String prefix = line.substring(0, line.indexOf("<object") + 7);
        sb.append( prefix );
        String postfix = line.substring(line.lastIndexOf('"') + 1);
        String id = getPropertyValue(line, "id");
        if (id != null) {
          sb.append(" id=" + id);
        }
        String cls = getPropertyValue(line, "class");
        assert(cls != null);
        if (cls.equals("\"java.lang.Enum\"")) {
          line = in.readLine();
          cls = "\"" + getElementValue(line, "class") + "\"";
          sb.append(" class=" + cls + " method=\"valueOf\"" );
        } else {
          sb.append(" class=" + cls);
        }

        sb.append(postfix);
        out.write(sb.toString());
      }

      out.write('\n');
    }

    in.close();
    out.close();
  }

  /**
   * Get the value of a property in line. The returned value has original quotes
   */
  private static String getPropertyValue(String line, String name) {
    int start = line.indexOf( name + "=" );
    if (start == -1) {
      return null;
    }
    start += name.length() + 1;
    int end = line.indexOf("\"", start + 1);
    return line.substring( start, end + 1 );
  }

  /**
   * Get the value of the element in input. (Note: the returned value has no quotes.)
   */
  private static String getElementValue(String line, String name) {
    assert(line.contains("<" + name + ">"));
    int start = line.indexOf("<" + name + ">") + name.length() + 2;
    int end = line.indexOf("</" + name + ">");
    return line.substring(start, end);
  }

  private Pattern[] toPattern(String[] patternStrs) {
    Pattern[] patterns = new Pattern[patternStrs.length];
    for (int i = 0; i < patternStrs.length; i++) {
      patterns[i] = Pattern.compile(patternStrs[i]);
    }
    return patterns;
  }

  private void maskPatterns(Pattern[] patterns, String fname) throws Exception {
    String maskPattern = "#### A masked pattern was here ####";
    String partialMaskPattern = "#### A PARTIAL masked pattern was here ####";

    String line;
    BufferedReader in;
    BufferedWriter out;

    File file = new File(fname);
    File fileOrig = new File(fname + ".orig");
    FileUtils.copyFile(file, fileOrig);

    in = new BufferedReader(new InputStreamReader(new FileInputStream(fileOrig), "UTF-8"));
    out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));

    boolean lastWasMasked = false;
    boolean partialMaskWasMatched = false;
    Matcher matcher;
    while (null != (line = in.readLine())) {
      if (clusterType == MiniClusterType.encrypted) {
        for (Pattern pattern : partialReservedPlanMask) {
          matcher = pattern.matcher(line);
          if (matcher.find()) {
            line = partialMaskPattern + " " + matcher.group(0);
            partialMaskWasMatched = true;
            break;
          }
        }
      }

      if (!partialMaskWasMatched) {
        for (Pattern pattern : patterns) {
          line = pattern.matcher(line).replaceAll(maskPattern);
        }
      }

      if (line.equals(maskPattern)) {
        // We're folding multiple masked lines into one.
        if (!lastWasMasked) {
          out.write(line);
          out.write("\n");
          lastWasMasked = true;
        }
      } else {
        out.write(line);
        out.write("\n");
        lastWasMasked = false;
        partialMaskWasMatched = false;
      }
    }

    in.close();
    out.close();
  }

  private final Pattern[] planMask = toPattern(new String[] {
      ".*file:.*",
      ".*pfile:.*",
      ".*hdfs:.*",
      ".*/tmp/.*",
      ".*invalidscheme:.*",
      ".*lastUpdateTime.*",
      ".*lastAccessTime.*",
      ".*lastModifiedTime.*",
      ".*[Oo]wner.*",
      ".*CreateTime.*",
      ".*LastAccessTime.*",
      ".*Location.*",
      ".*LOCATION '.*",
      ".*transient_lastDdlTime.*",
      ".*last_modified_.*",
      ".*at org.*",
      ".*at sun.*",
      ".*at java.*",
      ".*at junit.*",
      ".*Caused by:.*",
      ".*LOCK_QUERYID:.*",
      ".*LOCK_TIME:.*",
      ".*grantTime.*",
      ".*[.][.][.] [0-9]* more.*",
      ".*job_[0-9_]*.*",
      ".*job_local[0-9_]*.*",
      ".*USING 'java -cp.*",
      "^Deleted.*",
      ".*DagName:.*",
      ".*Input:.*/data/files/.*",
      ".*Output:.*/data/files/.*",
      ".*total number of created files now is.*",
      ".*.hive-staging.*"
  });

  private final Pattern[] partialReservedPlanMask = toPattern(new String[] {
      "data/warehouse/(.*?/)+\\.hive-staging"  // the directory might be db/table/partition
      //TODO: add more expected test result here
  });

  public int checkCliDriverResults(String tname) throws Exception {
    assert(qMap.containsKey(tname));

    String outFileExtension = getOutFileExtension(tname);
    String outFileName = outPath(outDir, tname + outFileExtension);

    File f = new File(logDir, tname + outFileExtension);

    maskPatterns(planMask, f.getPath());
    int exitVal = executeDiffCommand(f.getPath(),
                                     outFileName, false,
                                     qSortSet.contains(tname));

    if (exitVal != 0 && overWrite) {
      exitVal = overwriteResults(f.getPath(), outFileName);
    }

    return exitVal;
  }


  public int checkCompareCliDriverResults(String tname, List<String> outputs) throws Exception {
    assert outputs.size() > 1;
    maskPatterns(planMask, outputs.get(0));
    for (int i = 1; i < outputs.size(); ++i) {
      maskPatterns(planMask, outputs.get(i));
      int ecode = executeDiffCommand(
          outputs.get(i - 1), outputs.get(i), false, qSortSet.contains(tname));
      if (ecode != 0) {
        System.out.println("Files don't match: " + outputs.get(i - 1) + " and " + outputs.get(i));
        return ecode;
      }
    }
    return 0;
  }

  private static int overwriteResults(String inFileName, String outFileName) throws Exception {
    // This method can be replaced with Files.copy(source, target, REPLACE_EXISTING)
    // once Hive uses JAVA 7.
    System.out.println("Overwriting results " + inFileName + " to " + outFileName);
    return executeCmd(new String[] {
        "cp",
        getQuotedString(inFileName),
        getQuotedString(outFileName)
      });
  }

  private static int executeDiffCommand(String inFileName,
      String outFileName,
      boolean ignoreWhiteSpace,
      boolean sortResults
      ) throws Exception {

    int result = 0;

    if (sortResults) {
      // sort will try to open the output file in write mode on windows. We need to
      // close it first.
      SessionState ss = SessionState.get();
      if (ss != null && ss.out != null && ss.out != System.out) {
	ss.out.close();
      }

      String inSorted = inFileName + SORT_SUFFIX;
      String outSorted = outFileName + SORT_SUFFIX;

      result = sortFiles(inFileName, inSorted);
      result |= sortFiles(outFileName, outSorted);
      if (result != 0) {
        System.err.println("ERROR: Could not sort files before comparing");
        return result;
      }
      inFileName = inSorted;
      outFileName = outSorted;
    }

    ArrayList<String> diffCommandArgs = new ArrayList<String>();
    diffCommandArgs.add("diff");

    // Text file comparison
    diffCommandArgs.add("-a");

    // Ignore changes in the amount of white space
    if (ignoreWhiteSpace || Shell.WINDOWS) {
      diffCommandArgs.add("-b");
    }

    // Files created on Windows machines have different line endings
    // than files created on Unix/Linux. Windows uses carriage return and line feed
    // ("\r\n") as a line ending, whereas Unix uses just line feed ("\n").
    // Also StringBuilder.toString(), Stream to String conversions adds extra
    // spaces at the end of the line.
    if (Shell.WINDOWS) {
      diffCommandArgs.add("--strip-trailing-cr"); // Strip trailing carriage return on input
      diffCommandArgs.add("-B"); // Ignore changes whose lines are all blank
    }
    // Add files to compare to the arguments list
    diffCommandArgs.add(getQuotedString(inFileName));
    diffCommandArgs.add(getQuotedString(outFileName));

    result = executeCmd(diffCommandArgs);

    if (sortResults) {
      new File(inFileName).delete();
      new File(outFileName).delete();
    }

    return result;
  }

  private static int sortFiles(String in, String out) throws Exception {
    return executeCmd(new String[] {
        "sort",
        getQuotedString(in),
      }, out, null);
  }

  private static int executeCmd(Collection<String> args) throws Exception {
    return executeCmd(args, null, null);
  }

  private static int executeCmd(String[] args) throws Exception {
    return executeCmd(args, null, null);
  }

  private static int executeCmd(Collection<String> args, String outFile, String errFile) throws Exception {
    String[] cmdArray = args.toArray(new String[args.size()]);
    return executeCmd(cmdArray, outFile, errFile);
  }

  private static int executeCmd(String[] args, String outFile, String errFile) throws Exception {
    System.out.println("Running: " + org.apache.commons.lang.StringUtils.join(args, ' '));

    PrintStream out = outFile == null ?
      SessionState.getConsole().getChildOutStream() :
      new PrintStream(new FileOutputStream(outFile), true);
    PrintStream err = errFile == null ?
      SessionState.getConsole().getChildErrStream() :
      new PrintStream(new FileOutputStream(errFile), true);

    Process executor = Runtime.getRuntime().exec(args);

    StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, err);
    StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, out);

    outPrinter.start();
    errPrinter.start();

    int result = executor.waitFor();

    outPrinter.join();
    errPrinter.join();

    if (outFile != null) {
      out.close();
    }

    if (errFile != null) {
      err.close();
    }

    return result;
  }

  private static String getQuotedString(String str){
    return Shell.WINDOWS ? String.format("\"%s\"", str) : str;
  }

  public ASTNode parseQuery(String tname) throws Exception {
    return pd.parse(qMap.get(tname));
  }

  public void resetParser() throws SemanticException {
    drv.init();
    pd = new ParseDriver();
    sem = new SemanticAnalyzer(conf);
  }


  public List<Task<? extends Serializable>> analyzeAST(ASTNode ast) throws Exception {

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

  public TreeMap<String, String> getQMap() {
    return qMap;
  }

  /**
   * QTestSetup defines test fixtures which are reused across testcases,
   * and are needed before any test can be run
   */
  public static class QTestSetup
  {
    private MiniZooKeeperCluster zooKeeperCluster = null;
    private int zkPort;
    private ZooKeeper zooKeeper;

    public QTestSetup() {
    }

    public void preTest(HiveConf conf) throws Exception {

      if (zooKeeperCluster == null) {
        //create temp dir
        String tmpBaseDir =  System.getProperty("test.tmp.dir");
        File tmpDir = Utilities.createTempDir(tmpBaseDir);

        zooKeeperCluster = new MiniZooKeeperCluster();
        zkPort = zooKeeperCluster.startup(tmpDir);
      }

      if (zooKeeper != null) {
        zooKeeper.close();
      }

      int sessionTimeout =  (int) conf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
      zooKeeper = new ZooKeeper("localhost:" + zkPort, sessionTimeout, new Watcher() {
        @Override
        public void process(WatchedEvent arg0) {
        }
      });

      String zkServer = "localhost";
      conf.set("hive.zookeeper.quorum", zkServer);
      conf.set("hive.zookeeper.client.port", "" + zkPort);
    }

    public void postTest(HiveConf conf) throws Exception {
      if (zooKeeperCluster == null) {
        return;
      }

      if (zooKeeper != null) {
        zooKeeper.close();
      }

      ZooKeeperHiveLockManager.releaseAllLocks(conf);
    }

    public void tearDown() throws Exception {
      CuratorFrameworkSingleton.closeAndReleaseInstance();

      if (zooKeeperCluster != null) {
        zooKeeperCluster.shutdown();
        zooKeeperCluster = null;
      }
    }
  }

  /**
   * QTRunner: Runnable class for running a a single query file.
   *
   **/
  public static class QTRunner implements Runnable {
    private final QTestUtil qt;
    private final String fname;

    public QTRunner(QTestUtil qt, String fname) {
      this.qt = qt;
      this.fname = fname;
    }

    @Override
    public void run() {
      try {
        // assumption is that environment has already been cleaned once globally
        // hence each thread does not call cleanUp() and createSources() again
        qt.cliInit(fname, false);
        qt.executeClient(fname);
      } catch (Throwable e) {
        System.err.println("Query file " + fname + " failed with exception "
            + e.getMessage());
        e.printStackTrace();
        outputTestFailureHelpMessage();
      }
    }
  }

  /**
   * Setup to execute a set of query files. Uses QTestUtil to do so.
   *
   * @param qfiles
   *          array of input query files containing arbitrary number of hive
   *          queries
   * @param resDir
   *          output directory
   * @param logDir
   *          log directory
   * @return one QTestUtil for each query file
   */
  public static QTestUtil[] queryListRunnerSetup(File[] qfiles, String resDir,
      String logDir) throws Exception
  {
    QTestUtil[] qt = new QTestUtil[qfiles.length];
    for (int i = 0; i < qfiles.length; i++) {
      qt[i] = new QTestUtil(resDir, logDir, MiniClusterType.none, null, "0.20", "", "");
      qt[i].addFile(qfiles[i]);
      qt[i].clearTestSideEffects();
    }

    return qt;
  }

  /**
   * Executes a set of query files in sequence.
   *
   * @param qfiles
   *          array of input query files containing arbitrary number of hive
   *          queries
   * @param qt
   *          array of QTestUtils, one per qfile
   * @return true if all queries passed, false otw
   */
  public static boolean queryListRunnerSingleThreaded(File[] qfiles, QTestUtil[] qt)
    throws Exception
  {
    boolean failed = false;
    qt[0].cleanUp();
    qt[0].createSources();
    for (int i = 0; i < qfiles.length && !failed; i++) {
      qt[i].clearTestSideEffects();
      qt[i].cliInit(qfiles[i].getName(), false);
      qt[i].executeClient(qfiles[i].getName());
      int ecode = qt[i].checkCliDriverResults(qfiles[i].getName());
      if (ecode != 0) {
        failed = true;
        System.err.println("Test " + qfiles[i].getName()
            + " results check failed with error code " + ecode);
        outputTestFailureHelpMessage();
      }
      qt[i].clearPostTestEffects();
    }
    return (!failed);
  }

  /**
   * Executes a set of query files parallel.
   *
   * Each query file is run in a separate thread. The caller has to arrange
   * that different query files do not collide (in terms of destination tables)
   *
   * @param qfiles
   *          array of input query files containing arbitrary number of hive
   *          queries
   * @param qt
   *          array of QTestUtils, one per qfile
   * @return true if all queries passed, false otw
   *
   */
  public static boolean queryListRunnerMultiThreaded(File[] qfiles, QTestUtil[] qt)
    throws Exception
  {
    boolean failed = false;

    // in multithreaded mode - do cleanup/initialization just once

    qt[0].cleanUp();
    qt[0].createSources();
    qt[0].clearTestSideEffects();

    QTRunner[] qtRunners = new QTRunner[qfiles.length];
    Thread[] qtThread = new Thread[qfiles.length];

    for (int i = 0; i < qfiles.length; i++) {
      qtRunners[i] = new QTRunner(qt[i], qfiles[i].getName());
      qtThread[i] = new Thread(qtRunners[i]);
    }

    for (int i = 0; i < qfiles.length; i++) {
      qtThread[i].start();
    }

    for (int i = 0; i < qfiles.length; i++) {
      qtThread[i].join();
      int ecode = qt[i].checkCliDriverResults(qfiles[i].getName());
      if (ecode != 0) {
        failed = true;
        System.err.println("Test " + qfiles[i].getName()
            + " results check failed with error code " + ecode);
        outputTestFailureHelpMessage();
      }
    }
    return (!failed);
  }

  public static void outputTestFailureHelpMessage() {
    System.err.println(
      "See ./ql/target/tmp/log/hive.log or ./itests/qtest/target/tmp/log/hive.log, or check " +
        "./ql/target/surefire-reports or ./itests/qtest/target/surefire-reports/ for specific " +
        "test cases logs.");
    System.err.flush();
  }

  public static String ensurePathEndsInSlash(String path) {
    if(path == null) {
      throw new NullPointerException("Path cannot be null");
    }
    if(path.endsWith(File.separator)) {
      return path;
    } else {
      return path + File.separator;
    }
  }

  private static String[] cachedQvFileList = null;
  private static ImmutableList<String> cachedDefaultQvFileList = null;
  private static Pattern qvSuffix = Pattern.compile("_[0-9]+.qv$", Pattern.CASE_INSENSITIVE);

  public static List<String> getVersionFiles(String queryDir, String tname) {
    ensureQvFileList(queryDir);
    List<String> result = getVersionFilesInternal(tname);
    if (result == null) {
      result = cachedDefaultQvFileList;
    }
    return result;
  }

  private static void ensureQvFileList(String queryDir) {
    if (cachedQvFileList != null) return;
    // Not thread-safe.
    System.out.println("Getting versions from " + queryDir);
    cachedQvFileList = (new File(queryDir)).list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(".qv");
      }
    });
    if (cachedQvFileList == null) return; // no files at all
    Arrays.sort(cachedQvFileList, String.CASE_INSENSITIVE_ORDER);
    List<String> defaults = getVersionFilesInternal("default");
    cachedDefaultQvFileList = (defaults != null)
        ? ImmutableList.copyOf(defaults) : ImmutableList.<String>of();
  }

  private static List<String> getVersionFilesInternal(String tname) {
    if (cachedQvFileList == null) {
      return new ArrayList<String>();
    }
    int pos = Arrays.binarySearch(cachedQvFileList, tname, String.CASE_INSENSITIVE_ORDER);
    if (pos >= 0) {
      throw new BuildException("Unexpected file list element: " + cachedQvFileList[pos]);
    }
    List<String> result = null;
    for (pos = (-pos - 1); pos < cachedQvFileList.length; ++pos) {
      String candidate = cachedQvFileList[pos];
      if (candidate.length() <= tname.length()
          || !tname.equalsIgnoreCase(candidate.substring(0, tname.length()))
          || !qvSuffix.matcher(candidate.substring(tname.length())).matches()) {
        break;
      }
      if (result == null) {
        result = new ArrayList<String>();
      }
      result.add(candidate);
    }
    return result;
  }

  public void failed(int ecode, String fname, String debugHint) {
    String command = SessionState.get() != null ? SessionState.get().getLastCommand() : null;
    Assert.fail("Client Execution failed with error code = " + ecode +
        (command != null ? " running " + command : "") + (debugHint != null ? debugHint : ""));
  }

  // for negative tests, which is succeeded.. no need to print the query string
  public void failed(String fname, String debugHint) {
    Assert.fail("Client Execution was expected to fail, but succeeded with error code 0 " +
        (debugHint != null ? debugHint : ""));
  }

  public void failedDiff(int ecode, String fname, String debugHint) {
    Assert.fail("Client Execution results failed with error code = " + ecode +
        (debugHint != null ? debugHint : ""));
  }

  public void failed(Throwable e, String fname, String debugHint) {
    String command = SessionState.get() != null ? SessionState.get().getLastCommand() : null;
    System.err.println("Exception: " + e.getMessage());
    e.printStackTrace();
    System.err.println("Failed query: " + fname);
    System.err.flush();
    Assert.fail("Unexpected exception " +
        org.apache.hadoop.util.StringUtils.stringifyException(e) + "\n" +
        (command != null ? " running " + command : "") +
        (debugHint != null ? debugHint : ""));
  }

  public static void addTestsToSuiteFromQfileNames(
    String qFileNamesFile,
    Set<String> qFilesToExecute,
    TestSuite suite,
    Object setup,
    SuiteAddTestFunctor suiteAddTestCallback) {
    try {
      File qFileNames = new File(qFileNamesFile);
      FileReader fr = new FileReader(qFileNames.getCanonicalFile());
      BufferedReader br = new BufferedReader(fr);
      String fName = null;

      while ((fName = br.readLine()) != null) {
        if (fName.isEmpty() || fName.trim().equals("")) {
          continue;
        }

        int eIdx = fName.indexOf('.');

        if (eIdx == -1) {
          continue;
        }

        String tName = fName.substring(0, eIdx);

        if (qFilesToExecute.isEmpty() || qFilesToExecute.contains(fName)) {
          suiteAddTestCallback.addTestToSuite(suite, setup, tName);
        }
      }
      br.close();
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      Assert.fail("Unexpected exception " + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }
}
