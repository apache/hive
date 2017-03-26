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
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.control.AbstractCliConfig;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.io.DigestPrintStream;
import org.apache.hadoop.hive.common.io.SortAndDigestPrintStream;
import org.apache.hadoop.hive.common.io.SortPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapItUtils;
import org.apache.hadoop.hive.llap.daemon.MiniLlapCluster;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionState;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
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
import org.apache.logging.log4j.util.Strings;
import org.apache.tools.ant.BuildException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import junit.framework.TestSuite;

/**
 * QTestUtil.
 *
 */
public class QTestUtil {

  public static final String UTF_8 = "UTF-8";

  // security property names
  private static final String SECURITY_KEY_PROVIDER_URI_NAME = "dfs.encryption.key.provider.uri";
  private static final String CRLF = System.getProperty("line.separator");

  public static final String QTEST_LEAVE_FILES = "QTEST_LEAVE_FILES";
  private static final Logger LOG = LoggerFactory.getLogger("QTestUtil");
  private final static String defaultInitScript = "q_test_init.sql";
  private final static String defaultCleanupScript = "q_test_cleanup.sql";
  private final String[] testOnlyCommands = new String[]{"crypto"};

  private static final String TEST_TMP_DIR_PROPERTY = "test.tmp.dir"; // typically target/tmp
  private static final String BUILD_DIR_PROPERTY = "build.dir"; // typically target

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
  private final Set<String> qNoSessionReuseQuerySet;
  private final Set<String> qJavaVersionSpecificOutput;
  private static final String SORT_SUFFIX = ".sorted";
  private final HashSet<String> srcTables;
  private final Set<String> srcUDFs;
  private final MiniClusterType clusterType;
  private final FsType fsType;
  private ParseDriver pd;
  protected Hive db;
  protected QueryState queryState;
  protected HiveConf conf;
  private Driver drv;
  private BaseSemanticAnalyzer sem;
  protected final boolean overWrite;
  private CliDriver cliDriver;
  private HadoopShims.MiniMrShim mr = null;
  private HadoopShims.MiniDFSShim dfs = null;
  private FileSystem fs;
  private HadoopShims.HdfsEncryptionShim hes = null;
  private MiniLlapCluster llapCluster = null;
  private String hadoopVer = null;
  private QTestSetup setup = null;
  private TezSessionState tezSessionState = null;
  private SparkSession sparkSession = null;
  private boolean isSessionStateStarted = false;
  private static final String javaVersion = getJavaVersion();

  private final String initScript;
  private final String cleanupScript;
  private boolean useHBaseMetastore = false;

  public interface SuiteAddTestFunctor {
    public void addTestToSuite(TestSuite suite, Object setup, String tName);
  }
  private HBaseTestingUtility utility;

  HashSet<String> getSrcTables() {
    HashSet<String> srcTables = new HashSet<String>();
    // FIXME: moved default value to here...for now
    // i think this features is never really used from the command line
    String defaultTestSrcTables = "src,src1,srcbucket,srcbucket2,src_json,src_thrift,src_sequencefile,srcpart,alltypesorc,src_hbase,cbo_t1,cbo_t2,cbo_t3,src_cbo,part,lineitem";
    for (String srcTable : System.getProperty("test.src.tables", defaultTestSrcTables).trim().split(",")) {
      srcTable = srcTable.trim();
      if (!srcTable.isEmpty()) {
        srcTables.add(srcTable);
      }
    }
    if (srcTables.isEmpty()) {
      throw new RuntimeException("Source tables cannot be empty");
    }
    return srcTables;
  }

  /**
   * Returns the default UDF names which should not be removed when resetting the test database
   * @return The list of the UDF names not to remove
   */
  private Set<String> getSrcUDFs() {
    HashSet<String> srcUDFs = new HashSet<String>();
    // FIXME: moved default value to here...for now
    // i think this features is never really used from the command line
    String defaultTestSrcUDFs = "qtest_get_java_boolean";
    for (String srcUDF : System.getProperty("test.src.udfs", defaultTestSrcUDFs).trim().split(","))
    {
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

        if (file.isDirectory()) {
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

    if (!useHBaseMetastore) {
      // Plug verifying metastore in for testing DirectSQL.
      conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
        "org.apache.hadoop.hive.metastore.VerifyingObjectStore");
    } else {
      conf.setVar(ConfVars.METASTORE_RAW_STORE_IMPL, HBaseStore.class.getName());
      conf.setBoolVar(ConfVars.METASTORE_FASTPATH, true);
    }

    if (mr != null) {
      mr.setupConfiguration(conf);

      // TODO Ideally this should be done independent of whether mr is setup or not.
      setFsRelatedProperties(conf, fs.getScheme().equals("file"),fs);
    }

    if (llapCluster != null) {
      Configuration clusterSpecificConf = llapCluster.getClusterSpecificConfiguration();
      for (Map.Entry<String, String> confEntry : clusterSpecificConf) {
        // Conf.get takes care of parameter replacement, iterator.value does not.
        conf.set(confEntry.getKey(), clusterSpecificConf.get(confEntry.getKey()));
      }
    }
  }

  private void setFsRelatedProperties(HiveConf conf, boolean isLocalFs, FileSystem fs) {
    String fsUriString = fs.getUri().toString();

    // Different paths if running locally vs a remote fileSystem. Ideally this difference should not exist.
    Path warehousePath;
    Path jarPath;
    Path userInstallPath;
    if (isLocalFs) {
      String buildDir = System.getProperty(BUILD_DIR_PROPERTY);
      Preconditions.checkState(Strings.isNotBlank(buildDir));
      Path path = new Path(fsUriString, buildDir);

      // Create a fake fs root for local fs
      Path localFsRoot  = new Path(path, "localfs");
      warehousePath = new Path(localFsRoot, "warehouse");
      jarPath = new Path(localFsRoot, "jar");
      userInstallPath = new Path(localFsRoot, "user_install");
    } else {
      // TODO Why is this changed from the default in hive-conf?
      warehousePath = new Path(fsUriString, "/build/ql/test/data/warehouse/");
      jarPath = new Path(new Path(fsUriString, "/user"), "hive");
      userInstallPath = new Path(fsUriString, "/user");
    }

    warehousePath = fs.makeQualified(warehousePath);
    jarPath = fs.makeQualified(jarPath);
    userInstallPath = fs.makeQualified(userInstallPath);

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsUriString);

    // Remote dirs
    conf.setVar(ConfVars.METASTOREWAREHOUSE, warehousePath.toString());
    conf.setVar(ConfVars.HIVE_JAR_DIRECTORY, jarPath.toString());
    conf.setVar(ConfVars.HIVE_USER_INSTALL_DIR, userInstallPath.toString());
    // ConfVars.SCRATCHDIR - {test.tmp.dir}/scratchdir

    // Local dirs
    // ConfVars.LOCALSCRATCHDIR - {test.tmp.dir}/localscratchdir

    // TODO Make sure to cleanup created dirs.
  }

  private void createRemoteDirs() {
    assert fs != null;
    Path warehousePath = fs.makeQualified(new Path(conf.getVar(ConfVars.METASTOREWAREHOUSE)));
    assert warehousePath != null;
    Path hiveJarPath = fs.makeQualified(new Path(conf.getVar(ConfVars.HIVE_JAR_DIRECTORY)));
    assert hiveJarPath != null;
    Path userInstallPath = fs.makeQualified(new Path(conf.getVar(ConfVars.HIVE_USER_INSTALL_DIR)));
    assert userInstallPath != null;
    try {
      fs.mkdirs(warehousePath);
    } catch (IOException e) {
      LOG.error("Failed to create path={}. Continuing. Exception message={}", warehousePath,
          e.getMessage());
    }
    try {
      fs.mkdirs(hiveJarPath);
    } catch (IOException e) {
      LOG.error("Failed to create path={}. Continuing. Exception message={}", warehousePath,
          e.getMessage());
    }
    try {
      fs.mkdirs(userInstallPath);
    } catch (IOException e) {
      LOG.error("Failed to create path={}. Continuing. Exception message={}", warehousePath,
          e.getMessage());
    }
  }

  private enum CoreClusterType {
    MR,
    TEZ,
    SPARK
  }

  public enum FsType {
    local,
    hdfs,
    encrypted_hdfs,
  }

  public enum MiniClusterType {

    mr(CoreClusterType.MR, FsType.hdfs),
    tez(CoreClusterType.TEZ, FsType.hdfs),
    tez_local(CoreClusterType.TEZ, FsType.local),
    spark(CoreClusterType.SPARK, FsType.local),
    miniSparkOnYarn(CoreClusterType.SPARK, FsType.hdfs),
    llap(CoreClusterType.TEZ, FsType.hdfs),
    llap_local(CoreClusterType.TEZ, FsType.local),
    none(CoreClusterType.MR, FsType.local);


    private final CoreClusterType coreClusterType;
    private final FsType defaultFsType;

    MiniClusterType(CoreClusterType coreClusterType, FsType defaultFsType) {
      this.coreClusterType = coreClusterType;
      this.defaultFsType = defaultFsType;
    }

    public CoreClusterType getCoreClusterType() {
      return coreClusterType;
    }

    public FsType getDefaultFsType() {
      return defaultFsType;
    }

    public static MiniClusterType valueForString(String type) {
      // Replace this with valueOf.
      if (type.equals("miniMR")) {
        return mr;
      } else if (type.equals("tez")) {
        return tez;
      } else if (type.equals("tez_local")) {
        return tez_local;
      } else if (type.equals("spark")) {
        return spark;
      } else if (type.equals("miniSparkOnYarn")) {
        return miniSparkOnYarn;
      } else if (type.equals("llap")) {
        return llap;
      } else if (type.equals("llap_local")) {
        return llap_local;
      } else {
        return none;
      }
    }
  }


  private String getKeyProviderURI() {
    // Use the target directory if it is not specified
    String HIVE_ROOT = AbstractCliConfig.HIVE_ROOT;
    String keyDir = HIVE_ROOT + "ql/target/";

    // put the jks file in the current test path only for test purpose
    return "jceks://file" + new Path(keyDir, "test.jks").toUri();
  }

  private void startMiniHBaseCluster() throws Exception {
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.setInt("hbase.master.info.port", -1);
    utility = new HBaseTestingUtility(hbaseConf);
    utility.startMiniCluster();
    conf = new HiveConf(utility.getConfiguration(), Driver.class);
    HBaseAdmin admin = utility.getHBaseAdmin();
    // Need to use reflection here to make compilation pass since HBaseIntegrationTests
    // is not compiled in hadoop-1. All HBaseMetastore tests run under hadoop-2, so this
    // guarantee HBaseIntegrationTests exist when we hitting this code path
    java.lang.reflect.Method initHBaseMetastoreMethod = Class.forName(
        "org.apache.hadoop.hive.metastore.hbase.HBaseStoreTestUtil")
        .getMethod("initHBaseMetastore", HBaseAdmin.class, HiveConf.class);
    initHBaseMetastoreMethod.invoke(null, admin, conf);
    conf.setVar(ConfVars.METASTORE_RAW_STORE_IMPL, HBaseStore.class.getName());
    conf.setBoolVar(ConfVars.METASTORE_FASTPATH, true);
  }

  public QTestUtil(String outDir, String logDir, MiniClusterType clusterType,
                   String confDir, String hadoopVer, String initScript, String cleanupScript,
                   boolean useHBaseMetastore, boolean withLlapIo) throws Exception {
    this(outDir, logDir, clusterType, confDir, hadoopVer, initScript, cleanupScript,
        useHBaseMetastore, withLlapIo, null);
  }

  public QTestUtil(String outDir, String logDir, MiniClusterType clusterType,
      String confDir, String hadzoopVer, String initScript, String cleanupScript,
      boolean useHBaseMetastore, boolean withLlapIo, FsType fsType)
    throws Exception {
    LOG.info("Setting up QTestUtil with outDir={}, logDir={}, clusterType={}, confDir={}," +
        " hadoopVer={}, initScript={}, cleanupScript={}, useHbaseMetaStore={}, withLlapIo={}," +
            " fsType={}"
        , outDir, logDir, clusterType, confDir, hadoopVer, initScript, cleanupScript,
        useHBaseMetastore, withLlapIo, fsType);
    Preconditions.checkNotNull(clusterType, "ClusterType cannot be null");
    if (fsType != null) {
      this.fsType = fsType;
    } else {
      this.fsType = clusterType.getDefaultFsType();
    }
    this.outDir = outDir;
    this.logDir = logDir;
    this.useHBaseMetastore = useHBaseMetastore;
    this.srcTables=getSrcTables();
    this.srcUDFs = getSrcUDFs();

    // HIVE-14443 move this fall-back logic to CliConfigs
    if (confDir != null && !confDir.isEmpty()) {
      HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
      System.out.println("Setting hive-site: "+HiveConf.getHiveSiteLocation());
    }

    queryState = new QueryState(new HiveConf(Driver.class));
    if (useHBaseMetastore) {
      startMiniHBaseCluster();
    } else {
      conf = queryState.getConf();
    }
    this.hadoopVer = getHadoopMainVersion(hadoopVer);
    qMap = new TreeMap<String, String>();
    qSkipSet = new HashSet<String>();
    qSortSet = new HashSet<String>();
    qSortQuerySet = new HashSet<String>();
    qHashQuerySet = new HashSet<String>();
    qSortNHashQuerySet = new HashSet<String>();
    qNoSessionReuseQuerySet = new HashSet<String>();
    qJavaVersionSpecificOutput = new HashSet<String>();
    this.clusterType = clusterType;

    HadoopShims shims = ShimLoader.getHadoopShims();

    setupFileSystem(shims);

    setup = new QTestSetup();
    setup.preTest(conf);

    setupMiniCluster(shims, confDir);

    initConf();

    if (withLlapIo && (clusterType == MiniClusterType.none)) {
      LOG.info("initializing llap IO");
      LlapProxy.initializeLlapIo(conf);
    }


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

    this.initScript = scriptsDir + File.separator + initScript;
    this.cleanupScript = scriptsDir + File.separator + cleanupScript;

    overWrite = "true".equalsIgnoreCase(System.getProperty("test.output.overwrite"));

    init();
  }

  private void setupFileSystem(HadoopShims shims) throws IOException {

    if (fsType == FsType.local) {
      fs = FileSystem.getLocal(conf);
    } else if (fsType == FsType.hdfs || fsType == FsType.encrypted_hdfs) {
      int numDataNodes = 4;

      if (fsType == FsType.encrypted_hdfs) {
        // Set the security key provider so that the MiniDFS cluster is initialized
        // with encryption
        conf.set(SECURITY_KEY_PROVIDER_URI_NAME, getKeyProviderURI());
        conf.setInt("fs.trash.interval", 50);

        dfs = shims.getMiniDfs(conf, numDataNodes, true, null);
        fs = dfs.getFileSystem();

        // set up the java key provider for encrypted hdfs cluster
        hes = shims.createHdfsEncryptionShim(fs, conf);

        LOG.info("key provider is initialized");
      } else {
        dfs = shims.getMiniDfs(conf, numDataNodes, true, null);
        fs = dfs.getFileSystem();
      }
    } else {
      throw new IllegalArgumentException("Unknown or unhandled fsType [" + fsType + "]");
    }
  }

  private void setupMiniCluster(HadoopShims shims, String confDir) throws
      IOException {

    String uriString = fs.getUri().toString();

    if (clusterType.getCoreClusterType() == CoreClusterType.TEZ) {
      if (confDir != null && !confDir.isEmpty()) {
        conf.addResource(new URL("file://" + new File(confDir).toURI().getPath()
            + "/tez-site.xml"));
      }
      int numTrackers = 2;
      if (EnumSet.of(MiniClusterType.llap, MiniClusterType.llap_local).contains(clusterType)) {
        llapCluster = LlapItUtils.startAndGetMiniLlapCluster(conf, setup.zooKeeperCluster, confDir);
      } else {
      }
      if (EnumSet.of(MiniClusterType.llap_local, MiniClusterType.tez_local).contains(clusterType)) {
        mr = shims.getLocalMiniTezCluster(conf, clusterType == MiniClusterType.llap_local);
      } else {
        mr = shims.getMiniTezCluster(conf, numTrackers, uriString,
            EnumSet.of(MiniClusterType.llap, MiniClusterType.llap_local).contains(clusterType));
      }
    } else if (clusterType == MiniClusterType.miniSparkOnYarn) {
      mr = shims.getMiniSparkCluster(conf, 2, uriString, 1);
    } else if (clusterType == MiniClusterType.mr) {
      mr = shims.getMiniMrCluster(conf, 2, uriString, 1);
    }
  }


  public void shutdown() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) == null) {
      cleanUp();
    }

    if (clusterType.getCoreClusterType() == CoreClusterType.TEZ) {
      SessionState.get().getTezSession().close(false);
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
    if (useHBaseMetastore) {
      utility.shutdownMiniCluster();
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
    Hive.closeCurrent();
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

    if(checkHadoopVersionExclude(qf.getName(), query)) {
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
    if (matches(NO_SESSION_REUSE, query)) {
      qNoSessionReuseQuerySet.add(qf.getName());
    }
  }

  private static final Pattern SORT_BEFORE_DIFF = Pattern.compile("-- SORT_BEFORE_DIFF");
  private static final Pattern SORT_QUERY_RESULTS = Pattern.compile("-- SORT_QUERY_RESULTS");
  private static final Pattern HASH_QUERY_RESULTS = Pattern.compile("-- HASH_QUERY_RESULTS");
  private static final Pattern SORT_AND_HASH_QUERY_RESULTS = Pattern.compile("-- SORT_AND_HASH_QUERY_RESULTS");
  private static final Pattern NO_SESSION_REUSE = Pattern.compile("-- NO_SESSION_REUSE");

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

    conf.set("hive.metastore.filter.hook",
        "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl");
    db = Hive.get(conf);
    // Delete any tables other than the source tables
    // and any databases other than the default database.
    for (String dbName : db.getAllDatabases()) {
      SessionState.get().setCurrentDatabase(dbName);
      for (String tblName : db.getAllTables()) {
        if (!DEFAULT_DATABASE_NAME.equals(dbName) || !srcTables.contains(tblName)) {
          Table tblObj = null;
          try {
            tblObj = db.getTable(tblName);
          } catch (InvalidTableException e) {
            LOG.warn("Trying to drop table " + e.getTableName() + ". But it does not exist.");
            continue;
          }
          // dropping index table can not be dropped directly. Dropping the base
          // table will automatically drop all its index table
          if(tblObj.isIndexTable()) {
            continue;
          }
          db.dropTable(dbName, tblName, true, true, fsType == FsType.encrypted_hdfs);
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
          if (status.isDirectory() && !srcTables.contains(status.getPath().getName())) {
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

    // allocate and initialize a new conf since a test can
    // modify conf by using 'set' commands
    conf = new HiveConf(Driver.class);
    initConf();
    initConfFromSetup();

    // renew the metastore since the cluster type is unencrypted
    db = Hive.get(conf);  // propagate new conf to meta store

    clearTablesCreatedDuringTests();
    clearUDFsCreatedDuringTests();
    clearKeysCreatedInTests();
  }

  protected void initConfFromSetup() throws Exception {
    setup.preTest(conf);
  }

  public void cleanUp() throws Exception {
    cleanUp(null);
  }

  public void cleanUp(String tname) throws Exception {
    boolean canReuseSession = (tname == null) || !qNoSessionReuseQuerySet.contains(tname);
    if(!isSessionStateStarted) {
      startSessionState(canReuseSession);
    }
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }

    clearTablesCreatedDuringTests();
    clearUDFsCreatedDuringTests();
    clearKeysCreatedInTests();

    File cleanupFile = new File(cleanupScript);
    if (cleanupFile.isFile()) {
      String cleanupCommands = readEntireFileIntoString(cleanupFile);
      LOG.info("Cleanup (" + cleanupScript + "):\n" + cleanupCommands);
      if(cliDriver == null) {
        cliDriver = new CliDriver();
      }
      SessionState.get().getConf().setBoolean("hive.test.shutdown.phase", true);
      int result = cliDriver.processLine(cleanupCommands);
      if (result != 0) {
        LOG.error("Failed during cleanup processLine with code={}. Ignoring", result);
        // TODO Convert this to an Assert.fail once HIVE-14682 is fixed
      }
      SessionState.get().getConf().setBoolean("hive.test.shutdown.phase", false);
    } else {
      LOG.info("No cleanup script detected. Skipping.");
    }

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

    // TODO: Clean up all the other paths that are created.

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
    createSources(null);
  }

  public void createSources(String tname) throws Exception {
    boolean canReuseSession = (tname == null) || !qNoSessionReuseQuerySet.contains(tname);
    if(!isSessionStateStarted) {
      startSessionState(canReuseSession);
    }

    if(cliDriver == null) {
      cliDriver = new CliDriver();
    }
    cliDriver.processLine("set test.data.dir=" + testFiles + ";");
    File scriptFile = new File(this.initScript);
    if (!scriptFile.isFile()) {
      LOG.info("No init script detected. Skipping");
      return;
    }
    conf.setBoolean("hive.test.init.phase", true);

    String initCommands = readEntireFileIntoString(scriptFile);
    LOG.info("Initial setup (" + initScript + "):\n" + initCommands);

    int result = cliDriver.processLine(initCommands);
    LOG.info("Result from cliDrriver.processLine in createSources=" + result);
    if (result != 0) {
      Assert.fail("Failed during createSources processLine with code=" + result);
    }

    conf.setBoolean("hive.test.init.phase", false);
  }

  public void init() throws Exception {

    // Create remote dirs once.
    if (mr != null) {
      createRemoteDirs();
    }

    testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    String execEngine = conf.get("hive.execution.engine");
    conf.set("hive.execution.engine", "mr");
    SessionState.start(conf);
    conf.set("hive.execution.engine", execEngine);
    db = Hive.get(conf);
    drv = new Driver(conf);
    drv.init();
    pd = new ParseDriver();
    sem = new SemanticAnalyzer(queryState);
  }

  public void init(String tname) throws Exception {
    cleanUp(tname);
    createSources(tname);
    cliDriver.processCmd("set hive.cli.print.header=true;");
  }

  public void cliInit(String tname) throws Exception {
    cliInit(tname, true);
  }

  public String cliInit(String tname, boolean recreate) throws Exception {
    if (recreate) {
      cleanUp(tname);
      createSources(tname);
    }

    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
    "org.apache.hadoop.hive.ql.security.DummyAuthenticator");
    Utilities.clearWorkMap(conf);
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

    boolean canReuseSession = !qNoSessionReuseQuerySet.contains(tname);
    if (oldSs != null && canReuseSession && clusterType.getCoreClusterType() == CoreClusterType.TEZ) {
      // Copy the tezSessionState from the old CliSessionState.
      tezSessionState = oldSs.getTezSession();
      oldSs.setTezSession(null);
      ss.setTezSession(tezSessionState);
      oldSs.close();
    }

    if (oldSs != null && clusterType.getCoreClusterType() == CoreClusterType.SPARK) {
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
      ss.initFiles.add(AbstractCliConfig.HIVE_ROOT + "/data/scripts/test_init_file.sql");
    }
    cliDriver.processInitFiles(ss);

    return outf.getAbsolutePath();
  }

  private CliSessionState createSessionState() {
   return new CliSessionState(conf) {
      @Override
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

  private CliSessionState startSessionState(boolean canReuseSession)
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
    if (oldSs != null && canReuseSession && clusterType.getCoreClusterType() == CoreClusterType.TEZ) {
      // Copy the tezSessionState from the old CliSessionState.
      tezSessionState = oldSs.getTezSession();
      ss.setTezSession(tezSessionState);
      oldSs.setTezSession(null);
      oldSs.close();
    }

    if (oldSs != null && clusterType.getCoreClusterType() == CoreClusterType.SPARK) {
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

  public int executeAdhocCommand(String q) {
    if (!q.contains(";")) {
      return -1;
    }

    String q1 = q.split(";")[0] + ";";

    LOG.debug("Executing " + q1);
    return cliDriver.processLine(q1);
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
    List<String> cmds = CliDriver.splitSemiColon(commands);
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

    if (SessionState.get() != null) {
      SessionState.get().setLastCommand(commandName + " " + commandArgs.trim());
    }

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
    // remove the comments
    while (commentMatcher.find()) {
      newCommands.append(commands.substring(lastMatchEnd, commentMatcher.start()));
      lastMatchEnd = commentMatcher.end();
    }
    newCommands.append(commands.substring(lastMatchEnd, commands.length()));
    commands = newCommands.toString();
    return commands;
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
      if (fsType == FsType.encrypted_hdfs) {
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
        for (Pair<Pattern, String> pair : patternsWithMaskComments) {
          Pattern pattern = pair.getLeft();
          String maskComment = pair.getRight();

          matcher = pattern.matcher(line);
          if (matcher.find()) {
            line = matcher.replaceAll(maskComment);
            partialMaskWasMatched = true;
            break;
          }
        }

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
          partialMaskWasMatched = false;
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
      ".*DagId:.*",
      ".*Input:.*/data/files/.*",
      ".*Output:.*/data/files/.*",
      ".*total number of created files now is.*",
      ".*.hive-staging.*",
      "pk_-?[0-9]*_[0-9]*_[0-9]*",
      "fk_-?[0-9]*_[0-9]*_[0-9]*",
      ".*at com\\.sun\\.proxy.*",
      ".*at com\\.jolbox.*",
      ".*at com\\.zaxxer.*",
      "org\\.apache\\.hadoop\\.hive\\.metastore\\.model\\.MConstraint@([0-9]|[a-z])*"
  });

  private final Pattern[] partialReservedPlanMask = toPattern(new String[] {
      "data/warehouse/(.*?/)+\\.hive-staging"  // the directory might be db/table/partition
      //TODO: add more expected test result here
  });

  /* This list may be modified by specific cli drivers to mask strings that change on every test */
  private List<Pair<Pattern, String>> patternsWithMaskComments = new ArrayList<Pair<Pattern, String>>() {{
    add(toPatternPair("(pblob|s3.?|swift|wasb.?).*hive-staging.*","### BLOBSTORE_STAGING_PATH ###"));
  }};

  private Pair<Pattern, String> toPatternPair(String patternStr, String maskComment) {
    return ImmutablePair.of(Pattern.compile(patternStr), maskComment);
  }

  public void addPatternWithMaskComment(String patternStr, String maskComment) {
    patternsWithMaskComments.add(toPatternPair(patternStr, maskComment));
  }

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
    if (ignoreWhiteSpace) {
      diffCommandArgs.add("-b");
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
    return str;
  }

  public ASTNode parseQuery(String tname) throws Exception {
    return pd.parse(qMap.get(tname));
  }

  public void resetParser() throws SemanticException {
    drv.init();
    pd = new ParseDriver();
    queryState = new QueryState(conf);
    sem = new SemanticAnalyzer(queryState);
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
        String tmpBaseDir =  System.getProperty(TEST_TMP_DIR_PROPERTY);
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
      String logDir, String initScript, String cleanupScript) throws Exception
  {
    QTestUtil[] qt = new QTestUtil[qfiles.length];
    for (int i = 0; i < qfiles.length; i++) {
      qt[i] = new QTestUtil(resDir, logDir, MiniClusterType.none, null, "0.20",
        initScript == null ? defaultInitScript : initScript,
        cleanupScript == null ? defaultCleanupScript : cleanupScript,
        false, false);
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
    String message = "Client execution failed with error code = " + ecode +
        (command != null ? " running \"" + command : "") + "\" fname=" + fname + " " +
        (debugHint != null ? debugHint : "");
    LOG.error(message);
    Assert.fail(message);
  }

  // for negative tests, which is succeeded.. no need to print the query string
  public void failed(String fname, String debugHint) {
    Assert.fail(
        "Client Execution was expected to fail, but succeeded with error code 0 for fname=" +
            fname + (debugHint != null ? (" " + debugHint) : ""));
  }

  public void failedDiff(int ecode, String fname, String debugHint) {
    String message =
        "Client Execution results failed with error code = " + ecode + " while executing fname=" +
            fname + (debugHint != null ? (" " + debugHint) : "");
    LOG.error(message);
    Assert.fail(message);
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

  public static void setupMetaStoreTableColumnStatsFor30TBTPCDSWorkload(HiveConf conf) {
    Connection conn = null;
    ArrayList<Statement> statements = new ArrayList<Statement>(); // list of Statements, PreparedStatements

    try {
      Properties props = new Properties(); // connection properties
      props.put("user", conf.get("javax.jdo.option.ConnectionUserName"));
      props.put("password", conf.get("javax.jdo.option.ConnectionPassword"));
      conn = DriverManager.getConnection(conf.get("javax.jdo.option.ConnectionURL"), props);
      ResultSet rs = null;
      Statement s = conn.createStatement();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Connected to metastore database ");
      }

      String mdbPath =   AbstractCliConfig.HIVE_ROOT + "/data/files/tpcds-perf/metastore_export/";

      // Setup the table column stats
      BufferedReader br = new BufferedReader(new FileReader(new File(AbstractCliConfig.HIVE_ROOT + "/metastore/scripts/upgrade/derby/022-HIVE-11107.derby.sql")));
      String command;

      s.execute("DROP TABLE APP.TABLE_PARAMS");
      s.execute("DROP TABLE APP.TAB_COL_STATS");
      // Create the column stats table
      while ((command = br.readLine()) != null) {
        if (!command.endsWith(";")) {
          continue;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to run command : " + command);
        }
        try {
          PreparedStatement psCommand = conn.prepareStatement(command.substring(0, command.length()-1));
          statements.add(psCommand);
          psCommand.execute();
          if (LOG.isDebugEnabled()) {
            LOG.debug("successfully completed " + command);
          }
        } catch (SQLException e) {
          LOG.info("Got SQL Exception " + e.getMessage());
        }
      }
      br.close();

      File tabColStatsCsv = new File(mdbPath+"csv/TAB_COL_STATS.txt");
      File tabParamsCsv = new File(mdbPath+"csv/TABLE_PARAMS.txt");

      // Set up the foreign key constraints properly in the TAB_COL_STATS data
      String tmpBaseDir =  System.getProperty(TEST_TMP_DIR_PROPERTY);
      File tmpFileLoc1 = new File(tmpBaseDir+"/TAB_COL_STATS.txt");
      File tmpFileLoc2 = new File(tmpBaseDir+"/TABLE_PARAMS.txt");
      FileUtils.copyFile(tabColStatsCsv, tmpFileLoc1);
      FileUtils.copyFile(tabParamsCsv, tmpFileLoc2);

      class MyComp implements Comparator<String> {
        @Override
        public int compare(String str1, String str2) {
          if (str2.length() != str1.length()) {
            return str2.length() - str1.length();
          }
          return str1.compareTo(str2);
        }
      }

      SortedMap<String, Integer> tableNameToID = new TreeMap<String, Integer>(new MyComp());

     rs = s.executeQuery("SELECT * FROM APP.TBLS");
      while(rs.next()) {
        String tblName = rs.getString("TBL_NAME");
        Integer tblId = rs.getInt("TBL_ID");
        tableNameToID.put(tblName, tblId);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Resultset : " +  tblName + " | " + tblId);
        }
      }

      for (Map.Entry<String, Integer> entry : tableNameToID.entrySet()) {
        String toReplace1 = ",_" + entry.getKey() + "_" ;
        String replacementString1 = ","+entry.getValue();
        String toReplace2 = "_" + entry.getKey() + "_@" ;
        String replacementString2 = ""+entry.getValue()+"@";
        try {
          String content1 = FileUtils.readFileToString(tmpFileLoc1, "UTF-8");
          content1 = content1.replaceAll(toReplace1, replacementString1);
          FileUtils.writeStringToFile(tmpFileLoc1, content1, "UTF-8");
          String content2 = FileUtils.readFileToString(tmpFileLoc2, "UTF-8");
          content2 = content2.replaceAll(toReplace2, replacementString2);
          FileUtils.writeStringToFile(tmpFileLoc2, content2, "UTF-8");
        } catch (IOException e) {
          LOG.info("Generating file failed", e);
        }
      }

      // Load the column stats and table params with 30 TB scale
      String importStatement1 =  "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, '" + "TAB_COL_STATS" +
        "', '" + tmpFileLoc1.getAbsolutePath() +
        "', ',', null, 'UTF-8', 1)";
      String importStatement2 =  "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, '" + "TABLE_PARAMS" +
        "', '" + tmpFileLoc2.getAbsolutePath() +
        "', '@', null, 'UTF-8', 1)";
      try {
        PreparedStatement psImport1 = conn.prepareStatement(importStatement1);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to execute : " + importStatement1);
        }
        statements.add(psImport1);
        psImport1.execute();
        if (LOG.isDebugEnabled()) {
          LOG.debug("successfully completed " + importStatement1);
        }
        PreparedStatement psImport2 = conn.prepareStatement(importStatement2);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to execute : " + importStatement2);
        }
        statements.add(psImport2);
        psImport2.execute();
        if (LOG.isDebugEnabled()) {
          LOG.debug("successfully completed " + importStatement2);
        }
      } catch (SQLException e) {
        LOG.info("Got SQL Exception  " +  e.getMessage());
      }
    } catch (FileNotFoundException e1) {
        LOG.info("Got File not found Exception " + e1.getMessage());
	} catch (IOException e1) {
        LOG.info("Got IOException " + e1.getMessage());
	} catch (SQLException e1) {
        LOG.info("Got SQLException " + e1.getMessage());
	} finally {
      // Statements and PreparedStatements
      int i = 0;
      while (!statements.isEmpty()) {
        // PreparedStatement extend Statement
        Statement st = statements.remove(i);
        try {
          if (st != null) {
            st.close();
            st = null;
          }
        } catch (SQLException sqle) {
        }
      }

      //Connection
      try {
        if (conn != null) {
          conn.close();
          conn = null;
        }
      } catch (SQLException sqle) {
      }
    }
  }

}
