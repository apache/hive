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

package org.apache.hadoop.hive.ql.util;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.cli.CommonCliOptions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.HiveStrictManagedUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

public class HiveStrictManagedMigration {

  private static final Logger LOG = LoggerFactory.getLogger(HiveStrictManagedMigration.class);
  @VisibleForTesting
  static int RC = 0;

  public enum TableMigrationOption {
    NONE,      // Do nothing
    VALIDATE,  // No migration, just validate that the tables
    AUTOMATIC, // Automatically determine if the table should be managed or external
    EXTERNAL,  // Migrate tables to external tables
    MANAGED    // Migrate tables as managed transactional tables
  }

  private static class RunOptions {
    final String dbRegex;
    final String tableRegex;
    final String oldWarehouseRoot;
    final TableMigrationOption migrationOption;
    final Properties confProps;
    boolean shouldModifyManagedTableLocation;
    final boolean shouldModifyManagedTableOwner;
    final boolean shouldModifyManagedTablePermissions;
    boolean shouldMoveExternal;
    final boolean dryRun;
    final TableType tableType;
    final int tablePoolSize;
    final String fsOperationUser;
    final String controlFileUrl;

    RunOptions(String dbRegex,
               String tableRegex,
               String oldWarehouseRoot,
               TableMigrationOption migrationOption,
               Properties confProps,
               boolean shouldModifyManagedTableLocation,
               boolean shouldModifyManagedTableOwner,
               boolean shouldModifyManagedTablePermissions,
               boolean shouldMoveExternal,
               boolean dryRun,
               TableType tableType,
               int tablePoolSize,
               String fsOperationUser,
               String controlFileUrl) {
      super();
      this.dbRegex = dbRegex;
      this.tableRegex = tableRegex;
      this.oldWarehouseRoot = oldWarehouseRoot;
      this.migrationOption = migrationOption;
      this.confProps = confProps;
      this.shouldModifyManagedTableLocation = shouldModifyManagedTableLocation;
      this.shouldModifyManagedTableOwner = shouldModifyManagedTableOwner;
      this.shouldModifyManagedTablePermissions = shouldModifyManagedTablePermissions;
      this.shouldMoveExternal = shouldMoveExternal;
      this.dryRun = dryRun;
      this.tableType = tableType;
      this.tablePoolSize = tablePoolSize;
      this.fsOperationUser = fsOperationUser;
      this.controlFileUrl = controlFileUrl;
    }

    public void setShouldModifyManagedTableLocation(boolean shouldModifyManagedTableLocation) {
      this.shouldModifyManagedTableLocation = shouldModifyManagedTableLocation;
    }

    public void setShouldMoveExternal(boolean shouldMoveExternal) {
      this.shouldMoveExternal = shouldMoveExternal;
    }

    @Override
    public String toString() {
      return "RunOptions{" +
        "dbRegex='" + dbRegex + '\'' +
        ", tableRegex='" + tableRegex + '\'' +
        ", oldWarehouseRoot='" + oldWarehouseRoot + '\'' +
        ", migrationOption=" + migrationOption +
        ", confProps=" + confProps +
        ", shouldModifyManagedTableLocation=" + shouldModifyManagedTableLocation +
        ", shouldModifyManagedTableOwner=" + shouldModifyManagedTableOwner +
        ", shouldModifyManagedTablePermissions=" + shouldModifyManagedTablePermissions +
        ", shouldMoveExternal=" + shouldMoveExternal +
        ", dryRun=" + dryRun +
        ", tableType=" + tableType +
        ", tablePoolSize=" + tablePoolSize +
        ", fsOperationUser=" + fsOperationUser +
        ", controlFileUrl=" + controlFileUrl +
        '}';
    }
  }

  private static class OwnerPermsOptions {
    final String ownerName;
    final String groupName;
    final FsPermission dirPerms;
    final FsPermission filePerms;

    OwnerPermsOptions(String ownerName, String groupName, FsPermission dirPerms, FsPermission filePerms) {
      this.ownerName = ownerName;
      this.groupName = groupName;
      this.dirPerms = dirPerms;
      this.filePerms = filePerms;
    }
  }

  private static class WarehouseRootCheckResult {
    final boolean shouldModifyManagedTableLocation;
    final boolean shouldMoveExternal;
    final Path targetPath;
    final HadoopShims.HdfsEncryptionShim encryptionShim;
    final HadoopShims.HdfsErasureCodingShim ecShim;

    WarehouseRootCheckResult(
      boolean shouldModifyManagedTableLocation,
      boolean shouldMoveExternal,
      Path curWhRootPath,
      HadoopShims.HdfsEncryptionShim encryptionShim,
      HadoopShims.HdfsErasureCodingShim ecShim) {
      this.shouldModifyManagedTableLocation = shouldModifyManagedTableLocation;
      this.shouldMoveExternal = shouldMoveExternal;
      this.targetPath = curWhRootPath;
      this.encryptionShim = encryptionShim;
      this.ecShim = ecShim;
    }
  }

  public static void main(String[] args) throws Exception {
    RunOptions runOptions;
    RC = 0;

    try {
      Options opts = createOptions();
      CommandLine cli = new GnuParser().parse(opts, args);

      if (cli.hasOption('h')) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(HiveStrictManagedMigration.class.getName(), opts);
        return;
      }

      runOptions = createRunOptions(cli);
    } catch (Exception err) {
      throw new Exception("Error processing options", err);
    }

    HiveStrictManagedMigration migration = null;
    try {
      HiveConf conf = hiveConf == null ? new HiveConf() : hiveConf;
      WarehouseRootCheckResult warehouseRootCheckResult = checkOldWarehouseRoot(runOptions, conf);
      runOptions.setShouldModifyManagedTableLocation(
        warehouseRootCheckResult.shouldModifyManagedTableLocation);
      runOptions.setShouldMoveExternal(
        warehouseRootCheckResult.shouldMoveExternal);
      boolean createExternalDirsForDbs = checkExternalWarehouseDir(conf);
      OwnerPermsOptions ownerPermsOptions = checkOwnerPermsOptions(runOptions, conf);

      migration = new HiveStrictManagedMigration(
        conf, runOptions, createExternalDirsForDbs, ownerPermsOptions, warehouseRootCheckResult);
      migration.run();
    } catch (Exception err) {
      LOG.error("Failed with error", err);
      RC = -1;
    } finally {
      if (migration != null) {
        migration.cleanup();
      }
    }

    // TODO: Something is preventing the process from terminating after main(), adding exit() as hacky solution.
    if (hiveConf == null) {
      System.exit(RC);
    }
  }

  static Options createOptions() {
    Options result = new Options();

    // -hiveconf x=y
    result.addOption(OptionBuilder
      .withValueSeparator()
      .hasArgs(2)
      .withArgName("property=value")
      .withLongOpt("hiveconf")
      .withDescription("Use value for given property")
      .create());

    result.addOption(OptionBuilder
      .withLongOpt("dryRun")
      .withDescription("Show what migration actions would be taken without actually running commands")
      .create());

    result.addOption(OptionBuilder
      .withLongOpt("dbRegex")
      .withDescription("Regular expression to match database names on which this tool will be run")
      .hasArg()
      .create('d'));

    result.addOption(OptionBuilder
      .withLongOpt("tableRegex")
      .withDescription("Regular expression to match table names on which this tool will be run")
      .hasArg()
      .create('t'));

    result.addOption(OptionBuilder
      .withLongOpt("oldWarehouseRoot")
      .withDescription("Location of the previous warehouse root")
      .hasArg()
      .create());

    result.addOption(OptionBuilder
      .withLongOpt("migrationOption")
      .withDescription("Table migration option (automatic|external|managed|validate|none)")
      .hasArg()
      .create('m'));

    result.addOption(OptionBuilder
      .withLongOpt("shouldModifyManagedTableLocation")
      .withDescription("Whether managed tables should have their data moved from " +
        "the old warehouse path to the current warehouse path")
      .create());

    result.addOption(OptionBuilder
      .withLongOpt("shouldModifyManagedTableOwner")
      .withDescription("Whether managed tables should have their directory owners changed to the hive user")
      .create());

    result.addOption(OptionBuilder
      .withLongOpt("shouldModifyManagedTablePermissions")
      .withDescription("Whether managed tables should have their directory permissions changed to conform to " +
        "strict managed tables mode")
      .create());

    result.addOption(OptionBuilder
      .withLongOpt("modifyManagedTables")
      .withDescription("This setting enables the shouldModifyManagedTableLocation, " +
        "shouldModifyManagedTableOwner, shouldModifyManagedTablePermissions options")
      .create());

    result.addOption(OptionBuilder
      .withLongOpt("shouldMoveExternal")
      .withDescription("Whether tables living in the old warehouse path should have their data moved to the" +
        " default external location. Applicable only if migrationOption = external")
      .create());

    result.addOption(OptionBuilder
      .withLongOpt("help")
      .withDescription("print help message")
      .create('h'));

    result.addOption(OptionBuilder
      .withLongOpt("tablePoolSize")
      .withDescription("Number of threads to process tables.")
      .hasArg()
      .create("tn"));

    result.addOption(OptionBuilder
      .withLongOpt("tableType")
      .withDescription(String.format("Table type to match tables on which this tool will be run. " +
          "Possible values: %s Default: all tables",
        Arrays.stream(TableType.values()).map(Enum::name).collect(Collectors.joining("|"))))
      .hasArg()
      .withArgName("table type")
      .create("tt"));

    result.addOption(OptionBuilder
      .withLongOpt("fsOperationUser")
      .withDescription("If set, migration tool will impersonate this user to carry out write operations on file " +
        "system. Useful e.g. if this tool is run as hive, but chown-ing is also a requirement." +
        "If this is unset file operations will be run in the name of the user running this process (or kinit'ed " +
        "user in Kerberos environments)")
      .hasArg()
      .create());

    result.addOption(OptionBuilder
        .withLongOpt("controlFileUrl")
        .withDescription("If set, migration tool will expect either a YAML file on this path, or a directory " +
            "with YAML files in it. Such YAML file(s) shall contain which tables in which databases should the tool " +
            "care about. The value here will be parsed and handled by Hadoop filesystem, so absolute paths with " +
            "schemes are required.")
        .hasArg()
        .create());

    return result;
  }

  static RunOptions createRunOptions(CommandLine cli) throws Exception {
    // Process --hiveconf
    // Get hiveconf param values and set the System property values
    Properties confProps = cli.getOptionProperties("hiveconf");
    for (String propKey : confProps.stringPropertyNames()) {
      LOG.info("Setting {}={}", propKey, confProps.getProperty(propKey));
      if (propKey.equalsIgnoreCase("hive.root.logger")) {
        // TODO: logging currently goes to hive.log
        CommonCliOptions.splitAndSetLogger(propKey, confProps);
      } else {
        System.setProperty(propKey, confProps.getProperty(propKey));
      }
    }

    LogUtils.initHiveLog4j();

    String dbRegex = cli.getOptionValue("dbRegex", ".*");
    String tableRegex = cli.getOptionValue("tableRegex", ".*");
    String controlFileUrl = cli.getOptionValue("controlFileUrl", "");
    if (!StringUtils.isEmpty(controlFileUrl)) {
      if (cli.hasOption("dbRegex") || cli.hasOption("tableRegex")) {
        throw new IllegalArgumentException("No dbRegex or tableRegex options should be set if controlFileUrl is used");
      }
    }
    TableMigrationOption migrationOption =
      TableMigrationOption.valueOf(cli.getOptionValue("migrationOption", "none").toUpperCase());
    boolean shouldModifyManagedTableLocation = cli.hasOption("shouldModifyManagedTableLocation");
    boolean shouldModifyManagedTableOwner = cli.hasOption("shouldModifyManagedTableOwner");
    boolean shouldModifyManagedTablePermissions = cli.hasOption("shouldModifyManagedTablePermissions");
    if (cli.hasOption("modifyManagedTables")) {
      shouldModifyManagedTableLocation = true;
      shouldModifyManagedTableOwner = true;
      shouldModifyManagedTablePermissions = true;
    }
    String oldWarehouseRoot = cli.getOptionValue("oldWarehouseRoot");
    boolean shouldMoveExternal = cli.hasOption("shouldMoveExternal");
    if (shouldMoveExternal && !migrationOption.equals(TableMigrationOption.EXTERNAL)) {
      throw new IllegalArgumentException("Please select external as migration option, it is required for " +
        "shouldMoveExternal option.");
    }
    if (shouldModifyManagedTableLocation && shouldMoveExternal) {
      throw new IllegalArgumentException("Options shouldModifyManagedTableLocation and " +
        "shouldMoveExternal cannot be used at the same time. Migration with move option on " +
        " managed tables either ends up with them remaining managed or converted to external, but can't be both.");
    }
    boolean dryRun = cli.hasOption("dryRun");

    String fsOperationUser = cli.getOptionValue("fsOperationUser");

    String tableTypeText = cli.getOptionValue("tableType");

    int defaultPoolSize = Runtime.getRuntime().availableProcessors() / 2;
    if (defaultPoolSize < 1) {
      defaultPoolSize = 1;
    }

    int databasePoolSize = getIntOptionValue(cli, "databasePoolSize", defaultPoolSize);
    if (databasePoolSize < 1) {
      throw new IllegalArgumentException("Please specify a positive integer option value for databasePoolSize");
    }
    int tablePoolSize = getIntOptionValue(cli, "tablePoolSize", defaultPoolSize);
    if (tablePoolSize < 1) {
      throw new IllegalArgumentException("Please specify a positive integer option value for tablePoolSize");
    }

    RunOptions runOpts = new RunOptions(
      dbRegex,
      tableRegex,
      oldWarehouseRoot,
      migrationOption,
      confProps,
      shouldModifyManagedTableLocation,
      shouldModifyManagedTableOwner,
      shouldModifyManagedTablePermissions,
      shouldMoveExternal,
      dryRun,
      tableTypeText == null ? null : TableType.valueOf(tableTypeText),
      tablePoolSize,
      fsOperationUser,
      controlFileUrl);
    return runOpts;
  }

  private static int getIntOptionValue(CommandLine commandLine, String optionName, int defaultValue) {
    if (commandLine.hasOption(optionName)) {
      try {
        return Integer.parseInt(commandLine.getOptionValue(optionName));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Please specify a positive integer option value for " + optionName, e);
      }
    }
    return defaultValue;
  }

  private final HiveConf conf;
  private final RunOptions runOptions;
  private final boolean createExternalDirsForDbs;
  private final Path targetPath;
  private final HadoopShims.HdfsEncryptionShim encryptionShim;
  private final HadoopShims.HdfsErasureCodingShim ecShim;
  private final String ownerName;
  private final String groupName;
  private final FsPermission dirPerms;
  private final FsPermission filePerms;
  private final UserGroupInformation fsOperationUser;
  private final HiveStrictManagedMigrationControlConfig controlConfig;

  private CloseableThreadLocal<HiveMetaStoreClient> hms;
  private ThreadLocal<Warehouse> wh;
  private ThreadLocal<Warehouse> oldWh;
  private CloseableThreadLocal<HiveUpdater> hiveUpdater;

  private AtomicBoolean failuresEncountered;
  private AtomicBoolean failedValidationChecks;
  private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
  private final ObjectReader yamlReader = mapper.readerFor(HiveStrictManagedMigrationControlConfig.class);

  private void readControlConfigs(FileSystem fs, Path path) {
    if (!path.getName().endsWith(".yaml")) {
      throw new IllegalArgumentException("Only .yaml files are supported as control files, got: " + path);
    }
    try (FSDataInputStream fsDataInputStream = fs.open(path)) {
      controlConfig.putAllFromConfig(yamlReader.readValue(fsDataInputStream.getWrappedStream()));
      LOG.info("Control configs have been read in from " + path);
    } catch (IOException e) {
      throw new RuntimeException("Error reading control file: " + path, e);
    }
  }

  HiveStrictManagedMigration(HiveConf conf, RunOptions runOptions, boolean createExternalDirsForDbs,
                             OwnerPermsOptions ownerPermsOptions, WarehouseRootCheckResult warehouseRootCheckResult) {
    this.conf = conf;
    this.runOptions = runOptions;
    this.createExternalDirsForDbs = createExternalDirsForDbs;
    this.ownerName = ownerPermsOptions.ownerName;
    this.groupName = ownerPermsOptions.groupName;
    this.dirPerms = ownerPermsOptions.dirPerms;
    this.filePerms = ownerPermsOptions.filePerms;
    this.targetPath = warehouseRootCheckResult.targetPath;
    this.encryptionShim = warehouseRootCheckResult.encryptionShim;
    this.ecShim = warehouseRootCheckResult.ecShim;

    // Make sure all --hiveconf settings get added to the HiveConf.
    // This allows utility-specific settings (such as strict.managed.tables.migration.owner)
    // to be set via command line.
    if (runOptions.confProps != null) {
      for (String propKey : runOptions.confProps.stringPropertyNames()) {
        this.conf.set(propKey, runOptions.confProps.getProperty(propKey));
      }
    }

    try {
      if (runOptions.fsOperationUser != null) {
        fsOperationUser = UserGroupInformation.createProxyUser(runOptions.fsOperationUser,
          UserGroupInformation.getLoginUser());
      } else {
        fsOperationUser = UserGroupInformation.getLoginUser();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error while setting up UGI for FS operations.");
    }


    try {
      if (!StringUtils.isEmpty(runOptions.controlFileUrl)) {
        controlConfig = new HiveStrictManagedMigrationControlConfig();
        Path path = new Path(runOptions.controlFileUrl);
        FileSystem fs = path.getFileSystem(this.conf);
        if (fs.getFileStatus(path).isDirectory()) {
          for (FileStatus fileStatus : fs.listStatus(path)) {
            readControlConfigs(fs, fileStatus.getPath());
          }
        } else {
          readControlConfigs(fs, path);
        }
      } else {
        controlConfig = null;
      }
    } catch (IOException e) {
      throw new RuntimeException("Error while setting up control file content from path: "
          + runOptions.controlFileUrl, e);
    }

    this.hms = new CloseableThreadLocal<>(() -> {
      try {
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(conf);
        if (hiveConf != null) {
          SessionState ss = SessionState.start(conf);
          ss.applyAuthorizationPolicy();
        }
        return hiveMetaStoreClient;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, runOptions.tablePoolSize);
    wh = ThreadLocal.withInitial(() -> {
      try {
        return new Warehouse(conf);
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    });
    if (runOptions.shouldModifyManagedTableLocation || runOptions.shouldMoveExternal) {
      Configuration oldConf = new Configuration(conf);
      HiveConf.setVar(oldConf, HiveConf.ConfVars.METASTORE_WAREHOUSE, runOptions.oldWarehouseRoot);

      oldWh = ThreadLocal.withInitial(() -> {
        try {
          return new Warehouse(oldConf);
        } catch (MetaException e) {
          throw new RuntimeException(e);
        }
      });
    }
    this.hiveUpdater = new CloseableThreadLocal<>(() -> {
      try {
        return new HiveUpdater(conf, true);
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
    }, runOptions.tablePoolSize);

    this.failuresEncountered = new AtomicBoolean(false);
    this.failedValidationChecks = new AtomicBoolean(false);
  }

  void run() throws Exception {
    LOG.info("Starting with {}", runOptions);

    ForkJoinPool tablePool = new ForkJoinPool(
        runOptions.tablePoolSize,
        new NamedForkJoinWorkerThreadFactory("Table-"),
        getUncaughtExceptionHandler(),
        false);

    List<String> databases = null;

    if (controlConfig == null) {
      databases = hms.get().getDatabases(runOptions.dbRegex); //TException
    } else {
      databases = hms.get().getAllDatabases().stream()
          .filter(db -> controlConfig.getDatabaseIncludeLists().containsKey(db)).collect(toList());
    }
    LOG.info("Found {} databases", databases.size());

    databases.forEach(dbName -> processDatabase(dbName, tablePool));
    LOG.info("Done processing databases.");

    if (failuresEncountered.get()) {
      throw new HiveException("One or more failures encountered during processing.");
    }
    if (failedValidationChecks.get()) {
      throw new HiveException("One or more tables failed validation checks for strict managed table mode.");
    }
  }

  private Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
    return (t, e) -> LOG.error(String.format("Thread %s exited with error", t.getName()), e);
  }

  static WarehouseRootCheckResult checkOldWarehouseRoot(RunOptions runOptions, HiveConf conf) throws IOException {
    boolean shouldModifyManagedTableLocation = runOptions.shouldModifyManagedTableLocation;
    boolean shouldMoveExternal = runOptions.shouldMoveExternal;
    Path targetPath = null;
    HadoopShims.HdfsEncryptionShim encryptionShim = null;
    HadoopShims.HdfsErasureCodingShim ecShim = null;

    if (shouldMoveExternal && !checkExternalWarehouseDir(conf)) {
      LOG.info("External warehouse path not specified/empty. Disabling shouldMoveExternal");
      shouldMoveExternal = false;
    }

    if (shouldModifyManagedTableLocation || shouldMoveExternal) {
      if (runOptions.oldWarehouseRoot == null) {
        LOG.info("oldWarehouseRoot is not specified. Disabling shouldModifyManagedTableLocation and " +
          "shouldMoveExternal");
        shouldModifyManagedTableLocation = false;
        shouldMoveExternal = false;
      } else {
        String currentPathString = shouldModifyManagedTableLocation ?
          HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_WAREHOUSE) :
          HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL);
        if (arePathsEqual(conf, runOptions.oldWarehouseRoot, currentPathString)) {
          LOG.info("oldWarehouseRoot is the same as the target path {}."
              + " Disabling shouldModifyManagedTableLocation and shouldMoveExternal",
            runOptions.oldWarehouseRoot);
          shouldModifyManagedTableLocation = false;
          shouldMoveExternal = false;
        } else {
          Path oldWhRootPath = new Path(runOptions.oldWarehouseRoot);
          targetPath = new Path(currentPathString);
          FileSystem oldWhRootFs = oldWhRootPath.getFileSystem(conf);
          FileSystem curWhRootFs = targetPath.getFileSystem(conf);
          oldWhRootPath = oldWhRootFs.makeQualified(oldWhRootPath);
          targetPath = curWhRootFs.makeQualified(targetPath);
          if (!FileUtils.isEqualFileSystemAndSameOzoneBucket(oldWhRootFs, curWhRootFs, oldWhRootPath, targetPath)) {
            LOG.info("oldWarehouseRoot {} has a different FS than the target path {}."
                + " Disabling shouldModifyManagedTableLocation and shouldMoveExternal",
              runOptions.oldWarehouseRoot, currentPathString);
            shouldModifyManagedTableLocation = false;
            shouldMoveExternal = false;
          } else {
            if (!isHdfs(oldWhRootFs)) {
              LOG.info("Warehouse is using non-HDFS FileSystem {}. Disabling shouldModifyManagedTableLocation and" +
                "shouldMoveExternal", oldWhRootFs.getUri());
              shouldModifyManagedTableLocation = false;
              shouldMoveExternal = false;
            } else {
              encryptionShim = ShimLoader.getHadoopShims().createHdfsEncryptionShim(oldWhRootFs, conf);
              if (!hasEquivalentEncryption(encryptionShim, oldWhRootPath, targetPath)) {
                LOG.info("oldWarehouseRoot {} and target path {} have different encryption zones." +
                    " Disabling shouldModifyManagedTableLocation and shouldMoveExternal",
                  oldWhRootPath, targetPath);
                shouldModifyManagedTableLocation = false;
                shouldMoveExternal = false;
              } else {
                ecShim = ShimLoader.getHadoopShims().createHdfsErasureCodingShim(oldWhRootFs, conf);
                if (!hasEquivalentErasureCodingPolicy(ecShim, oldWhRootPath, targetPath)) {
                  LOG.info("oldWarehouseRoot {} and target path {} have different erasure coding policies." +
                      " Disabling shouldModifyManagedTableLocation and shouldMoveExternal",
                    oldWhRootPath, targetPath);
                  shouldModifyManagedTableLocation = false;
                  shouldMoveExternal = false;
                }
              }
            }
          }
        }
      }
    }

    return new WarehouseRootCheckResult(shouldModifyManagedTableLocation, shouldMoveExternal,
      targetPath, encryptionShim, ecShim);
  }

  static OwnerPermsOptions checkOwnerPermsOptions(RunOptions runOptions, HiveConf conf) {
    String ownerName = null;
    String groupName = null;
    FsPermission dirPerms = null;
    FsPermission filePerms = null;

    if (runOptions.shouldModifyManagedTableOwner) {
      ownerName = conf.get("strict.managed.tables.migration.owner", "hive");
      groupName = conf.get("strict.managed.tables.migration.group", null);
    }
    if (runOptions.shouldModifyManagedTablePermissions) {
      String dirPermsString = conf.get("strict.managed.tables.migration.dir.permissions", "700");
      if (dirPermsString != null) {
        dirPerms = new FsPermission(dirPermsString);
      }
      String filePermsString = conf.get("strict.managed.tables.migration.file.permissions", "700");
      if (filePermsString != null) {
        filePerms = new FsPermission(filePermsString);
      }
    }

    return new OwnerPermsOptions(ownerName, groupName, dirPerms, filePerms);
  }

  static boolean checkExternalWarehouseDir(HiveConf conf) {
    String externalWarehouseDir = conf.getVar(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL);
    return externalWarehouseDir != null && !externalWarehouseDir.isEmpty();
  }

  void processDatabase(String dbName, ForkJoinPool tablePool) {
    try {
      LOG.info("Processing database {}", dbName);
      Database dbObj = hms.get().getDatabase(dbName);

      if (createExternalDirsForDbs) {
        createExternalDbDir(dbObj);
      }

      boolean modifyLocation = shouldModifyDatabaseLocation(dbObj);

      if (modifyLocation) {
        Path newDefaultDbLocation = getDefaultDbPathManagedOrExternal(dbName);

        LOG.info("Changing location of database {} to {}", dbName, newDefaultDbLocation);
        if (!runOptions.dryRun) {
          FileSystem fs = getFS(newDefaultDbLocation, conf, fsOperationUser);
          FileUtils.mkdir(fs, newDefaultDbLocation, conf);
          // Set appropriate owner/perms of the DB dir only, no need to recurse
          checkAndSetFileOwnerPermissions(fs, newDefaultDbLocation,
            ownerName, groupName, dirPerms, null, runOptions.dryRun, false);
        }
      }

      List<String> tableNames;
      // If control file content is present we ask for all table names first, then remove those we don't care about
      String tableRegex = controlConfig == null ? runOptions.tableRegex : ".*";
      if (runOptions.tableType == null) {
        tableNames = hms.get().getTables(dbName, tableRegex);
        LOG.debug("found {} tables in {}", tableNames.size(), dbName);
      } else {
        tableNames = hms.get().getTables(dbName, tableRegex, runOptions.tableType);
        LOG.debug("found {} {}s in {}", tableNames.size(), runOptions.tableType.name(), dbName);
      }

      if (controlConfig != null) {
        List<String> tableIncludeList = controlConfig.getDatabaseIncludeLists().get(dbName);
        if (tableIncludeList == null) {
          tableNames.clear();
        } else {
          tableNames.removeAll(tableNames.stream().filter(t -> !tableIncludeList.contains(t)).collect(toList()));
        }
        LOG.debug("{} tables remained after control file filtering in {}", tableNames.size(), dbName);
      }

      boolean errorsInThisDb = !tablePool.submit(() -> tableNames.parallelStream()
        .map(tableName -> processTable(dbObj, tableName, modifyLocation))
        .reduce(true, (aBoolean, aBoolean2) -> aBoolean && aBoolean2)).get();
      if (errorsInThisDb) {
        failuresEncountered.set(true);
      }

      // Finally update the DB location. This would prevent subsequent runs of the migration from processing this DB.
      if (modifyLocation) {
        if (errorsInThisDb) {
          LOG.error("Not updating database location for {} since an error was encountered. " +
            "The migration must be run again for this database.", dbObj.getName());
        } else {
          if (!runOptions.dryRun) {
            Path newDefaultDbLocation = getDefaultDbPathManagedOrExternal(dbName);
            // dbObj after this call would have the new DB location.
            // Keep that in mind if anything below this requires the old DB path.
            hiveUpdater.get().updateDbLocation(dbObj, newDefaultDbLocation);
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Cancel processing " + dbName, e);
    } catch (TException | IOException | HiveException | ExecutionException ex) {
      LOG.error("Error processing database " + dbName, ex);
      failuresEncountered.set(true);
    }
  }

  private Path getDefaultDbPathManagedOrExternal(String dbName) throws MetaException {
    return runOptions.shouldMoveExternal ?
      wh.get().getDefaultExternalDatabasePath(dbName) :
      wh.get().getDefaultDatabasePath(dbName);
  }

  public static boolean migrateTable(Table tableObj, TableType tableType, TableMigrationOption migrationOption,
                                     boolean dryRun, HiveUpdater hiveUpdater, IMetaStoreClient hms, Configuration conf)
    throws HiveException, IOException, TException {
    switch (migrationOption) {
      case EXTERNAL:
        migrateToExternalTable(tableObj, tableType, dryRun, hiveUpdater);
        break;
      case MANAGED:
        migrateToManagedTable(tableObj, tableType, dryRun, hiveUpdater, hms, conf);
        break;
      case NONE:
        break;
      case VALIDATE:
        // Check that the table is valid under strict managed tables mode.
        String reason = HiveStrictManagedUtils.validateStrictManagedTable(conf, tableObj);
        if (reason != null) {
          LOG.warn(reason);
          return true;
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected table migration option " + migrationOption);
    }
    return false;
  }

  boolean processTable(Database dbObj, String tableName, boolean modifyLocation) {
    try {
      String dbName = dbObj.getName();
      LOG.debug("Processing table {}", getQualifiedName(dbName, tableName));

      Table tableObj = hms.get().getTable(dbName, tableName);
      TableType tableType = TableType.valueOf(tableObj.getTableType());

      TableMigrationOption migrationOption = runOptions.migrationOption;
      if (migrationOption == TableMigrationOption.AUTOMATIC) {
        migrationOption = determineMigrationTypeAutomatically(
          tableObj, tableType, ownerName, conf, hms.get(), null);
      }

      boolean failedValidationCheck = migrateTable(tableObj, tableType, migrationOption, runOptions.dryRun,
        hiveUpdater.get(), hms.get(), conf);

      if (failedValidationCheck) {
        this.failedValidationChecks.set(true);
        return true;
      }

      String tablePathString = tableObj.getSd().getLocation();
      if (StringUtils.isEmpty(tablePathString)) {
        // When using this tool in full automatic mode (no DB/table regexes and automatic migration option) we may
        // encounter sysdb / information_schema databases. These should not be moved, they have null location.
        return true;
      }
      Path tablePath = new Path(tablePathString);

      boolean shouldMoveTable = modifyLocation && (
        (MANAGED_TABLE.name().equals(tableObj.getTableType()) && runOptions.shouldModifyManagedTableLocation) ||
          (EXTERNAL_TABLE.name().equals(tableObj.getTableType()) && runOptions.shouldMoveExternal));

      if (shouldMoveTable && shouldModifyTableLocation(dbObj, tableObj)) {
        Path newTablePath = wh.get().getDnsPath(
          new Path(getDefaultDbPathManagedOrExternal(dbName),
            MetaStoreUtils.encodeTableName(tableName.toLowerCase())));
        moveTableData(dbObj, tableObj, newTablePath);
        if (!runOptions.dryRun) {
          // File ownership/permission checks should be done on the new table path.
          tablePath = newTablePath;
        }
      }

      if (MANAGED_TABLE.equals(tableType)) {
        if (runOptions.shouldModifyManagedTableOwner || runOptions.shouldModifyManagedTablePermissions) {
          FileSystem fs = tablePath.getFileSystem(conf);
          if (isHdfs(fs)) {
            // TODO: what about partitions not in the default location?
            checkAndSetFileOwnerPermissions(fs, tablePath,
              ownerName, groupName, dirPerms, filePerms, runOptions.dryRun, true);
          }
        }
      }
    } catch (Exception ex) {
      LOG.error("Error processing table " + getQualifiedName(dbObj.getName(), tableName), ex);
      return false;
    }
    return true;
  }

  boolean shouldModifyDatabaseLocation(Database dbObj) throws IOException, MetaException {
    String dbName = dbObj.getName();
    if (runOptions.shouldModifyManagedTableLocation || runOptions.shouldMoveExternal) {
      // Check if the database location is in the default location based on the old warehouse root.
      // If so then change the database location to the default based on the current warehouse root.
      String dbLocation = dbObj.getLocationUri();
      Path oldDefaultDbLocation = oldWh.get().getDefaultDatabasePath(dbName);
      if (arePathsEqual(conf, dbLocation, oldDefaultDbLocation.toString())) {
        if (hasEquivalentEncryption(encryptionShim, oldDefaultDbLocation, targetPath)) {
          if (hasEquivalentErasureCodingPolicy(ecShim, oldDefaultDbLocation, targetPath)) {
            return true;
          } else {
            LOG.info("{} and {} have different EC policies. Will not change database location for {}",
              oldDefaultDbLocation, targetPath, dbName);
          }
        } else {
          LOG.info("{} and {} are on different encryption zones. Will not change database location for {}",
            oldDefaultDbLocation, targetPath, dbName);
        }
      }
    }
    return false;
  }

  boolean shouldModifyTableLocation(Database dbObj, Table tableObj) throws IOException, MetaException {
    // Should only be managed tables passed in here.
    // Check if table is in the default table location based on the old warehouse root.
    // If so then change the table location to the default based on the current warehouse root.
    // The existing table directory will also be moved to the new default database directory.
    String tableLocation = tableObj.getSd().getLocation();
    Path oldDefaultTableLocation = oldWh.get().getDefaultTablePath(dbObj, tableObj.getTableName());
    if (arePathsEqual(conf, tableLocation, oldDefaultTableLocation.toString())) {
      if (hasEquivalentEncryption(encryptionShim, oldDefaultTableLocation, targetPath)) {
        if (hasEquivalentErasureCodingPolicy(ecShim, oldDefaultTableLocation, targetPath)) {
          return true;
        } else {
          LOG.info("{} and {} have different EC policies. Will not change table location for {}",
            oldDefaultTableLocation, targetPath, getQualifiedName(tableObj));
        }
      } else {
        LOG.info("{} and {} are on different encryption zones. Will not change table location for {}",
          oldDefaultTableLocation, targetPath, getQualifiedName(tableObj));
      }
    }
    return false;
  }

  boolean shouldModifyPartitionLocation(Database dbObj, Table tableObj, Partition partObj,
                                        Map<String, String> partSpec) throws IOException, MetaException {
    String partLocation = partObj.getSd().getLocation();
    Path oldDefaultPartLocation = runOptions.shouldMoveExternal  ?
      oldWh.get().getPartitionPath(dbObj, tableObj, partSpec.values().stream().collect(toList())):
      oldWh.get().getDefaultPartitionPath(dbObj, tableObj, partSpec);
    if (arePathsEqual(conf, partLocation, oldDefaultPartLocation.toString())) {
      // No need to check encryption zone and EC policy. Data was moved already along with the whole table.
      return true;
    }
    return false;
  }

  void createExternalDbDir(Database dbObj) throws IOException, MetaException {
    Path externalTableDbPath = wh.get().getDefaultExternalDatabasePath(dbObj.getName());
    FileSystem fs = getFS(externalTableDbPath, conf, fsOperationUser);
    if (!fs.exists(externalTableDbPath)) {
      String dbOwner = ownerName;
      String dbGroup = null;

      String dbOwnerName = dbObj.getOwnerName();
      if (dbOwnerName != null && !dbOwnerName.isEmpty()) {
        switch (dbObj.getOwnerType()) {
          case USER:
            dbOwner = dbOwnerName;
            break;
          case ROLE:
            break;
          case GROUP:
            dbGroup = dbOwnerName;
            break;
        }
      }

      if (dbOwner == null) {
        dbOwner = conf.get("strict.managed.tables.migration.owner", "hive");
      }
      LOG.info("Creating external table directory for database {} at {} with ownership {}/{}",
        dbObj.getName(), externalTableDbPath, dbOwner, dbGroup);
      if (!runOptions.dryRun) {
        // Just rely on parent perms/umask for permissions.
        fs.mkdirs(externalTableDbPath);
        checkAndSetFileOwnerPermissions(fs, externalTableDbPath, dbOwner, dbGroup,
          null, null, runOptions.dryRun, false);
      }
    } else {
      LOG.info("Not creating external table directory for database {} - {} already exists.",
        dbObj.getName(), externalTableDbPath);
      // Leave the directory owner/perms as-is if the path already exists.
    }
  }

  void moveTableData(Database dbObj, Table tableObj, Path newTablePath) throws HiveException, IOException, TException {
    String dbName = tableObj.getDbName();
    String tableName = tableObj.getTableName();

    Path oldTablePath = new Path(tableObj.getSd().getLocation());

    LOG.info("Moving location of {} from {} to {}", getQualifiedName(tableObj), oldTablePath, newTablePath);

    // Move table directory.
    if (!runOptions.dryRun) {
      FileSystem fs = getFS(newTablePath, conf, fsOperationUser);
      if (fs.exists(oldTablePath)) {
        boolean movedData = fs.rename(oldTablePath, newTablePath);
        if (!movedData) {
          String msg = String.format("Unable to move data directory for table %s from %s to %s",
            getQualifiedName(tableObj), oldTablePath, newTablePath);
          throw new HiveException(msg);
        }
      }
    }

    // An error occurring between here and before updating the table's location in the metastore
    // may potentially cause the data to reside in the new location, while the
    // table/partitions point to the old paths.
    // The migration would be _REQUIRED_ to run again (and pass) for the data and table/partition
    // locations to be in sync.

    if (isPartitionedTable(tableObj)) {
      List<String> partNames = hms.get().listPartitionNames(dbName, tableName, Short.MAX_VALUE);
      // TODO: Fetch partitions in batches?
      // TODO: Threadpool to process partitions?
      for (String partName : partNames) {
        Partition partObj = hms.get().getPartition(dbName, tableName, partName);
        Map<String, String> partSpec =
          Warehouse.makeSpecFromValues(tableObj.getPartitionKeys(), partObj.getValues());
        if (shouldModifyPartitionLocation(dbObj, tableObj, partObj, partSpec)) {
          // Table directory (which includes the partition directory) has already been moved,
          // just update the partition location in the metastore.
          if (!runOptions.dryRun) {
            Path newPartPath = wh.get().getPartitionPath(newTablePath, partSpec);
            hiveUpdater.get().updatePartitionLocation(dbName, tableObj, partName, partObj, newPartPath);
          }
        }
      }
    }

    // Finally update the table location. This would prevent this tool from processing this table again
    // on subsequent runs of the migration.
    if (!runOptions.dryRun) {
      hiveUpdater.get().updateTableLocation(tableObj, newTablePath);
    }
  }

  static void renameFilesToConformToAcid(Table tableObj, IMetaStoreClient hms, Configuration conf, boolean dryRun)
    throws IOException, TException {
    if (isPartitionedTable(tableObj)) {
      String dbName = tableObj.getDbName();
      String tableName = tableObj.getTableName();
      List<String> partNames = hms.listPartitionNames(dbName, tableName, Short.MAX_VALUE);
      for (String partName : partNames) {
        Partition partObj = hms.getPartition(dbName, tableName, partName);
        Path partPath = new Path(partObj.getSd().getLocation());
        FileSystem fs = partPath.getFileSystem(conf);
        if (fs.exists(partPath)) {
          UpgradeTool.handleRenameFiles(tableObj, partPath,
            !dryRun, conf, tableObj.getSd().getBucketColsSize() > 0, null);
        }
      }
    } else {
      Path tablePath = new Path(tableObj.getSd().getLocation());
      FileSystem fs = tablePath.getFileSystem(conf);
      if (fs.exists(tablePath)) {
        UpgradeTool.handleRenameFiles(tableObj, tablePath,
          !dryRun, conf, tableObj.getSd().getBucketColsSize() > 0, null);
      }
    }
  }

  public static TableMigrationOption determineMigrationTypeAutomatically(Table tableObj, TableType tableType,
                                                                         String ownerName, Configuration conf, IMetaStoreClient hms, Boolean isPathOwnedByHive)
    throws IOException, MetaException, TException {
    TableMigrationOption result = TableMigrationOption.NONE;
    String msg;
    switch (tableType) {
      case MANAGED_TABLE:
        if (AcidUtils.isTransactionalTable(tableObj)) {
          // Always keep transactional tables as managed tables.
          result = TableMigrationOption.MANAGED;
        } else {
          String reason = shouldTableBeExternal(tableObj, ownerName, conf, hms, isPathOwnedByHive);
          if (reason != null) {
            LOG.debug("Converting {} to external table. {}", getQualifiedName(tableObj), reason);
            result = TableMigrationOption.EXTERNAL;
          } else {
            result = TableMigrationOption.MANAGED;
          }
        }
        break;
      case EXTERNAL_TABLE:
        msg = String.format("Table %s is already an external table, not processing.",
          getQualifiedName(tableObj));
        LOG.debug(msg);
        result = TableMigrationOption.NONE;
        break;
      default: // VIEW/MATERIALIZED_VIEW
        msg = String.format("Ignoring table %s because it has table type %s",
          getQualifiedName(tableObj), tableType);
        LOG.debug(msg);
        result = TableMigrationOption.NONE;
        break;
    }

    return result;
  }

  private static final Map<String, String> convertToExternalTableProps = new HashMap<>();
  private static final Map<String, String> convertToAcidTableProps = new HashMap<>();
  private static final Map<String, String> convertToMMTableProps = new HashMap<>();
  private static final String KUDU_LEGACY_STORAGE_HANDLER = "com.cloudera.kudu.hive.KuduStorageHandler";
  private static final String KUDU_STORAGE_HANDLER = "org.apache.hadoop.hive.kudu.KuduStorageHandler";

  static {
    convertToExternalTableProps.put("EXTERNAL", "TRUE");
    convertToExternalTableProps.put("external.table.purge", "true");

    convertToAcidTableProps.put("transactional", "true");

    convertToMMTableProps.put("transactional", "true");
    convertToMMTableProps.put("transactional_properties", "insert_only");
  }

  static boolean migrateToExternalTable(Table tableObj, TableType tableType, boolean dryRun, HiveUpdater hiveUpdater)
    throws HiveException {
    String msg;
    switch (tableType) {
      case MANAGED_TABLE:
        if (AcidUtils.isTransactionalTable(tableObj)) {
          msg = createExternalConversionExcuse(tableObj,
            "Table is a transactional table");
          LOG.debug(msg);
          return false;
        }
        LOG.info("Converting {} to external table ...", getQualifiedName(tableObj));
        if (!dryRun) {
          tableObj.setTableType(EXTERNAL_TABLE.toString());
          hiveUpdater.updateTableProperties(tableObj, convertToExternalTableProps);
        }
        return true;
      case EXTERNAL_TABLE:
        // Might need to update storage_handler
        hiveUpdater.updateTableProperties(tableObj, new HashMap<>());
        msg = createExternalConversionExcuse(tableObj,
          "Table is already an external table");
        LOG.debug(msg);
        break;
      default: // VIEW/MATERIALIZED_VIEW
        msg = createExternalConversionExcuse(tableObj,
          "Table type " + tableType + " cannot be converted");
        LOG.debug(msg);
        break;
    }
    return false;
  }

  static boolean canTableBeFullAcid(Table tableObj) throws MetaException {
    // Table must be acid-compatible table format, and no sorting columns.
    return TransactionalValidationListener.conformToAcid(tableObj) &&
      (tableObj.getSd().getSortColsSize() <= 0);
  }

  static Map<String, String> getTablePropsForConversionToTransactional(Map<String, String> props,
                                                                       boolean convertFromExternal) {
    if (convertFromExternal) {
      // Copy the properties to a new map so we can add EXTERNAL=FALSE
      props = new HashMap<String, String>(props);
      props.put("EXTERNAL", "FALSE");
    }
    return props;
  }

  static boolean migrateToManagedTable(Table tableObj, TableType tableType, boolean dryRun, HiveUpdater hiveUpdater,
                                       IMetaStoreClient hms, Configuration conf)
    throws HiveException, IOException, MetaException, TException {

    boolean convertFromExternal = false;
    switch (tableType) {
      case EXTERNAL_TABLE:
        convertFromExternal = true;
        // fall through
      case MANAGED_TABLE:
        if (MetaStoreUtils.isNonNativeTable(tableObj)) {
          String msg = createManagedConversionExcuse(tableObj,
            "Table is a non-native (StorageHandler) table");
          LOG.debug(msg);
          return false;
        }
        if (HiveStrictManagedUtils.isAvroTableWithExternalSchema(tableObj)) {
          String msg = createManagedConversionExcuse(tableObj,
            "Table is an Avro table with an external schema url");
          LOG.debug(msg);
          return false;
        }
        // List bucketed table cannot be converted to transactional
        if (HiveStrictManagedUtils.isListBucketedTable(tableObj)) {
          String msg = createManagedConversionExcuse(tableObj,
            "Table is a list bucketed table");
          LOG.debug(msg);
          return false;
        }
        // If table is already transactional, no migration needed.
        if (AcidUtils.isTransactionalTable(tableObj)) {
          String msg = createManagedConversionExcuse(tableObj,
            "Table is already a transactional table");
          LOG.debug(msg);
          return false;
        }

        // ORC files can be converted to full acid transactional tables
        // Other formats can be converted to insert-only transactional tables
        if (canTableBeFullAcid(tableObj)) {
          // TODO: option to allow converting ORC file to insert-only transactional?
          LOG.info("Converting {} to full transactional table", getQualifiedName(tableObj));

          if (hiveUpdater.doFileRename) {
            renameFilesToConformToAcid(tableObj, hms, conf, dryRun);
          }

          if (!dryRun) {
            Map<String, String> props = getTablePropsForConversionToTransactional(
              convertToAcidTableProps, convertFromExternal);
            hiveUpdater.updateTableProperties(tableObj, props);
          }
          return true;
        } else {
          LOG.info("Converting {} to insert-only transactional table", getQualifiedName(tableObj));
          if (!dryRun) {
            Map<String, String> props = getTablePropsForConversionToTransactional(
              convertToMMTableProps, convertFromExternal);
            hiveUpdater.updateTableProperties(tableObj, props);
          }
          return true;
        }
      default: // VIEW/MATERIALIZED_VIEW
        String msg = createManagedConversionExcuse(tableObj,
          "Table type " + tableType + " cannot be converted");
        LOG.debug(msg);
        return false;
    }
  }

  static String shouldTableBeExternal(Table tableObj, String ownerName, Configuration conf,
                                      IMetaStoreClient hms, Boolean isPathOwnedByHive)
    throws IOException, MetaException, TException {
    if (MetaStoreUtils.isNonNativeTable(tableObj)) {
      return "Table is a non-native (StorageHandler) table";
    }
    if (HiveStrictManagedUtils.isAvroTableWithExternalSchema(tableObj)) {
      return "Table is an Avro table with an external schema url";
    }
    // List bucketed table cannot be converted to transactional
    if (HiveStrictManagedUtils.isListBucketedTable(tableObj)) {
      return "Table is a list bucketed table";
    }
    // If any table/partition directory is not owned by hive,
    // then assume table is using storage-based auth - set external.
    // Transactional tables should still remain transactional,
    // but we should have already checked for that before this point.
    if (isPathOwnedByHive != null) {
      // for replication flow, the path ownership must be verified at source cluster itself.
      return isPathOwnedByHive ? null :
        String.format("One or more table directories is not owned by hive or non-HDFS path at source cluster");
    } else if (shouldTablePathBeExternal(tableObj, ownerName, conf, hms)) {
      return String.format("One or more table directories not owned by %s, or non-HDFS path", ownerName);
    }

    return null;
  }

  static boolean shouldTablePathBeExternal(Table tableObj, String ownerName, Configuration conf, IMetaStoreClient hms)
    throws IOException, TException {
    boolean shouldBeExternal = false;
    String dbName = tableObj.getDbName();
    String tableName = tableObj.getTableName();

    if (!isPartitionedTable(tableObj)) {
      // Check the table directory.
      Path tablePath = new Path(tableObj.getSd().getLocation());
      FileSystem fs = tablePath.getFileSystem(conf);
      if (isHdfs(fs)) {
        boolean ownedByHive = checkDirectoryOwnership(fs, tablePath, ownerName, true);
        shouldBeExternal = !ownedByHive;
      } else {
        // Set non-hdfs tables to external, unless transactional (should have been checked before this).
        shouldBeExternal = true;
      }
    } else {
      // Check ownership for all partitions
      List<String> partNames = hms.listPartitionNames(dbName, tableName, Short.MAX_VALUE);
      for (String partName : partNames) {
        Partition partObj = hms.getPartition(dbName, tableName, partName);
        Path partPath = new Path(partObj.getSd().getLocation());
        FileSystem fs = partPath.getFileSystem(conf);
        if (isHdfs(fs)) {
          boolean ownedByHive = checkDirectoryOwnership(fs, partPath, ownerName, true);
          shouldBeExternal = !ownedByHive;
        } else {
          shouldBeExternal = true;
        }
        if (shouldBeExternal) {
          break;
        }
      }
    }

    return shouldBeExternal;
  }

  void cleanup() {
    hms.close();
    if (hiveUpdater != null) {
      runAndLogErrors(() -> hiveUpdater.close());
      hiveUpdater = null;
    }
  }

  public static HiveUpdater getHiveUpdater(HiveConf conf) throws HiveException {
    return new HiveUpdater(conf, false);
  }

  private static final class TxnCtx {
    public final long writeId;
    public final String validWriteIds;
    public final long txnId;

    public TxnCtx(long writeId, String validWriteIds, long txnId) {
      this.writeId = writeId;
      this.txnId = txnId;
      this.validWriteIds = validWriteIds;
    }
  }

  private static class HiveUpdater implements AutoCloseable {
    Hive hive;
    boolean doFileRename;

    HiveUpdater(HiveConf conf, boolean fileRename) throws HiveException {
      hive = Hive.get(conf);
      Hive.set(hive);
      doFileRename = fileRename;
    }

    @Override
    public void close() {
      if (hive != null) {
        runAndLogErrors(Hive::closeCurrent);
        hive = null;
      }
    }

    void updateDbLocation(Database db, Path newLocation) throws HiveException {
      String msg = String.format("ALTER DATABASE %s SET LOCATION '%s'", db.getName(), newLocation);
      LOG.info(msg);

      db.setLocationUri(newLocation.toString());
      hive.alterDatabase(db.getName(), db);
    }

    void updateTableLocation(Table table, Path newLocation) throws HiveException {
      String msg = String.format("ALTER TABLE %s SET LOCATION '%s'",
        getQualifiedName(table), newLocation);
      LOG.info(msg);
      boolean isTxn = TxnUtils.isTransactionalTable(table);

      org.apache.hadoop.hive.ql.metadata.Table modifiedTable =
        new org.apache.hadoop.hive.ql.metadata.Table(table);
      modifiedTable.setDataLocation(newLocation);

      alterTableInternal(isTxn, table, modifiedTable);
    }

    private void alterTableInternal(boolean wasTxn, Table table,
                                    org.apache.hadoop.hive.ql.metadata.Table modifiedTable) throws HiveException {
      IMetaStoreClient msc = getMSC();
      TxnCtx txnCtx = generateTxnCtxForAlter(table, msc, wasTxn);
      boolean isOk = false;
      try {
        String validWriteIds = null;
        if (txnCtx != null) {
          validWriteIds = txnCtx.validWriteIds;
          modifiedTable.getTTable().setWriteId(txnCtx.writeId);
        }
        msc.alter_table(table.getCatName(), table.getDbName(), table.getTableName(),
          modifiedTable.getTTable(), null, validWriteIds);
        isOk = true;
      } catch (TException ex) {
        throw new HiveException(ex);
      } finally {
        closeTxnCtx(txnCtx, msc, isOk);
      }
    }

    private void alterPartitionInternal(Table table,
                                        org.apache.hadoop.hive.ql.metadata.Partition modifiedPart) throws HiveException {
      IMetaStoreClient msc = getMSC();
      TxnCtx txnCtx = generateTxnCtxForAlter(table, msc, null);
      boolean isOk = false;
      try {
        String validWriteIds = null;
        if (txnCtx != null) {
          validWriteIds = txnCtx.validWriteIds;
          modifiedPart.getTPartition().setWriteId(txnCtx.writeId);
        }
        msc.alter_partition(table.getCatName(), table.getDbName(), table.getTableName(),
          modifiedPart.getTPartition(), null, validWriteIds);
        isOk = true;
      } catch (TException ex) {
        throw new HiveException(ex);
      } finally {
        closeTxnCtx(txnCtx, msc, isOk);
      }
    }

    private IMetaStoreClient getMSC() throws HiveException {
      try {
        return hive.getMSC();
      } catch (MetaException ex) {
        throw new HiveException(ex);
      }
    }

    private TxnCtx generateTxnCtxForAlter(
      Table table, IMetaStoreClient msc, Boolean wasTxn) throws HiveException {
      if ((wasTxn != null && !wasTxn) || !TxnUtils.isTransactionalTable(table.getParameters())) {
        return null;
      }
      try {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        long txnId = msc.openTxn(ugi == null ? "anonymous" : ugi.getShortUserName());
        TxnCtx result = null;
        try {
          ValidTxnList txns = msc.getValidTxns(txnId);
          String fqn = table.getDbName() + "." + table.getTableName();
          List<TableValidWriteIds> writeIdsObj = msc.getValidWriteIds(
            Lists.newArrayList(fqn), txns.toString());
          String validWriteIds = TxnCommonUtils.createValidTxnWriteIdList(txnId, writeIdsObj)
            .getTableValidWriteIdList(fqn).writeToString();
          long writeId = msc.allocateTableWriteId(txnId, table.getDbName(), table.getTableName());
          result = new TxnCtx(writeId, validWriteIds, txnId);
        } finally {
          if (result == null) {
            AbortTxnRequest abortTxnRequest = new AbortTxnRequest(txnId);
            abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_MIGRATION_TXN.getErrorCode());
            msc.rollbackTxn(abortTxnRequest);
          }
        }
        return result;
      } catch (IOException | TException ex) {
        throw new HiveException(ex);
      }
    }

    private void closeTxnCtx(TxnCtx txnCtx, IMetaStoreClient msc, boolean isOk)
      throws HiveException {
      if (txnCtx == null) return;
      try {
        if (isOk) {
          msc.commitTxn(txnCtx.txnId);
        } else {
          AbortTxnRequest abortTxnRequest = new AbortTxnRequest(txnCtx.txnId);
          abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_MIGRATION_TXN.getErrorCode());
          msc.rollbackTxn(abortTxnRequest);
        }
      } catch (TException ex) {
        throw new HiveException(ex);
      }
    }

    void updatePartitionLocation(String dbName, Table table, String partName,
                                 Partition part, Path newLocation) throws HiveException, TException {
      String msg = String.format("ALTER TABLE %s PARTITION (%s) SET LOCATION '%s'",
        getQualifiedName(table), partName, newLocation.toString());
      LOG.info(msg);

      org.apache.hadoop.hive.ql.metadata.Partition modifiedPart =
        new org.apache.hadoop.hive.ql.metadata.Partition(
          new org.apache.hadoop.hive.ql.metadata.Table(table),
          part);
      modifiedPart.setLocation(newLocation.toString());
      alterPartitionInternal(table, modifiedPart);
    }

    void updateTableProperties(Table table, Map<String, String> propsToApply) throws HiveException {
      Map<String, String> props = new HashMap<>(propsToApply);
      migrateKuduStorageHandlerType(table, props);
      StringBuilder sb = new StringBuilder();
      boolean isTxn = TxnUtils.isTransactionalTable(table);
      org.apache.hadoop.hive.ql.metadata.Table modifiedTable = doFileRename ?
        new org.apache.hadoop.hive.ql.metadata.Table(table) : null;
      if (props.size() == 0) {
        return;
      }
      boolean first = true;
      for (String key : props.keySet()) {
        String value = props.get(key);
        if (modifiedTable == null) {
          table.getParameters().put(key, value);
        } else {
          modifiedTable.getParameters().put(key, value);
        }

        // Build properties list for logging
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append("'");
        sb.append(key);
        sb.append("'='");
        sb.append(value);
        sb.append("'");
      }
      String msg = String.format("ALTER TABLE %s SET TBLPROPERTIES (%s)",
        getQualifiedName(table), sb.toString());
      LOG.info(msg);

      // Note: for now, this is always called to convert the table to either external, or ACID/MM,
      //       so the original table would be non-txn and the transaction wouldn't be opened.
      if (modifiedTable != null) {
        alterTableInternal(isTxn, table, modifiedTable);
      }
    }
  }

  interface ThrowableRunnable {
    void run() throws Exception;
  }

  static void runAndLogErrors(ThrowableRunnable r) {
    try {
      r.run();
    } catch (Exception err) {
      LOG.error("Error encountered", err);
    }
  }

  static String createExternalConversionExcuse(Table tableObj, String reason) {
    return String.format("Table %s cannot be converted to an external table in "
        + "strict managed table mode for the following reason: %s",
      getQualifiedName(tableObj), reason);
  }

  static String createManagedConversionExcuse(Table tableObj, String reason) {
    return String.format("Table %s cannot be converted to a managed table in "
        + "strict managed table mode for the following reason: %s",
      getQualifiedName(tableObj), reason);
  }

  static boolean isPartitionedTable(Table tableObj) {
    List<FieldSchema> partKeys = tableObj.getPartitionKeys();
    if (partKeys != null && partKeys.size() > 0) {
      return true;
    }
    return false;
  }

  static boolean isHdfs(FileSystem fs) {
    return scheme.equals(fs.getScheme());
  }

  static String getQualifiedName(Table tableObj) {
    return getQualifiedName(tableObj.getDbName(), tableObj.getTableName());
  }

  static String getQualifiedName(String dbName, String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append('`');
    sb.append(dbName);
    sb.append("`.`");
    sb.append(tableName);
    sb.append('`');
    return sb.toString();
  }

  static boolean arePathsEqual(Configuration conf, String path1, String path2) throws IOException {
    String qualified1 = getQualifiedPath(conf, new Path(path1));
    String qualified2 = getQualifiedPath(conf, new Path(path2));
    return qualified1.equals(qualified2);
  }

  static String getQualifiedPath(Configuration conf, Path path) throws IOException {
    FileSystem fs;
    if (path == null) {
      return null;
    }

    fs = path.getFileSystem(conf);
    return fs.makeQualified(path).toString();
  }

  /**
   * Recursively check the file owner and permissions, setting them to the passed in values
   * if the owner/perms of the file do not match.
   * @param fs
   * @param path
   * @param userName  Owner of the file to compare/set. Null to skip this check.
   * @param groupName Group of the file to compare/set. Null to skip this check.
   * @param dirPerms  Permissions to compare/set, if the file is a directory. Null to skip this check.
   * @param filePerms Permissions to compare/set, if the file is a file. Null to skip this check.
   * @param dryRun    Dry run - check but do not actually set
   * @param recurse   Whether to recursively check/set the contents of a directory
   * @throws IOException
   */
  static void checkAndSetFileOwnerPermissions(FileSystem fs, Path path,
                                              String userName, String groupName,
                                              FsPermission dirPerms, FsPermission filePerms,
                                              boolean dryRun, boolean recurse) throws IOException {
    FileStatus fStatus = getFileStatus(fs, path);
    checkAndSetFileOwnerPermissions(fs, fStatus, userName, groupName, dirPerms, filePerms, dryRun, recurse);
  }

  /**
   * Recursively check the file owner and permissions, setting them to the passed in values
   * if the owner/perms of the file do not match.
   * @param fs
   * @param fStatus
   * @param userName  Owner of the file to compare/set. Null to skip this check.
   * @param groupName Group of the file to compare/set. Null to skip this check.
   * @param dirPerms  Permissions to compare/set, if the file is a directory. Null to skip this check.
   * @param filePerms Permissions to compare/set, if the file is a file. Null to skip this check.
   * @param dryRun    Dry run - check but do not actually set
   * @param recurse   Whether to recursively check/set the contents of a directory
   * @throws IOException
   */
  static void checkAndSetFileOwnerPermissions(FileSystem fs, FileStatus fStatus,
                                              String userName, String groupName,
                                              FsPermission dirPerms, FsPermission filePerms,
                                              boolean dryRun, boolean recurse) throws IOException {
    if (fStatus == null) {
      return;
    }

    Path path = fStatus.getPath();
    boolean setOwner = false;
    if (userName != null && !userName.equals(fStatus.getOwner())) {
      setOwner = true;
    } else if (groupName != null && !groupName.equals(fStatus.getGroup())) {
      setOwner = true;
    }

    boolean isDir = fStatus.isDirectory();
    boolean setPerms = false;
    FsPermission perms = filePerms;
    if (isDir) {
      perms = dirPerms;
    }
    if (perms != null && !perms.equals(fStatus.getPermission())) {
      setPerms = true;
    }

    if (setOwner) {
      LOG.debug("Setting owner/group of {} to {}/{}", path, userName, groupName);
      if (!dryRun) {
        fs.setOwner(path, userName, groupName);
      }
    }
    if (setPerms) {
      LOG.debug("Setting perms of {} to {}", path, perms);
      if (!dryRun) {
        fs.setPermission(path, perms);
      }
    }

    if (isDir && recurse) {
      for (FileStatus subFile : fs.listStatus(path)) {
        // TODO: Use threadpool for more concurrency?
        // TODO: check/set all files, or only directories
        checkAndSetFileOwnerPermissions(fs, subFile, userName, groupName, dirPerms, filePerms, dryRun, recurse);
      }
    }
  }

  static boolean checkDirectoryOwnership(FileSystem fs,
                                         Path path,
                                         String userName,
                                         boolean recurse) throws IOException {
    FileStatus fStatus = getFileStatus(fs, path);
    return checkDirectoryOwnership(fs, fStatus, userName, recurse);
  }

  static boolean checkDirectoryOwnership(FileSystem fs,
                                         FileStatus fStatus,
                                         String userName,
                                         boolean recurse) throws IOException {
    if (fStatus == null) {
      // Non-existent file returns true.
      return true;
    }

    Path path = fStatus.getPath();
    boolean result = true;

    // Ignore non-directory files
    boolean isDir = fStatus.isDirectory();
    if (isDir) {
      if (userName != null && !userName.equals(fStatus.getOwner())) {
        return false;
      }

      if (recurse) {
        for (FileStatus subFile : fs.listStatus(path)) {
          if (!checkDirectoryOwnership(fs, subFile, userName, recurse)) {
            return false;
          }
        }
      }
    }

    return result;
  }

  static FileStatus getFileStatus(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      return null;
    }
    return fs.getFileStatus(path);
  }

  static FileStatus[] listStatus(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      return null;
    }
    return fs.listStatus(path);
  }

  static boolean hasEquivalentEncryption(HadoopShims.HdfsEncryptionShim encryptionShim,
                                         Path path1, Path path2) throws IOException {
    // Assumes these are both qualified paths are in the same FileSystem
    if (encryptionShim.isPathEncrypted(path1) || encryptionShim.isPathEncrypted(path2)) {
      if (!encryptionShim.arePathsOnSameEncryptionZone(path1, path2)) {
        return false;
      }
    }
    return true;
  }

  static boolean hasEquivalentErasureCodingPolicy(HadoopShims.HdfsErasureCodingShim ecShim,
                                                  Path path1, Path path2) throws IOException {
    HadoopShims.HdfsFileErasureCodingPolicy policy1 = ecShim.getErasureCodingPolicy(path1);
    HadoopShims.HdfsFileErasureCodingPolicy policy2 = ecShim.getErasureCodingPolicy(path2);
    if (policy1 != null) {
      return policy1.equals(policy2);
    } else {
      if (policy2 == null) {
        return true;
      }
      return false;
    }
  }

  /**
   * While upgrading from earlier versions we need to amend storage_handler value for Kudu tables that might
   * have the legacy value set.
   * @param table
   * @param props
   */
  private static void migrateKuduStorageHandlerType(Table table, Map<String, String> props) {
    Map<String, String> tableProperties = table.getParameters();
    if (tableProperties != null) {
      String storageHandler = tableProperties.get(META_TABLE_STORAGE);
      if (KUDU_LEGACY_STORAGE_HANDLER.equals(storageHandler)) {
        props.put(META_TABLE_STORAGE, KUDU_STORAGE_HANDLER);
      }
    }
  }

  private static FileSystem getFS(Path path, Configuration conf, UserGroupInformation fsOperationUser)
    throws IOException {
    try {
      return fsOperationUser.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return path.getFileSystem(conf);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * can set it from tests to test when config needs something other than default values.
   */
  @VisibleForTesting
  static HiveConf hiveConf = null;
  @VisibleForTesting
  static String scheme = "hdfs";
}