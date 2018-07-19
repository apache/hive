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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.cli.CommonCliOptions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.HiveStrictManagedUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.HiveParser.switchDatabaseStatement_return;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveStrictManagedMigration {

  private static final Logger LOG = LoggerFactory.getLogger(HiveStrictManagedMigration.class);

  enum TableMigrationOption {
    NONE,      // Do nothing
    VALIDATE,  // No migration, just validate that the tables
    AUTOMATIC, // Automatically determine if the table should be managed or external
    EXTERNAL,  // Migrate tables to external tables
    MANAGED    // Migrate tables as managed transactional tables
  }

  static class RunOptions {
    String dbRegex;
    String tableRegex;
    String oldWarehouseRoot;
    TableMigrationOption migrationOption;
    boolean shouldModifyManagedTableLocation;
    boolean shouldModifyManagedTableOwner;
    boolean shouldModifyManagedTablePermissions;
    boolean dryRun;

    public RunOptions(String dbRegex,
        String tableRegex,
        String oldWarehouseRoot,
        TableMigrationOption migrationOption,
        boolean shouldModifyManagedTableLocation,
        boolean shouldModifyManagedTableOwner,
        boolean shouldModifyManagedTablePermissions,
        boolean dryRun) {
      super();
      this.dbRegex = dbRegex;
      this.tableRegex = tableRegex;
      this.oldWarehouseRoot = oldWarehouseRoot;
      this.migrationOption = migrationOption;
      this.shouldModifyManagedTableLocation = shouldModifyManagedTableLocation;
      this.shouldModifyManagedTableOwner = shouldModifyManagedTableOwner;
      this.shouldModifyManagedTablePermissions = shouldModifyManagedTablePermissions;
      this.dryRun = dryRun;
    }
  }

  public static void main(String[] args) throws Exception {
    RunOptions runOptions;

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

    int rc = 0;
    HiveStrictManagedMigration migration = null;
    try {
      migration = new HiveStrictManagedMigration(runOptions);
      migration.run();
    } catch (Exception err) {
      LOG.error("Failed with error", err);
      rc = -1;
    } finally {
      if (migration != null) {
        migration.cleanup();
      }
    }

    // TODO: Something is preventing the process from terminating after main(), adding exit() as hacky solution.
    System.exit(rc);
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
        .withDescription("Whether managed tables should have their data moved from the old warehouse path to the current warehouse path")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("shouldModifyManagedTableOwner")
        .withDescription("Whether managed tables should have their directory owners changed to the hive user")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("shouldModifyManagedTablePermissions")
        .withDescription("Whether managed tables should have their directory permissions changed to conform to strict managed tables mode")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("modifyManagedTables")
        .withDescription("This setting enables the shouldModifyManagedTableLocation, shouldModifyManagedTableOwner, shouldModifyManagedTablePermissions options")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("print help message")
        .create('h'));

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
    boolean dryRun = cli.hasOption("dryRun");
    
    RunOptions runOpts = new RunOptions(
        dbRegex,
        tableRegex,
        oldWarehouseRoot,
        migrationOption,
        shouldModifyManagedTableLocation,
        shouldModifyManagedTableOwner,
        shouldModifyManagedTablePermissions,
        dryRun);
    return runOpts;
  }

  private RunOptions runOptions;
  private Configuration conf;
  private HiveMetaStoreClient hms;
  private boolean failedValidationChecks;
  private Warehouse wh;
  private Warehouse oldWh;
  private String ownerName;
  private String groupName;
  private FsPermission dirPerms;
  private FsPermission filePerms;

  HiveStrictManagedMigration(RunOptions runOptions) {
    this.runOptions = runOptions;
    this.conf = MetastoreConf.newMetastoreConf();
  }

  void run() throws Exception {
    checkOldWarehouseRoot();
    checkOwnerPermsOptions();

    hms = new HiveMetaStoreClient(conf);//MetaException
    try {
      List<String> databases = hms.getAllDatabases();//TException
      LOG.info("Found {} databases", databases.size());
      for (String dbName : databases) {
        if (dbName.matches(runOptions.dbRegex)) {
          processDatabase(dbName);
        }
      }
      LOG.info("Done processing databases.");
    } finally {
      hms.close();
    }

    if (failedValidationChecks) {
      throw new HiveException("One or more tables failed validation checks for strict managed table mode.");
    }
  }

  void checkOldWarehouseRoot() throws IOException, MetaException {
    if (runOptions.shouldModifyManagedTableLocation) {
      if (runOptions.oldWarehouseRoot == null) {
        LOG.info("oldWarehouseRoot is not specified. Disabling shouldModifyManagedTableLocation");
        runOptions.shouldModifyManagedTableLocation = false;
      } else {
        String curWarehouseRoot = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE);
        if (arePathsEqual(conf, runOptions.oldWarehouseRoot, curWarehouseRoot)) {
          LOG.info("oldWarehouseRoot is the same as the current warehouse root {}."
              + " Disabling shouldModifyManagedTableLocation",
              runOptions.oldWarehouseRoot);
          runOptions.shouldModifyManagedTableLocation = false;
        } else {
          FileSystem oldWhRootFs = new Path(runOptions.oldWarehouseRoot).getFileSystem(conf);
          FileSystem curWhRootFs = new Path(curWarehouseRoot).getFileSystem(conf);
          if (!FileUtils.equalsFileSystem(oldWhRootFs, curWhRootFs)) {
            LOG.info("oldWarehouseRoot {} has a different FS than the current warehouse root {}."
                + " Disabling shouldModifyManagedTableLocation",
                runOptions.oldWarehouseRoot, curWarehouseRoot);
            runOptions.shouldModifyManagedTableLocation = false;
          } else {
            if (!isHdfs(oldWhRootFs)) {
              LOG.info("Warehouse is using non-HDFS FileSystem {}. Disabling shouldModifyManagedTableLocation",
                  oldWhRootFs.getUri());
              runOptions.shouldModifyManagedTableLocation = false;
            }
          }
        }
      }
    }

    if (runOptions.shouldModifyManagedTableLocation) {
      wh = new Warehouse(conf);
      Configuration oldWhConf = new Configuration(conf);
      HiveConf.setVar(oldWhConf, HiveConf.ConfVars.METASTOREWAREHOUSE, runOptions.oldWarehouseRoot);
      oldWh = new Warehouse(oldWhConf);
    }
  }

  void checkOwnerPermsOptions() {
    if (runOptions.shouldModifyManagedTableOwner) {
      ownerName = conf.get("strict.managed.tables.migration.owner", "hive");
      groupName = conf.get("strict.managed.tables.migration.group", null);
    }
    if (runOptions.shouldModifyManagedTablePermissions) {
      String dirPermsString = conf.get("strict.managed.tables.migration.dir.permissions", "1700");
      if (dirPermsString != null) {
        dirPerms = new FsPermission(dirPermsString);
      }
      String filePermsString = conf.get("strict.managed.tables.migration.dir.permissions", "700");
      if (filePermsString != null) {
        filePerms = new FsPermission(filePermsString);
      }
    }
  }

  void processDatabase(String dbName) throws IOException, HiveException, MetaException, TException {
    LOG.info("Processing database {}", dbName);
    Database dbObj = hms.getDatabase(dbName);

    boolean modifyDefaultManagedLocation = shouldModifyDatabaseLocation(dbObj);
    if (modifyDefaultManagedLocation) {
      Path newDefaultDbLocation = wh.getDefaultDatabasePath(dbName);

      LOG.info("Changing location of database {} to {}", dbName, newDefaultDbLocation);
      if (!runOptions.dryRun) {
        FileSystem fs = newDefaultDbLocation.getFileSystem(conf);
        FileUtils.mkdir(fs, newDefaultDbLocation, conf);
        // Set appropriate owner/perms of the DB dir only, no need to recurse
        checkAndSetFileOwnerPermissions(fs, newDefaultDbLocation,
            ownerName, groupName, dirPerms, null, runOptions.dryRun, false);

        String command = String.format("ALTER DATABASE %s SET LOCATION '%s'", dbName, newDefaultDbLocation);
        runHiveCommand(command);
      }
    }

    List<String> tableNames = hms.getTables(dbName, runOptions.tableRegex);
    for (String tableName : tableNames) {
      // If we did not change the DB location, there is no need to move the table directories.
      processTable(dbObj, tableName, modifyDefaultManagedLocation);
    }
  }

  void processTable(Database dbObj, String tableName, boolean modifyDefaultManagedLocation)
      throws HiveException, IOException, TException {
    String dbName = dbObj.getName();
    LOG.debug("Processing table {}", getQualifiedName(dbName, tableName));

    Table tableObj = hms.getTable(dbName, tableName);
    TableType tableType = TableType.valueOf(tableObj.getTableType());
    boolean tableMigrated;

    TableMigrationOption migrationOption = runOptions.migrationOption;
    if (migrationOption == TableMigrationOption.AUTOMATIC) {
      migrationOption = determineMigrationTypeAutomatically(tableObj, tableType);
    }

    switch (migrationOption) {
    case EXTERNAL:
      tableMigrated = migrateToExternalTable(tableObj, tableType);
      if (tableMigrated) {
        tableType = TableType.EXTERNAL_TABLE;
      }
      break;
    case MANAGED:
      tableMigrated = migrateToManagedTable(tableObj, tableType);
      if (tableMigrated) {
        tableType = TableType.MANAGED_TABLE;
      }
      break;
    case NONE:
      break;
    case VALIDATE:
      // Check that the table is valid under strict managed tables mode.
      String reason = HiveStrictManagedUtils.validateStrictManagedTable(conf, tableObj);
      if (reason != null) {
        LOG.warn(reason);
        failedValidationChecks = true;
      }
      break;
    default:
      throw new IllegalArgumentException("Unexpected table migration option " + runOptions.migrationOption);
    }

    if (tableType == TableType.MANAGED_TABLE) {
      Path tablePath = new Path(tableObj.getSd().getLocation());
      if (modifyDefaultManagedLocation && shouldModifyTableLocation(dbObj, tableObj)) {
        Path newTablePath = wh.getDnsPath(
            new Path(wh.getDefaultDatabasePath(dbName),
                MetaStoreUtils.encodeTableName(tableName.toLowerCase())));
        moveTableData(dbObj, tableObj, newTablePath);
        if (!runOptions.dryRun) {
          // File ownership/permission checks should be done on the new table path.
          tablePath = newTablePath;
        }
      }

      if (runOptions.shouldModifyManagedTableOwner || runOptions.shouldModifyManagedTablePermissions) {
        FileSystem fs = tablePath.getFileSystem(conf);
        if (isHdfs(fs)) {
          // TODO: what about partitions not in the default location?
          checkAndSetFileOwnerPermissions(fs, tablePath,
              ownerName, groupName, dirPerms, filePerms, runOptions.dryRun, true);
        }
      }
    }
  }

  boolean shouldModifyDatabaseLocation(Database dbObj) throws IOException, MetaException {
    String dbName = dbObj.getName();
    if (runOptions.shouldModifyManagedTableLocation) {
      // Check if the database location is in the default location based on the old warehouse root.
      // If so then change the database location to the default based on the current warehouse root.
      String dbLocation = dbObj.getLocationUri();
      Path oldDefaultDbLocation = oldWh.getDefaultDatabasePath(dbName);
      if (arePathsEqual(conf, dbLocation, oldDefaultDbLocation.toString())) {
        return true;
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
    Path oldDefaultTableLocation = oldWh.getDefaultTablePath(dbObj, tableObj.getTableName());
    if (arePathsEqual(conf, tableLocation, oldDefaultTableLocation.toString())) {
      return true;
    }
    return false;
  }

  boolean shouldModifyPartitionLocation(Database dbObj, Table tableObj, Partition partObj, Map<String, String> partSpec)
      throws IOException, MetaException {
    String tableName = tableObj.getTableName();
    String partLocation = partObj.getSd().getLocation();
    Path oldDefaultPartLocation = oldWh.getDefaultPartitionPath(dbObj, tableObj, partSpec);
    return arePathsEqual(conf, partLocation, oldDefaultPartLocation.toString());
  }

  void moveTableData(Database dbObj, Table tableObj, Path newTablePath) throws HiveException, IOException, TException {
    String dbName = tableObj.getDbName();
    String tableName = tableObj.getTableName();
    
    Path oldTablePath = new Path(tableObj.getSd().getLocation());

    LOG.info("Moving location of {} from {} to {}", getQualifiedName(tableObj), oldTablePath, newTablePath);
    if (!runOptions.dryRun) {
      FileSystem fs = newTablePath.getFileSystem(conf);
      boolean movedData = fs.rename(oldTablePath, newTablePath);
      if (!movedData) {
        String msg = String.format("Unable to move data directory for table %s from %s to %s",
            getQualifiedName(tableObj), oldTablePath, newTablePath);
        throw new HiveException(msg);
      }
    }
    if (!runOptions.dryRun) {
      String command = String.format("ALTER TABLE %s SET LOCATION '%s'",
          getQualifiedName(tableObj), newTablePath);
      runHiveCommand(command);
    }
    if (isPartitionedTable(tableObj)) {
      List<String> partNames = hms.listPartitionNames(dbName, tableName, Short.MAX_VALUE);
      // TODO: Fetch partitions in batches?
      // TODO: Threadpool to process partitions?
      for (String partName : partNames) {
        Partition partObj = hms.getPartition(dbName, tableName, partName);
        Map<String, String> partSpec =
            Warehouse.makeSpecFromValues(tableObj.getPartitionKeys(), partObj.getValues());
        if (shouldModifyPartitionLocation(dbObj, tableObj, partObj, partSpec)) {
          // Table directory (which includes the partition directory) has already been moved,
          // just update the partition location in the metastore.
          if (!runOptions.dryRun) {
            Path newPartPath = wh.getPartitionPath(newTablePath, partSpec);
            String command = String.format("ALTER TABLE PARTITION (%s) SET LOCATION '%s'",
                partName, newPartPath.toString());
            runHiveCommand(command);
          }
        }
      }
    }
  }

  TableMigrationOption determineMigrationTypeAutomatically(Table tableObj, TableType tableType)
      throws IOException, MetaException, TException {
    TableMigrationOption result = TableMigrationOption.NONE;
    String msg;
    switch (tableType) {
    case MANAGED_TABLE:
      if (AcidUtils.isTransactionalTable(tableObj)) {
        // Always keep transactional tables as managed tables.
        result = TableMigrationOption.MANAGED;
      } else {
        String reason = shouldTableBeExternal(tableObj);
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

  boolean migrateToExternalTable(Table tableObj, TableType tableType) throws HiveException {
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
      if (!runOptions.dryRun) {
        String command = String.format(
            "ALTER TABLE %s SET TBLPROPERTIES ('EXTERNAL'='TRUE', 'external.table.purge'='true')",
            getQualifiedName(tableObj));
        runHiveCommand(command);
      }
      return true;
    case EXTERNAL_TABLE:
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

  boolean migrateToManagedTable(Table tableObj, TableType tableType) throws HiveException, MetaException {

    String externalFalse = "";
    switch (tableType) {
    case EXTERNAL_TABLE:
      externalFalse = "'EXTERNAL'='FALSE', ";
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
      if (AcidUtils.isFullAcidTable(tableObj)) {
        String msg = createManagedConversionExcuse(tableObj,
            "Table is already a transactional table");
        LOG.debug(msg);
        return false;
      }

      // ORC files can be converted to full acid transactional tables
      // Other formats can be converted to insert-only transactional tables
      if (TransactionalValidationListener.conformToAcid(tableObj)) {
        // TODO: option to allow converting ORC file to insert-only transactional?
        LOG.info("Converting {} to full transactional table", getQualifiedName(tableObj));
        if (!runOptions.dryRun) {
          String command = String.format(
              "ALTER TABLE %s SET TBLPROPERTIES ('transactional'='true')",
              getQualifiedName(tableObj));
          runHiveCommand(command);
        }
        return true;
      } else {
        LOG.info("Converting {} to insert-only transactional table", getQualifiedName(tableObj));
        if (!runOptions.dryRun) {
          String command = String.format(
              "ALTER TABLE %s SET TBLPROPERTIES (%s'transactional'='true', 'transactional_properties'='insert_only')",
              getQualifiedName(tableObj), externalFalse);
          runHiveCommand(command);
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

  String shouldTableBeExternal(Table tableObj) throws IOException, MetaException, TException {
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
    if (shouldTablePathBeExternal(tableObj, ownerName)) {
      return String.format("One or more table directories not owned by %s, or non-HDFS path", ownerName);
    }

    return null;
  }

  boolean shouldTablePathBeExternal(Table tableObj, String userName) throws IOException, MetaException, TException {
    boolean shouldBeExternal = false;
    String dbName = tableObj.getDbName();
    String tableName = tableObj.getTableName();

    if (!isPartitionedTable(tableObj)) {
      // Check the table directory.
      Path tablePath = new Path(tableObj.getSd().getLocation());
      FileSystem fs = tablePath.getFileSystem(conf);
      if (isHdfs(fs)) {
        shouldBeExternal = checkDirectoryOwnership(fs, tablePath, ownerName, true);
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
          shouldBeExternal = checkDirectoryOwnership(fs, partPath, ownerName, true);
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

  void runHiveCommand(String command) throws HiveException {
    LOG.info("Running command: {}", command);

    if (driver == null) {
      driver = new MyDriver(conf);
    }
  
    CommandProcessorResponse cpr = driver.driver.run(command);
    if (cpr.getResponseCode() != 0) {
      String msg = "Query returned non-zero code: " + cpr.getResponseCode()
          + ", cause: " + cpr.getErrorMessage();
      throw new HiveException(msg);
    }
  }

  void cleanup() {
    if (driver != null) {
      runAndLogErrors(() -> driver.close());
      driver = null;
    }
  }

  static class MyDriver {
    IDriver driver;

    MyDriver(Configuration conf) {
      HiveConf hiveConf = new HiveConf(conf, this.getClass());
      // TODO: Clean up SessionState/Driver/TezSession on exit
      SessionState.start(hiveConf);
      driver = DriverFactory.newDriver(hiveConf);
    }

    void close() {
      if (driver != null) {
        runAndLogErrors(() -> driver.close());
        runAndLogErrors(() -> driver.destroy());
        driver = null;
        runAndLogErrors(() -> SessionState.get().close());
      }
    }
  }

  MyDriver driver;

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
    if (partKeys != null || partKeys.size() > 0) {
      return true;
    }
    return false;
  }

  static boolean isHdfs(FileSystem fs) {
    return fs.getScheme().equals("hdfs");
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
    FileStatus fStatus = fs.getFileStatus(path);
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
    FileStatus fStatus = fs.getFileStatus(path);
    return checkDirectoryOwnership(fs, fStatus, userName, recurse);
  }

  static boolean checkDirectoryOwnership(FileSystem fs,
      FileStatus fStatus,
      String userName,
      boolean recurse) throws IOException {
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
}
