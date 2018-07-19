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
package org.apache.hadoop.hive.upgrade.acid;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.OrcAcidUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.escapeSQLString;

/**
 * This utility is designed to help with upgrading to Hive 3.0.  On-disk layout for transactional
 * tables has changed in 3.0 and require pre-processing before upgrade to ensure they are readable
 * by Hive 3.0.  Some transactional tables (identified by this utility) require Major compaction
 * to be run on them before upgrading to 3.0.  Once this compaction starts, no more
 * update/delete/merge statements may be executed on these tables until upgrade is finished.
 *
 * Additionally, a new type of transactional tables was added in 3.0 - insert-only tables.  These
 * tables support ACID semantics and work with any Input/OutputFormat.  Any Managed tables may
 * be made insert-only transactional table. These tables don't support Update/Delete/Merge commands.
 *
 * This utility works in 2 modes: preUpgrade and postUpgrade.
 * In preUpgrade mode it has to have 2.x Hive jars on the classpath.  It will perform analysis on
 * existing transactional tables, determine which require compaction and generate a set of SQL
 * commands to launch all of these compactions.
 *
 * Note that depending on the number of tables/partitions and amount of data in them compactions
 * may take a significant amount of time and resources.  The script output by this utility includes
 * some heuristics that may help estimate the time required.  If no script is produced, no action
 * is needed.  For compactions to run an instance of standalone Hive Metastore must be running.
 * Please make sure hive.compactor.worker.threads is sufficiently high - this specifies the limit
 * of concurrent compactions that may be run.  Each compaction job is a Map-Reduce job.
 * hive.compactor.job.queue may be used to set a Yarn queue ame where all compaction jobs will be
 * submitted.
 *
 * In postUpgrade mode, Hive 3.0 jars/hive-site.xml should be on the classpath. This utility will
 * find all the tables that may be made transactional (with ful CRUD support) and generate
 * Alter Table commands to do so.  It will also find all tables that may not support full CRUD
 * but can be made insert-only transactional tables and generate corresponding Alter Table commands.
 *
 * TODO: rename files
 *
 * "execute" option may be supplied in both modes to have the utility automatically execute the
 * equivalent of the generated commands
 *
 * "location" option may be supplied followed by a path to set the location for the generated
 * scripts.
 */
public class UpgradeTool {
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeTool.class);
  private static final int PARTITION_BATCH_SIZE = 10000;
  private final Options cmdLineOptions = new Options();

  public static void main(String[] args) throws Exception {
    UpgradeTool tool = new UpgradeTool();
    tool.init();
    CommandLineParser parser = new GnuParser();
    CommandLine line ;
    String outputDir = ".";
    boolean preUpgrade = false, postUpgrade = false, execute = false, nonBlocking = false;
    try {
      line = parser.parse(tool.cmdLineOptions, args);
    } catch (ParseException e) {
      System.err.println("UpgradeTool: Parsing failed.  Reason: " + e.getLocalizedMessage());
      printAndExit(tool);
      return;
    }
    if (line.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("upgrade-acid", tool.cmdLineOptions);
      return;
    }
    if(line.hasOption("location")) {
      outputDir = line.getOptionValue("location");
    }
    if(line.hasOption("execute")) {
      execute = true;
    }
    if(line.hasOption("preUpgrade")) {
      preUpgrade = true;
    }
    if(line.hasOption("postUpgrade")) {
      postUpgrade = true;
    }
    LOG.info("Starting with preUpgrade=" + preUpgrade + ", postUpgrade=" + postUpgrade +
        ", execute=" + execute + ", location=" + outputDir);
    if(preUpgrade && postUpgrade) {
      throw new IllegalArgumentException("Cannot specify both preUpgrade and postUpgrade");
    }

    try {
      String hiveVer = HiveVersionInfo.getShortVersion();
      if(preUpgrade) {
        if(!hiveVer.startsWith("2.")) {
          throw new IllegalStateException("preUpgrade requires Hive 2.x.  Actual: " + hiveVer);
        }
      }
      if(postUpgrade && execute && !isTestMode) {
        if(!hiveVer.startsWith("3.")) {
          throw new IllegalStateException("postUpgrade w/execute requires Hive 3.x.  Actual: " +
              hiveVer);
        }
      }
      tool.prepareAcidUpgradeInternal(outputDir, preUpgrade, postUpgrade, execute);
    }
    catch(Exception ex) {
      LOG.error("UpgradeTool failed", ex);
      throw ex;
    }
  }
  private static void printAndExit(UpgradeTool tool) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("upgrade-acid", tool.cmdLineOptions);
    System.exit(1);
  }

  private void init() {
    try {
      cmdLineOptions.addOption(new Option("help", "print this message"));
      cmdLineOptions.addOption(new Option("preUpgrade",
          "Generates a script to execute on 2.x cluster.  This requires 2.x binaries" +
              " on the classpath and hive-site.xml."));
      cmdLineOptions.addOption(new Option("postUpgrade",
          "Generates a script to execute on 3.x cluster.  This requires 3.x binaries" +
              " on the classpath and hive-site.xml."));
      Option exec = new Option("execute",
          "Executes commands equivalent to generated scrips");
      exec.setOptionalArg(true);
      cmdLineOptions.addOption(exec);
      cmdLineOptions.addOption(new Option("location", true,
          "Location to write scripts to. Default is CWD."));
    }
    catch(Exception ex) {
      LOG.error("init()", ex);
      throw ex;
    }
  }
  /**
   * todo: this should accept a file of table names to exclude from non-acid to acid conversion
   * todo: change script comments to a preamble instead of a footer
   *
   * how does rename script work?  "hadoop fs -mv oldname newname"    * and what what about S3?
   * How does this actually get executed?
   * all other actions are done via embedded JDBC
   *
   *
   */
  private void prepareAcidUpgradeInternal(String scriptLocation, boolean preUpgrade,
      boolean postUpgrade, boolean execute) throws HiveException, TException, IOException {
    HiveConf conf = hiveConf != null ? hiveConf : new HiveConf();
    boolean isAcidEnabled = isAcidEnabled(conf);
    HiveMetaStoreClient hms = new HiveMetaStoreClient(conf);//MetaException
    LOG.debug("Looking for databases");
    List<String> databases = hms.getAllDatabases();//TException
    LOG.debug("Found " + databases.size() + " databases to process");
    List<String> compactions = new ArrayList<>();
    List<String> convertToAcid = new ArrayList<>();
    List<String> convertToMM = new ArrayList<>();
    final CompactionMetaInfo compactionMetaInfo = new CompactionMetaInfo();
    ValidTxnList txns = null;
    Hive db = null;
    if(execute) {
      db = Hive.get(conf);
    }

    for(String dbName : databases) {
      List<String> tables = hms.getAllTables(dbName);
      LOG.debug("found " + tables.size() + " tables in " + dbName);
      for(String tableName : tables) {
        Table t = hms.getTable(dbName, tableName);
        LOG.debug("processing table " + Warehouse.getQualifiedName(t));
        if(preUpgrade && isAcidEnabled) {
          //if acid is off, there can't be any acid tables - nothing to compact
          if(execute && txns == null) {
          /*
           This API changed from 2.x to 3.0.  so this won't even compile with 3.0
           but it doesn't need to since we only run this preUpgrade
          */
            TxnStore txnHandler = TxnUtils.getTxnStore(conf);
            txns = TxnUtils.createValidCompactTxnList(txnHandler.getOpenTxnsInfo());
          }
          List<String> compactionCommands =
              getCompactionCommands(t, conf, hms, compactionMetaInfo, execute, db, txns);
          compactions.addAll(compactionCommands);
        }
        if(postUpgrade && isAcidEnabled) {
          //if acid is off post upgrade, you can't make any tables acid - will throw
          processConversion(t, convertToAcid, convertToMM, hms, db, execute);
        }
        /*todo: handle renaming files somewhere*/
      }
    }
    makeCompactionScript(compactions, scriptLocation, compactionMetaInfo);
    makeConvertTableScript(convertToAcid, convertToMM, scriptLocation);
    makeRenameFileScript(scriptLocation);//todo: is this pre or post upgrade?
    //todo: can different tables be in different FileSystems?
    if(preUpgrade && execute) {
      while(compactionMetaInfo.compactionIds.size() > 0) {
        LOG.debug("Will wait for " + compactionMetaInfo.compactionIds.size() +
            " compactions to complete");
        ShowCompactResponse resp = db.showCompactions();
        for(ShowCompactResponseElement e : resp.getCompacts()) {
          final String state = e.getState();
          boolean removed;
          switch (state) {
            case TxnStore.CLEANING_RESPONSE:
            case TxnStore.SUCCEEDED_RESPONSE:
              removed = compactionMetaInfo.compactionIds.remove(e.getId());
              if(removed) {
                LOG.debug("Required compaction succeeded: " + e.toString());
              }
              break;
            case TxnStore.ATTEMPTED_RESPONSE:
            case TxnStore.FAILED_RESPONSE:
              removed = compactionMetaInfo.compactionIds.remove(e.getId());
              if(removed) {
                LOG.warn("Required compaction failed: " + e.toString());
              }
              break;
            case TxnStore.INITIATED_RESPONSE:
              //may flood the log
              //LOG.debug("Still waiting  on: " + e.toString());
              break;
            case TxnStore.WORKING_RESPONSE:
              LOG.debug("Still working on: " + e.toString());
              break;
            default://shouldn't be any others
              LOG.error("Unexpected state for : " + e.toString());
          }
        }
        if(compactionMetaInfo.compactionIds.size() > 0) {
          try {
            if (callback != null) {
              callback.onWaitForCompaction();
            }
            Thread.sleep(pollIntervalMs);
          } catch (InterruptedException ex) {
            ;//this only responds to ^C
          }
        }
      }
    }
  }

  /**
   * Actualy makes the table transactional
   */
  private static void alterTable(Table t, Hive db, boolean isMM)
      throws HiveException, InvalidOperationException {
    org.apache.hadoop.hive.ql.metadata.Table metaTable =
        //clone to make sure new prop doesn't leak
        new org.apache.hadoop.hive.ql.metadata.Table(t.deepCopy());
    metaTable.getParameters().put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    if(isMM) {
      metaTable.getParameters()
          .put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "insert_only");
    }
    db.alterTable(Warehouse.getQualifiedName(t), metaTable, false, null);
  }

  /**
   * todo: handle exclusion list
   * Figures out which tables to make Acid, MM and (optionally, performs the operation)
   */
  private static void processConversion(Table t, List<String> convertToAcid,
      List<String> convertToMM, HiveMetaStoreClient hms, Hive db, boolean execute)
      throws TException, HiveException {
    if(isFullAcidTable(t)) {
      return;
    }
    if(!TableType.MANAGED_TABLE.name().equalsIgnoreCase(t.getTableType())) {
      return;
    }
    String fullTableName = Warehouse.getQualifiedName(t);
    if(t.getPartitionKeysSize() <= 0) {
      if(canBeMadeAcid(fullTableName, t.getSd())) {
        convertToAcid.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
            "'transactional'='true')");
        if(execute) {
          alterTable(t, db, false);
        }
      }
      else {
        convertToMM.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
            "'transactional'='true', 'transactional_properties'='insert_only')");
        if(execute) {
          alterTable(t, db, true);
        }
      }
    }
    else {
      /*
        each Partition may have different I/O Format so have to check them all before deciding to
        make a full CRUD table.
        Run in batches to prevent OOM
       */
      List<String> partNames = hms.listPartitionNames(t.getDbName(), t.getTableName(), (short)-1);
      int batchSize = PARTITION_BATCH_SIZE;
      int numWholeBatches = partNames.size()/batchSize;
      for(int i = 0; i < numWholeBatches; i++) {
        List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
            partNames.subList(i * batchSize, (i + 1) * batchSize));
        if(alterTable(fullTableName, partitionList, convertToMM, t, db, execute)) {
          return;
        }
      }
      if(numWholeBatches * batchSize < partNames.size()) {
        //last partial batch
        List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
            partNames.subList(numWholeBatches * batchSize, partNames.size()));
        if(alterTable(fullTableName, partitionList, convertToMM, t, db, execute)) {
          return;
        }
      }
      //if here checked all parts and they are Acid compatible - make it acid
      convertToAcid.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
          "'transactional'='true')");
      if(execute) {
        alterTable(t, db, false);
      }
    }
  }
  /**
   * @return true if table was converted/command generated
   */
  private static boolean alterTable(String fullTableName,  List<Partition> partitionList,
      List<String> convertToMM, Table t, Hive db, boolean execute)
      throws InvalidOperationException, HiveException {
    for(Partition p : partitionList) {
      if(!canBeMadeAcid(fullTableName, p.getSd())) {
        convertToMM.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
            "'transactional'='true', 'transactional_properties'='insert_only')");
        if(execute) {
          alterTable(t, db, true);
        }
        return true;
      }
    }
    return false;
  }
  private static boolean canBeMadeAcid(String fullTableName, StorageDescriptor sd) {
    return isAcidInputOutputFormat(fullTableName, sd) && sd.getSortColsSize() <= 0;
  }
  private static boolean isAcidInputOutputFormat(String fullTableName, StorageDescriptor sd) {
    try {
      Class inputFormatClass = sd.getInputFormat() == null ? null :
          Class.forName(sd.getInputFormat());
      Class outputFormatClass = sd.getOutputFormat() == null ? null :
          Class.forName(sd.getOutputFormat());

      if (inputFormatClass != null && outputFormatClass != null &&
          Class.forName("org.apache.hadoop.hive.ql.io.AcidInputFormat")
              .isAssignableFrom(inputFormatClass) &&
          Class.forName("org.apache.hadoop.hive.ql.io.AcidOutputFormat")
              .isAssignableFrom(outputFormatClass)) {
        return true;
      }
    } catch (ClassNotFoundException e) {
      //if a table is using some custom I/O format and it's not in the classpath, we won't mark
      //the table for Acid, but today (Hive 3.1 and earlier) OrcInput/OutputFormat is the only
      //Acid format
      LOG.error("Could not determine if " + fullTableName +
          " can be made Acid due to: " + e.getMessage(), e);
      return false;
    }
    return false;
  }
  /**
   * Generates a set compaction commands to run on pre Hive 3 cluster
   */
  private static void makeCompactionScript(List<String> commands, String scriptLocation,
      CompactionMetaInfo compactionMetaInfo) throws IOException {
    if (commands.isEmpty()) {
      LOG.info("No compaction is necessary");
      return;
    }
    String fileName = "compacts_" + System.currentTimeMillis() + ".sql";
    LOG.debug("Writing compaction commands to " + fileName);
    try(PrintWriter pw = createScript(commands, fileName, scriptLocation)) {
      //add post script
      pw.println("-- Generated total of " + commands.size() + " compaction commands");
      if(compactionMetaInfo.numberOfBytes < Math.pow(2, 20)) {
        //to see it working in UTs
        pw.println("-- The total volume of data to be compacted is " +
            String.format("%.6fMB", compactionMetaInfo.numberOfBytes/Math.pow(2, 20)));
      }
      else {
        pw.println("-- The total volume of data to be compacted is " +
            String.format("%.3fGB", compactionMetaInfo.numberOfBytes/Math.pow(2, 30)));
      }
      pw.println();
      //todo: should be at the top of the file...
      pw.println(
          "-- Please note that compaction may be a heavyweight and time consuming process.\n" +
              "-- Submitting all of these commands will enqueue them to a scheduling queue from\n" +
              "-- which they will be picked up by compactor Workers.  The max number of\n" +
              "-- concurrent Workers is controlled by hive.compactor.worker.threads configured\n" +
              "-- for the standalone metastore process.  Compaction itself is a Map-Reduce job\n" +
              "-- which is submitted to the YARN queue identified by hive.compactor.job.queue\n" +
              "-- property if defined or 'default' if not defined.  It's advisable to set the\n" +
              "-- capacity of this queue appropriately");
    }
  }
  private static void makeConvertTableScript(List<String> alterTableAcid, List<String> alterTableMm,
      String scriptLocation) throws IOException {
    if (alterTableAcid.isEmpty()) {
      LOG.info("No acid conversion is necessary");
    } else {
      String fileName = "convertToAcid_" + System.currentTimeMillis() + ".sql";
      LOG.debug("Writing CRUD conversion commands to " + fileName);
      try(PrintWriter pw = createScript(alterTableAcid, fileName, scriptLocation)) {
        //todo: fix this - it has to run in 3.0 since tables may be unbucketed
        pw.println("-- These commands may be executed by Hive 1.x later");
      }
    }

    if (alterTableMm.isEmpty()) {
      LOG.info("No managed table conversion is necessary");
    } else {
      String fileName = "convertToMM_" + System.currentTimeMillis() + ".sql";
      LOG.debug("Writing managed table conversion commands to " + fileName);
      try(PrintWriter pw = createScript(alterTableMm, fileName, scriptLocation)) {
        pw.println("-- These commands must be executed by Hive 3.0 or later");
      }
    }
  }

  private static PrintWriter createScript(List<String> commands, String fileName,
      String scriptLocation) throws IOException {
    FileWriter fw = new FileWriter(scriptLocation + "/" + fileName);
    PrintWriter pw = new PrintWriter(fw);
    for(String cmd : commands) {
      pw.println(cmd + ";");
    }
    return pw;
  }
  private static void makeRenameFileScript(String scriptLocation) throws IOException {
    List<String> commands = Collections.emptyList();
    if (commands.isEmpty()) {
      LOG.info("No file renaming is necessary");
    } else {
      String fileName = "normalizeFileNames_" + System.currentTimeMillis() + ".sh";
      LOG.debug("Writing file renaming commands to " + fileName);
      PrintWriter pw = createScript(commands, fileName, scriptLocation);
      pw.close();
    }
  }
  /**
   * @return any compaction commands to run for {@code Table t}
   */
  private static List<String> getCompactionCommands(Table t, HiveConf conf,
      HiveMetaStoreClient hms, CompactionMetaInfo compactionMetaInfo, boolean execute, Hive db,
      ValidTxnList txns) throws IOException, TException, HiveException {
    if(!isFullAcidTable(t)) {
      return Collections.emptyList();
    }
    if(t.getPartitionKeysSize() <= 0) {
      //not partitioned
      if(!needsCompaction(new Path(t.getSd().getLocation()), conf, compactionMetaInfo, txns)) {
        return Collections.emptyList();
      }

      List<String> cmds = new ArrayList<>();
      cmds.add(getCompactionCommand(t, null));
      if(execute) {
        scheduleCompaction(t, null, db, compactionMetaInfo);
      }
      return cmds;
    }
    List<String> partNames = hms.listPartitionNames(t.getDbName(), t.getTableName(), (short)-1);
    int batchSize = PARTITION_BATCH_SIZE;
    int numWholeBatches = partNames.size()/batchSize;
    List<String> compactionCommands = new ArrayList<>();
    for(int i = 0; i < numWholeBatches; i++) {
      List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
          partNames.subList(i * batchSize, (i + 1) * batchSize));
      getCompactionCommands(t, partitionList, db, execute, compactionCommands,
          compactionMetaInfo, conf, txns);
    }
    if(numWholeBatches * batchSize < partNames.size()) {
      //last partial batch
      List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
          partNames.subList(numWholeBatches * batchSize, partNames.size()));
      getCompactionCommands(t, partitionList, db, execute, compactionCommands,
          compactionMetaInfo, conf, txns);
    }
    return compactionCommands;
  }
  private static void getCompactionCommands(Table t, List<Partition> partitionList, Hive db,
      boolean execute, List<String> compactionCommands, CompactionMetaInfo compactionMetaInfo,
      HiveConf conf, ValidTxnList txns)
      throws IOException, TException, HiveException {
    for (Partition p : partitionList) {
      if (needsCompaction(new Path(p.getSd().getLocation()), conf, compactionMetaInfo, txns)) {
        compactionCommands.add(getCompactionCommand(t, p));
        if (execute) {
          scheduleCompaction(t, p, db, compactionMetaInfo);
        }
      }
    }
  }
  private static void scheduleCompaction(Table t, Partition p, Hive db,
      CompactionMetaInfo compactionMetaInfo) throws HiveException, MetaException {
    String partName = p == null ? null :
        Warehouse.makePartName(t.getPartitionKeys(), p.getValues());
    CompactionResponse resp =
        //this gives an easy way to get at compaction ID so we can only wait for those this
        //utility started
        db.compact2(t.getDbName(), t.getTableName(), partName, "major", null);
    if(!resp.isAccepted()) {
      LOG.info(Warehouse.getQualifiedName(t) + (p == null ? "" : "/" + partName) +
          " is already being compacted with id=" + resp.getId());
    }
    else {
      LOG.info("Scheduled compaction for " + Warehouse.getQualifiedName(t) +
          (p == null ? "" : "/" + partName) + " with id=" + resp.getId());
    }
    compactionMetaInfo.compactionIds.add(resp.getId());
  }
  /**
   *
   * @param location - path to a partition (or table if not partitioned) dir
   */
  private static boolean needsCompaction2(Path location, HiveConf conf,
      CompactionMetaInfo compactionMetaInfo) throws IOException {
    FileSystem fs = location.getFileSystem(conf);
    FileStatus[] deltas = fs.listStatus(location, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        //checking for delete_delta is only so that this functionality can be exercised by code 3.0
        //which cannot produce any deltas with mix of update/insert events
        return path.getName().startsWith("delta_") || path.getName().startsWith("delete_delta_");
      }
    });
    if(deltas == null || deltas.length == 0) {
      //base_n cannot contain update/delete.  Original files are all 'insert' and we need to compact
      //only if there are update/delete events.
      return false;
    }
    deltaLoop: for(FileStatus delta : deltas) {
      if(!delta.isDirectory()) {
        //should never happen - just in case
        continue;
      }
      FileStatus[] buckets = fs.listStatus(delta.getPath(), new PathFilter() {
        @Override
        public boolean accept(Path path) {
          //since this is inside a delta dir created by Hive 2.x or earlier it can only contain
          //bucket_x or bucket_x__flush_length
          return path.getName().startsWith("bucket_");
        }
      });
      for(FileStatus bucket : buckets) {
        if(bucket.getPath().getName().endsWith("_flush_length")) {
          //streaming ingest dir - cannot have update/delete events
          continue deltaLoop;
        }
        if(needsCompaction(bucket, fs)) {
          //found delete events - this 'location' needs compacting
          compactionMetaInfo.numberOfBytes += getDataSize(location, conf);
          //todo: this is not remotely accurate if you have many (relevant) original files
          return true;
        }
      }
    }
    return false;
  }
  /**
   *
   * @param location - path to a partition (or table if not partitioned) dir
   */
  private static boolean needsCompaction(Path location, HiveConf conf,
      CompactionMetaInfo compactionMetaInfo, ValidTxnList txns) throws IOException {
    FileSystem fs = location.getFileSystem(conf);
    FileStatus[] deltas = fs.listStatus(location, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        //checking for delete_delta is only so that this functionality can be exercised by code 3.0
        //which cannot produce any deltas with mix of update/insert events
        return path.getName().startsWith("delta_") || path.getName().startsWith("delete_delta_");
      }
    });
    if(deltas == null || deltas.length == 0) {
      //base_n cannot contain update/delete.  Original files are all 'insert' and we need to compact
      //only if there are update/delete events.
      return false;
    }
    /*getAcidState() is smart not to return any deltas in current if there is a base that covers
    * them, i.e. if they were compacted but not yet cleaned.  This means re-checking if
    * compaction is needed should cheap(er)*/
    AcidUtils.Directory dir = AcidUtils.getAcidState(location, conf, txns);
    deltaLoop: for(AcidUtils.ParsedDelta delta : dir.getCurrentDirectories()) {
      FileStatus[] buckets = fs.listStatus(delta.getPath(), new PathFilter() {
        @Override
        public boolean accept(Path path) {
          //since this is inside a delta dir created by Hive 2.x or earlier it can only contain
          //bucket_x or bucket_x__flush_length
          return path.getName().startsWith("bucket_");
        }
      });
      for(FileStatus bucket : buckets) {
        if(bucket.getPath().getName().endsWith("_flush_length")) {
          //streaming ingest dir - cannot have update/delete events
          continue deltaLoop;
        }
        if(needsCompaction(bucket, fs)) {
          //found delete events - this 'location' needs compacting
          compactionMetaInfo.numberOfBytes += getDataSize(location, conf);

          //if there are un-compacted original files, they will be included in compaction, so
          //count at the size for 'cost' estimation later
          for(HadoopShims.HdfsFileStatusWithId origFile : dir.getOriginalFiles()) {
            FileStatus fileStatus = origFile.getFileStatus();
            if(fileStatus != null) {
              compactionMetaInfo.numberOfBytes += fileStatus.getLen();
            }
          }
          return true;
        }
      }
    }
    return false;
  }

  /**
   * @param location - path to a partition (or table if not partitioned) dir
   */
  private static long getDataSize(Path location, HiveConf conf) throws IOException {
    FileSystem fs = location.getFileSystem(conf);
    ContentSummary cs = fs.getContentSummary(location);
    return cs.getLength();
  }
  private static boolean needsCompaction(FileStatus bucket, FileSystem fs) throws IOException {
    //create reader, look at footer
    //no need to check side file since it can only be in a streaming ingest delta
    Reader orcReader = OrcFile.createReader(bucket.getPath(),OrcFile.readerOptions(fs.getConf())
        .filesystem(fs));
    AcidStats as = OrcAcidUtils.parseAcidStats(orcReader);
    if(as == null) {
      //should never happen since we are reading bucket_x written by acid write
      throw new IllegalStateException("AcidStats missing in " + bucket.getPath());
    }
    return as.deletes > 0 || as.updates > 0;
  }
  private static String getCompactionCommand(Table t, Partition p) {
    StringBuilder sb = new StringBuilder("ALTER TABLE ").append(Warehouse.getQualifiedName(t));
    if(t.getPartitionKeysSize() > 0) {
      assert p != null : "must supply partition for partitioned table " +
          Warehouse.getQualifiedName(t);
      sb.append(" PARTITION(");
      for (int i = 0; i < t.getPartitionKeysSize(); i++) {
        sb.append(t.getPartitionKeys().get(i).getName()).append('=').append(
            genPartValueString(t.getPartitionKeys().get(i).getType(), p.getValues().get(i))).
            append(",");
      }
      sb.setCharAt(sb.length() - 1, ')');//replace trailing ','
    }
    return sb.append(" COMPACT 'major'").toString();
  }

  /**
   * This is copy-pasted from {@link org.apache.hadoop.hive.ql.parse.ColumnStatsSemanticAnalyzer},
   * which can't be refactored since this is linked against Hive 2.x
   */
  private static String genPartValueString(String partColType, String partVal)  {
    String returnVal = partVal;
    if (partColType.equals(serdeConstants.STRING_TYPE_NAME) ||
        partColType.contains(serdeConstants.VARCHAR_TYPE_NAME) ||
        partColType.contains(serdeConstants.CHAR_TYPE_NAME)) {
      returnVal = "'" + escapeSQLString(partVal) + "'";
    } else if (partColType.equals(serdeConstants.TINYINT_TYPE_NAME)) {
      returnVal = partVal + "Y";
    } else if (partColType.equals(serdeConstants.SMALLINT_TYPE_NAME)) {
      returnVal = partVal + "S";
    } else if (partColType.equals(serdeConstants.INT_TYPE_NAME)) {
      returnVal = partVal;
    } else if (partColType.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      returnVal = partVal + "L";
    } else if (partColType.contains(serdeConstants.DECIMAL_TYPE_NAME)) {
      returnVal = partVal + "BD";
    } else if (partColType.equals(serdeConstants.DATE_TYPE_NAME) ||
        partColType.equals(serdeConstants.TIMESTAMP_TYPE_NAME)) {
      returnVal = partColType + " '" + escapeSQLString(partVal) + "'";
    } else {
      //for other usually not used types, just quote the value
      returnVal = "'" + escapeSQLString(partVal) + "'";
    }

    return returnVal;
  }
  private static boolean isFullAcidTable(Table t) {
    if (t.getParametersSize() <= 0) {
      //cannot be acid
      return false;
    }
    String transacationalValue = t.getParameters()
        .get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (transacationalValue != null && "true".equalsIgnoreCase(transacationalValue)) {
      System.out.println("Found Acid table: " + Warehouse.getQualifiedName(t));
      return true;
    }
    return false;
  }
  private static boolean isAcidEnabled(HiveConf hiveConf) {
    String txnMgr = hiveConf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER);
    boolean concurrency =  hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
    String dbTxnMgr = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";
    return txnMgr.equals(dbTxnMgr) && concurrency;
  }

  private static class CompactionMetaInfo {
    /**
     * total number of bytes to be compacted across all compaction commands
     */
    long numberOfBytes;
    /**
     * IDs of compactions launched by this utility
     */
    Set<Long> compactionIds = new HashSet<>();
  }

  @VisibleForTesting
  static abstract class Callback {
    /**
     * This is a hack enable Unit testing.  Derby can't handle multiple concurrent threads but
     * somehow Compactor needs to run to test "execute" mode.  This callback can be used
     * to run Worker.  For TESTING ONLY.
     */
    void onWaitForCompaction() throws MetaException {}
  }
  @VisibleForTesting
  static Callback callback;
  @VisibleForTesting
  static int pollIntervalMs = 1000*30;
  /**
   * Also to enable testing until I set up Maven profiles to be able to run with 3.0 jars
   */
  @VisibleForTesting
  static boolean isTestMode = false;
  /**
   * can set it from tests to test when config needs something other than default values
   */
  @VisibleForTesting
  static HiveConf hiveConf = null;
}
