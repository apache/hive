
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


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
    boolean execute = false;
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
    LOG.info("Starting with execute=" + execute + ", location=" + outputDir);

    try {
      String hiveVer = HiveVersionInfo.getShortVersion();
      if(!hiveVer.startsWith("3.")) {
        throw new IllegalStateException("postUpgrade w/execute requires Hive 3.x.  Actual: " +
            hiveVer);
      }
      tool.performUpgradeInternal(outputDir, execute);
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
      cmdLineOptions.addOption(new Option("help", "Generates a script to execute on 3.x " +
          "cluster.  This requires 3.x binaries on the classpath and hive-site.xml."));
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
   */
  private void performUpgradeInternal(String scriptLocation, boolean execute)
      throws HiveException, TException, IOException {
    HiveConf conf = hiveConf != null ? hiveConf : new HiveConf();
    boolean isAcidEnabled = isAcidEnabled(conf);
    HiveMetaStoreClient hms = new HiveMetaStoreClient(conf);//MetaException
    LOG.debug("Looking for databases");
    List<String> databases = hms.getAllDatabases();//TException
    LOG.debug("Found " + databases.size() + " databases to process");
    List<String> convertToAcid = new ArrayList<>();
    List<String> convertToMM = new ArrayList<>();
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
        if(isAcidEnabled) {
          //if acid is off post upgrade, you can't make any tables acid - will throw
          processConversion(t, convertToAcid, convertToMM, hms, db, execute);
        }
        /*todo: handle renaming files somewhere*/
      }
    }
    makeConvertTableScript(convertToAcid, convertToMM, scriptLocation);
    makeRenameFileScript(scriptLocation);//todo: is this pre or post upgrade?
    //todo: can different tables be in different FileSystems?
  }

  /**
   * Actually makes the table transactional
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
   * assumes https://issues.apache.org/jira/browse/HIVE-19750 is in
   * How does this work with Storage Based Auth?
   * @param p partition root or table root if not partitioned
   */
  private static void handleRenameFiles(Table t, Path p, boolean execute, HiveConf conf, boolean isBucketed)
      throws IOException {
    AcidUtils.BUCKET_DIGIT_PATTERN.matcher("foo");
    if (isBucketed) {
      /* For bucketed tables we assume that Hive wrote them and 0000M_0 and 0000M_0_copy_8
      are the only possibilities.  Since we can't move files across buckets the only thing we
      can do is put 0000M_0_copy_N into delta_N_N as 0000M_0.
       *
       * If M > 4096 - should error out - better yet, make this table external one - can those be bucketed?  don't think so
       */
      //Known deltas
      Map<Integer, List<Path>> deltaToFileMap = new HashMap<>();
      FileSystem fs = FileSystem.get(conf);
      RemoteIterator<LocatedFileStatus> iter = fs.listFiles(p, true);
      Function<Integer, List<Path>> makeList = new Function<Integer, List<Path>>() {//lambda?
        @Override
        public List<Path> apply(Integer aVoid) {
          return new ArrayList<>();
        }
      };
      while (iter.hasNext()) {
        LocatedFileStatus lfs = iter.next();
        if (lfs.isDirectory()) {
          String msg = Warehouse.getQualifiedName(t) + " is bucketed and has a subdirectory: " +
              lfs.getPath();
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }
        AcidUtils.BucketMetaData bmd = AcidUtils.BucketMetaData.parse(lfs.getPath());
        if (bmd.bucketId < 0) {
          //non-standard file name - don't know what bucket the rows belong to and we can't
          //rename the file so tha it may end up treated like a different bucket id
          String msg = "Bucketed table " + Warehouse.getQualifiedName(t) + " contains file " +
              lfs.getPath() + " with non-standard name";
          LOG.error(msg);
          throw new IllegalArgumentException(msg);
        } else {
          if (bmd.bucketId > BucketCodec.MAX_BUCKET_ID) {
            String msg = "Bucketed table " + Warehouse.getQualifiedName(t) + " contains file " +
                lfs.getPath() + " with bucketId=" + bmd.bucketId + " that is out of range";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
          }
          if (bmd.copyNumber > 0) {
            deltaToFileMap.computeIfAbsent(bmd.copyNumber, makeList).add(lfs.getPath());
          }
        }
      }
      for (Map.Entry<Integer, List<Path>> ent : deltaToFileMap.entrySet()) {
        /* create delta and move each files to it.  HIVE-19750 ensures wer have reserved
         * enough write IDs to do this.*/
        Path deltaDir = new Path(p, AcidUtils.deltaSubdir(ent.getKey(), ent.getKey()));
        for (Path file : ent.getValue()) {
          Path newFile = new Path(deltaDir, stripCopySuffix(file.getName()));
          LOG.debug("need to rename: " + file + " to " + newFile);
          if (fs.exists(newFile)) {
            String msg = Warehouse.getQualifiedName(t) + ": " + newFile + " already exists?!";
            LOG.error(msg);
            throw new IllegalStateException(msg);
          }
          if (execute) {
            if (!fs.rename(file, newFile)) {
              String msg = Warehouse.getQualifiedName(t) + ": " + newFile + ": failed to rename";
              LOG.error(msg);
              throw new IllegalStateException(msg);
            }
          } else {
            //todo: generate a rename command; what if FS is not hdfs?
          }
        }
      }
      return;
    }
    List<RenamePair> renames = new ArrayList<>();
    FileSystem fs = FileSystem.get(conf);
    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(p, true);
    /**
     * count some heuristics - bad file is something not in {@link AcidUtils#ORIGINAL_PATTERN} or
     * {@link AcidUtils#ORIGINAL_PATTERN_COPY} format.  This has to be renamed for acid to work.
     */
    int numBadFileNames = 0;
    /**
     * count some heuristics - num files in {@link AcidUtils#ORIGINAL_PATTERN_COPY} format.  These
     * are supported but if there are a lot of them there will be a perf hit on read until
     * major compaction
     */
    int numCopyNFiles = 0;
    int fileId = 0;//ordinal of the file in the iterator
    long numBytesInPartition = getDataSize(p, conf);
    int numBuckets = guessNumBuckets(numBytesInPartition);
    while (iter.hasNext()) {
      LocatedFileStatus lfs = iter.next();
      if(lfs.isDirectory()) {
        continue;
      }
      AcidUtils.BucketMetaData bmd = AcidUtils.BucketMetaData.parse(lfs.getPath());
      if(bmd.bucketId < 0) {
        numBadFileNames++;
      }
      if(bmd.copyNumber > 0) {
        //todo: what about same file name in subdir like Union All?  ROW_ID generation will handle it
        //but will have to look at ORC footers - treat these as copyN files?
        numCopyNFiles++;
      }
      int wrtieId = fileId / numBuckets + 1;//start with delta_1 (not delta_0)
      Path deltaDir = new Path(p, AcidUtils.deltaSubdir(wrtieId, wrtieId));
      Path newPath = new Path(deltaDir, String.format(AcidUtils.BUCKET_DIGITS, fileId % numBuckets)+ "_0");
      /*we could track reason for rename in RenamePair so that the decision can be made later to
       rename or not.  For example, if we need to minimize renames (say we are on S3), then we'd
        only rename if it's absolutely required, i.e. if it's a 'bad file name'*/
      renames.add(new RenamePair(lfs.getPath(), newPath));
      fileId++;
    }
    if(numBadFileNames <= 0 && numCopyNFiles <=0) {
      //if here, the only reason we'd want to rename is to spread the data into logical buckets to
      //help 3.0 Compactor generated more balanced splits
      return;
    }
    for(RenamePair renamePair : renames) {
      LOG.debug("need to rename: " + renamePair.getOldPath() + " to " + renamePair.getNewPath());
      if (fs.exists(renamePair.getNewPath())) {
        String msg = Warehouse.getQualifiedName(t) + ": " + renamePair.getNewPath() + " already exists?!";
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }
      if (execute) {
        if (!fs.rename(renamePair.getOldPath(), renamePair.getNewPath())) {
          String msg = Warehouse.getQualifiedName(t) + ": " + renamePair.getNewPath() + ": failed to rename";
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }
      } else {
        //todo: generate a rename command; what if FS is not hdfs?
      }

    }
  }

  /**
   * Need better impl to be more memory efficient - there could be a lot of these objects.
   * For example, remember partition root Path elsewhere,
   * and have this object remember relative path to old file and bucketid/deletaid of new one
   */
  private static final class RenamePair {
    private Path oldPath;
    private Path newPath;
    private RenamePair(Path old, Path newPath) {
      this.oldPath = old;
      this.newPath = newPath;
    }
    private Path getOldPath() {
      return oldPath;
    }
    private Path getNewPath() {
      return newPath;
    }
  }
  /**
   * @param location - path to a partition (or table if not partitioned) dir
   */
  private static long getDataSize(Path location, HiveConf conf) throws IOException {
    FileSystem fs = location.getFileSystem(conf);
    ContentSummary cs = fs.getContentSummary(location);
    return cs.getLength();
  }

  /**
   * @param fileName - matching {@link AcidUtils#ORIGINAL_PATTERN_COPY}
   */
  private static String stripCopySuffix(String fileName) {
    //0000_0_copy_N -> 0000_0
    return fileName.substring(0, fileName.indexOf('_', 1 + fileName.indexOf('_', 0)));
  }

  /**
   * Since current compactor derives its parallelism from file names, we need to name files in
   * a way to control this parallelism.  This should be a function of data size.
   * @param partitionSizeInBytes
   * @return cannot exceed 4096
   */
  public static int guessNumBuckets(long partitionSizeInBytes) {
    long OneGB = 1000000000;
    if(partitionSizeInBytes <= 1000000000) {
      return 1;//1 bucket
    }
    if(partitionSizeInBytes <= 100 * OneGB) {
      return 8;
    }
    if(partitionSizeInBytes <= 1000 * OneGB) {//TB
      return 16;
    }
    if(partitionSizeInBytes <= 10 * 1000 * OneGB) {//10 TB
      return 32;
    }
    if(partitionSizeInBytes <= 100 * 1000 * OneGB) {//100TB
      return 64;
    }
    if(partitionSizeInBytes <= 1000 * 1000 * OneGB) {//PT
      return 128;
    }
    if(partitionSizeInBytes <= 10 * 1000* 1000 * OneGB) {//10 PT
      return 256;
    }
    if(partitionSizeInBytes <= 100 * 1000 * 1000 * OneGB) {//100 PT
      return 512;
    }
    if(partitionSizeInBytes <= 1000 * 1000 *1000 * OneGB) {//1000 PT
      return 1024;
    }
    return 2048;
  }
  /**
   * todo: handle exclusion list
   * Figures out which tables to make Acid, MM and (optionally, performs the operation)
   */
  private static void processConversion(Table t, List<String> convertToAcid,
      List<String> convertToMM, HiveMetaStoreClient hms, Hive db, boolean execute)
      throws TException, HiveException, IOException {
    if(isFullAcidTable(t)) {
      return;
    }
    if(!TableType.MANAGED_TABLE.name().equalsIgnoreCase(t.getTableType())) {
      return;
    }
    //todo: are HBase, Druid talbes managed in 2.x? 3.0?
    String fullTableName = Warehouse.getQualifiedName(t);
    /*
     * ORC uses table props for settings so things like bucketing, I/O Format, etc should
     * be the same for each partition.
     */
    boolean canBeMadeAcid = canBeMadeAcid(fullTableName, t.getSd());
    if(t.getPartitionKeysSize() <= 0) {
      if(canBeMadeAcid) {
        convertToAcid.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
            "'transactional'='true')");
        //do this before alterTable in case files need to be renamed, else
        // TransactionalMetastoreListerner will squak
        handleRenameFiles(t, new Path(t.getSd().getLocation()), execute, db.getConf(),
            t.getSd().getBucketColsSize() > 0);
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
      if(!canBeMadeAcid) {
        convertToMM.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
            "'transactional'='true', 'transactional_properties'='insert_only')");
        if(execute) {
          alterTable(t, db, true);
        }
        return;
      }
      //now that we know it can be made acid, rename files as needed
      //process in batches in case there is a huge # of partitions
      List<String> partNames = hms.listPartitionNames(t.getDbName(), t.getTableName(), (short)-1);
      int batchSize = PARTITION_BATCH_SIZE;
      int numWholeBatches = partNames.size()/batchSize;
      for(int i = 0; i < numWholeBatches; i++) {
        List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
            partNames.subList(i * batchSize, (i + 1) * batchSize));
        for(Partition part : partitionList) {
          handleRenameFiles(t, new Path(part.getSd().getLocation()), execute, db.getConf(),
              t.getSd().getBucketColsSize() > 0);
        }
      }
      if(numWholeBatches * batchSize < partNames.size()) {
        //last partial batch
        List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
            partNames.subList(numWholeBatches * batchSize, partNames.size()));
        for(Partition part : partitionList) {
          handleRenameFiles(t, new Path(part.getSd().getLocation()), execute, db.getConf(),
              t.getSd().getBucketColsSize() > 0);
        }
      }
      //if here, handled all parts and they are no wAcid compatible - make it acid
      convertToAcid.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
          "'transactional'='true')");
      if(execute) {
        alterTable(t, db, false);
      }
    }
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
  /**
   * can set it from tests to test when config needs something other than default values
   * For example, that acid is enabled
   */
  @VisibleForTesting
  static HiveConf hiveConf = null;
}
