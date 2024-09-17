
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.FileUtils.RemoteIteratorWithFilter;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;
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
 * A new type of transactional tables was added in 3.0 - insert-only tables.  These
 * tables support ACID semantics and work with any Input/OutputFormat.  Any Managed tables may
 * be made insert-only transactional table. These tables don't support Update/Delete/Merge commands.
 *
 * In postUpgrade mode, Hive 3.0 jars/hive-site.xml should be on the classpath. This utility will
 * find all the tables that may be made transactional (with ful CRUD support) and generate
 * Alter Table commands to do so.  It will also find all tables that may do not support full CRUD
 * but can be made insert-only transactional tables and generate corresponding Alter Table commands.
 *
 * Note that to convert a table to full CRUD table requires that all files follow a naming
 * convention, namely 0000N_0 or 0000N_0_copy_M, N &gt;= 0, M &gt; 0.  This utility can perform this
 * rename with "execute" option.  It will also produce a script (with and w/o "execute" to
 * perform the renames).
 *
 * "execute" option may be supplied in both modes to have the utility automatically execute the
 * equivalent of the generated commands
 *
 * "location" option may be supplied followed by a path to set the location for the generated
 * scripts.
 *
 * See also org.apache.hadoop.hive.upgrade.acid.PreUpgradeTool for steps which may be necessary to
 * perform before upgrading to Hive 3.
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
      LOG.info("Using Hive Version: " + HiveVersionInfo.getVersion() + " build: " +
          HiveVersionInfo.getBuildVersion());
      if(!(hiveVer.startsWith("3.") || hiveVer.startsWith("4."))) {
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
  private static IMetaStoreClient getHMS(HiveConf conf) {
    UserGroupInformation loggedInUser = null;
    try {
      loggedInUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      LOG.warn("Unable to get logged in user via UGI. err: {}", e.getMessage());
    }
    boolean secureMode = loggedInUser != null && loggedInUser.hasKerberosCredentials();
    if (secureMode) {
      MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_SASL, true);
    }
    try {
      LOG.info("Creating metastore client for {}", "PreUpgradeTool");
      return RetryingMetaStoreClient.getProxy(conf, true);
    } catch (MetaException e) {
      throw new RuntimeException("Error connecting to Hive Metastore URI: "
          + conf.getVar(HiveConf.ConfVars.METASTORE_URIS) + ". " + e.getMessage(), e);
    }
  }
  /**
   * todo: this should accept a file of table names to exclude from non-acid to acid conversion
   * todo: change script comments to a preamble instead of a footer
   */
  private void performUpgradeInternal(String scriptLocation, boolean execute)
      throws HiveException, TException, IOException {
    HiveConf conf = hiveConf != null ? hiveConf : new HiveConf();
    boolean isAcidEnabled = isAcidEnabled(conf);
    IMetaStoreClient hms = getHMS(conf);
    LOG.debug("Looking for databases");
    List<String> databases = hms.getAllDatabases();//TException
    LOG.debug("Found " + databases.size() + " databases to process");
    List<String> convertToAcid = new ArrayList<>();
    List<String> convertToMM = new ArrayList<>();
    Hive db = null;
    if(execute) {
      db = Hive.get(conf);
    }
    PrintWriter pw = makeRenameFileScript(scriptLocation);

    for(String dbName : databases) {
      List<String> tables = hms.getAllTables(dbName);
      LOG.debug("found " + tables.size() + " tables in " + dbName);
      for(String tableName : tables) {
        Table t = hms.getTable(dbName, tableName);
        LOG.debug("processing table " + Warehouse.getQualifiedName(t));
        if(isAcidEnabled) {
          //if acid is off post upgrade, you can't make any tables acid - will throw
          processConversion(t, convertToAcid, convertToMM, hms, db, execute, pw);
        }
      }
    }
    pw.close();
    makeConvertTableScript(convertToAcid, convertToMM, scriptLocation);
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
    EnvironmentContext ec = new EnvironmentContext();
    /*we are not modifying any data so stats should be exactly the same*/
    ec.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    db.alterTable(Warehouse.getQualifiedName(t), metaTable, false, ec, false);
  }

  /**
   * assumes https://issues.apache.org/jira/browse/HIVE-19750 is in
   * How does this work with Storage Based Auth?
   * @param p partition root or table root if not partitioned
   */
  static void handleRenameFiles(Table t, Path p, boolean execute, Configuration conf,
      boolean isBucketed, PrintWriter pw) throws IOException {
    if (isBucketed) {
      /* For bucketed tables we assume that Hive wrote them and 0000M_0 and 0000M_0_copy_8
      are the only possibilities.  Since we can't move files across buckets the only thing we
      can do is put 0000M_0_copy_N into delta_N_N as 0000M_0.

      If M > 4096 - should error out - better yet, make this table external one - can those
      be bucketed?  don't think so
      */
      //Known deltas
      Map<Integer, List<Path>> deltaToFileMap = new HashMap<>();
      FileSystem fs = FileSystem.get(conf);
      RemoteIteratorWithFilter iter =
          new RemoteIteratorWithFilter(fs.listFiles(p, true), RemoteIteratorWithFilter.HIDDEN_FILES_FULL_PATH_FILTER);
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
      if(!deltaToFileMap.isEmpty()) {
        println(pw, "#Begin file renames for bucketed table " + Warehouse.getQualifiedName(t));
      }
      for (Map.Entry<Integer, List<Path>> ent : deltaToFileMap.entrySet()) {
        /* create delta and move each files to it.  HIVE-19750 ensures wer have reserved
         * enough write IDs to do this.*/
        Path deltaDir = new Path(p, AcidUtils.deltaSubdir(ent.getKey(), ent.getKey()));
        if (execute) {
          if (!fs.mkdirs(deltaDir)) {
            String msg = "Failed to create directory " + deltaDir;
            LOG.error(msg);
            throw new IllegalStateException(msg);
          }
        }
        // Add to list of FS commands
        makeDirectoryCommand(deltaDir, pw);

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
          }
          //do this with and w/o execute to know what was done
          makeRenameCommand(file, newFile, pw);
        }
      }
      if(!deltaToFileMap.isEmpty()) {
        println(pw, "#End file renames for bucketed table " + Warehouse.getQualifiedName(t));
      }
      return;
    }
    List<RenamePair> renames = new ArrayList<>();
    FileSystem fs = FileSystem.get(conf);
    RemoteIteratorWithFilter iter =
        new RemoteIteratorWithFilter(fs.listFiles(p, true), RemoteIteratorWithFilter.HIDDEN_FILES_FULL_PATH_FILTER);
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
      int writeId = fileId / numBuckets + 1; // start with delta_1 (not delta_0)
      Path deltaDir = new Path(p, AcidUtils.deltaSubdir(writeId, writeId));
      if (execute) {
        if (!fs.mkdirs(deltaDir)) {
          String msg = "Failed to create directory " + deltaDir;
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }
      }
      // Add to list of FS commands
      makeDirectoryCommand(deltaDir, pw);

      Path newPath =
          new Path(deltaDir, String.format(AcidUtils.BUCKET_DIGITS, fileId % numBuckets)+ "_0");
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
    if(!renames.isEmpty()) {
      println(pw, "#Begin file renames for unbucketed table " + Warehouse.getQualifiedName(t));
    }
    for(RenamePair renamePair : renames) {
      LOG.debug("need to rename: " + renamePair.getOldPath() + " to " + renamePair.getNewPath());
      if (fs.exists(renamePair.getNewPath())) {
        String msg = Warehouse.getQualifiedName(t) + ": " + renamePair.getNewPath() +
            " already exists?!";
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }
      if (execute) {
        if (!fs.rename(renamePair.getOldPath(), renamePair.getNewPath())) {
          String msg = Warehouse.getQualifiedName(t) + ": " + renamePair.getNewPath() +
              ": failed to rename";
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }
      }
      //do this with and w/o execute to know what was done
      makeRenameCommand(renamePair.getOldPath(), renamePair.getNewPath(), pw);
    }
    if(!renames.isEmpty()) {
      println(pw, "#End file renames for unbucketed table " + Warehouse.getQualifiedName(t));
    }
  }
  private static void makeRenameCommand(Path file, Path newFile, PrintWriter pw) {
    //https://hadoop.apache.org/docs/r3.0.0-alpha2/hadoop-project-dist/hadoop-common/FileSystemShell.html#mv
    println(pw, "hadoop fs -mv " + file + " " + newFile + ";");
  }
  private static void makeDirectoryCommand(Path dir, PrintWriter pw) {
    println(pw, "hadoop fs -mkdir " + dir + ";");
  }

  private static void println(PrintWriter pw, String msg) {
    if (pw != null) {
      pw.println(msg);
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
  private static long getDataSize(Path location, Configuration conf) throws IOException {
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
      List<String> convertToMM, IMetaStoreClient hms, Hive db, boolean execute, PrintWriter pw)
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
    boolean canBeMadeAcid = AcidUtils.canBeMadeAcid(fullTableName, t.getSd());
    if(t.getPartitionKeysSize() <= 0) {
      if(canBeMadeAcid) {
        convertToAcid.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
            "'transactional'='true')");
        //do this before alterTable in case files need to be renamed, else
        // TransactionalMetastoreListerner will squak
        handleRenameFiles(t, new Path(t.getSd().getLocation()), execute, db.getConf(),
            t.getSd().getBucketColsSize() > 0, pw);
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
              t.getSd().getBucketColsSize() > 0, pw);
        }
      }
      if(numWholeBatches * batchSize < partNames.size()) {
        //last partial batch
        List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
            partNames.subList(numWholeBatches * batchSize, partNames.size()));
        for(Partition part : partitionList) {
          handleRenameFiles(t, new Path(part.getSd().getLocation()), execute, db.getConf(),
              t.getSd().getBucketColsSize() > 0, pw);
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


  private static void makeConvertTableScript(List<String> alterTableAcid, List<String> alterTableMm,
      String scriptLocation) throws IOException {
    if (alterTableAcid.isEmpty()) {
      LOG.info("No acid conversion is necessary");
    } else {
      String fileName = "convertToAcid_" + System.currentTimeMillis() + ".sql";
      LOG.debug("Writing CRUD conversion commands to " + fileName);
      try(PrintWriter pw = createScript(alterTableAcid, fileName, scriptLocation)) {
        pw.println("-- These commands may be executed by Hive 3.x later");
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
  private static PrintWriter makeRenameFileScript(String scriptLocation) throws IOException {
    String fileName = "normalizeFileNames_" + System.currentTimeMillis() + ".sh";
    LOG.info("Writing file renaming commands to " + fileName);
    return createScript(Collections.emptyList(), fileName, scriptLocation);
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
