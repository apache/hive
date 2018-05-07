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

package org.apache.hadoop.hive.metastore.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.OrcAcidUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ObjectStore;

/**
 * This class provides Hive admins a tool to
 * - execute JDOQL against the metastore using DataNucleus
 * - perform HA name node upgrade
 */

public class HiveMetaTool {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaTool.class.getName());
  private final Options cmdLineOptions = new Options();
  private ObjectStore objStore;
  private boolean isObjStoreInitialized;

  public HiveMetaTool() {
    this.isObjStoreInitialized = false;
  }

  @SuppressWarnings("static-access")
  private void init() {

    System.out.println("Initializing HiveMetaTool..");

    Option help = new Option("help", "print this message");
    Option listFSRoot = new Option("listFSRoot", "print the current FS root locations");
    Option executeJDOQL =
        OptionBuilder.withArgName("query-string")
            .hasArgs()
            .withDescription("execute the given JDOQL query")
            .create("executeJDOQL");

    /* Ideally we want to specify the different arguments to updateLocation as separate argNames.
     * However if we did that, HelpFormatter swallows all but the last argument. Note that this is
     * a know issue with the HelpFormatter class that has not been fixed. We specify all arguments
     * with a single argName to workaround this HelpFormatter bug.
     */
    Option updateFSRootLoc =
        OptionBuilder
            .withArgName("new-loc> " + "<old-loc")
            .hasArgs(2)
            .withDescription(
                "Update FS root location in the metastore to new location.Both new-loc and " +
                    "old-loc should be valid URIs with valid host names and schemes." +
                    "When run with the dryRun option changes are displayed but are not " +
                    "persisted. When run with the serdepropKey/tablePropKey option " +
                    "updateLocation looks for the serde-prop-key/table-prop-key that is " +
                    "specified and updates its value if found.")
                    .create("updateLocation");
    Option dryRun = new Option("dryRun" , "Perform a dry run of updateLocation changes.When " +
      "run with the dryRun option updateLocation changes are displayed but not persisted. " +
      "dryRun is valid only with the updateLocation option.");
    Option serdePropKey =
        OptionBuilder.withArgName("serde-prop-key")
        .hasArgs()
        .withValueSeparator()
        .withDescription("Specify the key for serde property to be updated. serdePropKey option " +
           "is valid only with updateLocation option.")
        .create("serdePropKey");
    Option tablePropKey =
        OptionBuilder.withArgName("table-prop-key")
        .hasArg()
        .withValueSeparator()
        .withDescription("Specify the key for table property to be updated. tablePropKey option " +
          "is valid only with updateLocation option.")
        .create("tablePropKey");
    Option prepareAcidUpgrade =
        OptionBuilder.withArgName("find-compactions")
            .hasOptionalArg() //directory to output results to
            .withDescription("Generates a set Compaction commands to run to prepare for Hive 2.x" +
                " to 3.0 upgrade")
            .create("prepareAcidUpgrade");

    cmdLineOptions.addOption(help);
    cmdLineOptions.addOption(listFSRoot);
    cmdLineOptions.addOption(executeJDOQL);
    cmdLineOptions.addOption(updateFSRootLoc);
    cmdLineOptions.addOption(dryRun);
    cmdLineOptions.addOption(serdePropKey);
    cmdLineOptions.addOption(tablePropKey);
    cmdLineOptions.addOption(prepareAcidUpgrade);
  }

  private void initObjectStore(Configuration conf) {
    if (!isObjStoreInitialized) {
      objStore = new ObjectStore();
      objStore.setConf(conf);
      isObjStoreInitialized = true;
    }
  }

  private void shutdownObjectStore() {
    if (isObjStoreInitialized) {
      objStore.shutdown();
      isObjStoreInitialized = false;
    }
  }

  private void listFSRoot() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    initObjectStore(conf);

    Set<String> hdfsRoots = objStore.listFSRoots();
    if (hdfsRoots != null) {
      System.out.println("Listing FS Roots..");
      for (String s : hdfsRoots) {
        System.out.println(s);
      }
    } else {
      System.err.println("Encountered error during listFSRoot - " +
        "commit of JDO transaction failed");
    }
  }

  private void executeJDOQLSelect(String query) {
    Configuration conf = MetastoreConf.newMetastoreConf();
    initObjectStore(conf);

    System.out.println("Executing query: " + query);
    try (ObjectStore.QueryWrapper queryWrapper = new ObjectStore.QueryWrapper()) {
      Collection<?> result = objStore.executeJDOQLSelect(query, queryWrapper);
      if (result != null) {
        Iterator<?> iter = result.iterator();
        while (iter.hasNext()) {
          Object o = iter.next();
          System.out.println(o.toString());
        }
      } else {
        System.err.println("Encountered error during executeJDOQLSelect -" +
            "commit of JDO transaction failed.");
      }
    }
  }

  private void executeJDOQLUpdate(String query) {
    Configuration conf = MetastoreConf.newMetastoreConf();
    initObjectStore(conf);

    System.out.println("Executing query: " + query);
    long numUpdated = objStore.executeJDOQLUpdate(query);
    if (numUpdated >= 0) {
      System.out.println("Number of records updated: " + numUpdated);
    } else {
      System.err.println("Encountered error during executeJDOQL -" +
        "commit of JDO transaction failed.");
    }
  }

  private int printUpdateLocations(Map<String, String> updateLocations) {
    int count = 0;
    for (String key: updateLocations.keySet()) {
      String value = updateLocations.get(key);
      System.out.println("old location: " + key + " new location: " + value);
      count++;
    }
    return count;
  }

  private void printTblURIUpdateSummary(ObjectStore.UpdateMStorageDescriptorTblURIRetVal retVal,
    boolean isDryRun) {
    String tblName = "SDS";
    String fieldName = "LOCATION";

    if (retVal == null) {
      System.err.println("Encountered error while executing updateMStorageDescriptorTblURI - " +
          "commit of JDO transaction failed. Failed to update FSRoot locations in " +
          fieldName + "field in " + tblName + " table.");
    } else {
      Map<String, String> updateLocations = retVal.getUpdateLocations();
      if (isDryRun) {
        System.out.println("Dry Run of updateLocation on table " + tblName + "..");
      } else {
        System.out.println("Successfully updated the following locations..");
      }
      int count = printUpdateLocations(updateLocations);
      if (isDryRun) {
        System.out.println("Found " + count + " records in " + tblName + " table to update");
      } else {
        System.out.println("Updated " + count + " records in " + tblName + " table");
      }
      List<String> badRecords = retVal.getBadRecords();
      if (badRecords.size() > 0) {
        System.err.println("Warning: Found records with bad " + fieldName +  " in " +
          tblName + " table.. ");
        for (String badRecord:badRecords) {
          System.err.println("bad location URI: " + badRecord);
        }
      }
      int numNullRecords = retVal.getNumNullRecords();
      if (numNullRecords != 0) {
        LOG.debug("Number of NULL location URI: " + numNullRecords +
            ". This can happen for View or Index.");
      }
    }
  }

  private void printDatabaseURIUpdateSummary(ObjectStore.UpdateMDatabaseURIRetVal retVal,
    boolean isDryRun) {
    String tblName = "DBS";
    String fieldName = "LOCATION_URI";

    if (retVal == null) {
      System.err.println("Encountered error while executing updateMDatabaseURI - " +
          "commit of JDO transaction failed. Failed to update FSRoot locations in " +
          fieldName + "field in " + tblName + " table.");
    } else {
      Map<String, String> updateLocations = retVal.getUpdateLocations();
      if (isDryRun) {
        System.out.println("Dry Run of updateLocation on table " + tblName + "..");
      } else {
        System.out.println("Successfully updated the following locations..");
      }
      int count = printUpdateLocations(updateLocations);
      if (isDryRun) {
        System.out.println("Found " + count + " records in " + tblName + " table to update");
      } else {
        System.out.println("Updated " + count + " records in " + tblName + " table");
      }
      List<String> badRecords = retVal.getBadRecords();
      if (badRecords.size() > 0) {
        System.err.println("Warning: Found records with bad " + fieldName +  " in " +
          tblName + " table.. ");
        for (String badRecord:badRecords) {
          System.err.println("bad location URI: " + badRecord);
        }
      }
    }
  }

  private void printPropURIUpdateSummary(ObjectStore.UpdatePropURIRetVal retVal, String
      tablePropKey, boolean isDryRun, String tblName, String methodName) {
    if (retVal == null) {
      System.err.println("Encountered error while executing " + methodName + " - " +
          "commit of JDO transaction failed. Failed to update FSRoot locations in " +
          "value field corresponding to" + tablePropKey + " in " + tblName + " table.");
    } else {
      Map<String, String> updateLocations = retVal.getUpdateLocations();
      if (isDryRun) {
        System.out.println("Dry Run of updateLocation on table " + tblName + "..");
      } else {
        System.out.println("Successfully updated the following locations..");
      }
      int count = printUpdateLocations(updateLocations);
      if (isDryRun) {
        System.out.println("Found " + count + " records in " + tblName + " table to update");
      } else {
        System.out.println("Updated " + count + " records in " + tblName + " table");
      }
      List<String> badRecords = retVal.getBadRecords();
      if (badRecords.size() > 0) {
        System.err.println("Warning: Found records with bad " + tablePropKey +  " key in " +
            tblName + " table.. ");
        for (String badRecord:badRecords) {
          System.err.println("bad location URI: " + badRecord);
        }
      }
    }
  }

  private void printSerdePropURIUpdateSummary(ObjectStore.UpdateSerdeURIRetVal retVal,
    String serdePropKey, boolean isDryRun) {
    String tblName = "SERDE_PARAMS";

    if (retVal == null) {
      System.err.println("Encountered error while executing updateSerdeURI - " +
        "commit of JDO transaction failed. Failed to update FSRoot locations in " +
        "value field corresponding to " + serdePropKey + " in " + tblName + " table.");
    } else {
      Map<String, String> updateLocations = retVal.getUpdateLocations();
      if (isDryRun) {
        System.out.println("Dry Run of updateLocation on table " + tblName + "..");
      } else {
        System.out.println("Successfully updated the following locations..");
      }
      int count = printUpdateLocations(updateLocations);
      if (isDryRun) {
        System.out.println("Found " + count + " records in " + tblName + " table to update");
      } else {
        System.out.println("Updated " + count + " records in " + tblName + " table");
      }
      List<String> badRecords = retVal.getBadRecords();
      if (badRecords.size() > 0) {
        System.err.println("Warning: Found records with bad " + serdePropKey +  " key in " +
        tblName + " table.. ");
        for (String badRecord:badRecords) {
          System.err.println("bad location URI: " + badRecord);
        }
      }
    }
  }

  public void updateFSRootLocation(URI oldURI, URI newURI, String serdePropKey, String
      tablePropKey, boolean isDryRun) {
    Configuration conf = MetastoreConf.newMetastoreConf();
    initObjectStore(conf);

    System.out.println("Looking for LOCATION_URI field in DBS table to update..");
    ObjectStore.UpdateMDatabaseURIRetVal updateMDBURIRetVal = objStore.updateMDatabaseURI(oldURI,
                                                                 newURI, isDryRun);
    printDatabaseURIUpdateSummary(updateMDBURIRetVal, isDryRun);

    System.out.println("Looking for LOCATION field in SDS table to update..");
    ObjectStore.UpdateMStorageDescriptorTblURIRetVal updateTblURIRetVal =
                         objStore.updateMStorageDescriptorTblURI(oldURI, newURI, isDryRun);
    printTblURIUpdateSummary(updateTblURIRetVal, isDryRun);

    if (tablePropKey != null) {
      System.out.println("Looking for value of " + tablePropKey + " key in TABLE_PARAMS table " +
          "to update..");
      ObjectStore.UpdatePropURIRetVal updateTblPropURIRetVal =
          objStore.updateTblPropURI(oldURI, newURI,
              tablePropKey, isDryRun);
      printPropURIUpdateSummary(updateTblPropURIRetVal, tablePropKey, isDryRun, "TABLE_PARAMS",
          "updateTblPropURI");

      System.out.println("Looking for value of " + tablePropKey + " key in SD_PARAMS table " +
        "to update..");
      ObjectStore.UpdatePropURIRetVal updatePropURIRetVal = objStore
          .updateMStorageDescriptorTblPropURI(oldURI, newURI, tablePropKey, isDryRun);
      printPropURIUpdateSummary(updatePropURIRetVal, tablePropKey, isDryRun, "SD_PARAMS",
          "updateMStorageDescriptorTblPropURI");
    }

    if (serdePropKey != null) {
      System.out.println("Looking for value of " + serdePropKey + " key in SERDE_PARAMS table " +
        "to update..");
      ObjectStore.UpdateSerdeURIRetVal updateSerdeURIretVal = objStore.updateSerdeURI(oldURI,
                                                                newURI, serdePropKey, isDryRun);
      printSerdePropURIUpdateSummary(updateSerdeURIretVal, serdePropKey, isDryRun);
    }
  }
  private void prepareAcidUpgrade(String scriptLocation) {
    try {
      prepareAcidUpgradeInternal(scriptLocation);
    }
    catch(TException|IOException ex) {
      System.err.println(StringUtils.stringifyException(ex));
      printAndExit(this);
    }
  }
  private static class CompactionMetaInfo {
    /**
     * total number of bytes to be compacted across all compaction commands
     */
    long numberOfBytes;
  }
  /**
   * todo: make sure compaction queue is configured and has ample capacity
   * todo: what to do on failure?  Suppose some table/part is not readable.  should it produce
   * todo: should probably suppor dryRun mode where we output scripts but instead of renaming files
   *  we generate a renaming script.  Alternatively, always generate a renaming script and have
   *  user execute it - this is probably a better option.  If script is not empty on rerun someone
   *  added files to table to be made Acid.
   * commands for all other tables?
   * todo: how do we test this?  even if we had 2.x data it won't be readable in 3.0.  even w/o any
   * updates, txnids in the data won't make sense in 3.0 w/o actually performing equivalent of
   * metastore upgrade to init writeid table.  Also, can we even create a new table and set location
   * to existing files to simulate a 2.x table?
   * todo: generate some instructions in compaction script to include tottal compactions to perform,
   * total data volume to handle and anything else that may help users guess at how long it will
   * take.  Also, highlight tuning options to speed this up.
   * todo: can we make the script blocking so that it waits for cleaner to delete files?
   * need to poll SHOW COMPACTIONS and make sure that all partitions are in "finished" state
   * todo: this should accept a file of table names to exclude from non-acid to acid conversion
   * todo: change script comments to a preamble instead of a footer
   *
   * @throws MetaException
   * @throws TException
   */
  private void prepareAcidUpgradeInternal(String scriptLocation) throws MetaException, TException, IOException {
    Configuration conf = MetastoreConf.newMetastoreConf();
    System.out.println("Looking for Acid tables that need to be compacted");
    //todo: check if acid is enabled and bail if not
    //todo: check that running on 2.x?
    HiveMetaStoreClient hms = new HiveMetaStoreClient(conf);//MetaException
    List<String> databases = hms.getAllDatabases();//TException
    System.out.println("Found " + databases.size() + " databases to process");
    List<String> compactions = new ArrayList<>();
    List<String> convertToAcid = new ArrayList<>();
    List<String> convertToMM = new ArrayList<>();
    final CompactionMetaInfo compactionMetaInfo = new CompactionMetaInfo();
    for(String dbName : databases) {
      List<String> tables = hms.getAllTables(dbName);
      System.out.println("found " + tables.size() + " tables in " + dbName);
      for(String tableName : tables) {
        Table t = hms.getTable(dbName, tableName);

        //ql depends on metastore and is not accessible here...  and if it was, I would not be using
        //2.6 exec jar, but 3.0.... which is not what we want
        List<String> compactionCommands = getCompactionCommands(t, conf, hms, compactionMetaInfo);
        compactions.addAll(compactionCommands);
        processConversion(t, convertToAcid, convertToMM, hms);
          /*todo: handle renaming files somewhere
           * */
      }
    }
    makeCompactionScript(compactions, scriptLocation, compactionMetaInfo);
    makeConvertTableScript(convertToAcid, convertToMM, scriptLocation);
    makeRenameFileScript(scriptLocation);
  }
  //todo: handle exclusion list
  private static void processConversion(Table t, List<String> convertToAcid,
      List<String> convertToMM, HiveMetaStoreClient hms) throws TException {
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
      }
      else {
        convertToMM.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
            "'transactional'='true', 'transactional_properties'='insert_only')");
      }
    }
    else {
      List<String> partNames = hms.listPartitionNames(t.getDbName(), t.getTableName(), (short)-1);
      int batchSize = 10000;//todo: right size?
      int numWholeBatches = partNames.size()/batchSize;
      for(int i = 0; i < numWholeBatches; i++) {
        List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
            partNames.subList(i * batchSize, (i + 1) * batchSize));
        for(Partition p : partitionList) {
          if(!canBeMadeAcid(fullTableName, p.getSd())) {
            convertToMM.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
                "'transactional'='true', 'transactional_properties'='insert_only')");
            return;
          }
        }
      }
      if(numWholeBatches * batchSize < partNames.size()) {
        //last partial batch
        List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(),
            partNames.subList(numWholeBatches * batchSize, partNames.size()));
        for (Partition p : partitionList) {
          if (!canBeMadeAcid(fullTableName, p.getSd())) {
            convertToMM.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
                "'transactional'='true', 'transactional_properties'='insert_only')");
            return;
          }
        }
      }
      //if here checked all parts and they are Acid compatible - make it acid
      convertToAcid.add("ALTER TABLE " + Warehouse.getQualifiedName(t) + " SET TBLPROPERTIES (" +
          "'transactional'='true')");
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
      System.err.println("Could not determine if " + fullTableName +
          " can be made Acid due to: " + e.getMessage());
      return false;
    }
    return false;
  }
  /**
   * currently writes to current dir (whatever that is).
   * If there is nothing to compact, outputs empty file so as not to confuse the output with a
   * failed run.
   * todo: add some config to tell it where to put the script
   */
  private static void makeCompactionScript(List<String> commands, String scriptLocation,
      CompactionMetaInfo compactionMetaInfo) throws IOException {
    if (commands.isEmpty()) {
      System.out.println("No compaction is necessary");
      return;
    }
    String fileName = "compacts_" + System.currentTimeMillis() + ".sql";
    System.out.println("Writing compaction commands to " + fileName);
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
      System.out.println("No acid conversion is necessary");
    } else {
      String fileName = "convertToAcid_" + System.currentTimeMillis() + ".sql";
      System.out.println("Writing acid conversion commands to " + fileName);
      try(PrintWriter pw = createScript(alterTableAcid, fileName, scriptLocation)) {
        pw.println("-- These commands may be executed by Hive 1.x later");
      }
    }
    
    if (alterTableMm.isEmpty()) {
      System.out.println("No managed table conversion is necessary");
    } else {
      String fileName = "convertToMM_" + System.currentTimeMillis() + ".sql";
      System.out.println("Writing managed table conversion commands to " + fileName);
      try(PrintWriter pw = createScript(alterTableMm, fileName, scriptLocation)) {
        pw.println("-- These commands must be executed by Hive 3.0 or later");
      }
    }
  }

  private static PrintWriter createScript(List<String> commands, String fileName,
      String scriptLocation) throws IOException {
    //todo: make sure to create the file in 'scriptLocation' dir
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
      System.out.println("No file renaming is necessary");
    } else {
      String fileName = "normalizeFileNames_" + System.currentTimeMillis() + ".sh";
      System.out.println("Writing file renaming commands to " + fileName);
      PrintWriter pw = createScript(commands, fileName, scriptLocation);
      pw.close();
    }
  }
  /**
   * @return any compaction commands to run for {@code Table t}
   */
  private static List<String> getCompactionCommands(Table t, Configuration conf,
      HiveMetaStoreClient hms, CompactionMetaInfo compactionMetaInfo)
      throws IOException, TException {
    if(!isFullAcidTable(t)) {
      return Collections.emptyList();
    }
    if(t.getPartitionKeysSize() <= 0) {
      //not partitioned
      if(!needsCompaction(new Path(t.getSd().getLocation()), conf, compactionMetaInfo)) {
        return Collections.emptyList();
      }

      List<String> cmds = new ArrayList<>();
      cmds.add(getCompactionCommand(t, null));
      return cmds;
    }
    List<String> partNames = hms.listPartitionNames(t.getDbName(), t.getTableName(), (short)-1);
    int batchSize = 10000;//todo: right size?
    int numWholeBatches = partNames.size()/batchSize;
    List<String> compactionCommands = new ArrayList<>();
    for(int i = 0; i < numWholeBatches; i++) {
      List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(), partNames.subList(i * batchSize, (i + 1) * batchSize));
      for(Partition p : partitionList) {
        if(needsCompaction(new Path(p.getSd().getLocation()), conf, compactionMetaInfo)) {
          compactionCommands.add(getCompactionCommand(t, p));
        }
      }
    }
    if(numWholeBatches * batchSize < partNames.size()) {
      //last partial batch
      List<Partition> partitionList = hms.getPartitionsByNames(t.getDbName(), t.getTableName(), partNames.subList(numWholeBatches * batchSize, partNames.size()));
      for (Partition p : partitionList) {
        if (needsCompaction(new Path(p.getSd().getLocation()), conf, compactionMetaInfo)) {
          compactionCommands.add(getCompactionCommand(t, p));
        }
      }
    }
    return compactionCommands;
  }
  /**
   *
   * @param location - path to a partition (or table if not partitioned) dir
   */
  private static boolean needsCompaction(Path location, Configuration conf,
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
          return true;
        }
      }
    }
    return false;
  }

  /**
   * @param location - path to a partition (or table if not partitioned) dir
   * @throws IOException
   */
  private static long getDataSize(Path location, Configuration conf) throws IOException {
    /*
     * todo: Figure out the size of the partition.  The
     * best way is to getAcidState() and look at each file - this way it takes care of
     * original files vs base and any other obsolete files.  For now just brute force it,
      * it's likely close enough for a rough estimate.*/
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
        //todo: should these be quoted?  HiveUtils.unparseIdentifier() - if value is String should definitely quote
        sb.append(t.getPartitionKeys().get(i).getName()).append('=')
            .append(p.getValues().get(i)).append(",");
      }
      sb.setCharAt(sb.length() - 1, ')');//replace trailing ','
    }
    return sb.append(" COMPACT 'major'").toString();
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
  private static void printAndExit(HiveMetaTool metaTool) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("metatool", metaTool.cmdLineOptions);
    System.exit(1);
  }

  public static void main(String[] args) {
    HiveMetaTool metaTool = new HiveMetaTool();
    metaTool.init();
    CommandLineParser parser = new GnuParser();
    CommandLine line = null;

    try {
      try {
        line = parser.parse(metaTool.cmdLineOptions, args);
      } catch (ParseException e) {
        System.err.println("HiveMetaTool:Parsing failed.  Reason: " + e.getLocalizedMessage());
        printAndExit(metaTool);
      }

      if (line.hasOption("help")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("metatool", metaTool.cmdLineOptions);
      } else if (line.hasOption("listFSRoot")) {
        if (line.hasOption("dryRun")) {
          System.err.println("HiveMetaTool: dryRun is not valid with listFSRoot");
          printAndExit(metaTool);
        } else if (line.hasOption("serdePropKey")) {
          System.err.println("HiveMetaTool: serdePropKey is not valid with listFSRoot");
          printAndExit(metaTool);
        } else if (line.hasOption("tablePropKey")) {
          System.err.println("HiveMetaTool: tablePropKey is not valid with listFSRoot");
          printAndExit(metaTool);
        }
        metaTool.listFSRoot();
      } else if (line.hasOption("executeJDOQL")) {
        String query = line.getOptionValue("executeJDOQL");
        if (line.hasOption("dryRun")) {
          System.err.println("HiveMetaTool: dryRun is not valid with executeJDOQL");
          printAndExit(metaTool);
        } else if (line.hasOption("serdePropKey")) {
          System.err.println("HiveMetaTool: serdePropKey is not valid with executeJDOQL");
          printAndExit(metaTool);
        } else if (line.hasOption("tablePropKey")) {
          System.err.println("HiveMetaTool: tablePropKey is not valid with executeJDOQL");
          printAndExit(metaTool);
        }
        if (query.toLowerCase().trim().startsWith("select")) {
          metaTool.executeJDOQLSelect(query);
        } else if (query.toLowerCase().trim().startsWith("update")) {
          metaTool.executeJDOQLUpdate(query);
        } else {
          System.err.println("HiveMetaTool:Unsupported statement type");
          printAndExit(metaTool);
        }
      } else if (line.hasOption("updateLocation")) {
        String[] loc = line.getOptionValues("updateLocation");
        boolean isDryRun = false;
        String serdepropKey = null;
        String tablePropKey = null;

        if (loc.length != 2 && loc.length != 3) {
          System.err.println("HiveMetaTool:updateLocation takes in 2 required and 1 " +
              "optional arguments but " +
              "was passed " + loc.length + " arguments");
          printAndExit(metaTool);
        }

        Path newPath = new Path(loc[0]);
        Path oldPath = new Path(loc[1]);

        URI oldURI = oldPath.toUri();
        URI newURI = newPath.toUri();

        if (line.hasOption("dryRun")) {
          isDryRun = true;
        }

        if (line.hasOption("serdePropKey")) {
          serdepropKey = line.getOptionValue("serdePropKey");
        }

        if (line.hasOption("tablePropKey")) {
          tablePropKey = line.getOptionValue("tablePropKey");
        }

        /*
         * validate input - Both new and old URI should contain valid host names and valid schemes.
         * port is optional in both the URIs since HDFS HA NN URI doesn't have a port.
         */
          if (oldURI.getHost() == null || newURI.getHost() == null) {
            System.err.println("HiveMetaTool:A valid host is required in both old-loc and new-loc");
          } else if (oldURI.getScheme() == null || newURI.getScheme() == null) {
            System.err.println("HiveMetaTool:A valid scheme is required in both old-loc and new-loc");
          } else {
            metaTool.updateFSRootLocation(oldURI, newURI, serdepropKey, tablePropKey, isDryRun);
          }
      } else if(line.hasOption("prepareAcidUpgrade")) {
        String[] values = line.getOptionValues("prepareAcidUpgrade");
        String targetDir = ".";
        if(values != null && values.length > 0) {
          if(values.length > 1) {
            System.err.println("HiveMetaTool: prepareAcidUpgrade");
            printAndExit(metaTool);
          } else {
            targetDir = values[0];
          }
        }
        metaTool.prepareAcidUpgrade(targetDir);
      } else {
          if (line.hasOption("dryRun")) {
            System.err.println("HiveMetaTool: dryRun is not a valid standalone option");
          } else if (line.hasOption("serdePropKey")) {
            System.err.println("HiveMetaTool: serdePropKey is not a valid standalone option");
          } else if (line.hasOption("tablePropKey")) {
            System.err.println("HiveMetaTool: tablePropKey is not a valid standalone option");
            printAndExit(metaTool);
          } else {
            System.err.print("HiveMetaTool:Parsing failed.  Reason: Invalid arguments: " );
            for (String s : line.getArgs()) {
              System.err.print(s + " ");
            }
            System.err.println();
          }
          printAndExit(metaTool);
        }
      } finally {
        metaTool.shutdownObjectStore();
      }
   }
}