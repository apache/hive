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

package org.apache.hadoop.hive.metastore.tools;

import java.net.URI;
import java.util.Collection;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;

/**
 * This class provides Hive admins a tool to
 * - execute JDOQL against the metastore using DataNucleus
 * - perform HA name node upgrade
 */

public class HiveMetaTool {

  private static final Log LOG = LogFactory.getLog(HiveMetaTool.class.getName());
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

    cmdLineOptions.addOption(help);
    cmdLineOptions.addOption(listFSRoot);
    cmdLineOptions.addOption(executeJDOQL);
    cmdLineOptions.addOption(updateFSRootLoc);
    cmdLineOptions.addOption(dryRun);
    cmdLineOptions.addOption(serdePropKey);
    cmdLineOptions.addOption(tablePropKey);
  }

  private void initObjectStore(HiveConf hiveConf) {
    if (!isObjStoreInitialized) {
      objStore = new ObjectStore();
      objStore.setConf(hiveConf);
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
    HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
    initObjectStore(hiveConf);

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
    HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
    initObjectStore(hiveConf);

    System.out.println("Executing query: " + query);
    Collection<?> result = objStore.executeJDOQLSelect(query);
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

  private long executeJDOQLUpdate(String query) {
    HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
    initObjectStore(hiveConf);

    System.out.println("Executing query: " + query);
    long numUpdated = objStore.executeJDOQLUpdate(query);
    if (numUpdated >= 0) {
      System.out.println("Number of records updated: " + numUpdated);
    } else {
      System.err.println("Encountered error during executeJDOQL -" +
        "commit of JDO transaction failed.");
    }
    return numUpdated;
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
    String tblName = new String("SDS");
    String fieldName = new String("LOCATION");

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
    }
  }

  private void printDatabaseURIUpdateSummary(ObjectStore.UpdateMDatabaseURIRetVal retVal,
    boolean isDryRun) {
    String tblName = new String("DBS");
    String fieldName = new String("LOCATION_URI");

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
    String tblName = new String("SERDE_PARAMS");

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
    HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
    initObjectStore(hiveConf);

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