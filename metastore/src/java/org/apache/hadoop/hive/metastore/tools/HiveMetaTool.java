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
import java.util.HashMap;
import java.util.Iterator;
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


  public HiveMetaTool() {
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
            .hasArgs(3)
            .withDescription(
                "update FS root location in the metastore to new location. Both new-loc and" +
                    " old-loc should be valid URIs with valid host names and schemes. " +
                    "when run with the dryRun option changes are displayed but are not persisted.")
            .create("updateLocation");
    Option dryRun = new Option("dryRun" , "dryRun is valid only with updateLocation option. when " +
      "run with the dryRun option updateLocation changes are displayed but are not persisted.");

    cmdLineOptions.addOption(help);
    cmdLineOptions.addOption(listFSRoot);
    cmdLineOptions.addOption(executeJDOQL);
    cmdLineOptions.addOption(updateFSRootLoc);
    cmdLineOptions.addOption(dryRun);

  }

  private void initObjectStore(HiveConf hiveConf) {
    objStore = new ObjectStore();
    objStore.setConf(hiveConf);
  }

  private void shutdownObjectStore() {
    if (objStore != null) {
      objStore.shutdown();
    }
  }

  private void listFSRoot() {
    Set<String> hdfsRoots = objStore.listFSRoots();
    if (hdfsRoots != null) {
      System.out.println("HiveMetaTool:Listing FS Roots..");
      for (String s : hdfsRoots) {
        System.out.println(s);
      }
    } else {
      System.err.println("HiveMetaTool:Encountered error during listFSRoot - " +
        "commit of JDO transaction failed");
    }
  }

  private void executeJDOQLSelect(String query) {
    Collection<?> result = objStore.executeJDOQLSelect(query);
    if (result != null) {
      Iterator<?> iter = result.iterator();
      while (iter.hasNext()) {
        Object o = iter.next();
        System.out.println(o.toString());
      }
    } else {
      System.err.println("HiveMetaTool:Encountered error during executeJDOQLSelect -" +
        "commit of JDO transaction failed.");
    }
  }

  private void executeJDOQLUpdate(String query) {
    long numUpdated = objStore.executeJDOQLUpdate(query);
    if (numUpdated >= 0) {
      System.out.println("HiveMetaTool:Number of records updated: " + numUpdated);
    } else {
      System.err.println("HiveMetaTool:Encountered error during executeJDOQL -" +
        "commit of JDO transaction failed.");
    }
  }

  private void printUpdateLocations(HashMap<String, String> updateLocations) {
    for (String key: updateLocations.keySet()) {
      String value = updateLocations.get(key);
      System.out.println("current location: " + key + " new location: " + value);
    }
  }

  private void updateFSRootLocation(URI oldURI, URI newURI, boolean dryRun) {
    HashMap<String, String> updateLocations = new HashMap<String, String>();
    int count = objStore.updateFSRootLocation(oldURI, newURI, updateLocations, dryRun);
    if (count == -1) {
      System.err.println("HiveMetaTool:Encountered error while executing updateFSRootLocation - " +
        "commit of JDO transaction failed, failed to update FS Root locations.");
    } else {
      if (!dryRun) {
        System.out.println("HiveMetaTool: Successfully updated " + count + "FS Root locations");
      } else {
        printUpdateLocations(updateLocations);
      }
    }
  }

  public static void main(String[] args) {

    HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
    HiveMetaTool metaTool = new HiveMetaTool();
    metaTool.init();
    CommandLineParser parser = new GnuParser();
    CommandLine line = null;

    try {
      try {
        line = parser.parse(metaTool.cmdLineOptions, args);
      } catch (ParseException e) {
        System.err.println("HiveMetaTool:Parsing failed.  Reason: " + e.getLocalizedMessage());
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("metatool", metaTool.cmdLineOptions);
        System.exit(1);
      }

      if (line.hasOption("help")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("metatool", metaTool.cmdLineOptions);
      } else if (line.hasOption("listFSRoot")) {
        metaTool.initObjectStore(hiveConf);
        metaTool.listFSRoot();
      } else if (line.hasOption("executeJDOQL")) {
        String query = line.getOptionValue("executeJDOQL");
        metaTool.initObjectStore(hiveConf);
        if (query.toLowerCase().trim().startsWith("select")) {
          metaTool.executeJDOQLSelect(query);
        } else if (query.toLowerCase().trim().startsWith("update")) {
          metaTool.executeJDOQLUpdate(query);
        } else {
          System.err.println("HiveMetaTool:Unsupported statement type");
        }
      } else if (line.hasOption("updateLocation")) {
        String[] loc = line.getOptionValues("updateLocation");
        boolean dryRun = false;

        if (loc.length != 2 && loc.length != 3) {
          System.err.println("HiveMetaTool:updateLocation takes in 2 required and 1 " +
              "optional arguements but " +
              "was passed " + loc.length + " arguements");
        }

        Path newPath = new Path(loc[0]);
        Path oldPath = new Path(loc[1]);

        URI oldURI = oldPath.toUri();
        URI newURI = newPath.toUri();

        if (line.hasOption("dryRun")) {
          dryRun = true;
        }

        /*
         * validate input - if the old uri contains a valid port, the new uri
         * should contain a valid port as well. Both new and old uri should
         * contain valid host names and valid schemes.
         */
          if (oldURI.getHost() == null || newURI.getHost() == null) {
            System.err.println("HiveMetaTool:A valid host is required in both old-loc and new-loc");
          } else if (oldURI.getPort() > 0 && newURI.getPort() < 0) {
            System.err.println("HiveMetaTool:old-loc has a valid port, new-loc should " +
                "also contain a valid port");
          } else if (oldURI.getScheme() == null || newURI.getScheme() == null) {
            System.err.println("HiveMetaTool:A valid scheme is required in both old-loc and new-loc");
          } else {
            metaTool.initObjectStore(hiveConf);
            metaTool.updateFSRootLocation(oldURI, newURI, dryRun);
          }
        } else {
          System.err.print("HiveMetaTool:Invalid option:" + line.getOptions());
          for (String s : line.getArgs()) {
            System.err.print(s + " ");
          }
          System.err.println();
          HelpFormatter formatter = new HelpFormatter();
          formatter.printHelp("metatool", metaTool.cmdLineOptions);
        }
      } finally {
        metaTool.shutdownObjectStore();
      }
   }
}
