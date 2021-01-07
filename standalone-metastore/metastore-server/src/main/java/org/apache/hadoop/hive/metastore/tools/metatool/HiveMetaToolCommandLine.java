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

package org.apache.hadoop.hive.metastore.tools.metatool;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.TerminalFactory;

class HiveMetaToolCommandLine {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveMetaToolCommandLine.class.getName());

  @SuppressWarnings("static-access")
  private static final Option LIST_FS_ROOT = OptionBuilder
      .withDescription("print the current FS root locations")
      .create("listFSRoot");

  @SuppressWarnings("static-access")
  private static final Option EXECUTE_JDOQL = OptionBuilder
      .withArgName("query-string")
      .hasArgs()
      .withDescription("execute the given JDOQL query")
      .create("executeJDOQL");

  @SuppressWarnings("static-access")
  private static final Option UPDATE_LOCATION = OptionBuilder
      .withArgName("new-loc> " + "<old-loc")
      .hasArgs(2)
      .withDescription("Update FS root location in the metastore to new location.Both new-loc and old-loc should " +
          "be valid URIs with valid host names and schemes. When run with the dryRun option changes are displayed " +
          "but are not persisted. When run with the serdepropKey/tablePropKey option updateLocation looks for the " +
          "serde-prop-key/table-prop-key that is specified and updates its value if found.")
      .create("updateLocation");

  @SuppressWarnings("static-access")
  private static final Option LIST_EXT_TBL_LOCS = OptionBuilder
          .withArgName("dbName> " + " <output-loc")
          .hasArgs(2)
          .withDescription("Generates a file containing a list of directories which cover external table data locations " +
                  "for the specified database. A database name or pattern must be specified, on which the tool will be run." +
                  "The output is generated at the specified location."
                  )
          .create("listExtTblLocs");

  @SuppressWarnings("static-access")
  private static final Option DIFF_EXT_TBL_LOCS = OptionBuilder
          .withArgName("file1> " + " <file2> " + "<output-loc")
          .hasArgs(3)
          .withDescription("Generates the difference between two output-files created using -listExtTblLocs option at the" +
                  " specified location. Output contains locations(keys) unique to each input file. For keys common to both " +
                  "input-files, those entities are listed which are deleted from the first file and introduced in the second."
          )
          .create("diffExtTblLocs");

  private static final Option DRY_RUN = OptionBuilder
      .withDescription("Perform a dry run of updateLocation changes.When run with the dryRun option updateLocation " +
          "changes are displayed but not persisted. dryRun is valid only with the updateLocation option.")
      .create("dryRun");

  @SuppressWarnings("static-access")
  private static final Option SERDE_PROP_KEY = OptionBuilder
      .withArgName("serde-prop-key")
      .hasArgs()
      .withValueSeparator()
      .withDescription("Specify the key for serde property to be updated. serdePropKey option is valid only with " +
          "updateLocation option.")
      .create("serdePropKey");

  @SuppressWarnings("static-access")
  private static final Option TABLE_PROP_KEY = OptionBuilder
      .withArgName("table-prop-key")
      .hasArg()
      .withValueSeparator()
      .withDescription("Specify the key for table property to be updated. tablePropKey option is valid only with " +
          "updateLocation option.")
      .create("tablePropKey");

  @SuppressWarnings("static-access")
  private static final Option HELP = OptionBuilder
      .withLongOpt("help")
      .withDescription("Print help information")
      .withArgName("help")
      .create('h');

  private static final Options OPTIONS = new Options();
  static {
    OPTIONS.addOption(LIST_FS_ROOT);
    OPTIONS.addOption(EXECUTE_JDOQL);
    OPTIONS.addOption(UPDATE_LOCATION);
    OPTIONS.addOption(LIST_EXT_TBL_LOCS);
    OPTIONS.addOption(DIFF_EXT_TBL_LOCS);
    OPTIONS.addOption(DRY_RUN);
    OPTIONS.addOption(SERDE_PROP_KEY);
    OPTIONS.addOption(TABLE_PROP_KEY);
    OPTIONS.addOption(HELP);
  }

  private boolean listFSRoot;
  private String jdoqlQuery;
  private String[] updateLocationParams;
  private String[] listExtTblLocsParams;
  private String[] diffExtTblLocsParams;
  private boolean dryRun;
  private String serdePropKey;
  private String tablePropKey;
  private boolean help;

  public static HiveMetaToolCommandLine parseArguments(String[] args) {
    HiveMetaToolCommandLine cl = null;
    try {
      cl = new HiveMetaToolCommandLine(args);
    } catch (Exception e) {
      LOGGER.error("Parsing the command line arguments failed", e);
      printUsage();
      System.exit(1);
    }

    if (cl.isHelp()) {
      printUsage();
      System.exit(0);
    }

    return cl;
  }

  HiveMetaToolCommandLine(String[] args) throws ParseException {
    LOGGER.info("Hive Meta Tool invoked with arguments = {}", Arrays.toString(args));
    parseCommandLine(args);
    printArguments();
  }

  private void parseCommandLine(String[] args) throws ParseException {
    CommandLine cl = new GnuParser().parse(OPTIONS, args);

    listFSRoot = cl.hasOption(LIST_FS_ROOT.getOpt());
    jdoqlQuery = cl.getOptionValue(EXECUTE_JDOQL.getOpt());
    updateLocationParams = cl.getOptionValues(UPDATE_LOCATION.getOpt());
    listExtTblLocsParams = cl.getOptionValues(LIST_EXT_TBL_LOCS.getOpt());
    diffExtTblLocsParams = cl.getOptionValues(DIFF_EXT_TBL_LOCS.getOpt());
    dryRun = cl.hasOption(DRY_RUN.getOpt());
    serdePropKey = cl.getOptionValue(SERDE_PROP_KEY.getOpt());
    tablePropKey = cl.getOptionValue(TABLE_PROP_KEY.getOpt());
    help = cl.hasOption(HELP.getOpt());

    int commandCount = (isListFSRoot() ? 1 : 0) + (isExecuteJDOQL() ? 1 : 0) + (isUpdateLocation() ? 1 : 0) +
          (isListExtTblLocs() ? 1 : 0) + (isDiffExtTblLocs() ? 1 : 0);
    if (commandCount != 1) {
      throw new IllegalArgumentException("exactly one of -listFSRoot, -executeJDOQL, -updateLocation, " +
              "-listExtTblLocs, -diffExtTblLocs must be set");
    }

    if (updateLocationParams != null && updateLocationParams.length != 2) {
      throw new IllegalArgumentException("HiveMetaTool:updateLocation takes in 2 arguments but was passed " +
          updateLocationParams.length + " arguments");
    }

    if (listExtTblLocsParams != null && listExtTblLocsParams.length != 2) {
      throw new IllegalArgumentException("HiveMetaTool:listExtTblLocs takes in 2 arguments but was passed " +
              listExtTblLocsParams.length + " arguments");
    }

    if (diffExtTblLocsParams != null && diffExtTblLocsParams.length != 3) {
      throw new IllegalArgumentException("HiveMetaTool:diffExtTblLocs takes in 3 arguments but was passed " +
              diffExtTblLocsParams.length + " arguments");
    }

    if ((dryRun || serdePropKey != null || tablePropKey != null) && !isUpdateLocation()) {
      throw new IllegalArgumentException("-dryRun, -serdePropKey, -tablePropKey may be used only for the " +
          "-updateLocation command");
    }
  }

  private static void printUsage() {
    HelpFormatter hf = new HelpFormatter();
    try {
      int width = hf.getWidth();
      int jlineWidth = TerminalFactory.get().getWidth();
      width = Math.min(160, Math.max(jlineWidth, width));
      hf.setWidth(width);
    } catch (Throwable t) { // Ignore
    }

    hf.printHelp("metatool", OPTIONS);
  }

  private void printArguments() {
    LOGGER.info("Hive Meta Tool is running with the following parsed arguments: \n" +
        "\tlistFSRoot    : " + listFSRoot + "\n" +
        "\tjdoqlQuery    : " + jdoqlQuery + "\n" +
        "\tupdateLocation: " + Arrays.toString(updateLocationParams) + "\n" +
        "\tlistExtTblLocs: " + Arrays.toString(listExtTblLocsParams) + "\n" +
        "\tdiffExtTblLocs: " + Arrays.toString(diffExtTblLocsParams) + "\n" +
        "\tdryRun        : " + dryRun + "\n" +
        "\tserdePropKey  : " + serdePropKey + "\n" +
        "\ttablePropKey  : " + tablePropKey);
  }

  boolean isListFSRoot() {
    return listFSRoot;
  }

  boolean isExecuteJDOQL() {
    return jdoqlQuery != null;
  }

  String getJDOQLQuery() {
    return jdoqlQuery;
  }

  boolean isUpdateLocation() {
    return updateLocationParams != null;
  }

  boolean isListExtTblLocs() {
    return listExtTblLocsParams != null;
  }

  boolean isDiffExtTblLocs() {
    return diffExtTblLocsParams != null;
  }

  String[] getUpddateLocationParams() {
    return updateLocationParams;
  }

  String[] getListExtTblLocsParams() {
    return listExtTblLocsParams;
  }

  String[] getDiffExtTblLocsParams() {
    return diffExtTblLocsParams;
  }

  boolean isDryRun() {
    return dryRun;
  }

  String getSerdePropKey() {
    return serdePropKey;
  }

  String getTablePropKey() {
    return tablePropKey;
  }

  private boolean isHelp() {
    return help;
  }
}
