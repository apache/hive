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
package org.apache.hadoop.hive.metastore.tools.schematool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class SchemaToolCommandLine {
  private final Options cmdLineOptions;

  @SuppressWarnings("static-access")
  private Options createOptions(OptionGroup additionalOptions) {
    Option help = new Option("help", "print this message");
    Option infoOpt = new Option("info", "Show config and schema details");
    Option upgradeOpt = new Option("upgradeSchema", "Schema upgrade");
    Option upgradeFromOpt = OptionBuilder.withArgName("upgradeFrom").hasArg()
        .withDescription("Schema upgrade from a version")
        .create("upgradeSchemaFrom");
    Option initOpt = new Option("initSchema", "Schema initialization");
    Option initToOpt = OptionBuilder.withArgName("initTo").hasArg()
        .withDescription("Schema initialization to a version")
        .create("initSchemaTo");
    Option initOrUpgradeSchemaOpt = new Option("initOrUpgradeSchema", "Initialize or upgrade schema to latest version");
    Option dropDbOpt = new Option("dropAllDatabases", "Drop all Hive databases (with CASCADE). " +
            "This will remove all managed data!");
    Option yesOpt = new Option("yes", "Don't ask for confirmation when using -dropAllDatabases.");
    Option validateOpt = new Option("validate", "Validate the database");
    Option createCatalog = OptionBuilder
        .hasArg()
        .withDescription("Create a catalog, requires --catalogLocation parameter as well")
        .create("createCatalog");
    Option alterCatalog = OptionBuilder
        .hasArg()
        .withDescription("Alter a catalog, requires --catalogLocation and/or --catalogDescription parameter as well")
        .create("alterCatalog");
    Option mergeCatalog = OptionBuilder
        .hasArg()
        .withDescription("Merge databases from a catalog into other, Argument is the source catalog name " +
            "Requires --toCatalog to indicate the destination catalog")
        .create("mergeCatalog");
    Option moveDatabase = OptionBuilder
        .hasArg()
        .withDescription("Move a database between catalogs.  Argument is the database name. " +
            "Requires --fromCatalog and --toCatalog parameters as well")
        .create("moveDatabase");
    Option moveTable = OptionBuilder
        .hasArg()
        .withDescription("Move a table to a different database.  Argument is the table name. " +
            "Requires --fromCatalog, --toCatalog, --fromDatabase, and --toDatabase " +
            " parameters as well.")
        .create("moveTable");
    Option createUserOpt = new Option("createUser", "Create the Hive user, set hiveUser to the db" +
        " admin user and the hive password to the db admin password with this");
    Option createLogsTable = OptionBuilder
      .hasArg()
      .withDescription("Create table for Hive warehouse/compute logs")
      .create("createLogsTable");

    OptionGroup optGroup = new OptionGroup();
    optGroup
      .addOption(help)
      .addOption(infoOpt)
      .addOption(upgradeOpt)
      .addOption(upgradeFromOpt)
      .addOption(initOpt)
      .addOption(dropDbOpt)
      .addOption(initToOpt)
      .addOption(initOrUpgradeSchemaOpt)
      .addOption(validateOpt)
      .addOption(createCatalog)
      .addOption(alterCatalog)
      .addOption(mergeCatalog)
      .addOption(moveDatabase)
      .addOption(moveTable)
      .addOption(createUserOpt)
      .addOption(createLogsTable);
    optGroup.setRequired(true);

    Option userNameOpt = OptionBuilder.withArgName("user")
        .hasArgs()
        .withDescription("Override config file user name")
        .create("userName");
    Option passwdOpt = OptionBuilder.withArgName("password")
        .hasArgs()
        .withDescription("Override config file password")
        .create("passWord");
    Option dbTypeOpt = OptionBuilder.withArgName("databaseType")
        .hasArgs().withDescription("Metastore database type").isRequired()
        .create("dbType");
    Option hiveUserOpt = OptionBuilder
        .hasArg()
        .withDescription("Hive user (for use with createUser)")
        .create("hiveUser");
    Option hivePasswdOpt = OptionBuilder
        .hasArg()
        .withDescription("Hive password (for use with createUser)")
        .create("hivePassword");
    Option hiveDbOpt = OptionBuilder
        .hasArg()
        .withDescription("Hive database (for use with createUser)")
        .create("hiveDb");
    /*
    Option metaDbTypeOpt = OptionBuilder.withArgName("metaDatabaseType")
        .hasArgs().withDescription("Used only if upgrading the system catalog for hive")
        .create("metaDbType");
        */
    Option urlOpt = OptionBuilder.withArgName("url")
        .hasArgs().withDescription("connection url to the database")
        .create("url");
    Option driverOpt = OptionBuilder.withArgName("driver")
        .hasArgs().withDescription("driver name for connection")
        .create("driver");
    Option dbOpts = OptionBuilder.withArgName("databaseOpts")
        .hasArgs().withDescription("Backend DB specific options")
        .create("dbOpts");
    Option dryRunOpt = new Option("dryRun", "list SQL scripts (no execute)");
    Option verboseOpt = new Option("verbose", "only print SQL statements");
    Option serversOpt = OptionBuilder.withArgName("serverList")
        .hasArgs().withDescription("a comma-separated list of servers used in location validation in the format of " +
            "scheme://authority (e.g. hdfs://localhost:8000)")
        .create("servers");
    Option catalogLocation = OptionBuilder
        .hasArg()
        .withDescription("Location of new catalog, required when adding a catalog")
        .create("catalogLocation");
    Option catalogDescription = OptionBuilder
        .hasArg()
        .withDescription("Description of new catalog")
        .create("catalogDescription");
    Option ifNotExists = OptionBuilder
        .withDescription("If passed then it is not an error to create an existing catalog")
        .create("ifNotExists");
    Option fromCatalog = OptionBuilder
        .hasArg()
        .withDescription("Catalog a moving database or table is coming from.  This is " +
            "required if you are moving a database or table.")
        .create("fromCatalog");
    Option toCatalog = OptionBuilder
        .hasArg()
        .withDescription("Catalog a moving database or table is going to.  This is " +
            "required if you are moving a database or table.")
        .create("toCatalog");
    Option fromDatabase = OptionBuilder
        .hasArg()
        .withDescription("Database a moving table is coming from.  This is " +
            "required if you are moving a table.")
        .create("fromDatabase");
    Option toDatabase = OptionBuilder
        .hasArg()
        .withDescription("Database a moving table is going to.  This is " +
            "required if you are moving a table.")
        .create("toDatabase");
    Option retentionPeriod = OptionBuilder.hasArg()
      .withDescription("Specify logs table retention period")
      .create("retentionPeriod");

    Options options = new Options();
    options.addOption(help);
    options.addOptionGroup(optGroup);
    options.addOption(dbTypeOpt);
    //options.addOption(metaDbTypeOpt);
    options.addOption(userNameOpt);
    options.addOption(passwdOpt);
    options.addOption(urlOpt);
    options.addOption(driverOpt);
    options.addOption(dbOpts);
    options.addOption(dryRunOpt);
    options.addOption(verboseOpt);
    options.addOption(serversOpt);
    options.addOption(catalogLocation);
    options.addOption(catalogDescription);
    options.addOption(ifNotExists);
    options.addOption(fromCatalog);
    options.addOption(toCatalog);
    options.addOption(fromDatabase);
    options.addOption(toDatabase);
    options.addOption(hiveUserOpt);
    options.addOption(hivePasswdOpt);
    options.addOption(hiveDbOpt);
    options.addOption(yesOpt);
    options.addOption(retentionPeriod);
    if (additionalOptions != null) options.addOptionGroup(additionalOptions);

    return options;
  }

  private final CommandLine cl;
  private final String dbType;
  private final String metaDbType;

  public SchemaToolCommandLine(String[] args, OptionGroup additionalOptions) throws ParseException {
    cmdLineOptions = createOptions(additionalOptions);
    cl = getCommandLine(args);
    if (cl.hasOption("help")) {
      printAndExit(null);
    }

    dbType = cl.getOptionValue("dbType");
    metaDbType = cl.getOptionValue("metaDbType");

    validate();
  }

  private CommandLine getCommandLine(String[] args)  throws ParseException {
    try {
      CommandLineParser parser = new GnuParser();
      return parser.parse(cmdLineOptions, args);
    } catch (ParseException e) {
      printAndExit("HiveSchemaTool:Parsing failed. Reason: " + e.getLocalizedMessage());
      return null;
    }
  }

  private static final Set<String> VALID_DB_TYPES = ImmutableSet.of(HiveSchemaHelper.DB_DERBY,
      HiveSchemaHelper.DB_HIVE, HiveSchemaHelper.DB_MSSQL, HiveSchemaHelper.DB_MYSQL,
      HiveSchemaHelper.DB_POSTGRACE, HiveSchemaHelper.DB_ORACLE);

  private static final Set<String> VALID_META_DB_TYPES = ImmutableSet.of(HiveSchemaHelper.DB_DERBY,
      HiveSchemaHelper.DB_MSSQL, HiveSchemaHelper.DB_MYSQL, HiveSchemaHelper.DB_POSTGRACE,
      HiveSchemaHelper.DB_ORACLE);

  private void validate() throws ParseException {
    if (!VALID_DB_TYPES.contains(dbType)) {
      printAndExit("Unsupported dbType " + dbType);
    }

    if (metaDbType != null) {
      if (!dbType.equals(HiveSchemaHelper.DB_HIVE)) {
        printAndExit("metaDbType may only be set if dbType is hive");
      }
      if (!VALID_META_DB_TYPES.contains(metaDbType)) {
        printAndExit("Unsupported metaDbType " + metaDbType);
      }
    } else if (dbType.equalsIgnoreCase(HiveSchemaHelper.DB_HIVE)) {
      System.err.println();
      printAndExit("metaDbType must be set if dbType is hive");
    }

    if ((cl.hasOption("createCatalog")) && !cl.hasOption("catalogLocation")) {
      System.err.println();
      printAndExit("catalogLocation must be set for createCatalog");
    }

    if (!cl.hasOption("createCatalog") && !cl.hasOption("alterCatalog") &&
        (cl.hasOption("catalogLocation") || cl.hasOption("catalogDescription"))) {
      printAndExit("catalogLocation and catalogDescription may be set only for createCatalog and alterCatalog");
    }

    if (!cl.hasOption("createCatalog") && cl.hasOption("ifNotExists")) {
      printAndExit("ifNotExists may be set only for createCatalog");
    }

    if (cl.hasOption("mergeCatalog") &&
        (!cl.hasOption("toCatalog"))) {
      printAndExit("mergeCatalog and toCatalog must be set for mergeCatalog");
    }

    if (cl.hasOption("moveDatabase") &&
        (!cl.hasOption("fromCatalog") || !cl.hasOption("toCatalog"))) {
      printAndExit("fromCatalog and toCatalog must be set for moveDatabase");
    }

    if (cl.hasOption("moveTable") &&
        (!cl.hasOption("fromCatalog") || !cl.hasOption("toCatalog") ||
         !cl.hasOption("fromDatabase") || !cl.hasOption("toDatabase"))) {
      printAndExit("fromCatalog, toCatalog, fromDatabase and toDatabase must be set for moveTable");
    }

    if ((!cl.hasOption("moveDatabase") && !cl.hasOption("moveTable") && !cl.hasOption("mergeCatalog")) &&
        (cl.hasOption("fromCatalog") || cl.hasOption("toCatalog"))) {
      printAndExit("fromCatalog and toCatalog may be set only for moveDatabase and moveTable");
    }

    if (!cl.hasOption("moveTable") &&
        (cl.hasOption("fromDatabase") || cl.hasOption("toDatabase"))) {
      printAndExit("fromDatabase and toDatabase may be set only for moveTable");
    }

    if (cl.hasOption("dropAllDatabases") && !HiveSchemaHelper.DB_HIVE.equals(dbType)) {
      printAndExit("dropAllDatabases can only be used with dbType=hive");
    }

    if (cl.hasOption("yes") && !cl.hasOption("dropAllDatabases")) {
      printAndExit("yes can only be used with dropAllDatabases");
    }
  }

  private void printAndExit(String reason) throws ParseException {
    if (reason != null) {
      System.err.println(reason);
    }
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("schemaTool", cmdLineOptions);
    if (reason != null) {
      throw new ParseException(reason);
    } else {
      System.exit(0);
    }
  }

  public String getDbType() {
    return dbType;
  }

  public String getMetaDbType() {
    return metaDbType;
  }

  boolean hasOption(String opt) {
    return cl.hasOption(opt);
  }

  String getOptionValue(String opt) {
    return cl.getOptionValue(opt);
  }
}
