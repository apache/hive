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
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds the parsed arguments taken from the command line
 */
public class SchemaToolCommandLine {

  private final Options cmdLineOptions;

  private Options createOptions(OptionGroup additionalOptions) {
    Option help = new Option("help", "print this message");
    Option infoOpt = new Option("info", "Show config and schema details");
    Option upgradeOpt = new Option("upgradeSchema", "Schema upgrade");
    Option upgradeFromOpt = Option.builder("upgradeSchemaFrom").argName("upgradeFrom").hasArg()
        .desc("Schema upgrade from a version. !DEPRECATED! This command is no longer able to upgrade the schema " +
            "manually from really old versions where the versioning table not yet exist. Upgrading from these versions " +
            "is no longer possible. This option kept only for backward compatiblity and behaves the same as the '-upgradeSchema' option.")
        .build();
    Option initOpt = new Option("initSchema", "Schema initialization");
    Option initToOpt = Option.builder("initSchemaTo").argName("initTo").hasArg()
        .desc("Schema initialization to a version")
        .build();
    Option initOrUpgradeSchemaOpt = new Option("initOrUpgradeSchema", "Initialize or upgrade schema to latest version");
    Option dropDbOpt = new Option("dropAllDatabases", "Drop all Hive databases (with CASCADE). " +
        "This will remove all managed data!");
    Option yesOpt = new Option("yes", "Don't ask for confirmation when using -dropAllDatabases.");
    Option validateOpt = new Option("validate", "Validate the database");
    Option createCatalog = Option.builder("createCatalog")
        .hasArg()
        .desc("Create a catalog, requires --catalogLocation parameter as well")
        .build();
    Option alterCatalog = Option.builder("alterCatalog")
        .hasArg()
        .desc("Alter a catalog, requires --catalogLocation and/or --catalogDescription parameter as well")
        .build();
    Option mergeCatalog = Option.builder("mergeCatalog")
        .hasArg()
        .desc("Merge databases from a catalog into other, Argument is the source catalog name " +
            "Requires --toCatalog to indicate the destination catalog")
        .build();
    Option moveDatabase = Option.builder("moveDatabase")
        .hasArg()
        .desc("Move a database between catalogs.  Argument is the database name. " +
            "Requires --fromCatalog and --toCatalog parameters as well")
        .build();
    Option moveTable = Option.builder("moveTable")
        .hasArg()
        .desc("Move a table to a different database.  Argument is the table name. " +
            "Requires --fromCatalog, --toCatalog, --fromDatabase, and --toDatabase " +
            " parameters as well.")
        .build();
    Option createUserOpt = new Option("createUser", "Create the Hive user, set hiveUser to the db" +
        " admin user and the hive password to the db admin password with this");
    Option createLogsTable = Option.builder("createLogsTable")
        .hasArg()
        .desc("Create table for Hive warehouse/compute logs")
        .build();

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

    Option userNameOpt = Option.builder("userName").argName("user")
        .hasArgs()
        .desc("Override config file user name")
        .build();
    Option passwdOpt = Option.builder("passWord").argName("password")
        .hasArgs()
        .desc("Override config file password")
        .build();
    Option dbTypeOpt = Option.builder("dbType").argName("databaseType")
        .hasArgs().desc("Metastore database type").required()
        .build();
    Option hiveUserOpt = Option.builder("hiveUser")
        .hasArg()
        .desc("Hive user (for use with createUser)")
        .build();
    Option hivePasswdOpt = Option.builder("hivePassword")
        .hasArg()
        .desc("Hive password (for use with createUser)")
        .build();
    Option hiveDbOpt = Option.builder("hiveDb")
        .hasArg()
        .desc("Hive database (for use with createUser)")
        .build();
    Option urlOpt = Option.builder("url").argName("url")
        .hasArgs().desc("connection url to the database")
        .build();
    Option driverOpt = Option.builder("driver").argName("driver")
        .hasArgs().desc("driver name for connection")
        .build();
    Option dbOpts = Option.builder("dbOpts").argName("databaseOpts")
        .hasArgs().desc("Backend DB specific options")
        .build();
    Option dryRunOpt = new Option("dryRun", "list SQL scripts (no execute)");
    Option verboseOpt = new Option("verbose", "only print SQL statements");
    Option serversOpt = Option.builder("servers").argName("serverList")
        .hasArgs().desc("a comma-separated list of servers used in location validation in the format of " +
            "scheme://authority (e.g. hdfs://localhost:8000)")
        .build();
    Option catalogLocation = Option.builder("catalogLocation")
        .hasArg()
        .desc("Location of new catalog, required when adding a catalog")
        .build();
    Option catalogDescription = Option.builder("catalogDescription")
        .hasArg()
        .desc("Description of new catalog")
        .build();
    Option ifNotExists = Option.builder("ifNotExists")
        .desc("If passed then it is not an error to create an existing catalog")
        .build();
    Option fromCatalog = Option.builder("fromCatalog")
        .hasArg()
        .desc("Catalog a moving database or table is coming from.  This is " +
            "required if you are moving a database or table.")
        .build();
    Option toCatalog = Option.builder("toCatalog")
        .hasArg()
        .desc("Catalog a moving database or table is going to.  This is " +
            "required if you are moving a database or table.")
        .build();
    Option fromDatabase = Option.builder("fromDatabase")
        .hasArg()
        .desc("Database a moving table is coming from.  This is " +
            "required if you are moving a table.")
        .build();
    Option toDatabase = Option.builder("toDatabase")
        .hasArg()
        .desc("Database a moving table is going to.  This is " +
            "required if you are moving a table.")
        .build();
    Option retentionPeriod = Option.builder("retentionPeriod")
        .hasArg()
        .desc("Specify logs table retention period")
        .build();
    Option contexts = Option.builder("contexts")
        .hasArg()
        .desc("One or more (comma separated) liqubase contexts to use during schema migration")
        .build();

    Options options = new Options();
    options.addOption(help);
    options.addOptionGroup(optGroup);
    options.addOption(dbTypeOpt);
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
    options.addOption(contexts);
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
  }

  @Nonnull
  private CommandLine getCommandLine(String[] args)  throws ParseException {
    try {
      CommandLineParser parser = new DefaultParser();
      return parser.parse(cmdLineOptions, args);
    } catch (ParseException e) {
      printAndExit("HiveSchemaTool:Parsing failed. Reason: " + e.getLocalizedMessage());
      return cl;
    }
  }

  private void printAndExit(String reason) throws ParseException {
    if (reason != null) {
      System.err.println(reason);
    }
    printHelp();
    if (reason != null) {
      throw new ParseException(reason);
    } else {
      System.exit(0);
    }
  }

  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("schemaTool", cmdLineOptions);
  }

  public Set<String> getDanglingArguments(Set<String> argumentsUsed) {
    return Arrays.stream(cl.getOptions())
        .map(Option::getOpt)
        .filter(o -> !argumentsUsed.contains(o))
        .collect(Collectors.toSet());
  }

  public String getDbType() {
    return dbType;
  }

  public String getMetaDbType() {
    return metaDbType;
  }

  public Option[] getOptions() {
    return cl.getOptions();
  }

  public boolean hasOption(String opt) {
    return cl.hasOption(opt);
  }

  public String getOptionValue(String opt) {
    return cl.getOptionValue(opt);
  }

}
