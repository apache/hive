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
package org.apache.hive.beeline.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * This class is used for parsing the options of Hive Cli
 */
public class CliOptionsProcessor {
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;

  public CliOptionsProcessor() {
    // -database database
    options.addOption(OptionBuilder.hasArg().withArgName("databasename").withLongOpt("database")
        .withDescription("Specify the database to use").create());

    // -e 'quoted-query-string'
    options.addOption(OptionBuilder.hasArg().withArgName("quoted-query-string").withDescription
        ("SQL from command line").create('e'));

    // -f <query-file>
    options.addOption(OptionBuilder.hasArg().withArgName("filename").withDescription("SQL from " +
        "files").create('f'));

    // -i <init-query-file>
    options.addOption(OptionBuilder.hasArg().withArgName("filename").withDescription
        ("Initialization SQL file").create('i'));

    // -hiveconf x=y
    options.addOption(OptionBuilder.withValueSeparator().hasArgs(2).withArgName("property=value")
        .withLongOpt("hiveconf").withDescription("Use value for given property").create());

    // Substitution option -d, --define
    options.addOption(OptionBuilder.withValueSeparator().hasArgs(2).withArgName("key=value")
        .withLongOpt("define").withDescription("Variable substitution to apply to Hive commands. e" +
            ".g. -d A=B or --define A=B").create('d'));

    // Substitution option --hivevar
    options.addOption(OptionBuilder.withValueSeparator().hasArgs(2).withArgName("key=value")
        .withLongOpt("hivevar").withDescription("Variable substitution to apply to Hive commands. " +
            "e.g. --hivevar A=B").create());

    // [-S|--silent]
    options.addOption(new Option("S", "silent", false, "Silent mode in interactive shell"));

    // [-v|--verbose]
    options.addOption(new Option("v", "verbose", false, "Verbose mode (echo executed SQL to the "
        + "console)"));

    // [-H|--help]
    options.addOption(new Option("H", "help", false, "Print help information"));
  }

  public boolean process(String []argv){
    try {
      commandLine = new GnuParser().parse(options, argv);

      if(commandLine.hasOption("help")){
        printCliUsage();
        return false;
      }
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printCliUsage();
      return false;
    }
    return true;
  }

  public void printCliUsage() {
    new HelpFormatter().printHelp("hive", options);
  }

  public CommandLine getCommandLine() {
    return commandLine;
  }

  public void setCommandLine(CommandLine commandLine) {
    this.commandLine = commandLine;
  }
}
