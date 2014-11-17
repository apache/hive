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

package org.apache.hadoop.hive.cli;

import java.util.HashMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * OptionsProcessor.
 *
 */
public class OptionsProcessor {
  protected static final Log l4j = LogFactory.getLog(OptionsProcessor.class.getName());
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;
  Map<String, String> hiveVariables = new HashMap<String, String>();

  @SuppressWarnings("static-access")
  public OptionsProcessor() {

    // -database database
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("databasename")
        .withLongOpt("database")
        .withDescription("Specify the database to use")
        .create());

    // -e 'quoted-query-string'
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("quoted-query-string")
        .withDescription("SQL from command line")
        .create('e'));

    // -f <query-file>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("filename")
        .withDescription("SQL from files")
        .create('f'));

    // -i <init-query-file>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("filename")
        .withDescription("Initialization SQL file")
        .create('i'));

    // -hiveconf x=y
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("hiveconf")
        .withDescription("Use value for given property")
        .create());

    // Substitution option -d, --define
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("key=value")
        .withLongOpt("define")
        .withDescription("Variable subsitution to apply to hive commands. e.g. -d A=B or --define A=B")
        .create('d'));

    // Substitution option --hivevar
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("key=value")
        .withLongOpt("hivevar")
        .withDescription("Variable subsitution to apply to hive commands. e.g. --hivevar A=B")
        .create());

    // [-S|--silent]
    options.addOption(new Option("S", "silent", false, "Silent mode in interactive shell"));

    // [-v|--verbose]
    options.addOption(new Option("v", "verbose", false, "Verbose mode (echo executed SQL to the console)"));

    // [-H|--help]
    options.addOption(new Option("H", "help", false, "Print help information"));

  }

  public boolean process_stage1(String[] argv) {
    try {
      commandLine = new GnuParser().parse(options, argv);
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        System.setProperty(propKey, confProps.getProperty(propKey));
      }

      Properties hiveVars = commandLine.getOptionProperties("define");
      for (String propKey : hiveVars.stringPropertyNames()) {
        hiveVariables.put(propKey, hiveVars.getProperty(propKey));
      }

      Properties hiveVars2 = commandLine.getOptionProperties("hivevar");
      for (String propKey : hiveVars2.stringPropertyNames()) {
        hiveVariables.put(propKey, hiveVars2.getProperty(propKey));
      }
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printUsage();
      return false;
    }
    return true;
  }

  public boolean process_stage2(CliSessionState ss) {
    ss.getConf();

    if (commandLine.hasOption('H')) {
      printUsage();
      return false;
    }

    ss.setIsSilent(commandLine.hasOption('S'));

    ss.database = commandLine.getOptionValue("database");

    ss.execString = commandLine.getOptionValue('e');

    ss.fileName = commandLine.getOptionValue('f');

    ss.setIsVerbose(commandLine.hasOption('v'));

    String[] initFiles = commandLine.getOptionValues('i');
    if (null != initFiles) {
      ss.initFiles = Arrays.asList(initFiles);
    }

    if (ss.execString != null && ss.fileName != null) {
      System.err.println("The '-e' and '-f' options cannot be specified simultaneously");
      printUsage();
      return false;
    }

    if (commandLine.hasOption("hiveconf")) {
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        ss.cmdProperties.setProperty(propKey, confProps.getProperty(propKey));
      }
    }

    return true;
  }

  private void printUsage() {
    new HelpFormatter().printHelp("hive", options);
  }

  public Map<String, String> getHiveVariables() {
    return hiveVariables;
  }
}
