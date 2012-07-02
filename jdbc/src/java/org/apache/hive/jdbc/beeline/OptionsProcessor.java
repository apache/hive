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

package org.apache.hive.jdbc.beeline;

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
  enum PrintMode {
    SILENT,
    NORMAL,
    VERBOSE
  };

  private static final Log l4j = LogFactory.getLog(OptionsProcessor.class.getName());
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;

  /**
   * -database option if any that the session has been invoked with.
   */
  private String database;

  /**
   * -e option if any that the session has been invoked with.
   */
  private String execString;

  /**
   * -f option if any that the session has been invoked with.
   */
  private String fileName;

  /**
   * properties set from -hiveconf via cmdline.
   */
  private final Properties cmdProperties = new Properties();

  /**
   * host name and port number of remote Hive server
   */
  private String host;
  private int port;

  /**
   * print mode
   */
  private PrintMode pMode = PrintMode.NORMAL;

  /**
   * hive var properties
   */
  private String hiveVars;

  /**
   * hive conf properties
   */
  private String hiveConfs;

  /**
   * hive session properties
   */
  private String sessVars;

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

    // -hiveconf x=y
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("hiveconf")
        .withDescription("Use value for given property")
        .create());

    // -sessVar x=y
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("sessVar")
        .withDescription("Use value for given property")
        .create());

    // -h hostname/ippaddress
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("hostname")
        .withDescription("connecting to Hive Server on remote host")
        .create('h'));

    // -p port
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("port")
        .withDescription("connecting to Hive Server on port number")
        .create('p'));

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

  public String getDatabase() {
    return database;
  }

  public String getExecString() {
    return execString;
  }

  public String getFileName() {
    return fileName;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public PrintMode getpMode() {
    return pMode;
  }

  public String getHiveVars() {
    return hiveVars;
  }

  public String getHiveConfs() {
    return hiveConfs;
  }

  public String getSessVars() {
    return sessVars;
  }

  public boolean processArgs(String[] argv) {
    try {
      commandLine = new GnuParser().parse(options, argv);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printUsage();
      return false;
    }

    if (commandLine.hasOption('H')) {
      printUsage();
      return false;
    }

    if (commandLine.hasOption('S')) {
      pMode = PrintMode.SILENT;
    } else if (commandLine.hasOption('v')) {
      pMode = PrintMode.VERBOSE;
    } else {
      pMode = PrintMode.NORMAL;
    }

    hiveConfs = commandLine.getOptionValue("hiveconf", "");
    hiveVars = commandLine.getOptionValue("define", "");
    hiveVars += commandLine.getOptionValue("hivevar", "");
    sessVars = commandLine.getOptionValue("sessvar", "");
    database = commandLine.getOptionValue("database", "");
    execString = commandLine.getOptionValue('e');
    fileName = commandLine.getOptionValue('f');
    host = (String) commandLine.getOptionValue('h');
    port = Integer.parseInt((String) commandLine.getOptionValue('p', "10000"));

    if (execString != null && fileName != null) {
      System.err.println("The '-e' and '-f' options cannot be specified simultaneously");
      printUsage();
      return false;
    }

    if (commandLine.hasOption("hiveconf")) {
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        cmdProperties.setProperty(propKey, confProps.getProperty(propKey));
      }
    }

    return true;
  }

  private void printUsage() {
    new HelpFormatter().printHelp("beeline", options);
  }

}
