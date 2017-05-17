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
package org.apache.hadoop.hive.common.cli;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;

/**
 * Reusable code for Hive Cli's.
 * <p>
 * Basic usage is: create an instance (usually a subclass if you want to
 * all your own options or processing instructions), parse, and then use
 * the resulting information.
 * <p>
 * See org.apache.hadoop.hive.service.HiveServer or
 *     org.apache.hadoop.hive.metastore.HiveMetaStore
 *     for examples of use.
 *
 */
public class CommonCliOptions {
  /**
   * Options for parsing the command line.
   */
  protected final Options OPTIONS = new Options();

  protected CommandLine commandLine;

  /**
   * The name of this cli.
   */
  protected final String cliname;

  private boolean verbose = false;

  /**
   * Create an instance with common options (help, verbose, etc...).
   *
   * @param cliname the name of the command
   * @param includeHiveConf include "hiveconf" as an option if true
   */
  @SuppressWarnings("static-access")
  public CommonCliOptions(String cliname, boolean includeHiveConf) {
    this.cliname = cliname;

    // [-v|--verbose]
    OPTIONS.addOption(new Option("v", "verbose", false, "Verbose mode"));

    // [-h|--help]
    OPTIONS.addOption(new Option("h", "help", false, "Print help information"));

    if (includeHiveConf) {
      OPTIONS.addOption(OptionBuilder
          .withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt("hiveconf")
          .withDescription("Use value for given property")
          .create());
    }
  }

  /**
   * Add the hiveconf properties to the Java system properties, override
   * anything therein.
   *
   * @return a copy of the properties specified in hiveconf
   */
  public Properties addHiveconfToSystemProperties() {
    Properties confProps = commandLine.getOptionProperties("hiveconf");
    for (String propKey : confProps.stringPropertyNames()) {
      if (verbose) {
        System.err.println(
            "hiveconf: " + propKey + "=" + confProps.getProperty(propKey));
      }
      if (propKey.equalsIgnoreCase("hive.root.logger")) {
        splitAndSetLogger(propKey, confProps);
      } else {
        System.setProperty(propKey, confProps.getProperty(propKey));
      }
    }
    return confProps;
  }

  public static void splitAndSetLogger(final String propKey, final Properties confProps) {
    String propVal = confProps.getProperty(propKey);
    if (propVal.contains(",")) {
      String[] tokens = propVal.split(",");
      for (String token : tokens) {
        if (Level.getLevel(token) == null) {
          System.setProperty("hive.root.logger", token);
        } else {
          System.setProperty("hive.log.level", token);
        }
      }
    } else {
      System.setProperty(propKey, confProps.getProperty(propKey));
    }
  }

  /**
   * Print usage information for the CLI.
   */
  public void printUsage() {
    new HelpFormatter().printHelp(cliname, OPTIONS);
  }

  /**
   * Parse the arguments.
   * @param args
   */
  public void parse(String[] args) {
    try {
      commandLine = new GnuParser().parse(OPTIONS, args);

      if (commandLine.hasOption('h')) {
        printUsage();
        System.exit(1);
      }
      if (commandLine.hasOption('v')) {
        verbose = true;
      }
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printUsage();
      System.exit(1);
    }

  }

  /**
   * Should the client be verbose.
   */
  public boolean isVerbose() {
    return verbose;
  }

}
