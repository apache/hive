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

package org.apache.hadoop.hive.llap.cli.status;

import java.util.Arrays;
import java.util.Properties;

import jline.TerminalFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Parses, verifies, prints and provides the command line arguments of the Llap Status program.
 */
public class LlapStatusServiceCommandLine {
  private static final Logger LOGGER = LoggerFactory.getLogger("LlapStatusServiceDriverConsole");

  @VisibleForTesting
  static final long DEFAULT_FIND_YARN_APP_TIMEOUT_MS = 20 * 1000L;
  @VisibleForTesting
  static final long DEFAULT_STATUS_REFRESH_INTERVAL_MS = 1 * 1000L;
  @VisibleForTesting
  static final long DEFAULT_WATCH_MODE_TIMEOUT_MS = 5 * 60 * 1000L;
  @VisibleForTesting
  static final float DEFAULT_RUNNING_NODES_THRESHOLD = 1.0f;

  @SuppressWarnings("static-access")
  private static final Option NAME = OptionBuilder
      .withLongOpt("name")
      .withDescription("LLAP cluster name")
      .withArgName("name")
      .hasArg()
      .create('n');

  @SuppressWarnings("static-access")
  private static final Option FIND_APP_TIMEOUT = OptionBuilder
      .withLongOpt("findAppTimeout")
      .withDescription("Amount of time(s) that the tool will sleep to wait for the YARN application to start." +
          "negative values=wait forever, 0=Do not wait. default=" + (DEFAULT_FIND_YARN_APP_TIMEOUT_MS / 1000) + "s")
      .withArgName("findAppTimeout")
      .hasArg()
      .create('f');

  @SuppressWarnings("static-access")
  private static final Option OUTPUT_FILE = OptionBuilder
      .withLongOpt("outputFile")
      .withDescription("File to which output should be written (Default stdout)")
      .withArgName("outputFile")
      .hasArg()
      .create('o');

  @SuppressWarnings("static-access")
  private static final Option WATCH_MODE = OptionBuilder
      .withLongOpt("watch")
      .withDescription("Watch mode waits until all LLAP daemons are running or subset of the nodes are running " +
          "(threshold can be specified via -r option) (Default wait until all nodes are running)")
      .withArgName("watch")
      .create('w');

  @SuppressWarnings("static-access")
  private static final Option NOT_LAUNCHED = OptionBuilder
      .withLongOpt("notLaunched")
      .withDescription("In watch mode, do not assume that the application was already launched if there's doubt " +
          "(e.g. if the last application instance has failed).")
      .withArgName("notLaunched")
      .create('l');

  @SuppressWarnings("static-access")
  private static final Option RUNNING_NODES_THRESHOLD = OptionBuilder
      .withLongOpt("runningNodesThreshold")
      .withDescription("When watch mode is enabled (-w), wait until the specified threshold of nodes are running " +
          "(Default 1.0 which means 100% nodes are running)")
      .withArgName("runningNodesThreshold")
      .hasArg()
      .create('r');

  @SuppressWarnings("static-access")
  private static final Option REFRESH_INTERVAL = OptionBuilder
      .withLongOpt("refreshInterval")
      .withDescription("Amount of time in seconds to wait until subsequent status checks in watch mode. Valid only " +
          "for watch mode. (Default " + (DEFAULT_STATUS_REFRESH_INTERVAL_MS / 1000) + "s)")
      .withArgName("refreshInterval")
      .hasArg()
      .create('i');

  @SuppressWarnings("static-access")
  private static final Option WATCH_TIMEOUT = OptionBuilder
      .withLongOpt("watchTimeout")
      .withDescription("Exit watch mode if the desired state is not attained until the specified timeout. (Default " +
          (DEFAULT_WATCH_MODE_TIMEOUT_MS / 1000) + "s)")
      .withArgName("watchTimeout")
      .hasArg()
      .create('t');

  @SuppressWarnings("static-access")
  private static final Option HIVECONF = OptionBuilder
      .withLongOpt("hiveconf")
      .withDescription("Use value for given property. Overridden by explicit parameters")
      .withArgName("property=value")
      .hasArgs(2)
      .create();

  @SuppressWarnings("static-access")
  private static final Option HELP = OptionBuilder
      .withLongOpt("help")
      .withDescription("Print help information")
      .withArgName("help")
      .create('h');

  private static final Options OPTIONS = new Options();
  static {
    OPTIONS.addOption(NAME);
    OPTIONS.addOption(FIND_APP_TIMEOUT);
    OPTIONS.addOption(OUTPUT_FILE);
    OPTIONS.addOption(WATCH_MODE);
    OPTIONS.addOption(NOT_LAUNCHED);
    OPTIONS.addOption(RUNNING_NODES_THRESHOLD);
    OPTIONS.addOption(REFRESH_INTERVAL);
    OPTIONS.addOption(WATCH_TIMEOUT);
    OPTIONS.addOption(HIVECONF);
    OPTIONS.addOption(HELP);
  }

  private String name;
  private long findAppTimeoutMs;
  private String outputFile;
  private boolean watchMode;
  private boolean isLaunched;
  private float runningNodesThreshold;
  private long refreshIntervalMs;
  private long watchTimeoutMs;
  private Properties hiveConf;
  private boolean isHelp;

  public static LlapStatusServiceCommandLine parseArguments(String[] args) {
    LlapStatusServiceCommandLine cl = null;
    try {
      cl = new LlapStatusServiceCommandLine(args);
    } catch (Exception e) {
      LOGGER.error("Parsing the command line arguments failed", e);
      printUsage();
      System.exit(ExitCode.INCORRECT_USAGE.getCode());
    }

    if (cl.isHelp()) {
      printUsage();
      System.exit(0);
    }

    return cl;
  }

  LlapStatusServiceCommandLine(String[] args) throws ParseException {
    LOGGER.info("LLAP status invoked with arguments = {}", Arrays.toString(args));
    parseCommandLine(args);
    printArguments();
  }

  private void parseCommandLine(String[] args) throws ParseException {
    CommandLine cl = new GnuParser().parse(OPTIONS, args);

    name = cl.getOptionValue(NAME.getLongOpt());

    findAppTimeoutMs = DEFAULT_FIND_YARN_APP_TIMEOUT_MS;
    if (cl.hasOption(FIND_APP_TIMEOUT.getLongOpt())) {
      findAppTimeoutMs = Long.parseLong(cl.getOptionValue(FIND_APP_TIMEOUT.getLongOpt())) * 1000;
    }

    if (cl.hasOption(OUTPUT_FILE.getLongOpt())) {
      outputFile = cl.getOptionValue(OUTPUT_FILE.getLongOpt());
    }

    watchMode = cl.hasOption(WATCH_MODE.getLongOpt());

    isLaunched = !cl.hasOption(NOT_LAUNCHED.getLongOpt());

    runningNodesThreshold = DEFAULT_RUNNING_NODES_THRESHOLD;
    if (cl.hasOption(RUNNING_NODES_THRESHOLD.getLongOpt())) {
      runningNodesThreshold = Float.parseFloat(cl.getOptionValue(RUNNING_NODES_THRESHOLD.getLongOpt()));
      if (runningNodesThreshold < 0.0f || runningNodesThreshold > 1.0f) {
        throw new IllegalArgumentException("Running nodes threshold value should be between 0.0 and 1.0 (inclusive)");
      }
    }

    refreshIntervalMs = DEFAULT_STATUS_REFRESH_INTERVAL_MS;
    if (cl.hasOption(REFRESH_INTERVAL.getLongOpt())) {
      long refreshIntervalSec = Long.parseLong(cl.getOptionValue(REFRESH_INTERVAL.getLongOpt()));
      if (refreshIntervalSec <= 0) {
        throw new IllegalArgumentException("Refresh interval should be >0");
      }
      refreshIntervalMs = refreshIntervalSec * 1000;
    }

    watchTimeoutMs = DEFAULT_WATCH_MODE_TIMEOUT_MS;
    if (cl.hasOption(WATCH_TIMEOUT.getLongOpt())) {
      long watchTimeoutSec = Long.parseLong(cl.getOptionValue(WATCH_TIMEOUT.getLongOpt()));
      if (watchTimeoutSec <= 0) {
        throw new IllegalArgumentException("Watch timeout should be >0");
      }
      watchTimeoutMs = watchTimeoutSec * 1000;
    }

    hiveConf = new Properties();
    if (cl.hasOption(HIVECONF.getLongOpt())) {
      hiveConf = cl.getOptionProperties(HIVECONF.getLongOpt());
    }

    isHelp = cl.hasOption(HELP.getOpt());
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

    hf.printHelp("llapstatus", OPTIONS);
  }

  private void printArguments() {
    LOGGER.info("LLAP status running with the following parsed arguments: \n" +
        "\tname                 : " + name + "\n" +
        "\tfindAppTimeoutMs     : " + findAppTimeoutMs + "\n" +
        "\toutputFile           : " + outputFile + "\n" +
        "\twatchMode            : " + watchMode + "\n" +
        "\tisLaunched           : " + isLaunched + "\n" +
        "\trunningNodesThreshold: " + runningNodesThreshold + "\n" +
        "\trefreshIntervalMs    : " + refreshIntervalMs + "\n" +
        "\twatchTimeoutMs       : " + watchTimeoutMs + "\n" +
        "\thiveConf             : " + hiveConf);
  }

  String getName() {
    return name;
  }

  long getFindAppTimeoutMs() {
    return findAppTimeoutMs;
  }

  String getOutputFile() {
    return outputFile;
  }

  boolean isWatchMode() {
    return watchMode;
  }

  boolean isLaunched() {
    return isLaunched;
  }

  float getRunningNodesThreshold() {
    return runningNodesThreshold;
  }

  long getRefreshIntervalMs() {
    return refreshIntervalMs;
  }

  long getWatchTimeoutMs() {
    return watchTimeoutMs;
  }

  Properties getHiveConf() {
    return hiveConf;
  }

  boolean isHelp() {
    return isHelp;
  }
}
