/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.cli;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import jline.TerminalFactory;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class LlapStatusOptionsProcessor {

  private static final String LLAPSTATUS_CONSTANT = "llapstatus";

  private static final long FIND_YARN_APP_TIMEOUT_MS = 20 * 1000l; // 20seconds to wait for app to be visible

  private static final long DEFAULT_STATUS_REFRESH_INTERVAL_MS = 1 * 1000l; // 1 seconds wait until subsequent status
  private static final long DEFAULT_WATCH_MODE_TIMEOUT_MS = 5 * 60 * 1000l; // 5 minutes timeout for watch mode
  private static final float DEFAULT_RUNNING_NODES_THRESHOLD = 1.0f;

  // TODO: why doesn't this use one of the existing options implementations?!
  enum OptionConstants {

    NAME("name", 'n', "LLAP cluster name", true),
    FIND_APP_TIMEOUT("findAppTimeout", 'f',
        "Amount of time(s) that the tool will sleep to wait for the YARN application to start. negative values=wait forever, 0=Do not wait. default=" +
            TimeUnit.SECONDS.convert(FIND_YARN_APP_TIMEOUT_MS, TimeUnit.MILLISECONDS) + "s", true),
    OUTPUT_FILE("outputFile", 'o', "File to which output should be written (Default stdout)", true),
    WATCH_MODE("watch", 'w', "Watch mode waits until all LLAP daemons are running or subset of the nodes are " +
      "running (threshold can be specified via -r option) (Default wait until all nodes are running)", false),
    // This is a negative because we want the positive to be the default when nothing is specified.
    NOT_LAUNCHED("notlaunched", 'l', "In watch mode, do not assume that the application was "
        + "already launched if there's doubt (e.g. if the last application instance has failed).",
        false),
    RUNNING_NODES_THRESHOLD("runningNodesThreshold", 'r', "When watch mode is enabled (-w), wait until the " +
      "specified threshold of nodes are running (Default 1.0 which means 100% nodes are running)", true),
    STATUS_REFRESH_INTERVAL("refreshInterval", 'i', "Amount of time in seconds to wait until subsequent status checks" +
      " in watch mode. Valid only for watch mode. (Default " +
      TimeUnit.SECONDS.convert(DEFAULT_STATUS_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS) + "s)", true),
    WATCH_MODE_TIMEOUT("watchTimeout", 't', "Exit watch mode if the desired state is not attained until the specified" +
      " timeout. (Default " + TimeUnit.SECONDS.convert(DEFAULT_WATCH_MODE_TIMEOUT_MS, TimeUnit.MILLISECONDS) +"s)", true),
    HIVECONF("hiveconf", null, "Use value for given property. Overridden by explicit parameters", "property=value", 2),
    HELP("help", 'H', "Print help information", false);


    private final String longOpt;
    private final Character shortOpt;
    private final String description;
    private final String argName;
    private final int numArgs;

    OptionConstants(String longOpt, char shortOpt, String description, boolean hasArgs) {
      this(longOpt, shortOpt, description, longOpt, hasArgs ? 1 : 0);
    }

    OptionConstants(String longOpt, Character shortOpt, String description, String argName, int numArgs) {
      this.longOpt = longOpt;
      this.shortOpt = shortOpt;
      this.description = description;
      this.argName = argName;
      this.numArgs = numArgs;
    }

    public String getLongOpt() {
      return longOpt;
    }

    public Character getShortOpt() {
      return shortOpt;
    }

    public String getDescription() {
      return description;
    }

    public String getArgName() {
      return argName;
    }

    public int getNumArgs() {
      return numArgs;
    }
  }


  public static class LlapStatusOptions {
    private final String name;
    private final Properties conf;
    private final long findAppTimeoutMs;
    private final String outputFile;
    private final long refreshIntervalMs;
    private final boolean watchMode;
    private final long watchTimeout;
    private final float runningNodesThreshold;
    private final boolean isLaunched;

    public LlapStatusOptions(final String name) {
      this(name, new Properties(), FIND_YARN_APP_TIMEOUT_MS, null, DEFAULT_STATUS_REFRESH_INTERVAL_MS, false,
        DEFAULT_WATCH_MODE_TIMEOUT_MS, DEFAULT_RUNNING_NODES_THRESHOLD, true);
    }

    public LlapStatusOptions(String name, Properties hiveProperties, long findAppTimeoutMs,
                              String outputFile, long refreshIntervalMs,
                              final boolean watchMode, final long watchTimeoutMs,
                              final float runningNodesThreshold, final boolean isLaunched) {
      this.name = name;
      this.conf = hiveProperties;
      this.findAppTimeoutMs = findAppTimeoutMs;
      this.outputFile = outputFile;
      this.refreshIntervalMs = refreshIntervalMs;
      this.watchMode = watchMode;
      this.watchTimeout = watchTimeoutMs;
      this.runningNodesThreshold = runningNodesThreshold;
      this.isLaunched = isLaunched;
    }

    public String getName() {
      return name;
    }

    public Properties getConf() {
      return conf;
    }

    public long getFindAppTimeoutMs() {
      return findAppTimeoutMs;
    }

    public String getOutputFile() {
      return outputFile;
    }

    public long getRefreshIntervalMs() {
      return refreshIntervalMs;
    }

    public boolean isWatchMode() {
      return watchMode;
    }

    public boolean isLaunched() {
      return isLaunched;
    }

    public long getWatchTimeoutMs() {
      return watchTimeout;
    }

    public float getRunningNodesThreshold() {
      return runningNodesThreshold;
    }
  }

  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;

  public LlapStatusOptionsProcessor() {

    for (OptionConstants optionConstant : OptionConstants.values()) {

      OptionBuilder optionBuilder = OptionBuilder.hasArgs(optionConstant.getNumArgs())
          .withArgName(optionConstant.getArgName()).withLongOpt(optionConstant.getLongOpt())
          .withDescription(optionConstant.getDescription());
      if (optionConstant.getShortOpt() == null) {
        options.addOption(optionBuilder.create());
      } else {
        options.addOption(optionBuilder.create(optionConstant.getShortOpt()));
      }
    }
  }

  public LlapStatusOptions processOptions(String[] args) throws ParseException {
    commandLine = new GnuParser().parse(options, args);
    if (commandLine.hasOption(OptionConstants.HELP.getShortOpt())) {
      printUsage();
      return null;
    }

    String name = commandLine.getOptionValue(OptionConstants.NAME.getLongOpt());

    long findAppTimeoutMs = FIND_YARN_APP_TIMEOUT_MS;
    if (commandLine.hasOption(OptionConstants.FIND_APP_TIMEOUT.getLongOpt())) {
      findAppTimeoutMs = TimeUnit.MILLISECONDS.convert(Long.parseLong(
          commandLine.getOptionValue(OptionConstants.FIND_APP_TIMEOUT.getLongOpt())),
          TimeUnit.SECONDS);
    }

    Properties hiveConf;
    if (commandLine.hasOption(OptionConstants.HIVECONF.getLongOpt())) {
      hiveConf = commandLine.getOptionProperties(OptionConstants.HIVECONF.getLongOpt());
    } else {
      hiveConf = new Properties();
    }

    String outputFile = null;
    if (commandLine.hasOption(OptionConstants.OUTPUT_FILE.getLongOpt())) {
      outputFile = commandLine.getOptionValue(OptionConstants.OUTPUT_FILE.getLongOpt());
    }

    long refreshIntervalMs = DEFAULT_STATUS_REFRESH_INTERVAL_MS;
    if (commandLine.hasOption(OptionConstants.STATUS_REFRESH_INTERVAL.getLongOpt())) {
      long refreshIntervalSec = Long.parseLong(commandLine.getOptionValue(OptionConstants.STATUS_REFRESH_INTERVAL
        .getLongOpt()));
      if (refreshIntervalSec <= 0) {
        throw new IllegalArgumentException("Refresh interval should be >0");
      }
      refreshIntervalMs = TimeUnit.MILLISECONDS.convert(refreshIntervalSec, TimeUnit.SECONDS);
    }

    boolean watchMode = commandLine.hasOption(OptionConstants.WATCH_MODE.getLongOpt());
    long watchTimeoutMs = DEFAULT_WATCH_MODE_TIMEOUT_MS;
    if (commandLine.hasOption(OptionConstants.WATCH_MODE_TIMEOUT.getLongOpt())) {
      long watchTimeoutSec = Long.parseLong(commandLine.getOptionValue(
          OptionConstants.WATCH_MODE_TIMEOUT.getLongOpt()));
      if (watchTimeoutSec <= 0) {
        throw new IllegalArgumentException("Watch timeout should be >0");
      }
      watchTimeoutMs = TimeUnit.MILLISECONDS.convert(watchTimeoutSec, TimeUnit.SECONDS);
    }
 
    boolean isLaunched = !commandLine.hasOption(OptionConstants.NOT_LAUNCHED.getLongOpt());

    float runningNodesThreshold = DEFAULT_RUNNING_NODES_THRESHOLD;
    if (commandLine.hasOption(OptionConstants.RUNNING_NODES_THRESHOLD.getLongOpt())) {
      runningNodesThreshold = Float.parseFloat(commandLine.getOptionValue(
          OptionConstants.RUNNING_NODES_THRESHOLD.getLongOpt()));
      if (runningNodesThreshold < 0.0f || runningNodesThreshold > 1.0f) {
        throw new IllegalArgumentException(
            "Running nodes threshold value should be between 0.0 and 1.0 (inclusive)");
      }
    }
    return new LlapStatusOptions(name, hiveConf, findAppTimeoutMs, outputFile, refreshIntervalMs,
        watchMode,  watchTimeoutMs, runningNodesThreshold, isLaunched);
  }


  public static void printUsage() {
    HelpFormatter hf = new HelpFormatter();
    try {
      int width = hf.getWidth();
      int jlineWidth = TerminalFactory.get().getWidth();
      width = Math.min(160, Math.max(jlineWidth, width)); // Ignore potentially incorrect values
      hf.setWidth(width);
    } catch (Throwable t) { // Ignore
    }

    LlapStatusOptionsProcessor optionsProcessor = new LlapStatusOptionsProcessor();
    hf.printHelp(LLAPSTATUS_CONSTANT, optionsProcessor.options);
  }

}
