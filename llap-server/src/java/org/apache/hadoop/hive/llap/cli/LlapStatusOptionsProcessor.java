/**
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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import jline.TerminalFactory;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapStatusOptionsProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(LlapStatusOptionsProcessor.class);

  private static final String LLAPSTATUS_CONSTANT = "llapstatus";

  private static final long FIND_YARN_APP_TIMEOUT_MS = 20 * 1000l; // 20seconds to wait for app to be visible

  enum OptionConstants {

    NAME("name", 'n', "LLAP cluster name", true),
    FIND_APP_TIMEOUT("findAppTimeout", 'f',
        "Amount of time(s) that the tool will sleep to wait for the YARN application to start. negative values=wait forever, 0=Do not wait. default=" +
            TimeUnit.SECONDS.convert(FIND_YARN_APP_TIMEOUT_MS, TimeUnit.MILLISECONDS) + "s", true),
    OUTPUT_FILE("outputFile", 'o', "File to which output should be written (Default stdout)", true),
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

    public LlapStatusOptions(String name, Properties hiveProperties, long findAppTimeoutMs,
                             String outputFile) {
      this.name = name;
      this.conf = hiveProperties;
      this.findAppTimeoutMs = findAppTimeoutMs;
      this.outputFile = outputFile;
    }

    public LlapStatusOptions(String name) {
      this(name, new Properties(), FIND_YARN_APP_TIMEOUT_MS, null);
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

    return new LlapStatusOptions(name, hiveConf, findAppTimeoutMs, outputFile);
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
