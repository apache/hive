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

package org.apache.hadoop.hive.llap.cli;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LlapOptionsProcessor {

  public class LlapOptions {
    private int instances = 0;
    private String directory = null;
    private String name;

    public LlapOptions(String name, int instances, String directory)
        throws ParseException {
      if (instances <= 0) {
        throw new ParseException("Invalid configuration: " + instances
            + " (should be greater than 0)");
      }
      this.instances = instances;
      this.directory = directory;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public int getInstances() {
      return instances;
    }

    public String getDirectory() {
      return directory;
    }
  }

  protected static final Log l4j = LogFactory.getLog(LlapOptionsProcessor.class.getName());
  private final Options options = new Options();
  Map<String, String> hiveVariables = new HashMap<String, String>();
  private org.apache.commons.cli.CommandLine commandLine;

  @SuppressWarnings("static-access")
  public LlapOptionsProcessor() {

    // set the number of instances on which llap should run
    options.addOption(OptionBuilder.hasArg().withArgName("instances").withLongOpt("instances")
        .withDescription("Specify the number of instances to run this on").create('i'));

    options.addOption(OptionBuilder.hasArg().withArgName("name").withLongOpt("name")
        .withDescription("Cluster name for YARN registry").create('n'));

    options.addOption(OptionBuilder.hasArg().withArgName("directory").withLongOpt("directory")
        .withDescription("Temp directory for jars etc.").create('d'));

    options.addOption(OptionBuilder.hasArg().withArgName("args").withLongOpt("args")
        .withDescription("java arguments to the llap instance").create('a'));

    options.addOption(OptionBuilder.hasArg().withArgName("loglevel").withLongOpt("loglevel")
        .withDescription("log levels for the llap instance").create('l'));

    // [-H|--help]
    options.addOption(new Option("H", "help", false, "Print help information"));
  }

  public LlapOptions processOptions(String argv[]) throws ParseException {
    commandLine = new GnuParser().parse(options, argv);
    if (commandLine.hasOption('H') || false == commandLine.hasOption("instances")) {
      // needs at least --instances
      printUsage();
      return null;
    }

    int instances = Integer.parseInt(commandLine.getOptionValue("instances"));
    String directory = commandLine.getOptionValue("directory");

    String name = commandLine.getOptionValue("name", null);
    // loglevel & args are parsed by the python processor

    return new LlapOptions(name, instances, directory);
  }

  private void printUsage() {
    new HelpFormatter().printHelp("llap", options);
  }
}
