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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils;

public class LlapOptionsProcessor {

  public static final String OPTION_INSTANCES = "instances"; //forward as arg
  public static final String OPTION_NAME = "name"; // forward as arg
  public static final String OPTION_DIRECTORY = "directory"; // work-dir
  public static final String OPTION_ARGS = "args"; // forward as arg
  public static final String OPTION_LOGLEVEL = "loglevel"; // forward as arg
  public static final String OPTION_CHAOS_MONKEY = "chaosmonkey"; // forward as arg
  public static final String OPTION_EXECUTORS = "executors"; // llap-daemon-site
  public static final String OPTION_CACHE = "cache"; // llap-daemon-site
  public static final String OPTION_SIZE = "size"; // forward via config.json
  public static final String OPTION_XMX = "xmx"; // forward as arg
  public static final String OPTION_AUXJARS = "auxjars"; // used to localize jars
  public static final String OPTION_AUXHBASE = "auxhbase"; // used to localize jars
  public static final String OPTION_JAVA_HOME = "javaHome"; // forward via config.json
  public static final String OPTION_HIVECONF = "hiveconf"; // llap-daemon-site if relevant parameter

  public class LlapOptions {
    private final int instances;
    private final String directory;
    private final String name;
    private final int executors;
    private final long cache;
    private final long size;
    private final long xmx;
    private final String jars;
    private final boolean isHbase;
    private final Properties conf;
    private final String javaPath;

    public LlapOptions(String name, int instances, String directory, int executors, long cache,
                       long size, long xmx, String jars, boolean isHbase,
                       @Nonnull Properties hiveconf, String javaPath)
        throws ParseException {
      if (instances <= 0) {
        throw new ParseException("Invalid configuration: " + instances
            + " (should be greater than 0)");
      }
      this.instances = instances;
      this.directory = directory;
      this.name = name;
      this.executors = executors;
      this.cache = cache;
      this.size = size;
      this.xmx = xmx;
      this.jars = jars;
      this.isHbase = isHbase;
      this.conf = hiveconf;
      this.javaPath = javaPath;
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

    public int getExecutors() {
      return executors;
    }

    public long getCache() {
      return cache;
    }

    public long getSize() {
      return size;
    }

    public long getXmx() {
      return xmx;
    }

    public String getAuxJars() {
      return jars;
    }

    public boolean getIsHBase() {
      return isHbase;
    }

    public Properties getConfig() {
      return conf;
    }

    public String getJavaPath() {
      return javaPath;
    }
  }

  protected static final Logger l4j = LoggerFactory.getLogger(LlapOptionsProcessor.class.getName());
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;

  @SuppressWarnings("static-access")
  public LlapOptionsProcessor() {

    // set the number of instances on which llap should run
    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_INSTANCES).withLongOpt(OPTION_INSTANCES)
        .withDescription("Specify the number of instances to run this on").create('i'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_NAME).withLongOpt(OPTION_NAME)
        .withDescription("Cluster name for YARN registry").create('n'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_DIRECTORY).withLongOpt(OPTION_DIRECTORY)
        .withDescription("Temp directory for jars etc.").create('d'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_ARGS).withLongOpt(OPTION_ARGS)
        .withDescription("java arguments to the llap instance").create('a'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_LOGLEVEL).withLongOpt(OPTION_LOGLEVEL)
        .withDescription("log levels for the llap instance").create('l'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_CHAOS_MONKEY).withLongOpt(OPTION_CHAOS_MONKEY)
        .withDescription("chaosmonkey interval").create('m'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_EXECUTORS).withLongOpt(OPTION_EXECUTORS)
        .withDescription("executor per instance").create('e'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_CACHE).withLongOpt(OPTION_CACHE)
        .withDescription("cache size per instance").create('c'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_SIZE).withLongOpt(OPTION_SIZE)
        .withDescription("container size per instance").create('s'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_XMX).withLongOpt(OPTION_XMX)
        .withDescription("working memory size").create('w'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_AUXJARS).withLongOpt(OPTION_AUXJARS)
        .withDescription("additional jars to package (by default, JSON SerDe jar is packaged"
            + " if available)").create('j'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_AUXHBASE).withLongOpt(OPTION_AUXHBASE)
        .withDescription("whether to package the HBase jars (true by default)").create('h'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_JAVA_HOME).withLongOpt(OPTION_JAVA_HOME)
        .withDescription(
            "Path to the JRE/JDK. This should be installed at the same location on all cluster nodes ($JAVA_HOME, java.home by default)")
        .create());

    // -hiveconf x=y
    options.addOption(OptionBuilder.withValueSeparator().hasArgs(2).withArgName("property=value")
        .withLongOpt(OPTION_HIVECONF)
        .withDescription("Use value for given property. Overridden by explicit parameters")
        .create());

    // [-H|--help]
    options.addOption(new Option("H", "help", false, "Print help information"));
  }

  private static long parseSuffixed(String value) {
    return StringUtils.TraditionalBinaryPrefix.string2long(value);
  }

  public LlapOptions processOptions(String argv[]) throws ParseException, IOException {
    commandLine = new GnuParser().parse(options, argv);
    if (commandLine.hasOption('H') || false == commandLine.hasOption("instances")) {
      // needs at least --instances
      printUsage();
      return null;
    }

    int instances = Integer.parseInt(commandLine.getOptionValue(OPTION_INSTANCES));
    String directory = commandLine.getOptionValue(OPTION_DIRECTORY);
    String jars = commandLine.getOptionValue(OPTION_AUXJARS);

    String name = commandLine.getOptionValue(OPTION_NAME, null);

    final int executors = Integer.parseInt(commandLine.getOptionValue(OPTION_EXECUTORS, "-1"));
    final long cache = parseSuffixed(commandLine.getOptionValue(OPTION_CACHE, "-1"));
    final long size = parseSuffixed(commandLine.getOptionValue(OPTION_SIZE, "-1"));
    final long xmx = parseSuffixed(commandLine.getOptionValue(OPTION_XMX, "-1"));
    final boolean isHbase = Boolean.parseBoolean(commandLine.getOptionValue(OPTION_AUXHBASE, "true"));

    final Properties hiveconf;

    if (commandLine.hasOption(OPTION_HIVECONF)) {
      hiveconf = commandLine.getOptionProperties(OPTION_HIVECONF);
    } else {
      hiveconf = new Properties();
    }

    String javaHome = null;
    if (commandLine.hasOption(OPTION_JAVA_HOME)) {
      javaHome = commandLine.getOptionValue(OPTION_JAVA_HOME);
    }
    // loglevel, chaosmonkey & args are parsed by the python processor

    return new LlapOptions(
        name, instances, directory, executors, cache, size, xmx, jars, isHbase, hiveconf, javaHome);
  }

  private void printUsage() {
    new HelpFormatter().printHelp("llap", options);
  }
}
