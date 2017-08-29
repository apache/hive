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

import com.google.common.base.Preconditions;
import jline.TerminalFactory;

import java.io.IOException;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.log.LogHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils;

public class LlapOptionsProcessor {

  public static final String OPTION_INSTANCES = "instances"; //forward as arg
  public static final String OPTION_NAME = "name"; // forward as arg
  public static final String OPTION_DIRECTORY = "directory"; // work-dir
  public static final String OPTION_EXECUTORS = "executors"; // llap-daemon-site
  public static final String OPTION_CACHE = "cache"; // llap-daemon-site
  public static final String OPTION_SIZE = "size"; // forward via config.json
  public static final String OPTION_XMX = "xmx"; // forward as arg
  public static final String OPTION_AUXJARS = "auxjars"; // used to localize jars
  public static final String OPTION_AUXHIVE = "auxhive"; // used to localize jars
  public static final String OPTION_AUXHBASE = "auxhbase"; // used to localize jars
  public static final String OPTION_JAVA_HOME = "javaHome"; // forward via config.json
  public static final String OPTION_HIVECONF = "hiveconf"; // llap-daemon-site if relevant parameter
  public static final String OPTION_SLIDER_AM_CONTAINER_MB = "slider-am-container-mb"; // forward as arg
  public static final String OPTION_SLIDER_APPCONFIG_GLOBAL = "slider-appconfig-global"; // forward as arg
  public static final String OPTION_LLAP_QUEUE = "queue"; // forward via config.json
  public static final String OPTION_IO_THREADS = "iothreads"; // llap-daemon-site

  // Options for the python script that are here because our option parser cannot ignore the unknown ones
  public static final String OPTION_ARGS = "args"; // forward as arg
  public static final String OPTION_LOGLEVEL = "loglevel"; // forward as arg
  public static final String OPTION_LOGGER = "logger"; // forward as arg
  public static final String OPTION_CHAOS_MONKEY = "chaosmonkey"; // forward as arg
  public static final String OPTION_SLIDER_KEYTAB_DIR = "slider-keytab-dir";
  public static final String OPTION_SLIDER_KEYTAB = "slider-keytab";
  public static final String OPTION_SLIDER_PRINCIPAL = "slider-principal";
  public static final String OPTION_SLIDER_PLACEMENT = "slider-placement";
  public static final String OPTION_SLIDER_DEFAULT_KEYTAB = "slider-default-keytab";
  public static final String OPTION_OUTPUT_DIR = "output";
  public static final String OPTION_START = "startImmediately";

  public static class LlapOptions {
    private final int instances;
    private final String directory;
    private final String name;
    private final int executors;
    private final int ioThreads;
    private final long cache;
    private final long size;
    private final long xmx;
    private final String jars;
    private final boolean isHbase;
    private final Properties conf;
    private final String javaPath;
    private final String llapQueueName;
    private final String logger;
    private final boolean isStarting;
    private final String output;
    private final boolean isHiveAux;

    public LlapOptions(String name, int instances, String directory, int executors, int ioThreads,
        long cache, long size, long xmx, String jars, boolean isHbase,
        @Nonnull Properties hiveconf, String javaPath, String llapQueueName, String logger,
        boolean isStarting, String output, boolean isHiveAux) throws ParseException {
      if (instances <= 0) {
        throw new ParseException("Invalid configuration: " + instances
            + " (should be greater than 0)");
      }
      this.instances = instances;
      this.directory = directory;
      this.name = name;
      this.executors = executors;
      this.ioThreads = ioThreads;
      this.cache = cache;
      this.size = size;
      this.xmx = xmx;
      this.jars = jars;
      this.isHbase = isHbase;
      this.isHiveAux = isHiveAux;
      this.conf = hiveconf;
      this.javaPath = javaPath;
      this.llapQueueName = llapQueueName;
      this.logger = logger;
      this.isStarting = isStarting;
      this.output = output;
    }

    public String getOutput() {
      return output;
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

    public int getIoThreads() {
      return ioThreads;
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

    public boolean getIsHiveAux() {
      return isHiveAux;
    }

    public Properties getConfig() {
      return conf;
    }

    public String getJavaPath() {
      return javaPath;
    }

    public String getLlapQueueName() {
      return llapQueueName;
    }

    public String getLogger() {
      return logger;
    }

    public boolean isStarting() {
      return isStarting;
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

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_LOGGER).withLongOpt(OPTION_LOGGER)
        .withDescription(
            "logger for llap instance ([" + LogHelpers.LLAP_LOGGER_NAME_RFA + "], " +
                LogHelpers.LLAP_LOGGER_NAME_QUERY_ROUTING + ", " + LogHelpers.LLAP_LOGGER_NAME_CONSOLE)
        .create());

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_CHAOS_MONKEY).withLongOpt(OPTION_CHAOS_MONKEY)
        .withDescription("chaosmonkey interval").create('m'));

    options.addOption(OptionBuilder.hasArg(false).withArgName(OPTION_SLIDER_DEFAULT_KEYTAB).withLongOpt(OPTION_SLIDER_DEFAULT_KEYTAB)
        .withDescription("try to set default settings for Slider AM keytab; mostly for dev testing").create());

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_SLIDER_KEYTAB_DIR).withLongOpt(OPTION_SLIDER_KEYTAB_DIR)
        .withDescription("Slider AM keytab directory on HDFS (where the headless user keytab is stored by Slider keytab installation, e.g. .slider/keytabs/llap)").create());

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_SLIDER_KEYTAB).withLongOpt(OPTION_SLIDER_KEYTAB)
        .withDescription("Slider AM keytab file name inside " + OPTION_SLIDER_KEYTAB_DIR).create());

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_SLIDER_PRINCIPAL).withLongOpt(OPTION_SLIDER_PRINCIPAL)
        .withDescription("Slider AM principal; should be the user running the cluster, e.g. hive@EXAMPLE.COM").create());

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_SLIDER_PLACEMENT).withLongOpt(OPTION_SLIDER_PLACEMENT)
        .withDescription("Slider placement policy; see slider documentation at https://slider.incubator.apache.org/docs/placement.html."
          + " 4 means anti-affinity (the default; unnecessary if LLAP is going to take more than half of the YARN capacity of a node), 0 is normal.").create());

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_EXECUTORS).withLongOpt(OPTION_EXECUTORS)
        .withDescription("executor per instance").create('e'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_CACHE).withLongOpt(OPTION_CACHE)
        .withDescription("cache size per instance").create('c'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_SIZE).withLongOpt(OPTION_SIZE)
        .withDescription("container size per instance").create('s'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_XMX).withLongOpt(OPTION_XMX)
        .withDescription("working memory size").create('w'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_LLAP_QUEUE)
        .withLongOpt(OPTION_LLAP_QUEUE)
        .withDescription("The queue within which LLAP will be started").create('q'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_OUTPUT_DIR)
        .withLongOpt(OPTION_OUTPUT_DIR)
        .withDescription("Output directory for the generated scripts").create());

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_AUXJARS).withLongOpt(OPTION_AUXJARS)
        .withDescription("additional jars to package (by default, JSON SerDe jar is packaged"
            + " if available)").create('j'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_AUXHBASE).withLongOpt(OPTION_AUXHBASE)
        .withDescription("whether to package the HBase jars (true by default)").create('h'));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_AUXHIVE).withLongOpt(OPTION_AUXHIVE)
        .withDescription("whether to package the Hive aux jars (true by default)").create(OPTION_AUXHIVE));

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_JAVA_HOME).withLongOpt(OPTION_JAVA_HOME)
        .withDescription(
            "Path to the JRE/JDK. This should be installed at the same location on all cluster nodes ($JAVA_HOME, java.home by default)")
        .create());

    // -hiveconf x=y
    options.addOption(OptionBuilder.withValueSeparator().hasArgs(2).withArgName("property=value")
        .withLongOpt(OPTION_HIVECONF)
        .withDescription("Use value for given property. Overridden by explicit parameters")
        .create());

    options.addOption(OptionBuilder.hasArg().withArgName("b")
        .withLongOpt(OPTION_SLIDER_AM_CONTAINER_MB)
        .withDescription("The size of the slider AppMaster container in MB").create('b'));

    options.addOption(OptionBuilder.withValueSeparator().hasArgs(2).withArgName("property=value")
        .withLongOpt(OPTION_SLIDER_APPCONFIG_GLOBAL)
        .withDescription("Property (key=value) to be set in the global section of the Slider appConfig")
        .create());

    options.addOption(OptionBuilder.hasArg().withArgName(OPTION_IO_THREADS)
        .withLongOpt(OPTION_IO_THREADS).withDescription("executor per instance").create('t'));

    options.addOption(OptionBuilder.hasArg(false).withArgName(OPTION_START)
        .withLongOpt(OPTION_START).withDescription("immediately start the cluster")
        .create('z'));

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
    final int ioThreads = Integer.parseInt(
        commandLine.getOptionValue(OPTION_IO_THREADS, Integer.toString(executors)));
    final long cache = parseSuffixed(commandLine.getOptionValue(OPTION_CACHE, "-1"));
    final long size = parseSuffixed(commandLine.getOptionValue(OPTION_SIZE, "-1"));
    final long xmx = parseSuffixed(commandLine.getOptionValue(OPTION_XMX, "-1"));
    final boolean isHbase = Boolean.parseBoolean(
        commandLine.getOptionValue(OPTION_AUXHBASE, "true"));
    final boolean isHiveAux = Boolean.parseBoolean(
        commandLine.getOptionValue(OPTION_AUXHIVE, "true"));
    final boolean doStart = commandLine.hasOption(OPTION_START);
    final String output = commandLine.getOptionValue(OPTION_OUTPUT_DIR, null);

    final String queueName = commandLine.getOptionValue(OPTION_LLAP_QUEUE,
        HiveConf.ConfVars.LLAP_DAEMON_QUEUE_NAME.getDefaultValue());

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

    String logger = null;
    if (commandLine.hasOption(OPTION_LOGGER)) {
      logger = commandLine.getOptionValue(OPTION_LOGGER);
      Preconditions.checkArgument(
          logger.equalsIgnoreCase(LogHelpers.LLAP_LOGGER_NAME_QUERY_ROUTING) ||
              logger.equalsIgnoreCase(LogHelpers.LLAP_LOGGER_NAME_CONSOLE) ||
              logger.equalsIgnoreCase(LogHelpers.LLAP_LOGGER_NAME_RFA));
    }

    // loglevel, chaosmonkey & args are parsed by the python processor

    return new LlapOptions(name, instances, directory, executors, ioThreads, cache, size, xmx,
        jars, isHbase, hiveconf, javaHome, queueName, logger, doStart, output, isHiveAux);
  }

  private void printUsage() {
    HelpFormatter hf = new HelpFormatter();
    try {
      int width = hf.getWidth();
      int jlineWidth = TerminalFactory.get().getWidth();
      width = Math.min(160, Math.max(jlineWidth, width)); // Ignore potentially incorrect values
      hf.setWidth(width);
    } catch (Throwable t) { // Ignore
    }
    hf.printHelp("llap", options);
  }
}
