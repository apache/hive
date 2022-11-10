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

package org.apache.hadoop.hive.llap.cli.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import jline.TerminalFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.log.LogHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;

@SuppressWarnings("static-access")
class LlapServiceCommandLine {
  private static final Logger LOG = LoggerFactory.getLogger(LlapServiceCommandLine.class.getName());

  private static final Option DIRECTORY = OptionBuilder
      .withLongOpt("directory")
      .withDescription("Temp directory for jars etc.")
      .withArgName("directory")
      .hasArg()
      .create('d');

  private static final Option NAME = OptionBuilder
      .withLongOpt("name")
      .withDescription("Cluster name for YARN registry")
      .withArgName("name")
      .hasArg()
      .create('n');

  private static final Option EXECUTORS = OptionBuilder
      .withLongOpt("executors")
      .withDescription("executor per instance")
      .withArgName("executors")
      .hasArg()
      .create('e');

  private static final Option IO_THREADS = OptionBuilder
      .withLongOpt("iothreads")
      .withDescription("iothreads per instance")
      .withArgName("iothreads")
      .hasArg()
      .create('t');

  private static final Option CACHE = OptionBuilder
      .withLongOpt("cache")
      .withDescription("cache size per instance")
      .withArgName("cache")
      .hasArg()
      .create('c');

  private static final Option SIZE = OptionBuilder
      .withLongOpt("size")
      .withDescription("cache size per instance")
      .withArgName("size")
      .hasArg()
      .create('s');

  private static final Option XMX = OptionBuilder
      .withLongOpt("xmx")
      .withDescription("working memory size")
      .withArgName("xmx")
      .hasArg()
      .create('w');

  private static final Option AUXJARS = OptionBuilder
      .withLongOpt("auxjars")
      .withDescription("additional jars to package (by default, JSON SerDe jar is packaged if available)")
      .withArgName("auxjars")
      .hasArg()
      .create('j');

  private static final Option AUXHBASE = OptionBuilder
      .withLongOpt("auxhbase")
      .withDescription("whether to package the HBase jars (true by default)")
      .withArgName("auxhbase")
      .hasArg()
      .create('h');

  private static final Option HBASEJARS = OptionBuilder
      .withLongOpt("hbasejars")
      .withDescription("HBase mapredcp jars to package")
      .withArgName("hbasejars")
      .hasArg()
      .create('k');

  private static final Option HIVECONF = OptionBuilder
      .withLongOpt("hiveconf")
      .withDescription("Use value for given property. Overridden by explicit parameters")
      .withArgName("property=value")
      .hasArgs(2)
      .withValueSeparator()
      .create();

  private static final Option JAVAHOME = OptionBuilder
      .withLongOpt("javaHome")
      .withDescription("Path to the JRE/JDK. This should be installed at the same location on all cluster nodes " +
          "($JAVA_HOME, java.home by default)")
      .withArgName("javaHome")
      .hasArg()
      .create();

  private static final Option QUEUE = OptionBuilder
      .withLongOpt("queue")
      .withDescription("The queue within which LLAP will be started")
      .withArgName("queue")
      .hasArg()
      .create('q');

  private static final Set<String> VALID_LOGGERS = ImmutableSet.of(LogHelpers.LLAP_LOGGER_NAME_RFA.toLowerCase(),
      LogHelpers.LLAP_LOGGER_NAME_QUERY_ROUTING.toLowerCase(), LogHelpers.LLAP_LOGGER_NAME_CONSOLE.toLowerCase());

  private static final Option LOGGER = OptionBuilder
      .withLongOpt("logger")
      .withDescription("logger for llap instance ([" + VALID_LOGGERS + "]")
      .withArgName("logger")
      .hasArg()
      .create();

  private static final Option START = OptionBuilder
      .withLongOpt("startImmediately")
      .withDescription("immediately start the cluster")
      .withArgName("startImmediately")
      .hasArg(false)
      .create('z');

  private static final Option OUTPUT = OptionBuilder
      .withLongOpt("output")
      .withDescription("Output directory for the generated scripts")
      .withArgName("output")
      .hasArg()
      .create();

  private static final Option AUXHIVE = OptionBuilder
      .withLongOpt("auxhive")
      .withDescription("whether to package the Hive aux jars (true by default)")
      .withArgName("auxhive")
      .hasArg()
      .create("auxhive");

  private static final Option HELP = OptionBuilder
      .withLongOpt("help")
      .withDescription("Print help information")
      .withArgName("help")
      .hasArg(false)
      .create('H');

  // Options for the python script that are here because our option parser cannot ignore the unknown ones
  private static final String OPTION_INSTANCES = "instances";
  private static final String OPTION_ARGS = "args";
  private static final String OPTION_LOGLEVEL = "loglevel";
  private static final String OPTION_SERVICE_KEYTAB_DIR = "service-keytab-dir";
  private static final String OPTION_SERVICE_KEYTAB = "service-keytab";
  private static final String OPTION_SERVICE_PRINCIPAL = "service-principal";
  private static final String OPTION_SERVICE_PLACEMENT = "service-placement";
  private static final String OPTION_SERVICE_DEFAULT_KEYTAB = "service-default-keytab";
  private static final String OPTION_HEALTH_PERCENT = "health-percent";
  private static final String OPTION_HEALTH_TIME_WINDOW_SECS = "health-time-window-secs";
  private static final String OPTION_HEALTH_INIT_DELAY_SECS = "health-init-delay-secs";
  private static final String OPTION_SERVICE_AM_CONTAINER_MB = "service-am-container-mb";
  private static final String OPTION_SERVICE_APPCONFIG_GLOBAL = "service-appconfig-global";

  private static final Options OPTIONS = new Options();
  static {
    OPTIONS.addOption(DIRECTORY);
    OPTIONS.addOption(NAME);
    OPTIONS.addOption(EXECUTORS);
    OPTIONS.addOption(IO_THREADS);
    OPTIONS.addOption(CACHE);
    OPTIONS.addOption(SIZE);
    OPTIONS.addOption(XMX);
    OPTIONS.addOption(AUXJARS);
    OPTIONS.addOption(AUXHBASE);
    OPTIONS.addOption(HBASEJARS);
    OPTIONS.addOption(HIVECONF);
    OPTIONS.addOption(JAVAHOME);
    OPTIONS.addOption(QUEUE);
    OPTIONS.addOption(LOGGER);
    OPTIONS.addOption(START);
    OPTIONS.addOption(OUTPUT);
    OPTIONS.addOption(AUXHIVE);
    OPTIONS.addOption(HELP);

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_INSTANCES)
        .withDescription("Specify the number of instances to run this on")
        .withArgName(OPTION_INSTANCES)
        .hasArg()
        .create('i'));

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_ARGS)
        .withDescription("java arguments to the llap instance")
        .withArgName(OPTION_ARGS)
        .hasArg()
        .create('a'));

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_LOGLEVEL)
        .withDescription("log levels for the llap instance")
        .withArgName(OPTION_LOGLEVEL)
        .hasArg()
        .create('l'));

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_SERVICE_KEYTAB_DIR)
        .withDescription("Service AM keytab directory on HDFS (where the headless user keytab is stored by Service " +
            "keytab installation, e.g. .yarn/keytabs/llap)")
        .withArgName(OPTION_SERVICE_KEYTAB_DIR)
        .hasArg()
        .create());

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_SERVICE_KEYTAB)
        .withDescription("Service AM keytab file name inside " + OPTION_SERVICE_KEYTAB_DIR)
        .withArgName(OPTION_SERVICE_KEYTAB)
        .hasArg()
        .create());

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_SERVICE_PRINCIPAL)
        .withDescription("Service AM principal; should be the user running the cluster, e.g. hive@EXAMPLE.COM")
        .withArgName(OPTION_SERVICE_PRINCIPAL)
        .hasArg()
        .create());

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_SERVICE_PLACEMENT)
        .withDescription("Service placement policy; see YARN documentation at " +
            "https://issues.apache.org/jira/browse/YARN-1042. This is unnecessary if LLAP is going to take more than " +
            "half of the YARN capacity of a node.")
        .withArgName(OPTION_SERVICE_PLACEMENT)
        .hasArg()
        .create());

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_SERVICE_DEFAULT_KEYTAB)
        .withDescription("try to set default settings for Service AM keytab; mostly for dev testing")
        .withArgName(OPTION_SERVICE_DEFAULT_KEYTAB)
        .hasArg(false)
        .create());

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_HEALTH_PERCENT)
        .withDescription("Percentage of running containers after which LLAP application is considered healthy" +
            " (Default: 80)")
        .withArgName(OPTION_HEALTH_PERCENT)
        .hasArg()
        .create());

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_HEALTH_TIME_WINDOW_SECS)
        .withDescription("Time window in seconds (after initial delay) for which LLAP application is allowed to be " +
            "in unhealthy state before being killed (Default: 300)")
        .withArgName(OPTION_HEALTH_TIME_WINDOW_SECS)
        .hasArg()
        .create());

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_HEALTH_INIT_DELAY_SECS)
        .withDescription("Delay in seconds after which health percentage is monitored (Default: 400)")
        .withArgName(OPTION_HEALTH_INIT_DELAY_SECS)
        .hasArg()
        .create());

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_SERVICE_AM_CONTAINER_MB)
        .withDescription("The size of the service AppMaster container in MB")
        .withArgName("b")
        .hasArg()
        .create('b'));

    OPTIONS.addOption(OptionBuilder
        .withLongOpt(OPTION_SERVICE_APPCONFIG_GLOBAL)
        .withDescription("Property (key=value) to be set in the global section of the Service appConfig")
        .withArgName("property=value")
        .hasArgs(2)
        .withValueSeparator()
        .create());
  }

  private String[] args;

  private String directory;
  private String name;
  private int executors;
  private int ioThreads;
  private long cache;
  private long size;
  private long xmx;
  private String jars;
  private String hbaseJars;
  private boolean isHbase;
  private Properties conf = new Properties();
  private String javaPath = null;
  private String llapQueueName;
  private String logger = null;
  private boolean isStarting;
  private String output;
  private boolean isHiveAux;
  private boolean isHelp;

  static LlapServiceCommandLine parseArguments(String[] args) {
    LlapServiceCommandLine cl = null;
    try {
      cl = new LlapServiceCommandLine(args);
    } catch (Exception e) {
      LOG.error("Parsing the command line arguments failed", e);
      printUsage();
      System.exit(1);
    }

    if (cl.isHelp) {
      printUsage();
      System.exit(0);
    }

    return cl;
  }

  LlapServiceCommandLine(String[] args) throws ParseException {
    LOG.info("LLAP invoked with arguments = {}", Arrays.toString(args));
    this.args = args;
    parseCommandLine(args);
  }

  private void parseCommandLine(String[] args) throws ParseException {
    CommandLine cl = new GnuParser().parse(OPTIONS, args);
    if (cl.hasOption(HELP.getOpt())) {
      isHelp = true;
      return;
    }

    if (!cl.hasOption(OPTION_INSTANCES)) {
      printUsage();
      throw new ParseException("instance must be set");
    }

    int instances = Integer.parseInt(cl.getOptionValue(OPTION_INSTANCES));
    if (instances <= 0) {
      throw new ParseException("Invalid configuration: " + instances + " (should be greater than 0)");
    }

    directory = cl.getOptionValue(DIRECTORY.getOpt());
    name = cl.getOptionValue(NAME.getOpt());
    executors = Integer.parseInt(cl.getOptionValue(EXECUTORS.getOpt(), "-1"));
    ioThreads = Integer.parseInt(cl.getOptionValue(IO_THREADS.getOpt(), Integer.toString(executors)));
    cache = TraditionalBinaryPrefix.string2long(cl.getOptionValue(CACHE.getOpt(), "-1"));
    size = TraditionalBinaryPrefix.string2long(cl.getOptionValue(SIZE.getOpt(), "-1"));
    xmx = TraditionalBinaryPrefix.string2long(cl.getOptionValue(XMX.getOpt(), "-1"));
    jars = cl.getOptionValue(AUXJARS.getOpt());
    hbaseJars = cl.getOptionValue(HBASEJARS.getOpt());
    isHbase = Boolean.parseBoolean(cl.getOptionValue(AUXHBASE.getOpt(), "true"));
    if (cl.hasOption(HIVECONF.getLongOpt())) {
      conf = cl.getOptionProperties(HIVECONF.getLongOpt());
    }
    if (cl.hasOption(JAVAHOME.getLongOpt())) {
      javaPath = cl.getOptionValue(JAVAHOME.getLongOpt());
    }
    llapQueueName = cl.getOptionValue(QUEUE.getOpt(), ConfVars.LLAP_DAEMON_QUEUE_NAME.getDefaultValue());
    if (cl.hasOption(LOGGER.getLongOpt())) {
      logger = cl.getOptionValue(LOGGER.getLongOpt());
      Preconditions.checkArgument(VALID_LOGGERS.contains(logger.toLowerCase()));
    }
    isStarting = cl.hasOption(START.getOpt());
    output = cl.getOptionValue(OUTPUT.getLongOpt());
    isHiveAux = Boolean.parseBoolean(cl.getOptionValue(AUXHIVE.getOpt(), "true"));
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

    hf.printHelp("llap", OPTIONS);
  }

  String[] getArgs() {
    return args;
  }

  String getDirectory() {
    return directory;
  }

  String getName() {
    return name;
  }

  int getExecutors() {
    return executors;
  }

  int getIoThreads() {
    return ioThreads;
  }

  long getCache() {
    return cache;
  }

  long getSize() {
    return size;
  }

  long getXmx() {
    return xmx;
  }

  String getAuxJars() {
    return jars;
  }

  String getHBaseJars() {
    return hbaseJars;
  }

  boolean getIsHBase() {
    return isHbase;
  }

  boolean getIsHiveAux() {
    return isHiveAux;
  }

  Properties getConfig() {
    return conf;
  }

  String getJavaPath() {
    return javaPath;
  }

  String getLlapQueueName() {
    return llapQueueName;
  }

  String getLogger() {
    return logger;
  }

  boolean isStarting() {
    return isStarting;
  }

  String getOutput() {
    return output;
  }
}
