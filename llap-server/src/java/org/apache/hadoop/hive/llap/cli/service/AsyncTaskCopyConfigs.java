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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.impl.LlapConstants;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Copy config files for the tarball. */
class AsyncTaskCopyConfigs implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskCopyConfigs.class.getName());

  private final LlapServiceCommandLine cl;
  private final HiveConf conf;
  private final Properties directProperties;
  private final FileSystem rawFs;
  private final Path confDir;

  AsyncTaskCopyConfigs(LlapServiceCommandLine cl, HiveConf conf, Properties directProperties, FileSystem rawFs,
      Path confDir) {
    this.cl = cl;
    this.conf = conf;
    this.directProperties = directProperties;
    this.rawFs = rawFs;
    this.confDir = confDir;
  }

  @Override
  public Void call() throws Exception {
    // Copy over the mandatory configs for the package.
    for (String f : LlapDaemonConfiguration.DAEMON_CONFIGS) {
      copyConfig(f);
    }
    for (String f : LlapDaemonConfiguration.SSL_DAEMON_CONFIGS) {
      try {
        copyConfig(f);
      } catch (Throwable t) {
        LOG.info("Error getting an optional config " + f + "; ignoring: " + t.getMessage());
      }
    }
    createLlapDaemonConfig();
    setUpLoggerConfig();
    setUpMetricsConfig();
    return null;
  }

  private void copyConfig(String f) throws IOException {
    HiveConf.getBoolVar(new Configuration(false), ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS);
    // they will be file:// URLs
    rawFs.copyFromLocalFile(new Path(conf.getResource(f).toString()), confDir);
  }

  private void createLlapDaemonConfig() throws IOException {
    FSDataOutputStream confStream = rawFs.create(new Path(confDir, LlapDaemonConfiguration.LLAP_DAEMON_SITE));

    Configuration llapDaemonConf = resolve();

    llapDaemonConf.writeXml(confStream);
    confStream.close();
  }

  private Configuration resolve() {
    Configuration target = new Configuration(false);

    populateConf(target, cl.getConfig(), "CLI hiveconf");
    populateConf(target, directProperties, "CLI direct");

    return target;
  }

  private void populateConf(Configuration target, Properties properties, String source) {
    for (Entry<Object, Object> entry : properties.entrySet()) {
      String key = (String) entry.getKey();
      String val = conf.get(key);
      if (val != null) {
        target.set(key, val, source);
      }
    }
  }

  private void setUpLoggerConfig() throws Exception {
    // logger can be a resource stream or a real file (cannot use copy)
    URL logger = conf.getResource(LlapConstants.LOG4j2_PROPERTIES_FILE);
    if (null == logger) {
      throw new Exception("Unable to find required config file: llap-daemon-log4j2.properties");
    }
    InputStream loggerContent = logger.openStream();
    IOUtils.copyBytes(loggerContent,
        rawFs.create(new Path(confDir, "llap-daemon-log4j2.properties"), true), conf, true);
  }

  private void setUpMetricsConfig() throws IOException {
    String metricsFile = LlapConstants.LLAP_HADOOP_METRICS2_PROPERTIES_FILE;
    URL metrics2 = conf.getResource(metricsFile);
    if (metrics2 == null) {
      LOG.warn(LlapConstants.LLAP_HADOOP_METRICS2_PROPERTIES_FILE + " cannot be found." +
          " Looking for " + LlapConstants.HADOOP_METRICS2_PROPERTIES_FILE);
      metricsFile = LlapConstants.HADOOP_METRICS2_PROPERTIES_FILE;
      metrics2 = conf.getResource(metricsFile);
    }
    if (metrics2 != null) {
      InputStream metrics2FileStream = metrics2.openStream();
      IOUtils.copyBytes(metrics2FileStream, rawFs.create(new Path(confDir, metricsFile), true), conf, true);
      LOG.info("Copied hadoop metrics2 properties file from " + metrics2);
    } else {
      LOG.warn("Cannot find " + LlapConstants.LLAP_HADOOP_METRICS2_PROPERTIES_FILE + " or " +
          LlapConstants.HADOOP_METRICS2_PROPERTIES_FILE + " in classpath.");
    }
  }
}
