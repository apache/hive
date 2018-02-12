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

package org.apache.hadoop.hive.metastore.utils;

import java.io.File;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.spi.DefaultThreadContextMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities common to logging operations.
 */
public class LogUtils {

  private static final String HIVE_L4J = "metastore-log4j2.properties";
  private static final Logger l4j = LoggerFactory.getLogger(LogUtils.class);

  @SuppressWarnings("serial")
  public static class LogInitializationException extends Exception {
    LogInitializationException(String msg) {
      super(msg);
    }
  }

  /**
   * Initialize log4j.
   *
   * @return an message suitable for display to the user
   * @throws LogInitializationException if log4j fails to initialize correctly
   */
  public static String initHiveLog4j(Configuration conf)
    throws LogInitializationException {
    return initHiveLog4jCommon(conf, MetastoreConf.ConfVars.LOG4J_FILE);
  }

  private static String initHiveLog4jCommon(Configuration conf, ConfVars confVarName)
    throws LogInitializationException {
    if (MetastoreConf.getVar(conf, confVarName).equals("")) {
      // if log4j configuration file not set, or could not found, use default setting
      return initHiveLog4jDefault(conf, "", confVarName);
    } else {
      // if log4j configuration file found successfully, use HiveConf property value
      String log4jFileName = MetastoreConf.getVar(conf, confVarName);
      File log4jConfigFile = new File(log4jFileName);
      boolean fileExists = log4jConfigFile.exists();
      if (!fileExists) {
        // if property specified file not found in local file system
        // use default setting
        return initHiveLog4jDefault(
          conf, "Not able to find conf file: " + log4jConfigFile, confVarName);
      } else {
        // property speficied file found in local file system
        // use the specified file
        final boolean async = checkAndSetAsyncLogging(conf);
        // required for MDC based routing appender so that child threads can inherit the MDC context
        System.setProperty(DefaultThreadContextMap.INHERITABLE_MAP, "true");
        Configurator.initialize(null, log4jFileName);
        logConfigLocation();
        return "Logging initialized using configuration in " + log4jConfigFile + " Async: " + async;
      }
    }
  }

  private static boolean checkAndSetAsyncLogging(final Configuration conf) {
    final boolean asyncLogging = MetastoreConf.getBoolVar(conf, ConfVars.ASYNC_LOG_ENABLED);
    if (asyncLogging) {
      System.setProperty("Log4jContextSelector",
          "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
      // default is ClassLoaderContextSelector which is created during automatic logging
      // initialization in a static initialization block.
      // Changing ContextSelector at runtime requires creating new context factory which will
      // internally create new context selector based on system property.
      LogManager.setFactory(new Log4jContextFactory());
    }
    return asyncLogging;
  }

  private static String initHiveLog4jDefault(Configuration conf, String logMessage, ConfVars confVarName)
    throws LogInitializationException {
    URL hive_l4j = null;
    switch (confVarName) {
      case LOG4J_FILE:
        hive_l4j = LogUtils.class.getClassLoader().getResource(HIVE_L4J);
        break;
      default:
        break;
    }
    if (hive_l4j != null) {
      final boolean async = checkAndSetAsyncLogging(conf);
      System.setProperty(DefaultThreadContextMap.INHERITABLE_MAP, "true");
      Configurator.initialize(null, hive_l4j.toString());
      logConfigLocation();
      return (logMessage + "\n" + "Logging initialized using configuration in " + hive_l4j +
          " Async: " + async);
    } else {
      throw new LogInitializationException(
        logMessage + "Unable to initialize logging using "
        + LogUtils.HIVE_L4J + ", not found on CLASSPATH!");
    }
  }

  private static void logConfigLocation() throws LogInitializationException {
    // Log a warning if hive-default.xml is found on the classpath
    if (MetastoreConf.getHiveDefaultLocation() != null) {
      l4j.warn("DEPRECATED: Ignoring hive-default.xml found on the CLASSPATH at "
        + MetastoreConf.getHiveDefaultLocation().getPath());
    }
    // Look for hive-site.xml on the CLASSPATH and log its location if found.
    if (MetastoreConf.getHiveSiteLocation() == null) {
      l4j.warn("hive-site.xml not found on CLASSPATH");
    } else {
      l4j.debug("Using hive-site.xml found on CLASSPATH at "
        + MetastoreConf.getHiveSiteLocation().getPath());
    }
  }
}
