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

package org.apache.hadoop.hive.common;

import java.net.URL;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

/**
 * Utilities common to logging operations.
 */
public class LogUtils {

  private static final String HIVE_L4J = "hive-log4j.properties";
  private static final String HIVE_EXEC_L4J = "hive-exec-log4j.properties";
  private static final Log l4j = LogFactory.getLog(LogUtils.class);

  @SuppressWarnings("serial")
  public static class LogInitializationException extends Exception {
    public LogInitializationException(String msg) {
      super(msg);
    }
  }

  /**
   * Initialize log4j.
   *
   * @return an message suitable for display to the user
   * @throws LogInitializationException if log4j fails to initialize correctly
   */
  public static String initHiveLog4j()
    throws LogInitializationException {
    return initHiveLog4jCommon(HiveConf.ConfVars.HIVE_LOG4J_FILE);
  }

  /**
   * Initialize log4j for execution mode.
   *
   * @return an message suitable for display to the user
   * @throws LogInitializationException if log4j-exec fails to initialize correctly
   */
  public static String initHiveExecLog4j()
    throws LogInitializationException {
    return initHiveLog4jCommon(HiveConf.ConfVars.HIVE_EXEC_LOG4J_FILE);
  }

  private static String initHiveLog4jCommon(ConfVars confVarName)
    throws LogInitializationException {
    HiveConf conf = new HiveConf();
    if (HiveConf.getVar(conf, confVarName).equals("")) {
      // if log4j configuration file not set, or could not found, use default setting
      return initHiveLog4jDefault(conf, "", confVarName);
    } else {
      // if log4j configuration file found successfully, use HiveConf property value
      String log4jFileName = HiveConf.getVar(conf, confVarName);
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
        if (confVarName == HiveConf.ConfVars.HIVE_EXEC_LOG4J_FILE) {
          String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID);
          if(queryId == null || (queryId = queryId.trim()).isEmpty()) {
            queryId = "unknown-" + System.currentTimeMillis();
          }
          System.setProperty(HiveConf.ConfVars.HIVEQUERYID.toString(), queryId);
        }
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(log4jFileName);
        logConfigLocation(conf);
        return ("Logging initialized using configuration in " + log4jConfigFile);
      }
    }
  }

  private static String initHiveLog4jDefault(
    HiveConf conf, String logMessage, ConfVars confVarName)
    throws LogInitializationException {
    URL hive_l4j = null;
    switch (confVarName) {
      case HIVE_EXEC_LOG4J_FILE:
        hive_l4j = LogUtils.class.getClassLoader().getResource(HIVE_EXEC_L4J);
        if (hive_l4j == null) {
          hive_l4j = LogUtils.class.getClassLoader().getResource(HIVE_L4J);
        }
        System.setProperty(HiveConf.ConfVars.HIVEQUERYID.toString(),
          HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID));
        break;
      case HIVE_LOG4J_FILE:
        hive_l4j = LogUtils.class.getClassLoader().getResource(HIVE_L4J);
        break;
      default:
        break;
    }
    if (hive_l4j != null) {
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(hive_l4j);
      logConfigLocation(conf);
      return (logMessage + "\n" + "Logging initialized using configuration in " + hive_l4j);
    } else {
      throw new LogInitializationException(
        logMessage + "Unable to initialize logging using "
        + LogUtils.HIVE_L4J + ", not found on CLASSPATH!");
    }
  }

  private static void logConfigLocation(HiveConf conf) throws LogInitializationException {
    // Log a warning if hive-default.xml is found on the classpath
    if (conf.getHiveDefaultLocation() != null) {
      l4j.warn("DEPRECATED: Ignoring hive-default.xml found on the CLASSPATH at "
        + conf.getHiveDefaultLocation().getPath());
    }
    // Look for hive-site.xml on the CLASSPATH and log its location if found.
    if (conf.getHiveSiteLocation() == null) {
      l4j.warn("hive-site.xml not found on CLASSPATH");
    } else {
      l4j.debug("Using hive-site.xml found on CLASSPATH at "
        + conf.getHiveSiteLocation().getPath());
    }
  }
}
