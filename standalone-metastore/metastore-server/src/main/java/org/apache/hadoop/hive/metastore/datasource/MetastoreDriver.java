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

package org.apache.hadoop.hive.metastore.datasource;

import com.google.common.annotations.VisibleForTesting;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.slf4j.LoggerFactory;

import static java.sql.DriverManager.registerDriver;

public class MetastoreDriver implements Driver {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MetastoreDriver.class);
  private static final String URL_PREFIX = "jdbc:metastore://";
  private static int majorVersion = -1;
  private static int minorVersion = -1;
  private static final Configuration configuration;
  private static final Driver delegateDriver;
  private static final String jdbcUrl;
  static {
    try {
      registerDriver(new MetastoreDriver());
      String versionString = MetastoreVersionInfo.getVersion();
      String[] versionNums = versionString.split("\\.");
      if (NumberUtils.isNumber(versionNums[0])) {
        majorVersion = Integer.parseInt(versionNums[0]);
      }
      if (versionNums.length >1 && NumberUtils.isNumber(versionNums[1])) {
        minorVersion = Integer.parseInt(versionNums[1]);
      }
      configuration = MetastoreConf.newMetastoreConf();
      jdbcUrl = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CONNECT_URL_KEY);
      delegateDriver = findRegisteredDriver(configuration);
    } catch (Exception e) {
      throw new RuntimeException("Failed to register Metastore driver", e);
    }
  }

  private static Driver findRegisteredDriver(Configuration configuration) throws SQLException {
    List<Driver> candidates = new ArrayList<>();
    for (Enumeration<Driver> drivers = DriverManager.getDrivers(); drivers.hasMoreElements();) {
      Driver driver = drivers.nextElement();
      try {
        if (driver.acceptsURL(jdbcUrl)) {
          candidates.add(driver);
        }
      } catch (Exception e) {
      }
    }

    String driverClassName = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CONNECTION_DRIVER);
    if (candidates.isEmpty()) {
      Class<Driver> driverClz = tryLoadDriver(driverClassName, Thread.currentThread().getContextClassLoader(),
          MetastoreDriver.class.getClassLoader());
      if (driverClz != null) {
        try {
          Driver driver = driverClz.getDeclaredConstructor().newInstance();
          if (!driver.acceptsURL(jdbcUrl)) {
            throw new Error("Driver " + driverClassName + " cannot accept jdbcUrl");
          }
          candidates.add(driver);
        } catch (Exception e) {
          LOG.warn("Failed to create instance of driver class {}", driverClassName, e);
        }
      }
    }
    return candidates.isEmpty() ? DriverManager.getDriver(jdbcUrl) : candidates.getFirst();
  }

  private static Class<Driver> tryLoadDriver(String driverClassName, ClassLoader... loaders) {
    for (ClassLoader loader : loaders) {
      if (loader != null) {
        try {
          return (Class<Driver>) loader.loadClass(driverClassName);
        } catch (ClassNotFoundException e) {
          LOG.debug("Driver class {} not found in class loader {}", driverClassName, loader);
        }
      }
    }
    return null;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return new MetastoreConnection(delegateDriver.connect(jdbcUrl, info), configuration);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return Pattern.matches(URL_PREFIX + ".*", url);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    // An empty array if no properties are required.
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return majorVersion;
  }

  @Override
  public int getMinorVersion() {
    return minorVersion;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegateDriver.getParentLogger();
  }

  public static String getMetastoreDbUrl(Configuration configuration) {
    return MetastoreDriver.URL_PREFIX + "internal-delegate-url";
  }

  @VisibleForTesting
  static Configuration getConfiguration() {
    return configuration;
  }
}
