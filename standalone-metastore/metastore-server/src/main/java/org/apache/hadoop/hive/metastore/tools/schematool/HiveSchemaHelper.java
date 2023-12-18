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
package org.apache.hadoop.hive.metastore.tools.schematool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveSchemaHelper {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaHelper.class);
  private static final Pattern dbTypePattern = Pattern.compile("jdbc:(.+?)(?:ql)?:");

  public static final String DB_DERBY = "derby";
  public static final String DB_HIVE = "hive";
  public static final String DB_MSSQL = "mssql";
  public static final String DB_MYSQL = "mysql";
  public static final String DB_POSTGRACE = "postgres";
  public static final String DB_ORACLE = "oracle";
  public static final String EMBEDDED_HS2_URL =
      "jdbc:hive2://?hive.conf.restricted.list=;hive.security.authorization.sqlstd.confwhitelist=.*;"
      + "hive.security.authorization.sqlstd.confwhitelist.append=.*;hive.security.authorization.enabled=false;"
      + "hive.metastore.uris=;hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory;"
      + "hive.support.concurrency=false;hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;"
      + "hive.metastore.rawstore.impl=org.apache.hadoop.hive.metastore.ObjectStore";
  public static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

  /***
   * Get JDBC connection to metastore db
   * @param userName metastore connection username
   * @param password metastore connection password
   * @param url Metastore URL.  If null will be read from config file.
   * @param driver Driver class.  If null will be read from config file.
   * @param printInfo print connection parameters
   * @param conf hive config object
   * @param schema the schema to create the connection for
   * @return metastore connection object
   * @throws org.apache.hadoop.hive.metastore.HiveMetaException
   */
  private static Connection getConnectionToMetastore(String userName, String password, String url,
      String driver, boolean printInfo, Configuration conf, String schema) throws HiveMetaException {
    try {
      url = url == null ? getValidConfVar(MetastoreConf.ConfVars.CONNECT_URL_KEY, conf) : url;
      driver = driver == null ? getValidConfVar(MetastoreConf.ConfVars.CONNECTION_DRIVER, conf) : driver;
      if (printInfo) {
        logAndPrintToStdout("Metastore connection URL:\t " + url);
        logAndPrintToStdout("Metastore connection Driver :\t " + driver);
        logAndPrintToStdout("Metastore connection User:\t " + userName);
        if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST)) {
          logAndPrintToStdout("Metastore connection Password:\t " + password);
        }
      }
      if ((userName == null) || userName.isEmpty()) {
        throw new HiveMetaException("UserName empty ");
      }

      // load required JDBC driver
      Class.forName(driver);

      // Connect using the JDBC URL and user/pass from conf
      Connection conn = DriverManager.getConnection(url, userName, password);
      if (schema != null) {
        conn.setSchema(schema);
      }
      return conn;
    } catch (IOException | SQLException e) {
      throw new HiveMetaException("Failed to get schema version.", e);
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to find driver class", e);
      throw new HiveMetaException("Failed to load driver", e);
    }
  }

  public static Connection getConnectionToMetastore(MetaStoreConnectionInfo info, Configuration conf, String schema)
      throws HiveMetaException {
    return getConnectionToMetastore(info.getUsername(), info.getPassword(), info.getUrl(), info.getDriver(),
        info.getPrintInfo(), conf, schema);
  }

  public static MetaStoreConnectionInfo getConnectionInfoFromConfiguration(Configuration configuration) throws IOException, HiveMetaException {
    String url = MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.CONNECT_URL_KEY);
    Matcher matcher = dbTypePattern.matcher(url);
    if (matcher.find()) {
      return new MetaStoreConnectionInfo(
          MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.CONNECTION_USER_NAME),
          MetastoreConf.getPassword(configuration, MetastoreConf.ConfVars.PWD),
          MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.CONNECT_URL_KEY),
          MetastoreConf.getAsString(configuration, MetastoreConf.ConfVars.CONNECTION_DRIVER),
          false, matcher.group(1)
      );
    } else {
      throw new HiveMetaException("Unable to determine database type from connection url!");
    }
  }

  public static String getValidConfVar(MetastoreConf.ConfVars confVar, Configuration conf)
      throws IOException {
    String confVarStr = MetastoreConf.getAsString(conf, confVar);
    if (confVarStr == null || confVarStr.isEmpty()) {
      throw new IOException("Empty " + confVar.getVarname());
    }
    return confVarStr.trim();
  }

  private static void logAndPrintToStdout(String msg) {
    LOG.info(msg);
    System.out.println(msg);
  }

  public static class MetaStoreConnectionInfo {
    private final String userName;
    private final String password;
    private final String url;
    private final String driver;
    private final boolean printInfo;
    private final String dbType;

    public MetaStoreConnectionInfo(String userName, String password, String url, String driver,
                                   boolean printInfo, String dbType) {
      super();
      this.userName = userName;
      this.password = password;
      this.url = url;
      this.driver = driver;
      this.printInfo = printInfo;
      this.dbType = dbType;
    }

    public String getPassword() {
      return password;
    }

    public String getUrl() {
      return url;
    }

    public String getDriver() {
      return driver;
    }

    public boolean isPrintInfo() {
      return printInfo;
    }

    public String getUsername() {
      return userName;
    }

    public boolean getPrintInfo() {
      return printInfo;
    }

    public String getDbType() {
      return dbType;
    }

  }
}
