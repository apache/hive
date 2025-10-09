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
package org.apache.hadoop.hive.metastore.txn.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class NoPoolConnectionPool implements DataSource {

  private static final Logger LOG = LoggerFactory.getLogger(NoPoolConnectionPool.class);
  
  // Note that this depends on the fact that no-one in this class calls anything but
  // getConnection.  If you want to use any of the Logger or wrap calls you'll have to
  // implement them.
  private final Configuration conf;
  private final DatabaseProduct dbProduct;
  private Driver driver;
  private String connString;
  private String user;
  private String passwd;  

  public NoPoolConnectionPool(Configuration conf, DatabaseProduct dbProduct) {
    this.conf = conf;
    this.dbProduct = dbProduct;
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (user == null) {
      user = DataSourceProvider.getMetastoreJdbcUser(conf);
      passwd = DataSourceProvider.getMetastoreJdbcPasswd(conf);
    }
    return getConnection(user, passwd);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    // Find the JDBC driver
    if (driver == null) {
      String driverName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER);
      if (driverName == null || driverName.equals("")) {
        String msg = "JDBC driver for transaction db not set in configuration " +
            "file, need to set " + MetastoreConf.ConfVars.CONNECTION_DRIVER.getVarname();
        LOG.error(msg);
        throw new RuntimeException(msg);
      }
      try {
        LOG.info("Going to load JDBC driver {}", driverName);
        driver = (Driver) Class.forName(driverName).newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException("Unable to instantiate driver " + driverName + ", " +
            e.getMessage(), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(
            "Unable to access driver " + driverName + ", " + e.getMessage(),
            e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Unable to find driver " + driverName + ", " + e.getMessage(),
            e);
      }
      connString = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY);
    }

    try {
      LOG.info("Connecting to transaction db with connection string {}", connString);
      Properties connectionProps = new Properties();
      connectionProps.setProperty("user", username);
      connectionProps.setProperty("password", password);
      Connection conn = driver.connect(connString, connectionProps);
      String prepareStmt = dbProduct != null ? dbProduct.getPrepareTxnStmt() : null;
      if (prepareStmt != null) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(prepareStmt);
        }
      }
      conn.setAutoCommit(false);
      return conn;
    } catch (SQLException e) {
      throw new RuntimeException("Unable to connect to transaction manager using " + connString
          + ", " + e.getMessage(), e);
    }
  }

  @Override
  public PrintWriter getLogWriter() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLogWriter(PrintWriter out) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLoginTimeout(int seconds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLoginTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.logging.Logger getParentLogger() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    throw new UnsupportedOperationException();
  }
}
