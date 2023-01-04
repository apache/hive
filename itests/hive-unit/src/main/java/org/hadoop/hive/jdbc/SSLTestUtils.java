/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hadoop.hive.jdbc;

import java.io.File;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;


public class SSLTestUtils {

  private static final String LOCALHOST_KEY_STORE_NAME = "keystore.jks";
  private static final String TRUST_STORE_NAME = "truststore.jks";
  private static final String KEY_STORE_TRUST_STORE_PASSWORD = "HiveJdbc";
  private static final String HS2_BINARY_MODE = "binary";
  private static final String HS2_HTTP_MODE = "http";
  private static final String HS2_HTTP_ENDPOINT = "cliservice";
  private static final String HS2_BINARY_AUTH_MODE = "NONE";

  private static final HiveConf conf = new HiveConf();
  private static final String dataFileDir = !System.getProperty("test.data.files", "").isEmpty() ? System.getProperty(
          "test.data.files") : conf.get("test.data.files").replace('\\', '/').replace("c:", "");

  public static final String SSL_CONN_PARAMS = "ssl=true;sslTrustStore="
      + URLEncoder.encode(dataFileDir + File.separator + TRUST_STORE_NAME) + ";trustStorePassword="
      + KEY_STORE_TRUST_STORE_PASSWORD;

  public static void setSslConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL.varname, "true");
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname,
            dataFileDir + File.separator + LOCALHOST_KEY_STORE_NAME);
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname,
            KEY_STORE_TRUST_STORE_PASSWORD);
  }

  public static void setMetastoreSslConf(HiveConf conf) {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.USE_SSL, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_PATH,
            dataFileDir + File.separator + LOCALHOST_KEY_STORE_NAME);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_PASSWORD,
            KEY_STORE_TRUST_STORE_PASSWORD);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PATH,
            dataFileDir + File.separator + TRUST_STORE_NAME);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PASSWORD,
            KEY_STORE_TRUST_STORE_PASSWORD);
  }

  public static void setMetastoreHttpsConf(HiveConf conf) {
    setMetastoreSslConf(conf);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_TRANSPORT_MODE, "http");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE, "http");
  }

  public static void clearSslConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL.varname, "false");
  }

  public static void setHttpConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, HS2_HTTP_MODE);
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, HS2_HTTP_ENDPOINT);
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "true");
    confOverlay.put(HiveConf.ConfVars.HIVE_IN_TEST_SSL.varname, "true");
  }

  public static void setBinaryConfOverlay(Map<String, String> confOverlay) {
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, HS2_BINARY_MODE);
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname, HS2_BINARY_AUTH_MODE);
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "true");
    confOverlay.put(HiveConf.ConfVars.HIVE_IN_TEST_SSL.varname, "true");
  }

  public static void setupTestTableWithData(String tableName, Path dataFilePath,
      Connection hs2Conn) throws Exception {
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("set hive.support.concurrency = false");
    stmt.execute("set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");

    stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName
        + " (under_col int comment 'the under column', value string)");

    // load data
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tableName);
    stmt.close();
  }

  public static String getDataFileDir() {
    return dataFileDir;
  }
}
