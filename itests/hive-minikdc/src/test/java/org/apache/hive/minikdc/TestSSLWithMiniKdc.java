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

package org.apache.hive.minikdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.hadoop.hive.jdbc.SSLTestUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestSSLWithMiniKdc {

  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    HiveConf hiveConf = new HiveConf();

    SSLTestUtils.setMetastoreSslConf(hiveConf);
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc(hiveConf);

    setHMSSaslConf(miniHiveKdc, hiveConf);

    miniHS2 = MiniHiveKdc.getMiniHS2WithKerbWithRemoteHMS(miniHiveKdc, hiveConf);

    Map<String, String> confOverlay = new HashMap<>();
    SSLTestUtils.setHttpConfOverlay(confOverlay);
    SSLTestUtils.setSslConfOverlay(confOverlay);

    miniHS2.start(confOverlay);
  }

  @AfterClass
  public static void afterTest() throws Exception {
    miniHS2.stop();
  }

  @Test
  public void testConnection() throws Exception {
    String tableName = "testTable";
    Path dataFilePath = new Path(SSLTestUtils.getDataFileDir(), "kv1.txt");
    Connection hs2Conn = getConnection(MiniHiveKdc.HIVE_TEST_USER_1);

    Statement stmt = hs2Conn.createStatement();

    SSLTestUtils.setupTestTableWithData(tableName, dataFilePath, hs2Conn);

    stmt.execute("select * from " + tableName);
    stmt.execute("drop table " + tableName);
    stmt.close();
  }

  private Connection getConnection(String userName) throws Exception {
    miniHiveKdc.loginUser(userName);
    return DriverManager.getConnection(miniHS2.getJdbcURL("default", SSLTestUtils.SSL_CONN_PARAMS),
        System.getProperty("user.name"), "bar");
  }

  private static void setHMSSaslConf(MiniHiveKdc miniHiveKdc, HiveConf conf) {
   String hivePrincipal =
        miniHiveKdc.getFullyQualifiedServicePrincipal(MiniHiveKdc.HIVE_SERVICE_PRINCIPAL);
    String hiveKeytab = miniHiveKdc.getKeyTabFile(
        miniHiveKdc.getServicePrincipalForUser(MiniHiveKdc.HIVE_SERVICE_PRINCIPAL));

    conf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, true);
    conf.setVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL, hivePrincipal);
    conf.setVar(ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, hiveKeytab);
  }
}
