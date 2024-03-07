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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.thrift.transport.TTransportException;
import org.hadoop.hive.jdbc.SSLTestUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestSSLWithMiniKdc {

  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    miniHiveKdc = new MiniHiveKdc();

    HiveConf hiveConf = new HiveConf();

    SSLTestUtils.setMetastoreSslConf(hiveConf);
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setBoolVar(ConfVars.HIVE_FETCH_TASK_CACHING, false);

    setHMSSaslConf(miniHiveKdc, hiveConf);

    miniHS2 = MiniHiveKdc.getMiniHS2WithKerbWithRemoteHMS(miniHiveKdc, hiveConf);

    Map<String, String> confOverlay = new HashMap<>();
    confOverlay.put(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_ENABLED.varname, "false");
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

  @Test
  public void testHmsThriftMaxMessageSize() throws Exception {
    Configuration clientConf = MetastoreConf.newMetastoreConf(new Configuration(miniHS2.getHiveConf()));
    MetastoreConf.setVar(clientConf, MetastoreConf.ConfVars.THRIFT_URIS, "thrift://localhost:" + miniHS2.getHmsPort());
    // set to a low value to prove THRIFT_METASTORE_CLIENT_MAX_MESSAGE_SIZE is being honored
    // (it should throw an exception)
    MetastoreConf.setVar(clientConf, MetastoreConf.ConfVars.THRIFT_METASTORE_CLIENT_MAX_MESSAGE_SIZE, "512");
    HiveMetaStoreClient limitedClient = new HiveMetaStoreClient(clientConf);
    String dbName = "default";
    String tableName = "testThriftMaxMessageSize";
    TableBuilder tblBuilder = new TableBuilder().setDbName(dbName).setTableName(tableName);
    for (int i = 0; i <= 10; i++) {
      tblBuilder.addCol("abcdefghijklmnopqrstuvwxyz" + i, ColumnType.STRING_TYPE_NAME);
    }
    tblBuilder.create(limitedClient, clientConf);
    Exception expectedException = assertThrows(TTransportException.class, () -> {
      limitedClient.getTable(dbName, tableName);
    });
    String exceptionMessage = expectedException.getMessage();
    // Verify the Thrift library is enforcing the limit
    assertTrue(exceptionMessage.contains("MaxMessageSize reached"));
    limitedClient.close();
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
