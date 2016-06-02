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
package org.apache.hive.service.cli.thrift;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.HiveServer2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThriftCliServiceMessageSizeTest {
  protected static int port;
  protected static String host = "localhost";
  protected static HiveServer2 hiveServer2;
  protected static ThriftCLIServiceClient client;
  protected static HiveConf hiveConf;
  protected static String USERNAME = "anonymous";
  protected static String PASSWORD = "anonymous";

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Find a free port
    port = MetaStoreUtils.findFreePort();
    hiveServer2 = new HiveServer2();
    hiveConf = new HiveConf();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  protected static void startHiveServer2WithConf(HiveServer2 hiveServer2, HiveConf hiveConf)
      throws Exception {
    hiveServer2.init(hiveConf);
    // Start HiveServer2 with given config
    // Fail if server doesn't start
    try {
      hiveServer2.start();
    } catch (Throwable t) {
      t.printStackTrace();
      fail();
    }
    // Wait for startup to complete
    Thread.sleep(2000);
    System.out.println("HiveServer2 started on port " + port);
  }

  protected static void stopHiveServer2(HiveServer2 hiveServer2) throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testMessageSize() throws Exception {
    String transportMode = "binary";

    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, AuthTypes.NONE.toString());
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, transportMode);

    HiveServer2 hiveServer2 = new HiveServer2();
    String url = "jdbc:hive2://localhost:" + port + "/default";
    Class.forName("org.apache.hive.jdbc.HiveDriver");

    try {
      // First start HS2 with high message size limit. This should allow connections
      hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE, 100*1024*1024);
      startHiveServer2WithConf(hiveServer2, hiveConf);

      System.out.println("Started Thrift CLI service with message size limit "
          + hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE));

      // With the high message size limit this connection should work
      Connection connection = DriverManager.getConnection(url, "hiveuser", "hive");
      Statement stmt = connection.createStatement();
      assertNotNull("Statement is null", stmt);
      stmt.execute("set hive.support.concurrency = false");
      connection.close();
      stopHiveServer2(hiveServer2);

      // Now start HS2 with low message size limit. This should prevent any connections
      hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE, 1);
      hiveServer2 = new HiveServer2();
      startHiveServer2WithConf(hiveServer2, hiveConf);
      System.out.println("Started Thrift CLI service with message size limit "
          + hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE));

      Exception caughtException = null;
      try {
        // This should fail
        connection = DriverManager.getConnection(url, "hiveuser", "hive");
      } catch (Exception err) {
        caughtException = err;
      }
      // Verify we hit an error while connecting
      assertNotNull(caughtException);
    } finally {
      stopHiveServer2(hiveServer2);
      hiveServer2 = null;
    }
  }
}
