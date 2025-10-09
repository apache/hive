/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.minikdc;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestJdbcWithMiniKdcDoAsHttp {

  private static MiniHS2 hs2;

  @BeforeClass
  public static void setup() throws Exception {
    MiniHiveKdc kdc = new MiniHiveKdc();
    HiveConf conf = new HiveConf();
    // Set connection retries to 1 to fail fast.
    conf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 1);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "http");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);
    hs2 = MiniHiveKdc.getMiniHS2WithKerbWithRemoteHMSWithKerb(kdc, conf);
    hs2.start(Collections.emptyMap());
    kdc.loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
  }

  @AfterClass
  public static void tearDown() {
    if (hs2 != null) {
      hs2.cleanup();
      hs2.stop();
      hs2 = null;
    }
  }

  @Test
  public void testConcurrentConnectionsWithSessionOverlapNoSaslException() throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(2);
    // Open, close, client connections repeatedly till interrupted; essentially leading to many HS2 sessions.
    // Simulate a client application that uses many short-lived connections.
    Future<Boolean> openCloseConnectionTask = service.submit(() -> {
      do {
        try (Connection c = DriverManager.getConnection(hs2.getJdbcURL())) {
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } while (!Thread.interrupted());
      return true;
    });
    // Open connection, execute a fixed amount of queries, and close; the HS2 session will remain open for the whole period.
    // Simulate a client that uses long-lived connections with many queries per connection.
    Future<Boolean> executeQueryTask = service.submit(() -> {
      try (Connection c = DriverManager.getConnection(hs2.getJdbcURL())) {
        for (int i = 0; i < 20; i++) {
          try (Statement s = c.createStatement()) {
            try (ResultSet r = s.executeQuery("show databases")) {
            }
          }
        }
        return true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    try {
      // Wait till all the queries are executed, or an exception is thrown
      executeQueryTask.get();
    } finally {
      openCloseConnectionTask.cancel(true);
      service.shutdown();
      service.awaitTermination(1, TimeUnit.MINUTES);
    }
  }
}
