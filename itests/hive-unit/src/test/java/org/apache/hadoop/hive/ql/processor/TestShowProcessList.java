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

package org.apache.hadoop.hive.ql.processor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestShowProcessList {
  protected static final Logger LOG = LoggerFactory.getLogger(TestShowProcessList.class);

  private static MiniHS2 miniHS2 = null;
  private static HiveConf conf;
  private static String user;
  private static ThreadPoolExecutor executor;

  static HiveConf defaultConf() throws Exception {
    String confDir = "../../data/conf/llap/";
    HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    HiveConf defaultConf = new HiveConf();
    defaultConf.addResource(new URL("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml"));
    return defaultConf;
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    conf = defaultConf();
    user = System.getProperty("user.name");
    conf.setVar(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE, user);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.set(MetastoreConf.ConfVars.WAREHOUSE.name(), new File(System.getProperty(
        "java.io.tmpdir") + File.separator + TestShowProcessList.class.getCanonicalName() + "-" + System.currentTimeMillis()).getPath()
        .replaceAll("\\\\", "/") + "/warehouse");
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
    MiniHS2.cleanupLocalDir();
    Class.forName(MiniHS2.getJdbcDriverName());
    miniHS2 = new MiniHS2(conf, MiniHS2.MiniClusterType.LLAP);
    Map<String, String> confOverlay = new HashMap<>();
    miniHS2.start(confOverlay);

    executor = new ThreadPoolExecutor(5, 10, 50, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
    TestTxnDbUtil.cleanDb(conf);
  }

  @Test
  public void testQueries() throws Exception {
    //Initiate several parallel connections, each with a query that may begin a transaction.
    for (int i = 0; i < 20; i++) {
      executor.submit(() -> {
        try (Connection con = DriverManager.getConnection(miniHS2.getJdbcURL(), user, "bar");
            Statement stmt = con.createStatement()) {
          stmt.execute("drop database if exists DB_" + Thread.currentThread().threadId() + " cascade");
        } catch (Exception ignored) {
        }
      });
    }
    try (Connection testCon = DriverManager.getConnection(miniHS2.getJdbcURL(), user, "bar");
        Statement s = testCon.createStatement()) {
      while (executor.getActiveCount() > 0) {
        long txnId = executeShowProcessList(s);
        System.out.println("txnId is " + txnId);
        if (txnId > -1) { // -1 implies that there are no queries running at that moment
          Assert.assertTrue(txnId >= 1);
          break;
        }
      }
    }
  }

  private static long executeShowProcessList(Statement s) {
    try (ResultSet rs = s.executeQuery("show processlist")) {
      while (rs.next()) {
        long txnId = Long.parseLong(rs.getString("Txn ID"));
        // TxnId can be 0 because the query has not yet opened txn when show processlist is run.
        if (txnId > 0) {
          return txnId;
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception when checking hive state", e);
    }
    return -1;
  }

}