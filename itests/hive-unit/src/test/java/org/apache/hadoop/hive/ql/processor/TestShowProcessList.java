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
import java.net.URI;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

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
    HiveConf.setHiveSiteLocation(new URI("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml").toURL());
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    HiveConf defaultConf = new HiveConf();
    defaultConf.addResource(new URI("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml").toURL());
    return defaultConf;
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    conf = defaultConf();
    user = System.getProperty("user.name");
    conf.setVar(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE, user);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    String suffix = TestShowProcessList.class.getCanonicalName() + "-" + System.currentTimeMillis();
    String dir = new File(System.getProperty("java.io.tmpdir") + File.separator + suffix).getPath()
        .replaceAll("\\\\", "/") + "/warehouse";
    conf.set(MetastoreConf.ConfVars.WAREHOUSE.name(), dir);
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
    int connections = 10;
    //Initiate 10 parallel connections, each with a query that begins a transaction.
    for (int i = 0; i < connections; i++) {
      executor.submit(() -> {
        try (Connection con = DriverManager.getConnection(miniHS2.getJdbcURL(), user, "bar");
            Statement stmt = con.createStatement()) {
          stmt.execute("drop database if exists DB_" + Thread.currentThread().threadId() + " cascade");
        } catch (Exception exception) {
          LOG.error(exception.getMessage());
          LOG.error(Arrays.toString(exception.getStackTrace()));
        }
      });
    }
    Set<Integer> txnIds = new HashSet<>();
    try (Connection testCon = DriverManager.getConnection(miniHS2.getJdbcURL(), user, "bar");
        Statement s = testCon.createStatement()) {
      while (executor.getActiveCount() > 0) {
        // retrieve txnIds from show processlist output
        txnIds.addAll(getTxnIdsFromShowProcesslist(s));
      }
    }
    System.out.println(txnIds);
    // max txnId should be equal to the number of connections
    int maxTxnId = Collections.max(txnIds);
    Assert.assertEquals(maxTxnId, connections);
  }

  private static Set<Integer> getTxnIdsFromShowProcesslist(Statement s) {
    Set<Integer> txnIds = new HashSet<>();
    try (ResultSet rs = s.executeQuery("show processlist")) {
      while (rs.next()) {
        int txnId = Integer.parseInt(rs.getString("Txn ID"));
        // TxnId can be 0 because the query has not yet opened txn when show processlist is run.
        if (txnId > 0) {
          txnIds.add(txnId);
        }
      }
    } catch (Exception exception) {
      LOG.error("Exception when checking hive state", exception);
      LOG.error(Arrays.toString(exception.getStackTrace()));

    }
    return txnIds;
  }

}
