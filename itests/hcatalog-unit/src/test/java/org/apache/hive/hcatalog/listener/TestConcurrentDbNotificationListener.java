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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.listener;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests DbNotificationListener when used as a transactional event listener by concurrent HMSes
 * (hive.metastore.transactional.event.listeners)
 */
public class TestConcurrentDbNotificationListener {
  private static final Logger LOG = LoggerFactory.getLogger(TestDbNotificationListener.class
      .getName());
  private static Map<String, String> emptyParameters = new HashMap<String, String>();
  private static IMetaStoreClient msClient;
  private static HiveConf conf;

  private long firstEventId;
  private final String testTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "testDbNotif").toString();

  @SuppressWarnings("rawtypes")
  @BeforeClass
  public static void connectToMetastore() throws Exception {
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY, JSONMessageEncoder.class.getName());
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(new CliSessionState(conf));
    msClient = new HiveMetaStoreClient(conf);
  }

  @Before
  public void setup() throws Exception {
    firstEventId = msClient.getCurrentNotificationEventId().getEventId();
  }

  /**
   * Test to add notification events concurrently and check whether the event ids are assigned in
   * monotonically increasing sequential order.
   * @throws Exception
   */
  @Test
  public void testConcurrentAddNotifications() throws Exception {
    int numThreads = 10;
    int numIterations = 5;
    Thread[] concurrentThreads = new Thread[numThreads];
    HiveMetaStoreClient[]  msClients = new HiveMetaStoreClient[numThreads];
    Exception[]  exceptions = new Exception[numThreads];
    String threadNamePrefix = "tcad_thread";
    for (int i = 0; i < numThreads; i++) {
      int tid = i;
      msClients[i] = new HiveMetaStoreClient(conf);
      exceptions[i] = null;
      concurrentThreads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          Thread.currentThread().setName(threadNamePrefix + tid);
          try {
            String dbLocationUri = testTempDir + "_" + tid;
            String dbDescription = "no description";
            String dbName = "tcan_db_" + tid;
            String userName = "tcan_user_" + tid;
            Database db = new Database(dbName, dbDescription, dbLocationUri, emptyParameters);
            for (int numIter = 0; numIter < numIterations; numIter++) {
              long txnId = msClients[tid].openTxn(userName);
              msClients[tid].createDatabase(db);
              msClients[tid].dropDatabase(dbName);
              msClients[tid].commitTxn(txnId);
            }
          } catch (Exception e) {
            exceptions[tid] = e;
          }
        }
      });
      concurrentThreads[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      concurrentThreads[i].join();
      if (exceptions[i] != null) {
        LOG.error("Exception occurred in thread " + concurrentThreads[i].getName() + ": " + exceptions[i].toString());
      }
    }

    // Check whether event ids are assigned in monotonically increasing order without gaps.
    msClient.getNextNotification(firstEventId, 0, null);
  }
}
