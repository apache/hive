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

package org.apache.hive.service.server;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.hive.service.rpc.thrift.TGetTablesResp;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for HIVE-27201
 */
public class TestHS2SessionHive {

  private static MiniHS2 miniHS2 = null;

  public static class DummyFilterHook extends DefaultMetaStoreFilterHookImpl {
    private static Map<String, Hive> threadHiveMap = new ConcurrentHashMap<>();
    private static Set<String> totalThreadCalls = Collections.synchronizedSet(new HashSet<>());

    public DummyFilterHook(Configuration conf) {
      super(conf);
    }

    @Override
    public List<TableMeta> filterTableMetas(List<TableMeta> tableMetas)
        throws MetaException {
      try {
        Assert.assertNotNull(SessionState.get());
        totalThreadCalls.add(Thread.currentThread().getName());
        synchronized (totalThreadCalls) {
          totalThreadCalls.notifyAll();
        }
        synchronized (threadHiveMap) {
          threadHiveMap.wait();
        }
        Hive localHive = Hive.get();
        threadHiveMap.put(Thread.currentThread().getName(), localHive);
        for (TableMeta tableMeta : tableMetas) {
          localHive.getTable(tableMeta.getDbName(), tableMeta.getTableName());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return super.filterTableMetas(tableMetas);
    }
  }

  @Test
  public void testSessionHive() throws Exception {
    Client client1 = newClient(), client2 = newClient();
    Task task1 = new Task("task1", client1.client, client2.sessionHandle),
         task2 = new Task("task2", client2.client, client1.sessionHandle);

    while (DummyFilterHook.totalThreadCalls.size() < 2) {
      synchronized (DummyFilterHook.totalThreadCalls) {
        DummyFilterHook.totalThreadCalls.wait();
      }
    }
    Thread.sleep(100);
    synchronized (DummyFilterHook.threadHiveMap) {
      DummyFilterHook.threadHiveMap.notifyAll();
    }
    Thread.sleep(3000L);
    // Check to see if there are any deadlocks
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    Assert.assertNull(threadBean.findDeadlockedThreads());
    task1.join();
    task2.join();
    Assert.assertTrue(task1.result);
    Assert.assertTrue(task2.result);
  }

  private class Task extends Thread {
    TCLIService.Iface client;
    TSessionHandle sessionHandle;
    boolean result = false;
    Task(String threadName,
         TCLIService.Iface client,
         TSessionHandle sessionHandle) {
      this.client = client;
      this.sessionHandle = sessionHandle;
      this.setName(threadName);
      this.setDaemon(true);
      this.start();
    }

    @Override
    public void run() {
      try {
        TGetTablesReq getTablesReq = new TGetTablesReq(sessionHandle);
        getTablesReq.setCatalogName("hive");
        getTablesReq.setSchemaName("*");
        getTablesReq.setTableName("*");
        TGetTablesResp resp = client.GetTables(getTablesReq);
        result = resp.getStatus().getStatusCode() == TStatusCode.SUCCESS_STATUS;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class Client {
    TCLIService.Iface client;
    TSessionHandle sessionHandle;
  }

  private Client newClient() throws Exception {
    Connection conn = DriverManager.
        getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "");
    Field clientField = HiveConnection.class.getDeclaredField("client");
    Field sesHanField = HiveConnection.class.getDeclaredField("sessHandle");
    clientField.setAccessible(true);
    sesHanField.setAccessible(true);
    try (Statement statement = conn.createStatement()) {
      statement.execute("select * from sessionhive1");
    }
    Client client = new Client();
    client.client = (TCLIService.Iface) clientField.get(conn);
    client.sessionHandle = (TSessionHandle) sesHanField.get(conn);
    return client;
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    MiniHS2.cleanupLocalDir();
    try {
      HiveConf conf = new HiveConf();
      conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
      conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL, false);
      conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
      MetastoreConf.setVar(conf, MetastoreConf.ConfVars.FILTER_HOOK, DummyFilterHook.class.getName());
      MiniHS2.Builder builder = new MiniHS2.Builder()
          .withConf(conf)
          .withRemoteMetastore()
          .cleanupLocalDirOnStartup(false);
      miniHS2 = builder.build();
      miniHS2.start(new HashMap<>());
    } catch (Exception e) {
      System.out.println("Unable to start MiniHS2: " + e);
      throw e;
    }

    miniHS2.getHiveConf().setVar(HiveConf.ConfVars.METASTORE_URIS, "thrift://localhost:" + miniHS2.getHmsPort());

    try (Connection conn = DriverManager.
        getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "");
        Statement statement = conn.createStatement()) {
      statement.execute("create table sessionhive1(a int, b string)");
      statement.execute("create table sessionhive2(a int, b string)");
      statement.execute("create table sessionhive3(a int, b string)");
    } catch (Exception e) {
      System.out.println("Unable to open default connections to MiniHS2: " + e);
      throw e;
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if ((miniHS2 != null) && (miniHS2.isStarted())) {
      miniHS2.stop();
    }
    if (miniHS2 != null) {
      miniHS2.cleanup();
    }
    MiniHS2.cleanupLocalDir();
  }

}
