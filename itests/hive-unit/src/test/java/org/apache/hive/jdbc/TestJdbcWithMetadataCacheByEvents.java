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

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.events.MetadataCacheEventConsumer;
import org.apache.hadoop.hive.ql.metadata.events.TableVersionRegistry;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The HS2 metadata cache by notification events (HIVE-30264) on external tables, against a real metastore with
 * the DbNotificationListener: a session sees what other sessions and other metastore clients changed.
 */
public class TestJdbcWithMetadataCacheByEvents {
  private static MiniHS2 miniHS2;

  private Connection reader;
  private Connection writer;

  @BeforeClass
  public static void setUpClass() throws Exception {
    MiniHS2.cleanupLocalDir();
    HiveConf conf = new HiveConfForTest(TestJdbcWithMetadataCacheByEvents.class);
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setBoolVar(HiveConf.ConfVars.MSC_CACHE_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_CONSUMERS, MetadataCacheEventConsumer.class.getName());
    conf.setVar(HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_POLL_INTERVAL, "1s");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, false);
    miniHS2 = new MiniHS2.Builder().withConf(conf).cleanupLocalDirOnStartup(false).build();
    miniHS2.start(new HashMap<>());
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
    if (miniHS2 != null) {
      miniHS2.cleanup();
    }
  }

  @Before
  public void setUp() throws Exception {
    assertTrue("the cache by events is on", TableVersionRegistry.get().isEnabled());
    reader = connect();
    writer = connect();
    execute(reader, "set hive.metadata.cache.nontransactional.mode=strict");
  }

  @After
  public void tearDown() throws Exception {
    reader.close();
    writer.close();
  }

  @Test
  public void testSchemaChangeFromAnotherSession() throws Exception {
    execute(writer, "create external table schema_change (c1 int)");
    execute(reader, "select c1 from schema_change");

    execute(writer, "alter table schema_change add columns (c2 int)");
    execute(reader, "select c2 from schema_change");
  }

  @Test
  public void testPartitionsFromAnotherSession() throws Exception {
    execute(writer, "create external table partitions (c1 int) partitioned by (p string)");
    execute(writer, "insert into partitions partition (p='old') values (1)");
    assertEquals(0, count(reader, "select count(*) from partitions where p='new'"));

    execute(writer, "insert into partitions partition (p='new') values (1), (2)");
    assertEquals(2, count(reader, "select count(*) from partitions where p='new'"));
  }

  @Test
  public void testStatsAnsweredQuery() throws Exception {
    execute(reader, "set hive.compute.query.using.stats=true");
    execute(writer, "create external table stats_answered (c1 int)");
    execute(writer, "insert into stats_answered values (1)");
    assertEquals(1, count(reader, "select count(*) from stats_answered"));

    execute(writer, "insert into stats_answered values (2), (3)");
    assertEquals(3, count(reader, "select count(*) from stats_answered"));
  }

  @Test
  public void testChangeFromAnotherMetastoreClient() throws Exception {
    execute(writer, "create external table other_client (c1 int)");
    execute(reader, "select c1 from other_client");

    // as another engine would, straight to the metastore
    IMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(miniHS2.getServerConf()));
    try {
      addColumn(client, "other_client", "c2");
    } finally {
      client.close();
    }
    execute(reader, "select c2 from other_client");
  }

  @Test
  public void testCachedUntilAnEvent() throws Exception {
    execute(writer, "create external table no_event (c1 int)");
    execute(reader, "select c1 from no_event");

    // a metastore client with no DbNotificationListener records the change without an event
    HiveConf conf = new HiveConf(miniHS2.getServerConf());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS, "");
    IMetaStoreClient client = new HiveMetaStoreClient(conf);
    try {
      addColumn(client, "no_event", "c2");
    } finally {
      client.close();
    }
    try {
      execute(reader, "select c2 from no_event");
      fail("the table is cached until an event, or hive.metadata.cache.nontransactional.max.staleness");
    } catch (SQLException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("c2"));
    }
    // a session that has not opted in loads the table from the metastore
    execute(writer, "select c2 from no_event");

    execute(writer, "alter table no_event set tblproperties ('touched'='true')");
    execute(reader, "select c2 from no_event");
  }

  private static void addColumn(IMetaStoreClient client, String tableName, String column) throws Exception {
    Table table = client.getTable("default", tableName);
    table.getSd().addToCols(new FieldSchema(column, "int", null));
    client.alter_table("default", tableName, table);
  }

  private static Connection connect() throws Exception {
    return DriverManager.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "");
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static long count(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }
}
