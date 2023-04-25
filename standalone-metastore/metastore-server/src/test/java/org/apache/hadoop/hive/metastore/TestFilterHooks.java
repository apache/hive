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

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.junit.experimental.categories.Category;

/**
 * Test the filtering behavior at HMS client and HMS server. The configuration at each test
 * changes, and therefore HMS client and server are created for each test case
 */
@Category(MetastoreUnitTest.class)
public class TestFilterHooks {
  public static class DummyMetaStoreFilterHookImpl implements MetaStoreFilterHook {
    private static boolean blockResults = false;

    public DummyMetaStoreFilterHookImpl(Configuration conf) {
    }

    @Override
    public List<String> filterDatabases(List<String> dbList) throws MetaException  {
      if (blockResults) {
        return new ArrayList<>();
      }
      return dbList;
    }

    @Override
    public Database filterDatabase(Database dataBase) throws NoSuchObjectException {
      if (blockResults) {
        throw new NoSuchObjectException("Blocked access");
      }
      return dataBase;
    }

    @Override
    public List<String> filterTableNames(String catName, String dbName, List<String> tableList)
        throws MetaException {
      if (blockResults) {
        return new ArrayList<>();
      }
      return tableList;
    }

    @Override
    public Table filterTable(Table table) throws NoSuchObjectException {
      if (blockResults) {
        throw new NoSuchObjectException("Blocked access");
      }
      return table;
    }

    @Override
    public List<Table> filterTables(List<Table> tableList) throws MetaException {
      if (blockResults) {
        return new ArrayList<>();
      }
      return tableList;
    }

    @Override
    @Deprecated
    public List<TableMeta> filterTableMetas(String catName, String dbName,List<TableMeta> tableMetas)
        throws MetaException {
      return filterTableMetas(tableMetas);
    }

    @Override
    public List<TableMeta> filterTableMetas(List<TableMeta> tableMetas) throws MetaException {
      return tableMetas;
    }

    @Override
    public List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException {
      if (blockResults) {
        return new ArrayList<>();
      }
      return partitionList;
    }

    @Override
    public List<PartitionSpec> filterPartitionSpecs(
        List<PartitionSpec> partitionSpecList) throws MetaException {
      if (blockResults) {
        return new ArrayList<>();
      }
      return partitionSpecList;
    }

    @Override
    public Partition filterPartition(Partition partition) throws NoSuchObjectException {
      if (blockResults) {
        throw new NoSuchObjectException("Blocked access");
      }
      return partition;
    }

    @Override
    public List<String> filterPartitionNames(String catName, String dbName, String tblName,
        List<String> partitionNames) throws MetaException {
      if (blockResults) {
        return new ArrayList<>();
      }
      return partitionNames;
    }

    @Override
    public List<String> filterDataConnectors(List<String> dcList) throws MetaException {
      if (blockResults) {
        return new ArrayList<>();
      }
      return dcList;
    }
  }

  protected static HiveMetaStoreClient client;
  protected static Configuration conf;
  protected static Warehouse warehouse;

  private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;

  private static String DBNAME1 = "testdb1";
  private static String DBNAME2 = "testdb2";
  private static final String TAB1 = "tab1";
  private static final String TAB2 = "tab2";
  private static String DCNAME1 = "test_connector1";
  private static String DCNAME2 = "test_connector2";
  private static String mysql_type = "mysql";
  private static String mysql_url = "jdbc:mysql://localhost:3306/hive";
  private static String postgres_type = "postgres";
  private static String postgres_url = "jdbc:postgresql://localhost:5432";


  protected HiveMetaStoreClient createClient(Configuration metaStoreConf) throws Exception {
    try {
      return new HiveMetaStoreClient(metaStoreConf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    DummyMetaStoreFilterHookImpl.blockResults = true;
  }

  @Before
  public void setUpForTest() throws Exception {

    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetastoreConf.setClass(conf, ConfVars.FILTER_HOOK, DummyMetaStoreFilterHookImpl.class,
        MetaStoreFilterHook.class);
    MetastoreConf.setBoolVar(conf, ConfVars.METRICS_ENABLED, true);
    conf.set("hive.key1", "value1");
    conf.set("hive.key2", "http://www.example.com");
    conf.set("hive.key3", "");
    conf.set("hive.key4", "0");
    conf.set("datanucleus.autoCreateTables", "false");
    conf.set("hive.in.test", "true");

    MetastoreConf.setLongVar(conf, ConfVars.BATCH_RETRIEVE_MAX, 2);
    MetastoreConf.setLongVar(conf, ConfVars.LIMIT_PARTITION_REQUEST, DEFAULT_LIMIT_PARTITION_REQUEST);
    MetastoreConf.setVar(conf, ConfVars.STORAGE_SCHEMA_READER_IMPL, "no.such.class");
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    warehouse = new Warehouse(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  /**
   * This is called in each test after the configuration is set in each test case
   * @throws Exception
   */
  protected void creatEnv(Configuration conf) throws Exception {
    client = createClient(conf);

    client.dropDatabase(DBNAME1, true, true, true);
    client.dropDatabase(DBNAME2, true, true, true);
    client.dropDataConnector(DCNAME1, true, true);
    client.dropDataConnector(DCNAME2, true, true);
    Database db1 = new DatabaseBuilder()
        .setName(DBNAME1)
        .setCatalogName(Warehouse.DEFAULT_CATALOG_NAME)
        .create(client, conf);
    Database db2 = new DatabaseBuilder()
        .setName(DBNAME2)
        .setCatalogName(Warehouse.DEFAULT_CATALOG_NAME)
        .create(client, conf);
    new TableBuilder()
        .setDbName(DBNAME1)
        .setTableName(TAB1)
        .addCol("id", "int")
        .addCol("name", "string")
        .create(client, conf);
    Table tab2 = new TableBuilder()
        .setDbName(DBNAME1)
        .setTableName(TAB2)
        .addCol("id", "int")
        .addPartCol("name", "string")
        .create(client, conf);
    new PartitionBuilder()
        .inTable(tab2)
        .addValue("value1")
        .addToTable(client, conf);
    new PartitionBuilder()
        .inTable(tab2)
        .addValue("value2")
        .addToTable(client, conf);
    DataConnector dc1 = new DataConnector(DCNAME1, mysql_type, mysql_url);
    DataConnector dc2 = new DataConnector(DCNAME2, postgres_type, postgres_url);
    client.createDataConnector(dc1);
    client.createDataConnector(dc2);

    TestTxnDbUtil.cleanDb(conf);
    TestTxnDbUtil.prepDb(conf);
    client.compact2(DBNAME1, TAB1, null, CompactionType.MAJOR, new HashMap<>());
    client.compact2(DBNAME1, TAB2, "name=value1", CompactionType.MINOR, new HashMap<>());
  }

  /**
   * The default configuration should be disable filtering at HMS server
   * Disable the HMS client side filtering in order to see HMS server filtering behavior
   * @throws Exception
   */
  @Test
  public void testHMSServerWithoutFilter() throws Exception {
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, false);
    DBNAME1 = "db_testHMSServerWithoutFilter_1";
    DBNAME2 = "db_testHMSServerWithoutFilter_2";
    creatEnv(conf);

    assertNotNull(client.getTable(DBNAME1, TAB1));
    assertEquals(2, client.getTables(DBNAME1, "*").size());
    assertEquals(2, client.getAllTables(DBNAME1).size());
    assertEquals(1, client.getTables(DBNAME1, TAB2).size());
    assertEquals(0, client.getAllTables(DBNAME2).size());

    assertNotNull(client.getDatabase(DBNAME1));
    assertEquals(2, client.getDatabases("*testHMSServerWithoutFilter*").size());
    assertEquals(1, client.getDatabases(DBNAME1).size());

    assertNotNull(client.getPartition(DBNAME1, TAB2, "name=value1"));
    assertEquals(1, client.getPartitionsByNames(DBNAME1, TAB2, Lists.newArrayList("name=value1")).size());

    assertEquals(2, client.showCompactions().getCompacts().size());

    assertEquals(2, client.getAllDataConnectorNames().size());
  }

  /**
   * Enable the HMS server side filtering
   * Disable the HMS client side filtering in order to see HMS server filtering behavior
   * @throws Exception
   */
  @Test
  public void testHMSServerWithFilter() throws Exception {
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, false);
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_SERVER_FILTER_ENABLED, true);
    DBNAME1 = "db_testHMSServerWithFilter_1";
    DBNAME2 = "db_testHMSServerWithFilter_2";
    creatEnv(conf);

    testFilterForDb(true);
    testFilterForTables(true);
    testFilterForPartition(true);
    testFilterForCompaction();
    testFilterForDataConnector();
  }

  /**
   * Disable filtering at HMS client
   * By default, the HMS server side filtering is disabled, so we can see HMS client filtering behavior
   * @throws Exception
   */
  @Test
  public void testHMSClientWithoutFilter() throws Exception {
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, false);
    DBNAME1 = "db_testHMSClientWithoutFilter_1";
    DBNAME2 = "db_testHMSClientWithoutFilter_2";
    creatEnv(conf);

    assertNotNull(client.getTable(DBNAME1, TAB1));
    assertEquals(2, client.getTables(DBNAME1, "*").size());
    assertEquals(2, client.getAllTables(DBNAME1).size());
    assertEquals(1, client.getTables(DBNAME1, TAB2).size());
    assertEquals(0, client.getAllTables(DBNAME2).size());

    assertNotNull(client.getDatabase(DBNAME1));
    assertEquals(2, client.getDatabases("*testHMSClientWithoutFilter*").size());
    assertEquals(1, client.getDatabases(DBNAME1).size());

    assertNotNull(client.getPartition(DBNAME1, TAB2, "name=value1"));
    assertEquals(1, client.getPartitionsByNames(DBNAME1, TAB2, Lists.newArrayList("name=value1")).size());

    assertEquals(2, client.showCompactions().getCompacts().size());

    assertEquals(2, client.getAllDataConnectorNames().size());
  }

  /**
   * By default, the HMS Client side filtering is enabled
   * Disable the HMS server side filtering in order to see HMS client filtering behavior
   * @throws Exception
   */
  @Test
  public void testHMSClientWithFilter() throws Exception {
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_SERVER_FILTER_ENABLED, false);
    DBNAME1 = "db_testHMSClientWithFilter_1";
    DBNAME2 = "db_testHMSClientWithFilter_2";
    creatEnv(conf);

    testFilterForDb(false);
    testFilterForTables(false);
    testFilterForPartition(false);
    testFilterForCompaction();
    testFilterForDataConnector();
  }

  protected void testFilterForDb(boolean filterAtServer) throws Exception {

    // Skip this call when testing filter hook at HMS server because HMS server calls authorization
    // API for getDatabase(), and does not call filter hook
    if (!filterAtServer) {
      try {
        assertNotNull(client.getDatabase(DBNAME1));
        fail("getDatabase() should fail with blocking mode");
      } catch (NoSuchObjectException e) {
        // Excepted
      }
    }

    assertEquals(0, client.getDatabases("*").size());
    assertEquals(0, client.getAllDatabases().size());
    assertEquals(0, client.getDatabases(DBNAME1).size());
  }

  protected void testFilterForTables(boolean filterAtServer) throws Exception {

    // Skip this call when testing filter hook at HMS server because HMS server calls authorization
    // API for getTable(), and does not call filter hook
    if (!filterAtServer) {
      try {
        client.getTable(DBNAME1, TAB1);
        fail("getTable() should fail with blocking mode");
      } catch (NoSuchObjectException e) {
        // Excepted
      }
    }

    assertEquals(0, client.getTables(DBNAME1, "*").size());
    assertEquals(0, client.getTables(DBNAME1, "*", TableType.MANAGED_TABLE).size());
    assertEquals(0, client.getAllTables(DBNAME1).size());
    assertEquals(0, client.getTables(DBNAME1, TAB2).size());
  }

  protected void testFilterForPartition(boolean filterAtServer) throws Exception {
    try {
      assertNotNull(client.getPartition(DBNAME1, TAB2, "name=value1"));
      fail("getPartition() should fail with blocking mode");
    } catch (NoSuchObjectException e) {
      // Excepted
    }

    if (filterAtServer) {
      // at HMS server, the table of the partitions should be filtered out and result in
      // NoSuchObjectException
      try {
        client.getPartitionsByNames(DBNAME1, TAB2,
            Lists.newArrayList("name=value1")).size();
        fail("getPartitionsByNames() should fail with blocking mode at server side");
      } catch (NoSuchObjectException e) {
        // Excepted
      }
    } else {
      // at HMS client, we cannot filter the table of the partitions due to
      // HIVE-21227: HIVE-20776 causes view access regression
      assertEquals(0, client.getPartitionsByNames(DBNAME1, TAB2,
          Lists.newArrayList("name=value1")).size());
    }
  }

  protected void testFilterForCompaction() throws Exception {
    assertEquals(0, client.showCompactions().getCompacts().size());
  }

  protected void testFilterForDataConnector() throws Exception {
    assertNotNull(client.getDataConnector(DCNAME1));
    assertEquals(0, client.getAllDataConnectorNames().size());
  }
}
