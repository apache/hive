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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests with HBase Mini-cluster for HBaseStore
 */
public class TestHBaseStoreIntegration {

  private static final Log LOG = LogFactory.getLog(TestHBaseStoreIntegration.class.getName());

  private static HBaseTestingUtility utility;
  private static HTableInterface tblTable;
  private static HTableInterface sdTable;
  private static HTableInterface partTable;
  private static HTableInterface dbTable;
  private static HTableInterface roleTable;
  private static Map<String, String> emptyParameters = new HashMap<String, String>();

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock private HConnection hconn;
  private HBaseStore store;
  private HiveConf conf;

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniCluster();
    byte[][] families = new byte[][] {HBaseReadWrite.CATALOG_CF, HBaseReadWrite.STATS_CF};
    tblTable = utility.createTable(HBaseReadWrite.TABLE_TABLE.getBytes(HBaseUtils.ENCODING),
        families);
    sdTable = utility.createTable(HBaseReadWrite.SD_TABLE.getBytes(HBaseUtils.ENCODING),
        HBaseReadWrite.CATALOG_CF);
    partTable = utility.createTable(HBaseReadWrite.PART_TABLE.getBytes(HBaseUtils.ENCODING),
        families);
    dbTable = utility.createTable(HBaseReadWrite.DB_TABLE.getBytes(HBaseUtils.ENCODING),
        HBaseReadWrite.CATALOG_CF);
    roleTable = utility.createTable(HBaseReadWrite.ROLE_TABLE.getBytes(HBaseUtils.ENCODING),
        HBaseReadWrite.CATALOG_CF);
  }

  @AfterClass
  public static void shutdownMiniCluster() throws Exception {
    utility.shutdownMiniCluster();
  }

  @Before
  public void setupConnection() throws IOException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(hconn.getTable(HBaseReadWrite.SD_TABLE)).thenReturn(sdTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.TABLE_TABLE)).thenReturn(tblTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.PART_TABLE)).thenReturn(partTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.DB_TABLE)).thenReturn(dbTable);
    Mockito.when(hconn.getTable(HBaseReadWrite.ROLE_TABLE)).thenReturn(roleTable);
    conf = new HiveConf();
    // Turn off caching, as we want to test actual interaction with HBase
    conf.setBoolean(HBaseReadWrite.NO_CACHE_CONF, true);
    HBaseReadWrite hbase = HBaseReadWrite.getInstance(conf);
    hbase.setConnection(hconn);
    store = new HBaseStore();
    store.setConf(conf);
  }

  @Test
  public void createDb() throws Exception {
    String dbname = "mydb";
    Database db = new Database(dbname, "no description", "file:///tmp", emptyParameters);
    store.createDatabase(db);

    Database d = store.getDatabase("mydb");
    Assert.assertEquals(dbname, d.getName());
    Assert.assertEquals("no description", d.getDescription());
    Assert.assertEquals("file:///tmp", d.getLocationUri());
  }

  @Test
  public void dropDb() throws Exception {
    String dbname = "anotherdb";
    Database db = new Database(dbname, "no description", "file:///tmp", emptyParameters);
    store.createDatabase(db);

    Database d = store.getDatabase(dbname);
    Assert.assertNotNull(d);

    store.dropDatabase(dbname);
    thrown.expect(NoSuchObjectException.class);
    store.getDatabase(dbname);
  }

  @Test
  public void getAllDbs() throws Exception {
    String[] dbNames = new String[3];
    for (int i = 0; i < dbNames.length; i++) {
      dbNames[i] = "db" + i;
      Database db = new Database(dbNames[i], "no description", "file:///tmp", emptyParameters);
      store.createDatabase(db);
    }

    List<String> dbs = store.getAllDatabases();
    Assert.assertEquals(3, dbs.size());
    String[] namesFromStore = dbs.toArray(new String[3]);
    Arrays.sort(namesFromStore);
    Assert.assertArrayEquals(dbNames, namesFromStore);
  }

  @Test
  public void getDbsRegex() throws Exception {
    String[] dbNames = new String[3];
    for (int i = 0; i < dbNames.length; i++) {
      dbNames[i] = "db" + i;
      Database db = new Database(dbNames[i], "no description", "file:///tmp", emptyParameters);
      store.createDatabase(db);
    }

    List<String> dbs = store.getDatabases("db1|db2");
    Assert.assertEquals(2, dbs.size());
    String[] namesFromStore = dbs.toArray(new String[2]);
    Arrays.sort(namesFromStore);
    Assert.assertArrayEquals(Arrays.copyOfRange(dbNames, 1, 3), namesFromStore);

    dbs = store.getDatabases("db*");
    Assert.assertEquals(3, dbs.size());
    namesFromStore = dbs.toArray(new String[3]);
    Arrays.sort(namesFromStore);
    Assert.assertArrayEquals(dbNames, namesFromStore);
  }

  @Test
  public void createTable() throws Exception {
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("mytable", "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    Table t = store.getTable("default", "mytable");
    Assert.assertEquals(1, t.getSd().getColsSize());
    Assert.assertEquals("col1", t.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", t.getSd().getCols().get(0).getType());
    Assert.assertEquals("nocomment", t.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", t.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", t.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp", t.getSd().getLocation());
    Assert.assertEquals("input", t.getSd().getInputFormat());
    Assert.assertEquals("output", t.getSd().getOutputFormat());
    Assert.assertEquals("me", t.getOwner());
    Assert.assertEquals("default", t.getDbName());
    Assert.assertEquals("mytable", t.getTableName());
  }

  @Test
  public void alterTable() throws Exception {
    String tableName = "alttable";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table(tableName, "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    startTime += 10;
    table.setLastAccessTime(startTime);
    store.alterTable("default", tableName, table);

    Table t = store.getTable("default", tableName);
    Assert.assertEquals(1, t.getSd().getColsSize());
    Assert.assertEquals("col1", t.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", t.getSd().getCols().get(0).getType());
    Assert.assertEquals("nocomment", t.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", t.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", t.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp", t.getSd().getLocation());
    Assert.assertEquals("input", t.getSd().getInputFormat());
    Assert.assertEquals("output", t.getSd().getOutputFormat());
    Assert.assertEquals("me", t.getOwner());
    Assert.assertEquals("default", t.getDbName());
    Assert.assertEquals(tableName, t.getTableName());
    Assert.assertEquals(startTime, t.getLastAccessTime());
  }

  @Test
  public void getAllTables() throws Exception {
    String dbNames[] = new String[]{"db0", "db1"}; // named to match getAllDbs so we get the
    // right number of databases in that test.
    String tableNames[] = new String[]{"curly", "larry", "moe"};

    for (int i = 0; i < dbNames.length; i++) {
      store.createDatabase(new Database(dbNames[i], "no description", "file:///tmp",
          emptyParameters));
    }

    for (int i = 0; i < dbNames.length; i++) {
      for (int j = 0; j < tableNames.length; j++) {
        int startTime = (int) (System.currentTimeMillis() / 1000);
        List<FieldSchema> cols = new ArrayList<FieldSchema>();
        cols.add(new FieldSchema("col1", "int", "nocomment"));
        SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
        StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false,
            0,
            serde, null, null, emptyParameters);
        Table table = new Table(tableNames[j], dbNames[i], "me", startTime, startTime, 0, sd,
            null,
            emptyParameters, null, null, null);
        store.createTable(table);
      }
    }

    List<String> fetchedNames = store.getAllTables(dbNames[0]);
    Assert.assertEquals(3, fetchedNames.size());
    String[] sortedFetchedNames = fetchedNames.toArray(new String[fetchedNames.size()]);
    Arrays.sort(sortedFetchedNames);
    Assert.assertArrayEquals(tableNames, sortedFetchedNames);

    List<String> regexNames = store.getTables(dbNames[0], "*y");
    Assert.assertEquals(2, regexNames.size());
    String[] sortedRegexNames = regexNames.toArray(new String[regexNames.size()]);
    Arrays.sort(sortedRegexNames);
    Assert.assertArrayEquals(Arrays.copyOfRange(tableNames, 0, 2), sortedRegexNames);

    List<Table> fetchedTables = store.getTableObjectsByName(dbNames[1],
        Arrays.asList(Arrays.copyOfRange(tableNames, 1, 3)));
    Assert.assertEquals(2, fetchedTables.size());
    sortedFetchedNames = new String[fetchedTables.size()];
    for (int i = 0; i < fetchedTables.size(); i++) {
      sortedFetchedNames[i] = fetchedTables.get(i).getTableName();
    }
    Arrays.sort(sortedFetchedNames);
    Assert.assertArrayEquals(Arrays.copyOfRange(tableNames, 1, 3), sortedFetchedNames);
  }

  @Test
  public void dropTable() throws Exception {
    String tableName = "dtable";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table(tableName, "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    Table t = store.getTable("default", tableName);
    Assert.assertNotNull(t);

    store.dropTable("default", tableName);
    Assert.assertNull(store.getTable("default", tableName));
  }

  @Test
  public void createPartition() throws Exception {
    String dbName = "default";
    String tableName = "myparttable";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> vals = new ArrayList<String>();
    vals.add("fred");
    StorageDescriptor psd = new StorageDescriptor(sd);
    psd.setLocation("file:/tmp/pc=fred");
    Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
        emptyParameters);
    store.addPartition(part);

    Partition p = store.getPartition(dbName, tableName, vals);
    Assert.assertEquals(1, p.getSd().getColsSize());
    Assert.assertEquals("col1", p.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", p.getSd().getCols().get(0).getType());
    Assert.assertEquals("nocomment", p.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", p.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", p.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp/pc=fred", p.getSd().getLocation());
    Assert.assertEquals("input", p.getSd().getInputFormat());
    Assert.assertEquals("output", p.getSd().getOutputFormat());
    Assert.assertEquals(dbName, p.getDbName());
    Assert.assertEquals(tableName, p.getTableName());
    Assert.assertEquals(1, p.getValuesSize());
    Assert.assertEquals("fred", p.getValues().get(0));
  }

  @Test
  public void addPartitions() throws Exception {
    String dbName = "default";
    String tableName = "addParts";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> partVals = Arrays.asList("alan", "bob", "carl", "doug", "ethan");
    List<Partition> partitions = new ArrayList<Partition>();
    for (String val : partVals) {
      List<String> vals = new ArrayList<String>();
      vals.add(val);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/pc=" + val);
      Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
          emptyParameters);
      partitions.add(part);
    }
    store.addPartitions(dbName, tableName, partitions);

    List<String> partNames = store.listPartitionNames(dbName, tableName, (short) -1);
    Assert.assertEquals(5, partNames.size());
    String[] names = partNames.toArray(new String[partNames.size()]);
    Arrays.sort(names);
    String[] canonicalNames = partVals.toArray(new String[partVals.size()]);
    for (int i = 0; i < canonicalNames.length; i++) canonicalNames[i] = "pc=" + canonicalNames[i];
    Assert.assertArrayEquals(canonicalNames, names);
  }

  @Test
  public void alterPartitions() throws Exception {
    String dbName = "default";
    String tableName = "alterParts";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> partVals = Arrays.asList("alan", "bob", "carl", "doug", "ethan");
    List<Partition> partitions = new ArrayList<Partition>();
    List<List<String>> allVals = new ArrayList<List<String>>();
    for (String val : partVals) {
      List<String> vals = new ArrayList<String>();
      allVals.add(vals);
      vals.add(val);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/pc=" + val);
      Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
          emptyParameters);
      partitions.add(part);
    }
    store.addPartitions(dbName, tableName, partitions);

    for (Partition p : partitions) p.setLastAccessTime(startTime + 10);
    store.alterPartitions(dbName, tableName, allVals, partitions);

    partitions = store.getPartitions(dbName, tableName, -1);
    for (Partition part : partitions) {
      Assert.assertEquals(startTime + 10, part.getLastAccessTime());
    }
  }

  // TODO - Fix this and the next test.  They depend on test execution order and are bogus.
  @Test
  public void createManyPartitions() throws Exception {
    String dbName = "default";
    String tableName = "manyParts";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> partVals = Arrays.asList("alan", "bob", "carl", "doug", "ethan");
    for (String val : partVals) {
      List<String> vals = new ArrayList<String>();
      vals.add(val);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/pc=" + val);
      Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
          emptyParameters);
      store.addPartition(part);

      Partition p = store.getPartition(dbName, tableName, vals);
      Assert.assertEquals("file:/tmp/pc=" + val, p.getSd().getLocation());
    }

    Assert.assertEquals(2, HBaseReadWrite.getInstance(conf).countStorageDescriptor());

  }

  @Test
  public void createDifferentPartition() throws Exception {
    int startTime = (int)(System.currentTimeMillis() / 1000);
    Map<String, String> emptyParameters = new HashMap<String, String>();
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input2", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("differenttable", "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    Assert.assertEquals(3, HBaseReadWrite.getInstance(conf).countStorageDescriptor());

  }

  @Test
  public void getPartitions() throws Exception {
    String dbName = "default";
    String tableName = "manyParts";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> partVals = Arrays.asList("alan", "bob", "carl", "doug", "ethan");
    for (String val : partVals) {
      List<String> vals = new ArrayList<String>();
      vals.add(val);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/pc=" + val);
      Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
          emptyParameters);
      store.addPartition(part);

      Partition p = store.getPartition(dbName, tableName, vals);
      Assert.assertEquals("file:/tmp/pc=" + val, p.getSd().getLocation());
    }

    List<Partition> parts = store.getPartitions(dbName, tableName, -1);
    Assert.assertEquals(5, parts.size());
    String[] pv = new String[5];
    for (int i = 0; i < 5; i++) pv[i] = parts.get(i).getValues().get(0);
    Arrays.sort(pv);
    Assert.assertArrayEquals(pv, partVals.toArray(new String[5]));
  }

  @Test
  public void listPartitions() throws Exception {
    String dbName = "default";
    String tableName = "listParts";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    partCols.add(new FieldSchema("region", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    String[][] partVals = new String[][]{{"today", "north america"}, {"tomorrow", "europe"}};
    for (String[] pv : partVals) {
      List<String> vals = new ArrayList<String>();
      for (String v : pv) vals.add(v);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/pc=" + pv[0] + "/region=" + pv[1]);
      Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
          emptyParameters);
      store.addPartition(part);
    }

    List<String> names = store.listPartitionNames(dbName, tableName, (short) -1);
    Assert.assertEquals(2, names.size());
    String[] resultNames = names.toArray(new String[names.size()]);
    Arrays.sort(resultNames);
    Assert.assertArrayEquals(resultNames, new String[]{"pc=today/region=north america",
        "pc=tomorrow/region=europe"});

    List<Partition> parts = store.getPartitionsByNames(dbName, tableName, names);
    Assert.assertArrayEquals(partVals[0], parts.get(0).getValues().toArray(new String[2]));
    Assert.assertArrayEquals(partVals[1], parts.get(1).getValues().toArray(new String[2]));

    store.dropPartitions(dbName, tableName, names);
    List<Partition> afterDropParts = store.getPartitions(dbName, tableName, -1);
    Assert.assertEquals(0, afterDropParts.size());
  }

  @Test
  public void listPartitionsWithPs() throws Exception {
    String dbName = "default";
    String tableName = "listPartsPs";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    partCols.add(new FieldSchema("region", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    String[][] partVals = new String[][]{{"today", "north america"}, {"today", "europe"},
        {"tomorrow", "north america"}, {"tomorrow", "europe"}};
    for (String[] pv : partVals) {
      List<String> vals = new ArrayList<String>();
      for (String v : pv) vals.add(v);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/ds=" + pv[0] + "/region=" + pv[1]);
      Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
          emptyParameters);
      store.addPartition(part);
    }

    // We only test listPartitionNamesPs since it calls listPartitionsPsWithAuth anyway.
    // Test the case where we completely specify the partition
    List<String> partitionNames =
        store.listPartitionNamesPs(dbName, tableName, Arrays.asList(partVals[0]), (short) -1);
    Assert.assertEquals(1, partitionNames.size());
    Assert.assertEquals("ds=today/region=north america", partitionNames.get(0));

    // Leave off the last value of the partition
    partitionNames =
        store.listPartitionNamesPs(dbName, tableName, Arrays.asList(partVals[0][0]), (short)-1);
    Assert.assertEquals(2, partitionNames.size());
    String[] names = partitionNames.toArray(new String[partitionNames.size()]);
    Arrays.sort(names);
    Assert.assertArrayEquals(new String[] {"ds=today/region=europe",
        "ds=today/region=north america"}, names);

    // Put a star in the last value of the partition
    partitionNames =
        store.listPartitionNamesPs(dbName, tableName, Arrays.asList("today", "*"), (short)-1);
    Assert.assertEquals(2, partitionNames.size());
    names = partitionNames.toArray(new String[partitionNames.size()]);
    Arrays.sort(names);
    Assert.assertArrayEquals(new String[] {"ds=today/region=europe",
        "ds=today/region=north america"}, names);

    // Put a star in the first value of the partition
    partitionNames =
        store.listPartitionNamesPs(dbName, tableName, Arrays.asList("*", "europe"), (short)-1);
    Assert.assertEquals(2, partitionNames.size());
    names = partitionNames.toArray(new String[partitionNames.size()]);
    Arrays.sort(names);
    Assert.assertArrayEquals(new String[] {"ds=today/region=europe",
        "ds=tomorrow/region=europe"}, names);
  }

  @Test
  public void dropPartition() throws Exception {
    String dbName = "default";
    String tableName = "myparttable2";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> vals = Arrays.asList("fred");
    StorageDescriptor psd = new StorageDescriptor(sd);
    psd.setLocation("file:/tmp/pc=fred");
    Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
        emptyParameters);
    store.addPartition(part);

    Assert.assertNotNull(store.getPartition(dbName, tableName, vals));
    store.dropPartition(dbName, tableName, vals);
    thrown.expect(NoSuchObjectException.class);
    store.getPartition(dbName, tableName, vals);
  }

  @Test
  public void createRole() throws Exception {
    int now = (int)System.currentTimeMillis()/1000;
    String roleName = "myrole";
    store.addRole(roleName, "me");

    Role r = store.getRole(roleName);
    Assert.assertEquals(roleName, r.getRoleName());
    Assert.assertEquals("me", r.getOwnerName());
    Assert.assertTrue(now <= r.getCreateTime());
  }

  @Test
  public void dropRole() throws Exception {
    String roleName = "anotherrole";
    store.addRole(roleName, "me");

    Role r = store.getRole(roleName);
    Assert.assertEquals(roleName, r.getRoleName());

    store.removeRole(roleName);
    thrown.expect(NoSuchObjectException.class);
    store.getRole(roleName);
  }

  @Test
  public void tableStatistics() throws Exception {
    long now = System.currentTimeMillis();
    String dbname = "default";
    String tableName = "statstable";
    String boolcol = "boolcol";
    String longcol = "longcol";
    String doublecol = "doublecol";
    String stringcol = "stringcol";
    String binarycol = "bincol";
    String decimalcol = "deccol";
    long trues = 37;
    long falses = 12;
    long booleanNulls = 2;
    long longHigh = 120938479124L;
    long longLow = -12341243213412124L;
    long longNulls = 23;
    long longDVs = 213L;
    double doubleHigh = 123423.23423;
    double doubleLow = 0.00001234233;
    long doubleNulls = 92;
    long doubleDVs = 1234123421L;
    long strMaxLen = 1234;
    double strAvgLen = 32.3;
    long strNulls = 987;
    long strDVs = 906;
    long binMaxLen = 123412987L;
    double binAvgLen = 76.98;
    long binNulls = 976998797L;
    Decimal decHigh = new Decimal();
    decHigh.setScale((short)3);
    decHigh.setUnscaled("3876".getBytes()); // I have no clue how this is translated, but it
    // doesn't matter
    Decimal decLow = new Decimal();
    decLow.setScale((short)3);
    decLow.setUnscaled("38".getBytes());
    long decNulls = 13;
    long decDVs = 923947293L;

    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema(boolcol, "boolean", "nocomment"));
    cols.add(new FieldSchema(longcol, "long", "nocomment"));
    cols.add(new FieldSchema(doublecol, "double", "nocomment"));
    cols.add(new FieldSchema(stringcol, "varchar(32)", "nocomment"));
    cols.add(new FieldSchema(binarycol, "binary", "nocomment"));
    cols.add(new FieldSchema(decimalcol, "decimal(5, 3)", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table(tableName, dbname, "me", (int)now / 1000, (int)now / 1000, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(now);
    desc.setDbName(dbname);
    desc.setTableName(tableName);
    desc.setIsTblLevel(true);
    stats.setStatsDesc(desc);

    // Do one column of each type
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName(boolcol);
    obj.setColType("boolean");
    ColumnStatisticsData data = new ColumnStatisticsData();
    BooleanColumnStatsData boolData = new BooleanColumnStatsData();
    boolData.setNumTrues(trues);
    boolData.setNumFalses(falses);
    boolData.setNumNulls(booleanNulls);
    data.setBooleanStats(boolData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    obj = new ColumnStatisticsObj();
    obj.setColName(longcol);
    obj.setColType("long");
    data = new ColumnStatisticsData();
    LongColumnStatsData longData = new LongColumnStatsData();
    longData.setHighValue(longHigh);
    longData.setLowValue(longLow);
    longData.setNumNulls(longNulls);
    longData.setNumDVs(longDVs);
    data.setLongStats(longData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    obj = new ColumnStatisticsObj();
    obj.setColName(doublecol);
    obj.setColType("double");
    data = new ColumnStatisticsData();
    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    doubleData.setHighValue(doubleHigh);
    doubleData.setLowValue(doubleLow);
    doubleData.setNumNulls(doubleNulls);
    doubleData.setNumDVs(doubleDVs);
    data.setDoubleStats(doubleData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    store.updateTableColumnStatistics(stats);

    stats = store.getTableColumnStatistics(dbname, tableName,
        Arrays.asList(boolcol, longcol, doublecol));

    // We'll check all of the individual values later.
    Assert.assertEquals(3, stats.getStatsObjSize());

    // check that we can fetch just some of the columns
    stats = store.getTableColumnStatistics(dbname, tableName, Arrays.asList(boolcol));
    Assert.assertEquals(1, stats.getStatsObjSize());

    stats = new ColumnStatistics();
    stats.setStatsDesc(desc);


    obj = new ColumnStatisticsObj();
    obj.setColName(stringcol);
    obj.setColType("string");
    data = new ColumnStatisticsData();
    StringColumnStatsData strData = new StringColumnStatsData();
    strData.setMaxColLen(strMaxLen);
    strData.setAvgColLen(strAvgLen);
    strData.setNumNulls(strNulls);
    strData.setNumDVs(strDVs);
    data.setStringStats(strData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    obj = new ColumnStatisticsObj();
    obj.setColName(binarycol);
    obj.setColType("binary");
    data = new ColumnStatisticsData();
    BinaryColumnStatsData binData = new BinaryColumnStatsData();
    binData.setMaxColLen(binMaxLen);
    binData.setAvgColLen(binAvgLen);
    binData.setNumNulls(binNulls);
    data.setBinaryStats(binData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    obj = new ColumnStatisticsObj();
    obj.setColName(decimalcol);
    obj.setColType("decimal(5,3)");
    data = new ColumnStatisticsData();
    DecimalColumnStatsData decData = new DecimalColumnStatsData();
    LOG.debug("Setting decimal high value to " + decHigh.getScale() + " <" + new String(decHigh.getUnscaled()) + ">");
    decData.setHighValue(decHigh);
    decData.setLowValue(decLow);
    decData.setNumNulls(decNulls);
    decData.setNumDVs(decDVs);
    data.setDecimalStats(decData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    store.updateTableColumnStatistics(stats);

    stats = store.getTableColumnStatistics(dbname, tableName,
        Arrays.asList(boolcol, longcol, doublecol, stringcol, binarycol, decimalcol));
    Assert.assertEquals(now, stats.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbname, stats.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, stats.getStatsDesc().getTableName());
    Assert.assertTrue(stats.getStatsDesc().isIsTblLevel());

    Assert.assertEquals(6, stats.getStatsObjSize());

    ColumnStatisticsData colData = stats.getStatsObj().get(0).getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS, colData.getSetField());
    boolData = colData.getBooleanStats();
    Assert.assertEquals(trues, boolData.getNumTrues());
    Assert.assertEquals(falses, boolData.getNumFalses());
    Assert.assertEquals(booleanNulls, boolData.getNumNulls());

    colData = stats.getStatsObj().get(1).getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS, colData.getSetField());
    longData = colData.getLongStats();
    Assert.assertEquals(longHigh, longData.getHighValue());
    Assert.assertEquals(longLow, longData.getLowValue());
    Assert.assertEquals(longNulls, longData.getNumNulls());
    Assert.assertEquals(longDVs, longData.getNumDVs());

    colData = stats.getStatsObj().get(2).getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.DOUBLE_STATS, colData.getSetField());
    doubleData = colData.getDoubleStats();
    Assert.assertEquals(doubleHigh, doubleData.getHighValue(), 0.01);
    Assert.assertEquals(doubleLow, doubleData.getLowValue(), 0.01);
    Assert.assertEquals(doubleNulls, doubleData.getNumNulls());
    Assert.assertEquals(doubleDVs, doubleData.getNumDVs());

    colData = stats.getStatsObj().get(3).getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.STRING_STATS, colData.getSetField());
    strData = colData.getStringStats();
    Assert.assertEquals(strMaxLen, strData.getMaxColLen());
    Assert.assertEquals(strAvgLen, strData.getAvgColLen(), 0.01);
    Assert.assertEquals(strNulls, strData.getNumNulls());
    Assert.assertEquals(strDVs, strData.getNumDVs());

    colData = stats.getStatsObj().get(4).getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.BINARY_STATS, colData.getSetField());
    binData = colData.getBinaryStats();
    Assert.assertEquals(binMaxLen, binData.getMaxColLen());
    Assert.assertEquals(binAvgLen, binData.getAvgColLen(), 0.01);
    Assert.assertEquals(binNulls, binData.getNumNulls());

    colData = stats.getStatsObj().get(5).getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.DECIMAL_STATS, colData.getSetField());
    decData = colData.getDecimalStats();
    Assert.assertEquals(decHigh, decData.getHighValue());
    Assert.assertEquals(decLow, decData.getLowValue());
    Assert.assertEquals(decNulls, decData.getNumNulls());
    Assert.assertEquals(decDVs, decData.getNumDVs());

  }

  @Test
  public void partitionStatistics() throws Exception {
    long now = System.currentTimeMillis();
    String dbname = "default";
    String tableName = "statspart";
    String[] partNames = {"ds=today", "ds=yesterday"};
    String[] partVals = {"today", "yesterday"};
    String boolcol = "boolcol";
    String longcol = "longcol";
    String doublecol = "doublecol";
    String stringcol = "stringcol";
    String binarycol = "bincol";
    String decimalcol = "deccol";
    long trues = 37;
    long falses = 12;
    long booleanNulls = 2;
    long strMaxLen = 1234;
    double strAvgLen = 32.3;
    long strNulls = 987;
    long strDVs = 906;

    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema(boolcol, "boolean", "nocomment"));
    cols.add(new FieldSchema(longcol, "long", "nocomment"));
    cols.add(new FieldSchema(doublecol, "double", "nocomment"));
    cols.add(new FieldSchema(stringcol, "varchar(32)", "nocomment"));
    cols.add(new FieldSchema(binarycol, "binary", "nocomment"));
    cols.add(new FieldSchema(decimalcol, "decimal(5, 3)", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    Table table = new Table(tableName, dbname, "me", (int)now / 1000, (int)now / 1000, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    for (int i = 0; i < partNames.length; i++) {
      ColumnStatistics stats = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
      desc.setLastAnalyzed(now);
      desc.setDbName(dbname);
      desc.setTableName(tableName);
      desc.setIsTblLevel(false);
      desc.setPartName(partNames[i]);
      stats.setStatsDesc(desc);

      ColumnStatisticsObj obj = new ColumnStatisticsObj();
      obj.setColName(boolcol);
      obj.setColType("boolean");
      ColumnStatisticsData data = new ColumnStatisticsData();
      BooleanColumnStatsData boolData = new BooleanColumnStatsData();
      boolData.setNumTrues(trues);
      boolData.setNumFalses(falses);
      boolData.setNumNulls(booleanNulls);
      data.setBooleanStats(boolData);
      obj.setStatsData(data);
      stats.addToStatsObj(obj);

      store.updatePartitionColumnStatistics(stats, Arrays.asList(partVals[i]));
    }

    List<ColumnStatistics> statsList = store.getPartitionColumnStatistics(dbname, tableName,
        Arrays.asList(partNames), Arrays.asList(boolcol));

    Assert.assertEquals(2, statsList.size());
    for (int i = 0; i < partNames.length; i++) {
      Assert.assertEquals(1, statsList.get(i).getStatsObjSize());
    }

    for (int i = 0; i < partNames.length; i++) {
      ColumnStatistics stats = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
      desc.setLastAnalyzed(now);
      desc.setDbName(dbname);
      desc.setTableName(tableName);
      desc.setIsTblLevel(false);
      desc.setPartName(partNames[i]);
      stats.setStatsDesc(desc);

      ColumnStatisticsObj obj = new ColumnStatisticsObj();
      obj.setColName(stringcol);
      obj.setColType("string");
      ColumnStatisticsData data = new ColumnStatisticsData();
      StringColumnStatsData strData = new StringColumnStatsData();
      strData.setMaxColLen(strMaxLen);
      strData.setAvgColLen(strAvgLen);
      strData.setNumNulls(strNulls);
      strData.setNumDVs(strDVs);
      data.setStringStats(strData);
      obj.setStatsData(data);
      stats.addToStatsObj(obj);

      store.updatePartitionColumnStatistics(stats, Arrays.asList(partVals[i]));
    }

    // Make sure when we ask for one we only get one
    statsList = store.getPartitionColumnStatistics(dbname, tableName,
        Arrays.asList(partNames), Arrays.asList(boolcol));

    Assert.assertEquals(2, statsList.size());
    for (int i = 0; i < partNames.length; i++) {
      Assert.assertEquals(1, statsList.get(i).getStatsObjSize());
    }

    statsList = store.getPartitionColumnStatistics(dbname, tableName,
        Arrays.asList(partNames), Arrays.asList(boolcol, stringcol));

    Assert.assertEquals(2, statsList.size());
    for (int i = 0; i < partNames.length; i++) {
      Assert.assertEquals(2, statsList.get(i).getStatsObjSize());
      // Just check one piece of the data, I don't need to check it all again
      Assert.assertEquals(booleanNulls,
          statsList.get(i).getStatsObj().get(0).getStatsData().getBooleanStats().getNumNulls());
      Assert.assertEquals(strDVs,
          statsList.get(i).getStatsObj().get(1).getStatsData().getStringStats().getNumDVs());
    }
  }
}
