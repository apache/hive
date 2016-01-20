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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
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
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Integration tests with HBase Mini-cluster for HBaseStore
 */
public class TestHBaseStoreIntegration extends HBaseIntegrationTests {

  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseStoreIntegration.class.getName());

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void startup() throws Exception {
    HBaseIntegrationTests.startMiniCluster();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    HBaseIntegrationTests.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    setupConnection();
    setupHBaseStore();
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
  public void getFuncsRegex() throws Exception {
    String dbname = "default";
    int now = (int)(System.currentTimeMillis()/1000);
    String[] funcNames = new String[3];
    for (int i = 0; i < funcNames.length; i++) {
      funcNames[i] = "func" + i;
      store.createFunction(new Function(funcNames[i], dbname, "o.a.h.h.myfunc", "me",
                                        PrincipalType.USER, now, FunctionType.JAVA,
                                        Arrays.asList(new ResourceUri(ResourceType.JAR,
                                        "file:/tmp/somewhere"))));
    }

    List<String> funcs = store.getFunctions(dbname, "func1|func2");
    Assert.assertEquals(2, funcs.size());
    String[] namesFromStore = funcs.toArray(new String[2]);
    Arrays.sort(namesFromStore);
    Assert.assertArrayEquals(Arrays.copyOfRange(funcNames, 1, 3), namesFromStore);

    funcs = store.getFunctions(dbname, "func*");
    Assert.assertEquals(3, funcs.size());
    namesFromStore = funcs.toArray(new String[3]);
    Arrays.sort(namesFromStore);
    Assert.assertArrayEquals(funcNames, namesFromStore);

    funcs = store.getFunctions("nosuchdb", "func*");
    Assert.assertEquals(0, funcs.size());
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
    LOG.debug("XXX alter table test");
    store.alterTable("default", tableName, table);

    Table t = store.getTable("default", tableName);
    LOG.debug("Alter table time " + t.getLastAccessTime());
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
    String tableName = "listPartitionsWithPs";
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
  public void getPartitionsByFilter() throws Exception {
    String dbName = "default";
    String tableName = "getPartitionsByFilter";
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

    String[][] partVals = new String[][]{{"20010101", "north america"}, {"20010101", "europe"},
        {"20010102", "north america"}, {"20010102", "europe"}, {"20010103", "north america"}};
    for (String[] pv : partVals) {
      List<String> vals = new ArrayList<String>();
      for (String v : pv) vals.add(v);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/ds=" + pv[0] + "/region=" + pv[1]);
      Partition part = new Partition(vals, dbName, tableName, startTime, startTime, psd,
          emptyParameters);
      store.addPartition(part);
    }

    // We only test getPartitionsByFilter since it calls same code as getPartitionsByExpr anyway.
    // Test the case where we completely specify the partition
    List<Partition> parts = null;
    parts = store.getPartitionsByFilter(dbName, tableName, "ds > '20010101'", (short) -1);
    checkPartVals(parts, "[20010102, north america]", "[20010102, europe]",
        "[20010103, north america]");

    parts = store.getPartitionsByFilter(dbName, tableName, "ds >= '20010102'", (short) -1);
    checkPartVals(parts, "[20010102, north america]", "[20010102, europe]",
        "[20010103, north america]");

    parts = store.getPartitionsByFilter(dbName, tableName,
        "ds >= '20010102' and region = 'europe' ", (short) -1);
    // filtering on first partition is only implemented as of now, so it will
    // not filter on region
    checkPartVals(parts, "[20010102, north america]", "[20010102, europe]",
        "[20010103, north america]");

    parts = store.getPartitionsByFilter(dbName, tableName,
        "ds >= '20010101' and ds < '20010102'", (short) -1);
    checkPartVals(parts,"[20010101, north america]", "[20010101, europe]");

    parts = store.getPartitionsByFilter(dbName, tableName,
        "ds = '20010102' or ds < '20010103'", (short) -1);
    checkPartVals(parts, "[20010101, north america]", "[20010101, europe]",
        "[20010102, north america]", "[20010102, europe]");

    // test conversion to DNF
    parts = store.getPartitionsByFilter(dbName, tableName,
        "ds = '20010102' and (ds = '20010102' or region = 'europe')", (short) -1);
    // filtering on first partition is only implemented as of now, so it will not filter on region
    checkPartVals(parts, "[20010102, north america]", "[20010102, europe]");

    parts = store.getPartitionsByFilter(dbName, tableName,
        "region = 'europe'", (short) -1);
    // filtering on first partition is only implemented as of now, so it will not filter on region
    checkPartVals(parts, "[20010101, north america]", "[20010101, europe]",
        "[20010102, north america]", "[20010102, europe]", "[20010103, north america]");

  }

  /**
   * Check if the given partitions have same values as given partitions value strings
   * @param parts given partitions
   * @param expectedPartVals
   */
  private void checkPartVals(List<Partition> parts, String ... expectedPartVals) {
    Assert.assertEquals("number of partitions", expectedPartVals.length, parts.size());
    Set<String> partValStrings = new TreeSet<String>();
    for(Partition part : parts) {
      partValStrings.add(part.getValues().toString());
    }
    partValStrings.equals(new TreeSet(Arrays.asList(expectedPartVals)));
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
  public void grantRevokeRoles() throws Exception {
    int now = (int)(System.currentTimeMillis()/1000);
    String roleName1 = "role1";
    store.addRole(roleName1, "me");
    String roleName2 = "role2";
    store.addRole(roleName2, "me");

    Role role1 = store.getRole(roleName1);
    Role role2 = store.getRole(roleName2);

    store.grantRole(role1, "fred", PrincipalType.USER, "bob", PrincipalType.USER, false);
    store.grantRole(role2, roleName1, PrincipalType.ROLE, "admin", PrincipalType.ROLE, true);
    store.grantRole(role2, "fred", PrincipalType.USER, "admin", PrincipalType.ROLE, false);

    List<Role> roles = store.listRoles("fred", PrincipalType.USER);
    Assert.assertEquals(3, roles.size());
    boolean sawRole1 = false, sawRole2 = false, sawPublic = false;
    for (Role role : roles) {
      if (role.getRoleName().equals(roleName1)) {
        sawRole1 = true;
      } else if (role.getRoleName().equals(roleName2)) {
        sawRole2 = true;
      } else if (role.getRoleName().equals(HiveMetaStore.PUBLIC)) {
        sawPublic = true;
      } else {
        Assert.fail("Unknown role name " + role.getRoleName());
      }
    }
    Assert.assertTrue(sawRole1 && sawRole2 && sawPublic);

    roles = store.listRoles("fred", PrincipalType.ROLE);
    Assert.assertEquals(0, roles.size());

    roles = store.listRoles(roleName1, PrincipalType.ROLE);
    Assert.assertEquals(1, roles.size());
    Role role = roles.get(0);
    Assert.assertEquals(roleName2, role.getRoleName());

    // Test listing all members in a role
    List<RolePrincipalGrant> grants = store.listRoleMembers(roleName1);
    Assert.assertEquals(1, grants.size());
    Assert.assertEquals("fred", grants.get(0).getPrincipalName());
    Assert.assertEquals(PrincipalType.USER, grants.get(0).getPrincipalType());
    Assert.assertTrue("Expected grant time of " + now + " got " + grants.get(0).getGrantTime(),
        grants.get(0).getGrantTime() >= now);
    Assert.assertEquals("bob", grants.get(0).getGrantorName());
    Assert.assertEquals(PrincipalType.USER, grants.get(0).getGrantorPrincipalType());
    Assert.assertFalse(grants.get(0).isGrantOption());

    grants = store.listRoleMembers(roleName2);
    Assert.assertEquals(2, grants.size());
    boolean sawFred = false;
    sawRole1 = false;
    for (RolePrincipalGrant m : grants) {
      if ("fred".equals(m.getPrincipalName())) sawFred = true;
      else if (roleName1.equals(m.getPrincipalName())) sawRole1 = true;
      else Assert.fail("Unexpected principal " + m.getPrincipalName());
    }
    Assert.assertTrue(sawFred && sawRole1);

    // Revoke a role with grant option, make sure it just goes to no grant option
    store.revokeRole(role2, roleName1, PrincipalType.ROLE, true);
    roles = store.listRoles(roleName1, PrincipalType.ROLE);
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals(roleName2, roles.get(0).getRoleName());

    grants = store.listRoleMembers(roleName1);
    Assert.assertFalse(grants.get(0).isGrantOption());

    // Drop a role, make sure it is properly removed from the map
    store.removeRole(roleName1);
    roles = store.listRoles("fred", PrincipalType.USER);
    Assert.assertEquals(2, roles.size());
    sawRole2 = sawPublic = false;
    for (Role m : roles) {
      if (m.getRoleName().equals(roleName2)) sawRole2 = true;
      else if (m.getRoleName().equals(HiveMetaStore.PUBLIC)) sawPublic = true;
      else Assert.fail("Unknown role " + m.getRoleName());
    }
    Assert.assertTrue(sawRole2 && sawPublic);
    roles = store.listRoles(roleName1, PrincipalType.ROLE);
    Assert.assertEquals(0, roles.size());

    // Revoke a role without grant option, make sure it goes away
    store.revokeRole(role2, "fred", PrincipalType.USER, false);
    roles = store.listRoles("fred", PrincipalType.USER);
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals(HiveMetaStore.PUBLIC, roles.get(0).getRoleName());
  }

  @Test
  public void userToRoleMap() throws Exception {
    String roleName1 = "utrm1";
    store.addRole(roleName1, "me");
    String roleName2 = "utrm2";
    store.addRole(roleName2, "me");
    String user1 = "wilma";
    String user2 = "betty";

    Role role1 = store.getRole(roleName1);
    Role role2 = store.getRole(roleName2);

    store.grantRole(role1, user1, PrincipalType.USER, "bob", PrincipalType.USER, false);
    store.grantRole(role1, roleName2, PrincipalType.ROLE, "admin", PrincipalType.ROLE, true);

    List<String> roles = HBaseReadWrite.getInstance().getUserRoles(user1);
    Assert.assertEquals(2, roles.size());
    String[] roleNames = roles.toArray(new String[roles.size()]);
    Arrays.sort(roleNames);
    Assert.assertArrayEquals(new String[]{roleName1, roleName2}, roleNames);

    store.grantRole(role2, user1, PrincipalType.USER, "admin", PrincipalType.ROLE, false);
    store.grantRole(role1, user2, PrincipalType.USER, "bob", PrincipalType.USER, false);

    HBaseReadWrite.setConf(conf);
    roles = HBaseReadWrite.getInstance().getUserRoles(user2);
    Assert.assertEquals(2, roles.size());
    roleNames = roles.toArray(new String[roles.size()]);
    Arrays.sort(roleNames);
    Assert.assertArrayEquals(new String[]{roleName1, roleName2}, roleNames);

    store.revokeRole(role1, roleName2, PrincipalType.ROLE, false);

    // user1 should still have both roles since she was granted into role1 specifically.  user2
    // should only have role2 now since role2 was revoked from role1.
    roles = HBaseReadWrite.getInstance().getUserRoles(user1);
    Assert.assertEquals(2, roles.size());
    roleNames = roles.toArray(new String[roles.size()]);
    Arrays.sort(roleNames);
    Assert.assertArrayEquals(new String[]{roleName1, roleName2}, roleNames);

    roles = HBaseReadWrite.getInstance().getUserRoles(user2);
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals(roleName1, roles.get(0));
  }

  @Test
  public void userToRoleMapOnDrop() throws Exception {
    String roleName1 = "utrmod1";
    store.addRole(roleName1, "me");
    String roleName2 = "utrmod2";
    store.addRole(roleName2, "me");
    String user1 = "pebbles";
    String user2 = "bam-bam";

    Role role1 = store.getRole(roleName1);
    Role role2 = store.getRole(roleName2);

    store.grantRole(role1, user1, PrincipalType.USER, "bob", PrincipalType.USER, false);
    store.grantRole(role1, roleName2, PrincipalType.ROLE, "admin", PrincipalType.ROLE, true);
    store.grantRole(role1, user2, PrincipalType.USER, "bob", PrincipalType.USER, false);

    List<String> roles = HBaseReadWrite.getInstance().getUserRoles(user2);
    Assert.assertEquals(2, roles.size());
    String[] roleNames = roles.toArray(new String[roles.size()]);
    Arrays.sort(roleNames);
    Assert.assertArrayEquals(new String[]{roleName1, roleName2}, roleNames);

    store.removeRole(roleName2);

    HBaseReadWrite.setConf(conf);
    roles = HBaseReadWrite.getInstance().getUserRoles(user1);
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals(roleName1, roles.get(0));

    roles = HBaseReadWrite.getInstance().getUserRoles(user2);
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals(roleName1, roles.get(0));
  }

  @Test
  public void grantRevokeGlobalPrivileges() throws Exception {
    doGrantRevoke(HiveObjectType.GLOBAL, null, null, new String[] {"grpg1", "grpg2"},
        new String[] {"bugs", "elmer", "daphy", "wiley"});
  }

  @Test
  public void grantRevokeDbPrivileges() throws Exception {
    String dbName = "grdbp_db";
    try {
      Database db = new Database(dbName, "no description", "file:///tmp", emptyParameters);
      store.createDatabase(db);
      doGrantRevoke(HiveObjectType.DATABASE, dbName, null,
          new String[] {"grdbp_role1", "grdbp_role2"},
          new String[] {"fred", "barney", "wilma", "betty"});
    } finally {
      store.dropDatabase(dbName);
    }
  }

  @Test
  public void grantRevokeTablePrivileges() throws Exception {
    String dbName = "grtp_db";
    String tableName = "grtp_table";
    try {
      Database db = new Database(dbName, "no description", "file:///tmp", emptyParameters);
      store.createDatabase(db);
      int startTime = (int)(System.currentTimeMillis() / 1000);
      List<FieldSchema> cols = new ArrayList<FieldSchema>();
      cols.add(new FieldSchema("col1", "int", "nocomment"));
      SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
      StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
          serde, null, null, emptyParameters);
      Table table = new Table(tableName, dbName, "me", startTime, startTime, 0, sd, null,
          emptyParameters, null, null, null);
      store.createTable(table);
      doGrantRevoke(HiveObjectType.TABLE, dbName, tableName,
          new String[] {"grtp_role1", "grtp_role2"},
          new String[] {"batman", "robin", "superman", "wonderwoman"});

    } finally {
      if (store.getTable(dbName, tableName) != null) store.dropTable(dbName, tableName);
      store.dropDatabase(dbName);
    }
  }

  private void doGrantRevoke(HiveObjectType objectType, String dbName, String tableName,
                             String[] roleNames, String[] userNames)
      throws Exception {
    store.addRole(roleNames[0], "me");
    store.addRole(roleNames[1], "me");
    int now = (int)(System.currentTimeMillis() / 1000);

    Role role1 = store.getRole(roleNames[0]);
    Role role2 = store.getRole(roleNames[1]);
    store.grantRole(role1, userNames[0], PrincipalType.USER, "bob", PrincipalType.USER, false);
    store.grantRole(role1, roleNames[1], PrincipalType.ROLE, "admin", PrincipalType.ROLE, true);
    store.grantRole(role2, userNames[1], PrincipalType.USER, "bob", PrincipalType.USER, false);

    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    HiveObjectRef hiveObjRef = new HiveObjectRef(objectType, dbName, tableName, null, null);
    PrivilegeGrantInfo grantInfo =
        new PrivilegeGrantInfo("read", now, "me", PrincipalType.USER, false);
    HiveObjectPrivilege hop = new HiveObjectPrivilege(hiveObjRef, userNames[0], PrincipalType.USER,
        grantInfo);
    privileges.add(hop);

    hiveObjRef = new HiveObjectRef(objectType, dbName, tableName, null, null);
    grantInfo = new PrivilegeGrantInfo("write", now, "me", PrincipalType.USER, true);
    hop = new HiveObjectPrivilege(hiveObjRef, roleNames[0], PrincipalType.ROLE, grantInfo);
    privileges.add(hop);

    hiveObjRef = new HiveObjectRef(objectType, dbName, tableName, null, null);
    grantInfo = new PrivilegeGrantInfo("exec", now, "me", PrincipalType.USER, false);
    hop = new HiveObjectPrivilege(hiveObjRef, roleNames[1], PrincipalType.ROLE, grantInfo);
    privileges.add(hop);

    hiveObjRef = new HiveObjectRef(objectType, dbName, tableName, null, null);
    grantInfo = new PrivilegeGrantInfo("create", now, "me", PrincipalType.USER, true);
    hop = new HiveObjectPrivilege(hiveObjRef, userNames[2], PrincipalType.USER, grantInfo);
    privileges.add(hop);

    hiveObjRef = new HiveObjectRef(objectType, dbName, tableName, null, null);
    grantInfo = new PrivilegeGrantInfo("create2", now, "me", PrincipalType.USER, true);
    hop = new HiveObjectPrivilege(hiveObjRef, userNames[2], PrincipalType.USER, grantInfo);
    privileges.add(hop);

    PrivilegeBag pBag = new PrivilegeBag(privileges);
    store.grantPrivileges(pBag);

    PrincipalPrivilegeSet pps = getPPS(objectType, dbName, tableName, userNames[0]);

    Assert.assertEquals(1, pps.getUserPrivilegesSize());
    Assert.assertEquals(1, pps.getUserPrivileges().get(userNames[0]).size());
    grantInfo = pps.getUserPrivileges().get(userNames[0]).get(0);
    Assert.assertEquals("read", grantInfo.getPrivilege());
    Assert.assertTrue(now <= grantInfo.getCreateTime());
    Assert.assertEquals("me", grantInfo.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantInfo.getGrantorType());
    Assert.assertFalse(grantInfo.isGrantOption());

    Assert.assertEquals(2, pps.getRolePrivilegesSize());
    Assert.assertEquals(1, pps.getRolePrivileges().get(roleNames[0]).size());
    grantInfo = pps.getRolePrivileges().get(roleNames[0]).get(0);
    Assert.assertEquals("write", grantInfo.getPrivilege());
    Assert.assertTrue(now <= grantInfo.getCreateTime());
    Assert.assertEquals("me", grantInfo.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantInfo.getGrantorType());
    Assert.assertTrue(grantInfo.isGrantOption());

    Assert.assertEquals(1, pps.getRolePrivileges().get(roleNames[1]).size());
    grantInfo = pps.getRolePrivileges().get(roleNames[1]).get(0);
    Assert.assertEquals("exec", grantInfo.getPrivilege());
    Assert.assertTrue(now <= grantInfo.getCreateTime());
    Assert.assertEquals("me", grantInfo.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantInfo.getGrantorType());
    Assert.assertFalse(grantInfo.isGrantOption());

    pps = getPPS(objectType, dbName, tableName, userNames[1]);

    Assert.assertEquals(0, pps.getUserPrivilegesSize());

    Assert.assertEquals(1, pps.getRolePrivilegesSize());
    Assert.assertEquals(1, pps.getRolePrivileges().get(roleNames[1]).size());
    grantInfo = pps.getRolePrivileges().get(roleNames[1]).get(0);
    Assert.assertEquals("exec", grantInfo.getPrivilege());
    Assert.assertTrue(now <= grantInfo.getCreateTime());
    Assert.assertEquals("me", grantInfo.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantInfo.getGrantorType());
    Assert.assertFalse(grantInfo.isGrantOption());

    pps = getPPS(objectType, dbName, tableName, userNames[2]);

    Assert.assertEquals(1, pps.getUserPrivilegesSize());
    Assert.assertEquals(2, pps.getUserPrivileges().get(userNames[2]).size());
    Assert.assertEquals(0, pps.getRolePrivilegesSize());

    pps = getPPS(objectType, dbName, tableName, userNames[3]);
    Assert.assertEquals(0, pps.getUserPrivilegesSize());
    Assert.assertEquals(0, pps.getRolePrivilegesSize());

    // Test that removing role removes the role grants
    store.removeRole(roleNames[1]);
    checkRoleRemovedFromAllPrivileges(objectType, dbName, tableName, roleNames[1]);
    pps = getPPS(objectType, dbName, tableName, userNames[0]);

    Assert.assertEquals(1, pps.getRolePrivilegesSize());
    Assert.assertEquals(1, pps.getRolePrivileges().get(roleNames[0]).size());

    pps = getPPS(objectType, dbName, tableName, userNames[1]);

    Assert.assertEquals(0, pps.getRolePrivilegesSize());

    // Test that revoking with grant option = true just removes grant option
    privileges.clear();
    hiveObjRef = new HiveObjectRef(objectType, dbName, tableName, null, null);
    grantInfo = new PrivilegeGrantInfo("write", now, "me", PrincipalType.USER, true);
    hop = new HiveObjectPrivilege(hiveObjRef, roleNames[0], PrincipalType.ROLE, grantInfo);
    privileges.add(hop);

    hiveObjRef = new HiveObjectRef(objectType, dbName, tableName, null, null);
    grantInfo = new PrivilegeGrantInfo("create2", now, "me", PrincipalType.USER, true);
    hop = new HiveObjectPrivilege(hiveObjRef, userNames[2], PrincipalType.USER, grantInfo);
    privileges.add(hop);

    pBag = new PrivilegeBag(privileges);
    store.revokePrivileges(pBag, true);
    pps = getPPS(objectType, dbName, tableName, userNames[0]);

    Assert.assertEquals(1, pps.getRolePrivilegesSize());
    Assert.assertEquals(1, pps.getRolePrivileges().get(roleNames[0]).size());
    grantInfo = pps.getRolePrivileges().get(roleNames[0]).get(0);
    Assert.assertEquals("write", grantInfo.getPrivilege());
    Assert.assertTrue(now <= grantInfo.getCreateTime());
    Assert.assertEquals("me", grantInfo.getGrantor());
    Assert.assertEquals(PrincipalType.USER, grantInfo.getGrantorType());
    Assert.assertFalse(grantInfo.isGrantOption());

    pps = getPPS(objectType, dbName, tableName, userNames[2]);

    Assert.assertEquals(1, pps.getUserPrivilegesSize());
    Assert.assertEquals(2, pps.getUserPrivileges().get(userNames[2]).size());
    for (PrivilegeGrantInfo pgi : pps.getUserPrivileges().get(userNames[2])) {
      if (pgi.getPrivilege().equals("create")) Assert.assertTrue(pgi.isGrantOption());
      else if (pgi.getPrivilege().equals("create2")) Assert.assertFalse(pgi.isGrantOption());
      else Assert.fail("huh?");
    }

    // Test revoking revokes
    store.revokePrivileges(pBag, false);

    pps = getPPS(objectType, dbName, tableName, userNames[0]);

    Assert.assertEquals(1, pps.getUserPrivilegesSize());
    Assert.assertEquals(1, pps.getRolePrivilegesSize());
    Assert.assertEquals(0, pps.getRolePrivileges().get(roleNames[0]).size());

    pps = getPPS(objectType, dbName, tableName, userNames[2]);
    Assert.assertEquals(1, pps.getUserPrivilegesSize());
    Assert.assertEquals(1, pps.getUserPrivileges().get(userNames[2]).size());
    Assert.assertEquals("create", pps.getUserPrivileges().get(userNames[2]).get(0).getPrivilege());
    Assert.assertEquals(0, pps.getRolePrivilegesSize());
  }

  private PrincipalPrivilegeSet getPPS(HiveObjectType objectType, String dbName, String tableName,
                                       String userName)
      throws InvalidObjectException, MetaException {
    switch (objectType) {
      case GLOBAL: return store.getUserPrivilegeSet(userName, null);
      case DATABASE: return store.getDBPrivilegeSet(dbName, userName, null);
      case TABLE: return store.getTablePrivilegeSet(dbName, tableName, userName, null);
      default: throw new RuntimeException("huh?");
    }
  }

  private void checkRoleRemovedFromAllPrivileges(HiveObjectType objectType, String dbName,
                                                 String tableName, String roleName)
      throws IOException, NoSuchObjectException, MetaException {
    List<PrivilegeGrantInfo> pgi = null;
    switch (objectType) {
      case GLOBAL:
        pgi = HBaseReadWrite.getInstance().getGlobalPrivs().getRolePrivileges().get(roleName);
        break;

      case DATABASE:
        pgi = store.getDatabase(dbName).getPrivileges().getRolePrivileges().get(roleName);
        break;

      case TABLE:
        pgi = store.getTable(dbName, tableName).getPrivileges().getRolePrivileges().get(roleName);
        break;

      default:
        Assert.fail();
    }

    Assert.assertNull("Expected null for role " + roleName + " for type " + objectType.toString()
      + " with db " + dbName + " and table " + tableName, pgi);
  }

  @Test
  public void listDbGrants() throws Exception {
    String dbNames[] = new String[] {"ldbg_db1", "ldbg_db2"};
    try {
      Database db = new Database(dbNames[0], "no description", "file:///tmp", emptyParameters);
      store.createDatabase(db);
      db = new Database(dbNames[1], "no description", "file:///tmp", emptyParameters);
      store.createDatabase(db);
      String[] roleNames = new String[]{"ldbg_role1", "ldbg_role2"};
      String[] userNames = new String[]{"frodo", "sam"};

      store.addRole(roleNames[0], "me");
      store.addRole(roleNames[1], "me");
      int now = (int)(System.currentTimeMillis() / 1000);

      Role role1 = store.getRole(roleNames[0]);
      Role role2 = store.getRole(roleNames[1]);
      store.grantRole(role1, userNames[0], PrincipalType.USER, "bob", PrincipalType.USER, false);
      store.grantRole(role1, roleNames[1], PrincipalType.ROLE, "admin", PrincipalType.ROLE, true);
      store.grantRole(role2, userNames[1], PrincipalType.USER, "bob", PrincipalType.USER, false);

      List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
      HiveObjectRef hiveObjRef =
          new HiveObjectRef(HiveObjectType.DATABASE, dbNames[0], null, null, null);
      PrivilegeGrantInfo grantInfo =
          new PrivilegeGrantInfo("read", now, "me", PrincipalType.USER, false);
      HiveObjectPrivilege hop = new HiveObjectPrivilege(hiveObjRef, userNames[0], PrincipalType.USER,
          grantInfo);
      privileges.add(hop);

      grantInfo = new PrivilegeGrantInfo("write", now, "me", PrincipalType.USER, true);
      hop = new HiveObjectPrivilege(hiveObjRef, roleNames[0], PrincipalType.ROLE, grantInfo);
      privileges.add(hop);

      PrivilegeBag pBag = new PrivilegeBag(privileges);
      store.grantPrivileges(pBag);

      List<HiveObjectPrivilege> hops =
          store.listPrincipalDBGrants(roleNames[0], PrincipalType.ROLE, dbNames[0]);
      Assert.assertEquals(1, hops.size());
      Assert.assertEquals(PrincipalType.ROLE, hops.get(0).getPrincipalType());
      Assert.assertEquals(HiveObjectType.DATABASE, hops.get(0).getHiveObject().getObjectType());
      Assert.assertEquals("write", hops.get(0).getGrantInfo().getPrivilege());

      hops = store.listPrincipalDBGrants(userNames[0], PrincipalType.USER, dbNames[0]);
      Assert.assertEquals(1, hops.size());
      Assert.assertEquals(PrincipalType.USER, hops.get(0).getPrincipalType());
      Assert.assertEquals(HiveObjectType.DATABASE, hops.get(0).getHiveObject().getObjectType());
      Assert.assertEquals("read", hops.get(0).getGrantInfo().getPrivilege());

      hops = store.listPrincipalDBGrants(roleNames[1], PrincipalType.ROLE, dbNames[0]);
      Assert.assertEquals(0, hops.size());
      hops = store.listPrincipalDBGrants(userNames[1], PrincipalType.USER, dbNames[0]);
      Assert.assertEquals(0, hops.size());

      hops = store.listPrincipalDBGrants(roleNames[0], PrincipalType.ROLE, dbNames[1]);
      Assert.assertEquals(0, hops.size());
      hops = store.listPrincipalDBGrants(userNames[0], PrincipalType.USER, dbNames[1]);
      Assert.assertEquals(0, hops.size());

      hops = store.listDBGrantsAll(dbNames[0]);
      Assert.assertEquals(2, hops.size());
      boolean sawUser = false, sawRole = false;
      for (HiveObjectPrivilege h : hops) {
        if (h.getPrincipalName().equals(userNames[0])) {
          Assert.assertEquals(PrincipalType.USER, h.getPrincipalType());
          Assert.assertEquals(HiveObjectType.DATABASE, h.getHiveObject().getObjectType());
          Assert.assertEquals("read", h.getGrantInfo().getPrivilege());
          sawUser = true;
        } else if (h.getPrincipalName().equals(roleNames[0])) {
          Assert.assertEquals(PrincipalType.ROLE, h.getPrincipalType());
          Assert.assertEquals(HiveObjectType.DATABASE, h.getHiveObject().getObjectType());
          Assert.assertEquals("write", h.getGrantInfo().getPrivilege());
          sawRole = true;
        }
      }
      Assert.assertTrue(sawUser && sawRole);

      hops = store.listPrincipalDBGrantsAll(roleNames[0], PrincipalType.ROLE);
      Assert.assertEquals(1, hops.size());
      Assert.assertEquals(PrincipalType.ROLE, hops.get(0).getPrincipalType());
      Assert.assertEquals(HiveObjectType.DATABASE, hops.get(0).getHiveObject().getObjectType());
      Assert.assertEquals("write", hops.get(0).getGrantInfo().getPrivilege());

      hops = store.listPrincipalDBGrantsAll(userNames[0], PrincipalType.USER);
      Assert.assertEquals(1, hops.size());
      Assert.assertEquals(PrincipalType.USER, hops.get(0).getPrincipalType());
      Assert.assertEquals(HiveObjectType.DATABASE, hops.get(0).getHiveObject().getObjectType());
      Assert.assertEquals("read", hops.get(0).getGrantInfo().getPrivilege());

      hops = store.listPrincipalDBGrantsAll(roleNames[1], PrincipalType.ROLE);
      Assert.assertEquals(0, hops.size());
      hops = store.listPrincipalDBGrantsAll(userNames[1], PrincipalType.USER);
      Assert.assertEquals(0, hops.size());


    } finally {
      store.dropDatabase(dbNames[0]);
      store.dropDatabase(dbNames[1]);
    }
  }

  @Test
  public void listGlobalGrants() throws Exception {
    String[] roleNames = new String[]{"lgg_role1", "lgg_role2"};
    String[] userNames = new String[]{"merry", "pippen"};

    store.addRole(roleNames[0], "me");
    store.addRole(roleNames[1], "me");
    int now = (int)(System.currentTimeMillis() / 1000);

    Role role1 = store.getRole(roleNames[0]);
    Role role2 = store.getRole(roleNames[1]);
    store.grantRole(role1, userNames[0], PrincipalType.USER, "bob", PrincipalType.USER, false);
    store.grantRole(role1, roleNames[1], PrincipalType.ROLE, "admin", PrincipalType.ROLE, true);
    store.grantRole(role2, userNames[1], PrincipalType.USER, "bob", PrincipalType.USER, false);

    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    HiveObjectRef hiveObjRef =
        new HiveObjectRef(HiveObjectType.GLOBAL, null, null, null, null);
    PrivilegeGrantInfo grantInfo =
        new PrivilegeGrantInfo("read", now, "me", PrincipalType.USER, false);
    HiveObjectPrivilege hop = new HiveObjectPrivilege(hiveObjRef, userNames[0], PrincipalType.USER,
        grantInfo);
    privileges.add(hop);

    grantInfo = new PrivilegeGrantInfo("write", now, "me", PrincipalType.USER, true);
    hop = new HiveObjectPrivilege(hiveObjRef, roleNames[0], PrincipalType.ROLE, grantInfo);
    privileges.add(hop);

    PrivilegeBag pBag = new PrivilegeBag(privileges);
    store.grantPrivileges(pBag);

    List<HiveObjectPrivilege> hops =
        store.listPrincipalGlobalGrants(roleNames[0], PrincipalType.ROLE);
    Assert.assertEquals(1, hops.size());
    Assert.assertEquals(PrincipalType.ROLE, hops.get(0).getPrincipalType());
    Assert.assertEquals(HiveObjectType.GLOBAL, hops.get(0).getHiveObject().getObjectType());
    Assert.assertEquals("write", hops.get(0).getGrantInfo().getPrivilege());

    hops = store.listPrincipalGlobalGrants(userNames[0], PrincipalType.USER);
    Assert.assertEquals(1, hops.size());
    Assert.assertEquals(PrincipalType.USER, hops.get(0).getPrincipalType());
    Assert.assertEquals(HiveObjectType.GLOBAL, hops.get(0).getHiveObject().getObjectType());
    Assert.assertEquals("read", hops.get(0).getGrantInfo().getPrivilege());

    hops = store.listPrincipalGlobalGrants(roleNames[1], PrincipalType.ROLE);
    Assert.assertEquals(0, hops.size());
    hops = store.listPrincipalGlobalGrants(userNames[1], PrincipalType.USER);
    Assert.assertEquals(0, hops.size());

    hops = store.listGlobalGrantsAll();
    Assert.assertEquals(2, hops.size());
    boolean sawUser = false, sawRole = false;
    for (HiveObjectPrivilege h : hops) {
      if (h.getPrincipalName().equals(userNames[0])) {
        Assert.assertEquals(PrincipalType.USER, h.getPrincipalType());
        Assert.assertEquals(HiveObjectType.GLOBAL, h.getHiveObject().getObjectType());
        Assert.assertEquals("read", h.getGrantInfo().getPrivilege());
        sawUser = true;
      } else if (h.getPrincipalName().equals(roleNames[0])) {
        Assert.assertEquals(PrincipalType.ROLE, h.getPrincipalType());
        Assert.assertEquals(HiveObjectType.GLOBAL, h.getHiveObject().getObjectType());
        Assert.assertEquals("write", h.getGrantInfo().getPrivilege());
        sawRole = true;
      }
    }
    Assert.assertTrue(sawUser && sawRole);
  }

  @Test
  public void listTableGrants() throws Exception {
    String dbName = "ltg_db";
    String[] tableNames = new String[] {"ltg_t1", "ltg_t2"};
    try {
      Database db = new Database(dbName, "no description", "file:///tmp", emptyParameters);
      store.createDatabase(db);
      int startTime = (int)(System.currentTimeMillis() / 1000);
      List<FieldSchema> cols = new ArrayList<FieldSchema>();
      cols.add(new FieldSchema("col1", "int", "nocomment"));
      SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
      StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
          serde, null, null, emptyParameters);
      Table table = new Table(tableNames[0], dbName, "me", startTime, startTime, 0, sd, null,
          emptyParameters, null, null, null);
      store.createTable(table);
      table = new Table(tableNames[1], dbName, "me", startTime, startTime, 0, sd, null,
          emptyParameters, null, null, null);
      store.createTable(table);
      String[] roleNames = new String[]{"ltg_role1", "ltg_role2"};
      String[] userNames = new String[]{"gandalf", "radagast"};

      store.addRole(roleNames[0], "me");
      store.addRole(roleNames[1], "me");
      int now = (int)(System.currentTimeMillis() / 1000);

      Role role1 = store.getRole(roleNames[0]);
      Role role2 = store.getRole(roleNames[1]);
      store.grantRole(role1, userNames[0], PrincipalType.USER, "bob", PrincipalType.USER, false);
      store.grantRole(role1, roleNames[1], PrincipalType.ROLE, "admin", PrincipalType.ROLE, true);
      store.grantRole(role2, userNames[1], PrincipalType.USER, "bob", PrincipalType.USER, false);

      List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
      HiveObjectRef hiveObjRef =
          new HiveObjectRef(HiveObjectType.TABLE, dbName, tableNames[0], null, null);
      PrivilegeGrantInfo grantInfo =
          new PrivilegeGrantInfo("read", now, "me", PrincipalType.USER, false);
      HiveObjectPrivilege hop = new HiveObjectPrivilege(hiveObjRef, userNames[0], PrincipalType.USER,
          grantInfo);
      privileges.add(hop);

      grantInfo = new PrivilegeGrantInfo("write", now, "me", PrincipalType.USER, true);
      hop = new HiveObjectPrivilege(hiveObjRef, roleNames[0], PrincipalType.ROLE, grantInfo);
      privileges.add(hop);

      PrivilegeBag pBag = new PrivilegeBag(privileges);
      store.grantPrivileges(pBag);

      List<HiveObjectPrivilege> hops =
          store.listAllTableGrants(roleNames[0], PrincipalType.ROLE, dbName, tableNames[0]);
      Assert.assertEquals(1, hops.size());
      Assert.assertEquals(PrincipalType.ROLE, hops.get(0).getPrincipalType());
      Assert.assertEquals(HiveObjectType.TABLE, hops.get(0).getHiveObject().getObjectType());
      Assert.assertEquals("write", hops.get(0).getGrantInfo().getPrivilege());

      hops = store.listAllTableGrants(userNames[0], PrincipalType.USER, dbName, tableNames[0]);
      Assert.assertEquals(1, hops.size());
      Assert.assertEquals(PrincipalType.USER, hops.get(0).getPrincipalType());
      Assert.assertEquals(HiveObjectType.TABLE, hops.get(0).getHiveObject().getObjectType());
      Assert.assertEquals("read", hops.get(0).getGrantInfo().getPrivilege());

      hops = store.listAllTableGrants(roleNames[1], PrincipalType.ROLE, dbName, tableNames[0]);
      Assert.assertEquals(0, hops.size());
      hops = store.listAllTableGrants(userNames[1], PrincipalType.USER, dbName, tableNames[0]);
      Assert.assertEquals(0, hops.size());

      hops = store.listAllTableGrants(roleNames[0], PrincipalType.ROLE, dbName, tableNames[1]);
      Assert.assertEquals(0, hops.size());
      hops = store.listAllTableGrants(userNames[0], PrincipalType.USER, dbName, tableNames[1]);
      Assert.assertEquals(0, hops.size());

      hops = store.listTableGrantsAll(dbName, tableNames[0]);
      Assert.assertEquals(2, hops.size());
      boolean sawUser = false, sawRole = false;
      for (HiveObjectPrivilege h : hops) {
        if (h.getPrincipalName().equals(userNames[0])) {
          Assert.assertEquals(PrincipalType.USER, h.getPrincipalType());
          Assert.assertEquals(HiveObjectType.TABLE, h.getHiveObject().getObjectType());
          Assert.assertEquals("read", h.getGrantInfo().getPrivilege());
          sawUser = true;
        } else if (h.getPrincipalName().equals(roleNames[0])) {
          Assert.assertEquals(PrincipalType.ROLE, h.getPrincipalType());
          Assert.assertEquals(HiveObjectType.TABLE, h.getHiveObject().getObjectType());
          Assert.assertEquals("write", h.getGrantInfo().getPrivilege());
          sawRole = true;
        }
      }
      Assert.assertTrue(sawUser && sawRole);

      hops = store.listPrincipalTableGrantsAll(roleNames[0], PrincipalType.ROLE);
      Assert.assertEquals(1, hops.size());
      Assert.assertEquals(PrincipalType.ROLE, hops.get(0).getPrincipalType());
      Assert.assertEquals(HiveObjectType.TABLE, hops.get(0).getHiveObject().getObjectType());
      Assert.assertEquals("write", hops.get(0).getGrantInfo().getPrivilege());

      hops = store.listPrincipalTableGrantsAll(userNames[0], PrincipalType.USER);
      Assert.assertEquals(1, hops.size());
      Assert.assertEquals(PrincipalType.USER, hops.get(0).getPrincipalType());
      Assert.assertEquals(HiveObjectType.TABLE, hops.get(0).getHiveObject().getObjectType());
      Assert.assertEquals("read", hops.get(0).getGrantInfo().getPrivilege());

      hops = store.listPrincipalDBGrantsAll(roleNames[1], PrincipalType.ROLE);
      Assert.assertEquals(0, hops.size());
      hops = store.listPrincipalDBGrantsAll(userNames[1], PrincipalType.USER);
      Assert.assertEquals(0, hops.size());


    } finally {
      store.dropTable(dbName, tableNames[0]);
      store.dropTable(dbName, tableNames[1]);
      store.dropDatabase(dbName);
    }
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
    for (String partVal : partVals) {
      Partition part = new Partition(Arrays.asList(partVal), dbname, tableName, (int) now / 1000,
          (int) now / 1000, sd, emptyParameters);
      store.addPartition(part);
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

  @Test
  public void delegationToken() throws Exception {
    store.addToken("abc", "def");
    store.addToken("ghi", "jkl");

    Assert.assertEquals("def", store.getToken("abc"));
    Assert.assertEquals("jkl", store.getToken("ghi"));
    Assert.assertNull(store.getToken("wabawaba"));
    String[] allToks = store.getAllTokenIdentifiers().toArray(new String[2]);
    Arrays.sort(allToks);
    Assert.assertArrayEquals(new String[]{"abc", "ghi"}, allToks);

    store.removeToken("abc");
    store.removeToken("wabawaba");

    Assert.assertNull(store.getToken("abc"));
    Assert.assertEquals("jkl", store.getToken("ghi"));
    allToks = store.getAllTokenIdentifiers().toArray(new String[1]);
    Assert.assertArrayEquals(new String[]{"ghi"}, allToks);
  }

  @Test
  public void masterKey() throws Exception {
    Assert.assertEquals(0, store.addMasterKey("k1"));
    Assert.assertEquals(1, store.addMasterKey("k2"));

    String[] keys = store.getMasterKeys();
    Arrays.sort(keys);
    Assert.assertArrayEquals(new String[]{"k1", "k2"}, keys);

    store.updateMasterKey(0, "k3");
    keys = store.getMasterKeys();
    Arrays.sort(keys);
    Assert.assertArrayEquals(new String[]{"k2", "k3"}, keys);

    store.removeMasterKey(1);
    keys = store.getMasterKeys();
    Assert.assertArrayEquals(new String[]{"k3"}, keys);

    thrown.expect(NoSuchObjectException.class);
    store.updateMasterKey(72, "whatever");
  }

}
