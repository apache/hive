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
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 */
public class TestHBaseStore {
  private static final Log LOG = LogFactory.getLog(TestHBaseStore.class.getName());
  static Map<String, String> emptyParameters = new HashMap<String, String>();

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock HTableInterface htable;
  SortedMap<String, Cell> rows = new TreeMap<String, Cell>();
  HBaseStore store;



  @Before
  public void init() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();
    conf.setBoolean(HBaseReadWrite.NO_CACHE_CONF, true);
    store = MockUtils.init(conf, htable, rows);
  }

  @Test
  public void createDb() throws Exception {
    String dbname = "mydb";
    Database db = new Database(dbname, "no description", "file:///tmp", emptyParameters);
    store.createDatabase(db);

    Database d = store.getDatabase(dbname);
    Assert.assertEquals(dbname, d.getName());
    Assert.assertEquals("no description", d.getDescription());
    Assert.assertEquals("file:///tmp", d.getLocationUri());
  }

  @Test
  public void alterDb() throws Exception {
    String dbname = "mydb";
    Database db = new Database(dbname, "no description", "file:///tmp", emptyParameters);
    store.createDatabase(db);
    db.setDescription("a description");
    store.alterDatabase(dbname, db);

    Database d = store.getDatabase(dbname);
    Assert.assertEquals(dbname, d.getName());
    Assert.assertEquals("a description", d.getDescription());
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
  public void createFunction() throws Exception {
    String dbname = "default";
    String funcName = "createfunc";
    int now = (int)(System.currentTimeMillis()/ 1000);
    Function func = new Function(funcName, dbname, "o.a.h.h.myfunc", "me", PrincipalType.USER,
        now, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR,
        "file:/tmp/somewhere")));
    store.createFunction(func);

    Function f = store.getFunction(dbname, funcName);
    Assert.assertEquals(dbname, f.getDbName());
    Assert.assertEquals(funcName, f.getFunctionName());
    Assert.assertEquals("o.a.h.h.myfunc", f.getClassName());
    Assert.assertEquals("me", f.getOwnerName());
    Assert.assertEquals(PrincipalType.USER, f.getOwnerType());
    Assert.assertTrue(now <= f.getCreateTime());
    Assert.assertEquals(FunctionType.JAVA, f.getFunctionType());
    Assert.assertEquals(1, f.getResourceUrisSize());
    Assert.assertEquals(ResourceType.JAR, f.getResourceUris().get(0).getResourceType());
    Assert.assertEquals("file:/tmp/somewhere", f.getResourceUris().get(0).getUri());
  }

  @Test
  public void alterFunction() throws Exception {
    String dbname = "default";
    String funcName = "alterfunc";
    int now = (int)(System.currentTimeMillis()/ 1000);
    List<ResourceUri> uris = new ArrayList<ResourceUri>();
    uris.add(new ResourceUri(ResourceType.FILE, "whatever"));
    Function func = new Function(funcName, dbname, "o.a.h.h.myfunc", "me", PrincipalType.USER,
        now, FunctionType.JAVA, uris);
    store.createFunction(func);

    Function f = store.getFunction(dbname, funcName);
    Assert.assertEquals(ResourceType.FILE, f.getResourceUris().get(0).getResourceType());

    func.addToResourceUris(new ResourceUri(ResourceType.ARCHIVE, "file"));
    store.alterFunction(dbname, funcName, func);

    f = store.getFunction(dbname, funcName);
    Assert.assertEquals(2, f.getResourceUrisSize());
    Assert.assertEquals(ResourceType.FILE, f.getResourceUris().get(0).getResourceType());
    Assert.assertEquals(ResourceType.ARCHIVE, f.getResourceUris().get(1).getResourceType());

  }

  @Test
  public void dropFunction() throws Exception {
    String dbname = "default";
    String funcName = "delfunc";
    int now = (int)(System.currentTimeMillis()/ 1000);
    Function func = new Function(funcName, dbname, "o.a.h.h.myfunc", "me", PrincipalType.USER,
        now, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR, "file:/tmp/somewhere")));
    store.createFunction(func);

    Function f = store.getFunction(dbname, funcName);
    Assert.assertNotNull(f);

    store.dropFunction(dbname, funcName);
    //thrown.expect(NoSuchObjectException.class);
    Assert.assertNull(store.getFunction(dbname, funcName));
  }

  @Test
  public void createTable() throws Exception {
    String tableName = "mytable";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17,
        serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    Table table = new Table(tableName, "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    Table t = store.getTable("default", tableName);
    Assert.assertEquals(1, t.getSd().getColsSize());
    Assert.assertEquals("col1", t.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", t.getSd().getCols().get(0).getType());
    Assert.assertEquals("", t.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", t.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", t.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp", t.getSd().getLocation());
    Assert.assertEquals("input", t.getSd().getInputFormat());
    Assert.assertEquals("output", t.getSd().getOutputFormat());
    Assert.assertFalse(t.getSd().isCompressed());
    Assert.assertEquals(17, t.getSd().getNumBuckets());
    Assert.assertEquals(1, t.getSd().getBucketColsSize());
    Assert.assertEquals("bucketcol", t.getSd().getBucketCols().get(0));
    Assert.assertEquals(1, t.getSd().getSortColsSize());
    Assert.assertEquals("sortcol", t.getSd().getSortCols().get(0).getCol());
    Assert.assertEquals(1, t.getSd().getSortCols().get(0).getOrder());
    Assert.assertEquals(1, t.getSd().getParametersSize());
    Assert.assertEquals("value", t.getSd().getParameters().get("key"));
    Assert.assertEquals("me", t.getOwner());
    Assert.assertEquals("default", t.getDbName());
    Assert.assertEquals(tableName, t.getTableName());
    Assert.assertEquals(0, t.getParametersSize());
  }

  @Test
  public void skewInfo() throws Exception {
    String tableName = "mytable";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", true, 0,
        serde, null, null, emptyParameters);

    Map<List<String>, String> map = new HashMap<List<String>, String>();
    map.put(Arrays.asList("col3"), "col4");
    SkewedInfo skew = new SkewedInfo(Arrays.asList("col1"), Arrays.asList(Arrays.asList("col2")),
        map);
    sd.setSkewedInfo(skew);
    Table table = new Table(tableName, "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    Table t = store.getTable("default", tableName);
    Assert.assertEquals(1, t.getSd().getColsSize());
    Assert.assertEquals("col1", t.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", t.getSd().getCols().get(0).getType());
    Assert.assertEquals("", t.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", t.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", t.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp", t.getSd().getLocation());
    Assert.assertEquals("input", t.getSd().getInputFormat());
    Assert.assertEquals("output", t.getSd().getOutputFormat());
    Assert.assertTrue(t.getSd().isCompressed());
    Assert.assertEquals(0, t.getSd().getNumBuckets());
    Assert.assertEquals(0, t.getSd().getSortColsSize());
    Assert.assertEquals("me", t.getOwner());
    Assert.assertEquals("default", t.getDbName());
    Assert.assertEquals(tableName, t.getTableName());
    Assert.assertEquals(0, t.getParametersSize());

    skew = t.getSd().getSkewedInfo();
    Assert.assertNotNull(skew);
    Assert.assertEquals(1, skew.getSkewedColNamesSize());
    Assert.assertEquals("col1", skew.getSkewedColNames().get(0));
    Assert.assertEquals(1, skew.getSkewedColValuesSize());
    Assert.assertEquals("col2", skew.getSkewedColValues().get(0).get(0));
    Assert.assertEquals(1, skew.getSkewedColValueLocationMapsSize());
    Assert.assertEquals("col4", skew.getSkewedColValueLocationMaps().get(Arrays.asList("col3")));

  }

  @Test
  public void hashSd() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", true, 0,
        serde, null, null, emptyParameters);

    Map<List<String>, String> map = new HashMap<List<String>, String>();
    map.put(Arrays.asList("col3"), "col4");
    SkewedInfo skew = new SkewedInfo(Arrays.asList("col1"), Arrays.asList(Arrays.asList("col2")),
        map);
    sd.setSkewedInfo(skew);

    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] baseHash = HBaseUtils.hashStorageDescriptor(sd, md);

    StorageDescriptor changeSchema = new StorageDescriptor(sd);
    changeSchema.getCols().add(new FieldSchema("col2", "varchar(32)", "a comment"));
    byte[] schemaHash = HBaseUtils.hashStorageDescriptor(changeSchema, md);
    Assert.assertFalse(Arrays.equals(baseHash, schemaHash));

    StorageDescriptor changeLocation = new StorageDescriptor(sd);
    changeLocation.setLocation("file:/somewhere/else");
    byte[] locationHash = HBaseUtils.hashStorageDescriptor(changeLocation, md);
    Assert.assertArrayEquals(baseHash, locationHash);
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

    List<String> vals = Arrays.asList("fred");
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

    Assert.assertTrue(store.doesPartitionExist(dbName, tableName, vals));
    Assert.assertFalse(store.doesPartitionExist(dbName, tableName, Arrays.asList("bob")));
  }

  @Test
  public void alterPartition() throws Exception {
    String dbName = "default";
    String tableName = "alterparttable";
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

    part.setLastAccessTime(startTime + 10);
    store.alterPartition(dbName, tableName, vals, part);

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
    Assert.assertEquals(startTime + 10, p.getLastAccessTime());

    Assert.assertTrue(store.doesPartitionExist(dbName, tableName, vals));
    Assert.assertFalse(store.doesPartitionExist(dbName, tableName, Arrays.asList("bob")));
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
  public void listGetDropPartitionNames() throws Exception {
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

    Role role = store.getRole(roleName);
    Assert.assertNotNull(role);

    store.removeRole(roleName);
    thrown.expect(NoSuchObjectException.class);
    store.getRole(roleName);
  }

  // Due to the way our mock stuff works, we can only insert one column at a time, so we'll test
  // each stat type separately.  We'll test them together in hte integration tests.
  @Test
  public void booleanTableStatistics() throws Exception {
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    long now = System.currentTimeMillis();
    String dbname = "default";
    String tableName = "statstable";
    String boolcol = "boolcol";
    long trues = 37;
    long falses = 12;
    long booleanNulls = 2;

    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(now);
    desc.setDbName(dbname);
    desc.setTableName(tableName);
    desc.setIsTblLevel(true);
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

    store.updateTableColumnStatistics(stats);

    stats = store.getTableColumnStatistics(dbname, tableName, Arrays.asList(boolcol));
    Assert.assertEquals(now, stats.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbname, stats.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, stats.getStatsDesc().getTableName());
    Assert.assertTrue(stats.getStatsDesc().isIsTblLevel());

    Assert.assertEquals(1, stats.getStatsObjSize());
    ColumnStatisticsData colData = obj.getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS, colData.getSetField());
    boolData = colData.getBooleanStats();
    Assert.assertEquals(trues, boolData.getNumTrues());
    Assert.assertEquals(falses, boolData.getNumFalses());
    Assert.assertEquals(booleanNulls, boolData.getNumNulls());
  }

  @Test
  public void longTableStatistics() throws Exception {
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    long now = System.currentTimeMillis();
    String dbname = "default";
    String tableName = "statstable";
    String longcol = "longcol";
    long longHigh = 120938479124L;
    long longLow = -12341243213412124L;
    long longNulls = 23;
    long longDVs = 213L;

    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(now);
    desc.setDbName(dbname);
    desc.setTableName(tableName);
    desc.setIsTblLevel(true);
    stats.setStatsDesc(desc);

    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName(longcol);
    obj.setColType("long");
    ColumnStatisticsData data = new ColumnStatisticsData();
    LongColumnStatsData longData = new LongColumnStatsData();
    longData.setHighValue(longHigh);
    longData.setLowValue(longLow);
    longData.setNumNulls(longNulls);
    longData.setNumDVs(longDVs);
    data.setLongStats(longData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    store.updateTableColumnStatistics(stats);

    stats = store.getTableColumnStatistics(dbname, tableName, Arrays.asList(longcol));
    Assert.assertEquals(now, stats.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbname, stats.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, stats.getStatsDesc().getTableName());
    Assert.assertTrue(stats.getStatsDesc().isIsTblLevel());

    Assert.assertEquals(1, stats.getStatsObjSize());
    ColumnStatisticsData colData = obj.getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS, colData.getSetField());
    longData = colData.getLongStats();
    Assert.assertEquals(longHigh, longData.getHighValue());
    Assert.assertEquals(longLow, longData.getLowValue());
    Assert.assertEquals(longNulls, longData.getNumNulls());
    Assert.assertEquals(longDVs, longData.getNumDVs());
  }

  @Test
  public void doubleTableStatistics() throws Exception {
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    long now = System.currentTimeMillis();
    String dbname = "default";
    String tableName = "statstable";
    String doublecol = "doublecol";
    double doubleHigh = 123423.23423;
    double doubleLow = 0.00001234233;
    long doubleNulls = 92;
    long doubleDVs = 1234123421L;

    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(now);
    desc.setDbName(dbname);
    desc.setTableName(tableName);
    desc.setIsTblLevel(true);
    stats.setStatsDesc(desc);

    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName(doublecol);
    obj.setColType("double");
    ColumnStatisticsData data = new ColumnStatisticsData();
    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    doubleData.setHighValue(doubleHigh);
    doubleData.setLowValue(doubleLow);
    doubleData.setNumNulls(doubleNulls);
    doubleData.setNumDVs(doubleDVs);
    data.setDoubleStats(doubleData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    store.updateTableColumnStatistics(stats);

    stats = store.getTableColumnStatistics(dbname, tableName, Arrays.asList(doublecol));
    Assert.assertEquals(now, stats.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbname, stats.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, stats.getStatsDesc().getTableName());
    Assert.assertTrue(stats.getStatsDesc().isIsTblLevel());

    Assert.assertEquals(1, stats.getStatsObjSize());
    ColumnStatisticsData colData = obj.getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.DOUBLE_STATS, colData.getSetField());
    doubleData = colData.getDoubleStats();
    Assert.assertEquals(doubleHigh, doubleData.getHighValue(), 0.01);
    Assert.assertEquals(doubleLow, doubleData.getLowValue(), 0.01);
    Assert.assertEquals(doubleNulls, doubleData.getNumNulls());
    Assert.assertEquals(doubleDVs, doubleData.getNumDVs());
  }

  @Test
  public void stringTableStatistics() throws Exception {
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    long now = System.currentTimeMillis();
    String dbname = "default";
    String tableName = "statstable";
    String stringcol = "stringcol";
    long strMaxLen = 1234;
    double strAvgLen = 32.3;
    long strNulls = 987;
    long strDVs = 906;

    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(now);
    desc.setDbName(dbname);
    desc.setTableName(tableName);
    desc.setIsTblLevel(true);
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

    store.updateTableColumnStatistics(stats);

    stats = store.getTableColumnStatistics(dbname, tableName, Arrays.asList(stringcol));
    Assert.assertEquals(now, stats.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbname, stats.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, stats.getStatsDesc().getTableName());
    Assert.assertTrue(stats.getStatsDesc().isIsTblLevel());

    Assert.assertEquals(1, stats.getStatsObjSize());
    ColumnStatisticsData colData = obj.getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.STRING_STATS, colData.getSetField());
    strData = colData.getStringStats();
    Assert.assertEquals(strMaxLen, strData.getMaxColLen());
    Assert.assertEquals(strAvgLen, strData.getAvgColLen(), 0.01);
    Assert.assertEquals(strNulls, strData.getNumNulls());
    Assert.assertEquals(strDVs, strData.getNumDVs());
  }

  @Test
  public void binaryTableStatistics() throws Exception {
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    long now = System.currentTimeMillis();
    String dbname = "default";
    String tableName = "statstable";
    String binarycol = "bincol";
    long binMaxLen = 123412987L;
    double binAvgLen = 76.98;
    long binNulls = 976998797L;

    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(now);
    desc.setDbName(dbname);
    desc.setTableName(tableName);
    desc.setIsTblLevel(true);
    stats.setStatsDesc(desc);

    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName(binarycol);
    obj.setColType("binary");
    ColumnStatisticsData data = new ColumnStatisticsData();
    BinaryColumnStatsData binData = new BinaryColumnStatsData();
    binData.setMaxColLen(binMaxLen);
    binData.setAvgColLen(binAvgLen);
    binData.setNumNulls(binNulls);
    data.setBinaryStats(binData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    store.updateTableColumnStatistics(stats);

    stats = store.getTableColumnStatistics(dbname, tableName, Arrays.asList(binarycol));
    Assert.assertEquals(now, stats.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbname, stats.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, stats.getStatsDesc().getTableName());
    Assert.assertTrue(stats.getStatsDesc().isIsTblLevel());

    Assert.assertEquals(1, stats.getStatsObjSize());
    ColumnStatisticsData colData = obj.getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.BINARY_STATS, colData.getSetField());
    binData = colData.getBinaryStats();
    Assert.assertEquals(binMaxLen, binData.getMaxColLen());
    Assert.assertEquals(binAvgLen, binData.getAvgColLen(), 0.01);
    Assert.assertEquals(binNulls, binData.getNumNulls());
  }

  @Test
  public void decimalTableStatistics() throws Exception {
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    long now = System.currentTimeMillis();
    String dbname = "default";
    String tableName = "statstable";
    String decimalcol = "deccol";
    Decimal decHigh = new Decimal();
    decHigh.setScale((short)3);
    decHigh.setUnscaled("3876".getBytes()); // I have not clue how this is translated, but it
    // doesn't matter
    Decimal decLow = new Decimal();
    decLow.setScale((short)3);
    decLow.setUnscaled("38".getBytes());
    long decNulls = 13;
    long decDVs = 923947293L;

    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(now);
    desc.setDbName(dbname);
    desc.setTableName(tableName);
    desc.setIsTblLevel(true);
    stats.setStatsDesc(desc);

    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName(decimalcol);
    obj.setColType("decimal(5,3)");
    ColumnStatisticsData data = new ColumnStatisticsData();
    DecimalColumnStatsData decData = new DecimalColumnStatsData();
    decData.setHighValue(decHigh);
    decData.setLowValue(decLow);
    decData.setNumNulls(decNulls);
    decData.setNumDVs(decDVs);
    data.setDecimalStats(decData);
    obj.setStatsData(data);
    stats.addToStatsObj(obj);

    store.updateTableColumnStatistics(stats);

    stats = store.getTableColumnStatistics(dbname, tableName, Arrays.asList(decimalcol));
    Assert.assertEquals(now, stats.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(dbname, stats.getStatsDesc().getDbName());
    Assert.assertEquals(tableName, stats.getStatsDesc().getTableName());
    Assert.assertTrue(stats.getStatsDesc().isIsTblLevel());

    Assert.assertEquals(1, stats.getStatsObjSize());
    ColumnStatisticsData colData = obj.getStatsData();
    Assert.assertEquals(ColumnStatisticsData._Fields.DECIMAL_STATS, colData.getSetField());
    decData = colData.getDecimalStats();
    Assert.assertEquals(decHigh, decData.getHighValue());
    Assert.assertEquals(decLow, decData.getLowValue());
    Assert.assertEquals(decNulls, decData.getNumNulls());
    Assert.assertEquals(decDVs, decData.getNumDVs());
  }
}
