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

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
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
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestHBaseStore {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseStore.class.getName());
  static Map<String, String> emptyParameters = new HashMap<String, String>();
  // Table with NUM_PART_KEYS partitioning keys and NUM_PARTITIONS values per key
  static final int NUM_PART_KEYS = 1;
  static final int NUM_PARTITIONS = 5;
  static final String DB = "db";
  static final String TBL = "tbl";
  static final String COL = "col";
  static final String PART_KEY_PREFIX = "part";
  static final String PART_VAL_PREFIX = "val";
  static final String PART_KV_SEPARATOR = "=";
  static final List<String> PART_KEYS = new ArrayList<String>();
  static final List<String> PART_VALS = new ArrayList<String>();
  // Initialize mock partitions
  static {
    for (int i = 1; i <= NUM_PART_KEYS; i++) {
      PART_KEYS.add(PART_KEY_PREFIX + i);
    }
    for (int i = 1; i <= NUM_PARTITIONS; i++) {
      PART_VALS.add(PART_VAL_PREFIX + i);
    }
  }
  static final long DEFAULT_TIME = System.currentTimeMillis();
  static final String PART_KEY = "part";
  static final String BOOLEAN_COL = "boolCol";
  static final String BOOLEAN_TYPE = "boolean";
  static final String BOOLEAN_VAL = "true";
  static final String LONG_COL = "longCol";
  static final String LONG_TYPE = "long";
  static final String INT_TYPE = "int";
  static final String INT_VAL = "1234";
  static final String DOUBLE_COL = "doubleCol";
  static final String DOUBLE_TYPE = "double";
  static final String DOUBLE_VAL = "3.1415";
  static final String STRING_COL = "stringCol";
  static final String STRING_TYPE = "string";
  static final String STRING_VAL = "stringval";
  static final String BINARY_COL = "binaryCol";
  static final String BINARY_TYPE = "binary";
  static final String BINARY_VAL = "1";
  static final String DECIMAL_COL = "decimalCol";
  static final String DECIMAL_TYPE = "decimal(5,3)";
  static final String DECIMAL_VAL = "12.123";
  static List<ColumnStatisticsObj> booleanColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);
  static List<ColumnStatisticsObj> longColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);
  static List<ColumnStatisticsObj> doubleColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);
  static List<ColumnStatisticsObj> stringColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);
  static List<ColumnStatisticsObj> binaryColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);
  static List<ColumnStatisticsObj> decimalColStatsObjs = new ArrayList<ColumnStatisticsObj>(
      NUM_PARTITIONS);

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock HTableInterface htable;
  SortedMap<String, Cell> rows = new TreeMap<>();
  HBaseStore store;


  @BeforeClass
  public static void beforeTest() {
    // All data intitializations
    populateMockStats();
  }

  private static void populateMockStats() {
    ColumnStatisticsObj statsObj;
    // Add NUM_PARTITIONS ColumnStatisticsObj of each type
    // For aggregate stats test, we'll treat each ColumnStatisticsObj as stats for 1 partition
    // For the rest, we'll just pick the 1st ColumnStatisticsObj from this list and use it
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      statsObj = mockBooleanStats(i);
      booleanColStatsObjs.add(statsObj);
      statsObj = mockLongStats(i);
      longColStatsObjs.add(statsObj);
      statsObj = mockDoubleStats(i);
      doubleColStatsObjs.add(statsObj);
      statsObj = mockStringStats(i);
      stringColStatsObjs.add(statsObj);
      statsObj = mockBinaryStats(i);
      binaryColStatsObjs.add(statsObj);
      statsObj = mockDecimalStats(i);
      decimalColStatsObjs.add(statsObj);
    }
  }

  private static ColumnStatisticsObj mockBooleanStats(int i) {
    long trues = 37 + 100*i;
    long falses = 12 + 50*i;
    long nulls = 2 + i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(BOOLEAN_COL);
    colStatsObj.setColType(BOOLEAN_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    BooleanColumnStatsData boolData = new BooleanColumnStatsData();
    boolData.setNumTrues(trues);
    boolData.setNumFalses(falses);
    boolData.setNumNulls(nulls);
    data.setBooleanStats(boolData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockLongStats(int i) {
    long high = 120938479124L + 100*i;
    long low = -12341243213412124L - 50*i;
    long nulls = 23 + i;
    long dVs = 213L + 10*i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(LONG_COL);
    colStatsObj.setColType(LONG_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    LongColumnStatsData longData = new LongColumnStatsData();
    longData.setHighValue(high);
    longData.setLowValue(low);
    longData.setNumNulls(nulls);
    longData.setNumDVs(dVs);
    data.setLongStats(longData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockDoubleStats(int i) {
    double high = 123423.23423 + 100*i;
    double low = 0.00001234233 - 50*i;
    long nulls = 92 + i;
    long dVs = 1234123421L + 10*i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(DOUBLE_COL);
    colStatsObj.setColType(DOUBLE_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    doubleData.setHighValue(high);
    doubleData.setLowValue(low);
    doubleData.setNumNulls(nulls);
    doubleData.setNumDVs(dVs);
    data.setDoubleStats(doubleData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockStringStats(int i) {
    long maxLen = 1234 + 10*i;
    double avgLen = 32.3 + i;
    long nulls = 987 + 10*i;
    long dVs = 906 + i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(STRING_COL);
    colStatsObj.setColType(STRING_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    StringColumnStatsData stringData = new StringColumnStatsData();
    stringData.setMaxColLen(maxLen);
    stringData.setAvgColLen(avgLen);
    stringData.setNumNulls(nulls);
    stringData.setNumDVs(dVs);
    data.setStringStats(stringData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockBinaryStats(int i) {;
    long maxLen = 123412987L + 10*i;
    double avgLen = 76.98 + i;
    long nulls = 976998797L + 10*i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(BINARY_COL);
    colStatsObj.setColType(BINARY_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    BinaryColumnStatsData binaryData = new BinaryColumnStatsData();
    binaryData.setMaxColLen(maxLen);
    binaryData.setAvgColLen(avgLen);
    binaryData.setNumNulls(nulls);
    data.setBinaryStats(binaryData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockDecimalStats(int i) {
    Decimal high = new Decimal();
    high.setScale((short)3);
    String strHigh = String.valueOf(3876 + 100*i);
    high.setUnscaled(strHigh.getBytes());
    Decimal low = new Decimal();
    low.setScale((short)3);
    String strLow = String.valueOf(38 + i);
    low.setUnscaled(strLow.getBytes());
    long nulls = 13 + i;
    long dVs = 923947293L + 100*i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(DECIMAL_COL);
    colStatsObj.setColType(DECIMAL_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    DecimalColumnStatsData decimalData = new DecimalColumnStatsData();
    decimalData.setHighValue(high);
    decimalData.setLowValue(low);
    decimalData.setNumNulls(nulls);
    decimalData.setNumDVs(dVs);
    data.setDecimalStats(decimalData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  @AfterClass
  public static void afterTest() {
  }


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
    String funcName = "createfunc";
    int now = (int)(System.currentTimeMillis()/ 1000);
    Function func = new Function(funcName, DB, "o.a.h.h.myfunc", "me", PrincipalType.USER,
        now, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR,
        "file:/tmp/somewhere")));
    store.createFunction(func);

    Function f = store.getFunction(DB, funcName);
    Assert.assertEquals(DB, f.getDbName());
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
    String funcName = "alterfunc";
    int now = (int)(System.currentTimeMillis()/ 1000);
    List<ResourceUri> uris = new ArrayList<ResourceUri>();
    uris.add(new ResourceUri(ResourceType.FILE, "whatever"));
    Function func = new Function(funcName, DB, "o.a.h.h.myfunc", "me", PrincipalType.USER,
        now, FunctionType.JAVA, uris);
    store.createFunction(func);

    Function f = store.getFunction(DB, funcName);
    Assert.assertEquals(ResourceType.FILE, f.getResourceUris().get(0).getResourceType());

    func.addToResourceUris(new ResourceUri(ResourceType.ARCHIVE, "file"));
    store.alterFunction(DB, funcName, func);

    f = store.getFunction(DB, funcName);
    Assert.assertEquals(2, f.getResourceUrisSize());
    Assert.assertEquals(ResourceType.FILE, f.getResourceUris().get(0).getResourceType());
    Assert.assertEquals(ResourceType.ARCHIVE, f.getResourceUris().get(1).getResourceType());

  }

  @Test
  public void dropFunction() throws Exception {
    String funcName = "delfunc";
    int now = (int)(System.currentTimeMillis()/ 1000);
    Function func = new Function(funcName, DB, "o.a.h.h.myfunc", "me", PrincipalType.USER,
        now, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR, "file:/tmp/somewhere")));
    store.createFunction(func);

    Function f = store.getFunction(DB, funcName);
    Assert.assertNotNull(f);

    store.dropFunction(DB, funcName);
    //thrown.expect(NoSuchObjectException.class);
    Assert.assertNull(store.getFunction(DB, funcName));
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
    String tableName = "myparttable";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, DB, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> vals = Arrays.asList("fred");
    StorageDescriptor psd = new StorageDescriptor(sd);
    psd.setLocation("file:/tmp/pc=fred");
    Partition part = new Partition(vals, DB, tableName, startTime, startTime, psd,
        emptyParameters);
    store.addPartition(part);

    Partition p = store.getPartition(DB, tableName, vals);
    Assert.assertEquals(1, p.getSd().getColsSize());
    Assert.assertEquals("col1", p.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", p.getSd().getCols().get(0).getType());
    Assert.assertEquals("nocomment", p.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", p.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", p.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp/pc=fred", p.getSd().getLocation());
    Assert.assertEquals("input", p.getSd().getInputFormat());
    Assert.assertEquals("output", p.getSd().getOutputFormat());
    Assert.assertEquals(DB, p.getDbName());
    Assert.assertEquals(tableName, p.getTableName());
    Assert.assertEquals(1, p.getValuesSize());
    Assert.assertEquals("fred", p.getValues().get(0));

    Assert.assertTrue(store.doesPartitionExist(DB, tableName, vals));
    Assert.assertFalse(store.doesPartitionExist(DB, tableName, Arrays.asList("bob")));
  }

  @Test
  public void alterPartition() throws Exception {
    String tableName = "alterparttable";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, DB, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> vals = Arrays.asList("fred");
    StorageDescriptor psd = new StorageDescriptor(sd);
    psd.setLocation("file:/tmp/pc=fred");
    Partition part = new Partition(vals, DB, tableName, startTime, startTime, psd,
        emptyParameters);
    store.addPartition(part);

    part.setLastAccessTime(startTime + 10);
    store.alterPartition(DB, tableName, vals, part);

    Partition p = store.getPartition(DB, tableName, vals);
    Assert.assertEquals(1, p.getSd().getColsSize());
    Assert.assertEquals("col1", p.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", p.getSd().getCols().get(0).getType());
    Assert.assertEquals("nocomment", p.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", p.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", p.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp/pc=fred", p.getSd().getLocation());
    Assert.assertEquals("input", p.getSd().getInputFormat());
    Assert.assertEquals("output", p.getSd().getOutputFormat());
    Assert.assertEquals(DB, p.getDbName());
    Assert.assertEquals(tableName, p.getTableName());
    Assert.assertEquals(1, p.getValuesSize());
    Assert.assertEquals("fred", p.getValues().get(0));
    Assert.assertEquals(startTime + 10, p.getLastAccessTime());

    Assert.assertTrue(store.doesPartitionExist(DB, tableName, vals));
    Assert.assertFalse(store.doesPartitionExist(DB, tableName, Arrays.asList("bob")));
  }

  @Test
  public void getPartitions() throws Exception {
    String tableName = "manyParts";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, DB, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> partVals = Arrays.asList("alan", "bob", "carl", "doug", "ethan");
    for (String val : partVals) {
      List<String> vals = new ArrayList<String>();
      vals.add(val);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/pc=" + val);
      Partition part = new Partition(vals, DB, tableName, startTime, startTime, psd,
          emptyParameters);
      store.addPartition(part);

      Partition p = store.getPartition(DB, tableName, vals);
      Assert.assertEquals("file:/tmp/pc=" + val, p.getSd().getLocation());
    }

    List<Partition> parts = store.getPartitions(DB, tableName, -1);
    Assert.assertEquals(5, parts.size());
    String[] pv = new String[5];
    for (int i = 0; i < 5; i++) pv[i] = parts.get(i).getValues().get(0);
    Arrays.sort(pv);
    Assert.assertArrayEquals(pv, partVals.toArray(new String[5]));
  }

  @Test
  public void listGetDropPartitionNames() throws Exception {
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
    Table table = new Table(tableName, DB, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    String[][] partVals = new String[][]{{"today", "north america"}, {"tomorrow", "europe"}};
    for (String[] pv : partVals) {
      List<String> vals = new ArrayList<String>();
      for (String v : pv) vals.add(v);
      StorageDescriptor psd = new StorageDescriptor(sd);
      psd.setLocation("file:/tmp/pc=" + pv[0] + "/region=" + pv[1]);
      Partition part = new Partition(vals, DB, tableName, startTime, startTime, psd,
          emptyParameters);
      store.addPartition(part);
    }

    List<String> names = store.listPartitionNames(DB, tableName, (short) -1);
    Assert.assertEquals(2, names.size());
    String[] resultNames = names.toArray(new String[names.size()]);
    Arrays.sort(resultNames);
    Assert.assertArrayEquals(resultNames, new String[]{"pc=today/region=north america",
      "pc=tomorrow/region=europe"});

    List<Partition> parts = store.getPartitionsByNames(DB, tableName, names);
    Assert.assertArrayEquals(partVals[0], parts.get(0).getValues().toArray(new String[2]));
    Assert.assertArrayEquals(partVals[1], parts.get(1).getValues().toArray(new String[2]));

    store.dropPartitions(DB, tableName, names);
    List<Partition> afterDropParts = store.getPartitions(DB, tableName, -1);
    Assert.assertEquals(0, afterDropParts.size());
  }


  @Test
  public void dropPartition() throws Exception {
    String tableName = "myparttable2";
    int startTime = (int)(System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pc", "string", ""));
    Table table = new Table(tableName, DB, "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    store.createTable(table);

    List<String> vals = Arrays.asList("fred");
    StorageDescriptor psd = new StorageDescriptor(sd);
    psd.setLocation("file:/tmp/pc=fred");
    Partition part = new Partition(vals, DB, tableName, startTime, startTime, psd,
        emptyParameters);
    store.addPartition(part);

    Assert.assertNotNull(store.getPartition(DB, tableName, vals));
    store.dropPartition(DB, tableName, vals);
    thrown.expect(NoSuchObjectException.class);
    store.getPartition(DB, tableName, vals);
  }

  @Test
  public void createIndex() throws Exception {
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

    String indexName = "myindex";
    String indexTableName = tableName + "__" + indexName + "__";
    Index index = new Index(indexName, null, "default", tableName, startTime, startTime,
        indexTableName, sd, emptyParameters, false);
    store.addIndex(index);

    Index ind = store.getIndex("default", tableName, indexName);
    Assert.assertEquals(1, ind.getSd().getColsSize());
    Assert.assertEquals("col1", ind.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", ind.getSd().getCols().get(0).getType());
    Assert.assertEquals("", ind.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", ind.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", ind.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp", ind.getSd().getLocation());
    Assert.assertEquals("input", ind.getSd().getInputFormat());
    Assert.assertEquals("output", ind.getSd().getOutputFormat());
    Assert.assertFalse(ind.getSd().isCompressed());
    Assert.assertEquals(17, ind.getSd().getNumBuckets());
    Assert.assertEquals(1, ind.getSd().getBucketColsSize());
    Assert.assertEquals("bucketcol", ind.getSd().getBucketCols().get(0));
    Assert.assertEquals(1, ind.getSd().getSortColsSize());
    Assert.assertEquals("sortcol", ind.getSd().getSortCols().get(0).getCol());
    Assert.assertEquals(1, ind.getSd().getSortCols().get(0).getOrder());
    Assert.assertEquals(1, ind.getSd().getParametersSize());
    Assert.assertEquals("value", ind.getSd().getParameters().get("key"));
    Assert.assertEquals(indexName, ind.getIndexName());
    Assert.assertNull(ind.getIndexHandlerClass());
    Assert.assertEquals("default", ind.getDbName());
    Assert.assertEquals(tableName, ind.getOrigTableName());
    Assert.assertEquals(0, ind.getParametersSize());
    Assert.assertEquals(startTime, ind.getCreateTime());
    Assert.assertEquals(startTime, ind.getLastAccessTime());
    Assert.assertEquals(false, ind.isDeferredRebuild());
  }

  @Test
  public void alterIndex() throws Exception {
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

    String indexName = "myindex";
    Index index = new Index(indexName, null, "default", tableName, startTime, startTime,
        tableName + "__" + indexName + "__", sd, emptyParameters, false);
    store.addIndex(index);

    startTime += 10;
    index.setLastAccessTime(startTime);
    store.alterIndex("default", tableName, indexName, index); 

    Index ind = store.getIndex("default", tableName, indexName);
    Assert.assertEquals(1, ind.getSd().getColsSize());
    Assert.assertEquals("col1", ind.getSd().getCols().get(0).getName());
    Assert.assertEquals("int", ind.getSd().getCols().get(0).getType());
    Assert.assertEquals("", ind.getSd().getCols().get(0).getComment());
    Assert.assertEquals("serde", ind.getSd().getSerdeInfo().getName());
    Assert.assertEquals("seriallib", ind.getSd().getSerdeInfo().getSerializationLib());
    Assert.assertEquals("file:/tmp", ind.getSd().getLocation());
    Assert.assertEquals("input", ind.getSd().getInputFormat());
    Assert.assertEquals("output", ind.getSd().getOutputFormat());
    Assert.assertFalse(ind.getSd().isCompressed());
    Assert.assertEquals(17, ind.getSd().getNumBuckets());
    Assert.assertEquals(1, ind.getSd().getBucketColsSize());
    Assert.assertEquals("bucketcol", ind.getSd().getBucketCols().get(0));
    Assert.assertEquals(1, ind.getSd().getSortColsSize());
    Assert.assertEquals("sortcol", ind.getSd().getSortCols().get(0).getCol());
    Assert.assertEquals(1, ind.getSd().getSortCols().get(0).getOrder());
    Assert.assertEquals(1, ind.getSd().getParametersSize());
    Assert.assertEquals("value", ind.getSd().getParameters().get("key"));
    Assert.assertEquals(indexName, ind.getIndexName());
    Assert.assertNull(ind.getIndexHandlerClass());
    Assert.assertEquals("default", ind.getDbName());
    Assert.assertEquals(tableName, ind.getOrigTableName());
    Assert.assertEquals(0, ind.getParametersSize());
    Assert.assertEquals(startTime, ind.getLastAccessTime());
    Assert.assertEquals(false, ind.isDeferredRebuild());
  }

  @Test
  public void dropIndex() throws Exception {
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

    String indexName = "myindex";
    Index index = new Index(indexName, null, "default", tableName, startTime, startTime,
        tableName + "__" + indexName + "__", sd, emptyParameters, false);
    store.addIndex(index);

    store.dropIndex("default", tableName, indexName);

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
  // each stat type separately.  We'll test them together in the integration tests.
  @Test
  public void booleanTableStatistics() throws Exception {
    // Add a boolean table stats for BOOLEAN_COL to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = booleanColStatsObjs.get(0);
    BooleanColumnStatsData boolData = obj.getStatsData().getBooleanStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(BOOLEAN_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS, dataFromDB.getSetField());
    // Compare BooleanColumnStatsData
    BooleanColumnStatsData boolDataFromDB = dataFromDB.getBooleanStats();
    Assert.assertEquals(boolData.getNumTrues(), boolDataFromDB.getNumTrues());
    Assert.assertEquals(boolData.getNumFalses(), boolDataFromDB.getNumFalses());
    Assert.assertEquals(boolData.getNumNulls(), boolDataFromDB.getNumNulls());
  }

  @Test
  public void longTableStatistics() throws Exception {
    createMockTable(LONG_TYPE);
    // Add a long table stats for LONG_COL to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = longColStatsObjs.get(0);
    LongColumnStatsData longData = obj.getStatsData().getLongStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(LONG_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS, dataFromDB.getSetField());
    // Compare LongColumnStatsData
    LongColumnStatsData longDataFromDB = dataFromDB.getLongStats();
    Assert.assertEquals(longData.getHighValue(), longDataFromDB.getHighValue());
    Assert.assertEquals(longData.getLowValue(), longDataFromDB.getLowValue());
    Assert.assertEquals(longData.getNumNulls(), longDataFromDB.getNumNulls());
    Assert.assertEquals(longData.getNumDVs(), longDataFromDB.getNumDVs());
  }

  @Test
  public void doubleTableStatistics() throws Exception {
    createMockTable(DOUBLE_TYPE);
    // Add a double table stats for DOUBLE_COL to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = doubleColStatsObjs.get(0);
    DoubleColumnStatsData doubleData = obj.getStatsData().getDoubleStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(DOUBLE_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.DOUBLE_STATS, dataFromDB.getSetField());
    // Compare DoubleColumnStatsData
    DoubleColumnStatsData doubleDataFromDB = dataFromDB.getDoubleStats();
    Assert.assertEquals(doubleData.getHighValue(), doubleDataFromDB.getHighValue(), 0.01);
    Assert.assertEquals(doubleData.getLowValue(), doubleDataFromDB.getLowValue(), 0.01);
    Assert.assertEquals(doubleData.getNumNulls(), doubleDataFromDB.getNumNulls());
    Assert.assertEquals(doubleData.getNumDVs(), doubleDataFromDB.getNumDVs());
  }

  @Test
  public void stringTableStatistics() throws Exception {
    createMockTable(STRING_TYPE);
    // Add a string table stats for STRING_COL to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = stringColStatsObjs.get(0);
    StringColumnStatsData stringData = obj.getStatsData().getStringStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(STRING_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.STRING_STATS, dataFromDB.getSetField());
    // Compare StringColumnStatsData
    StringColumnStatsData stringDataFromDB = dataFromDB.getStringStats();
    Assert.assertEquals(stringData.getMaxColLen(), stringDataFromDB.getMaxColLen());
    Assert.assertEquals(stringData.getAvgColLen(), stringDataFromDB.getAvgColLen(), 0.01);
    Assert.assertEquals(stringData.getNumNulls(), stringDataFromDB.getNumNulls());
    Assert.assertEquals(stringData.getNumDVs(), stringDataFromDB.getNumDVs());
  }

  @Test
  public void binaryTableStatistics() throws Exception {
    createMockTable(BINARY_TYPE);
    // Add a binary table stats for BINARY_COL to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = binaryColStatsObjs.get(0);
    BinaryColumnStatsData binaryData = obj.getStatsData().getBinaryStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(BINARY_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.BINARY_STATS, dataFromDB.getSetField());
    // Compare BinaryColumnStatsData
    BinaryColumnStatsData binaryDataFromDB = dataFromDB.getBinaryStats();
    Assert.assertEquals(binaryData.getMaxColLen(), binaryDataFromDB.getMaxColLen());
    Assert.assertEquals(binaryData.getAvgColLen(), binaryDataFromDB.getAvgColLen(), 0.01);
    Assert.assertEquals(binaryData.getNumNulls(), binaryDataFromDB.getNumNulls());
  }

  @Test
  public void decimalTableStatistics() throws Exception {
    createMockTable(DECIMAL_TYPE);
    // Add a decimal table stats for DECIMAL_COL to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for table level stats
    ColumnStatisticsDesc desc = getMockTblColStatsDesc();
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = decimalColStatsObjs.get(0);
    DecimalColumnStatsData decimalData = obj.getStatsData().getDecimalStats();
    // Add to DB
    stats.addToStatsObj(obj);
    store.updateTableColumnStatistics(stats);
    // Get from DB
    ColumnStatistics statsFromDB = store.getTableColumnStatistics(DB, TBL, Arrays.asList(DECIMAL_COL));
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.getStatsDesc().getTableName());
    Assert.assertTrue(statsFromDB.getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.DECIMAL_STATS, dataFromDB.getSetField());
    // Compare DecimalColumnStatsData
    DecimalColumnStatsData decimalDataFromDB = dataFromDB.getDecimalStats();
    Assert.assertEquals(decimalData.getHighValue(), decimalDataFromDB.getHighValue());
    Assert.assertEquals(decimalData.getLowValue(), decimalDataFromDB.getLowValue());
    Assert.assertEquals(decimalData.getNumNulls(), decimalDataFromDB.getNumNulls());
    Assert.assertEquals(decimalData.getNumDVs(), decimalDataFromDB.getNumDVs());
  }

  @Test
  public void booleanPartitionStatistics() throws Exception {
    createMockTableAndPartition(BOOLEAN_TYPE, BOOLEAN_VAL);
    // Add partition stats for: BOOLEAN_COL and partition: {PART_KEY, BOOLEAN_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, BOOLEAN_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = booleanColStatsObjs.get(0);
    BooleanColumnStatsData boolData = obj.getStatsData().getBooleanStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(BOOLEAN_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.BOOLEAN_STATS, dataFromDB.getSetField());
    // Compare BooleanColumnStatsData
    BooleanColumnStatsData boolDataFromDB = dataFromDB.getBooleanStats();
    Assert.assertEquals(boolData.getNumTrues(), boolDataFromDB.getNumTrues());
    Assert.assertEquals(boolData.getNumFalses(), boolDataFromDB.getNumFalses());
    Assert.assertEquals(boolData.getNumNulls(), boolDataFromDB.getNumNulls());
  }

  @Test
  public void longPartitionStatistics() throws Exception {
    createMockTableAndPartition(INT_TYPE, INT_VAL);
    // Add partition stats for: LONG_COL and partition: {PART_KEY, INT_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, INT_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = longColStatsObjs.get(0);
    LongColumnStatsData longData = obj.getStatsData().getLongStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(INT_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.LONG_STATS, dataFromDB.getSetField());
    // Compare LongColumnStatsData
    LongColumnStatsData longDataFromDB = dataFromDB.getLongStats();
    Assert.assertEquals(longData.getHighValue(), longDataFromDB.getHighValue());
    Assert.assertEquals(longData.getLowValue(), longDataFromDB.getLowValue());
    Assert.assertEquals(longData.getNumNulls(), longDataFromDB.getNumNulls());
    Assert.assertEquals(longData.getNumDVs(), longDataFromDB.getNumDVs());
  }

  @Test
  public void doublePartitionStatistics() throws Exception {
    createMockTableAndPartition(DOUBLE_TYPE, DOUBLE_VAL);
    // Add partition stats for: DOUBLE_COL and partition: {PART_KEY, DOUBLE_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, DOUBLE_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = doubleColStatsObjs.get(0);
    DoubleColumnStatsData doubleData = obj.getStatsData().getDoubleStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(DOUBLE_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.DOUBLE_STATS, dataFromDB.getSetField());
    // Compare DoubleColumnStatsData
    DoubleColumnStatsData doubleDataFromDB = dataFromDB.getDoubleStats();
    Assert.assertEquals(doubleData.getHighValue(), doubleDataFromDB.getHighValue(), 0.01);
    Assert.assertEquals(doubleData.getLowValue(), doubleDataFromDB.getLowValue(), 0.01);
    Assert.assertEquals(doubleData.getNumNulls(), doubleDataFromDB.getNumNulls());
    Assert.assertEquals(doubleData.getNumDVs(), doubleDataFromDB.getNumDVs());
  }

  @Test
  public void stringPartitionStatistics() throws Exception {
    createMockTableAndPartition(STRING_TYPE, STRING_VAL);
    // Add partition stats for: STRING_COL and partition: {PART_KEY, STRING_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, STRING_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = stringColStatsObjs.get(0);
    StringColumnStatsData stringData = obj.getStatsData().getStringStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(STRING_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.STRING_STATS, dataFromDB.getSetField());
    // Compare StringColumnStatsData
    StringColumnStatsData stringDataFromDB = dataFromDB.getStringStats();
    Assert.assertEquals(stringData.getMaxColLen(), stringDataFromDB.getMaxColLen());
    Assert.assertEquals(stringData.getAvgColLen(), stringDataFromDB.getAvgColLen(), 0.01);
    Assert.assertEquals(stringData.getNumNulls(), stringDataFromDB.getNumNulls());
    Assert.assertEquals(stringData.getNumDVs(), stringDataFromDB.getNumDVs());
  }

  @Test
  public void binaryPartitionStatistics() throws Exception {
    createMockTableAndPartition(BINARY_TYPE, BINARY_VAL);
    // Add partition stats for: BINARY_COL and partition: {PART_KEY, BINARY_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, BINARY_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = binaryColStatsObjs.get(0);
    BinaryColumnStatsData binaryData = obj.getStatsData().getBinaryStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(BINARY_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.BINARY_STATS, dataFromDB.getSetField());
    // Compare BinaryColumnStatsData
    BinaryColumnStatsData binaryDataFromDB = dataFromDB.getBinaryStats();
    Assert.assertEquals(binaryData.getMaxColLen(), binaryDataFromDB.getMaxColLen());
    Assert.assertEquals(binaryData.getAvgColLen(), binaryDataFromDB.getAvgColLen(), 0.01);
    Assert.assertEquals(binaryData.getNumNulls(), binaryDataFromDB.getNumNulls());
  }

  @Test
  public void decimalPartitionStatistics() throws Exception {
    createMockTableAndPartition(DECIMAL_TYPE, DECIMAL_VAL);
    // Add partition stats for: DECIMAL_COL and partition: {PART_KEY, DECIMAL_VAL} to DB
    // Because of the way our mock implementation works we actually need to not create the table
    // before we set statistics on it.
    ColumnStatistics stats = new ColumnStatistics();
    // Get a default ColumnStatisticsDesc for partition level stats
    ColumnStatisticsDesc desc = getMockPartColStatsDesc(PART_KEY, DECIMAL_VAL);
    stats.setStatsDesc(desc);
    // Get one of the pre-created ColumnStatisticsObj
    ColumnStatisticsObj obj = decimalColStatsObjs.get(0);
    DecimalColumnStatsData decimalData = obj.getStatsData().getDecimalStats();
    // Add to DB
    stats.addToStatsObj(obj);
    List<String> parVals = new ArrayList<String>();
    parVals.add(DECIMAL_VAL);
    store.updatePartitionColumnStatistics(stats, parVals);
    // Get from DB
    List<String> partNames = new ArrayList<String>();
    partNames.add(desc.getPartName());
    List<String> colNames = new ArrayList<String>();
    colNames.add(obj.getColName());
    List<ColumnStatistics> statsFromDB = store.getPartitionColumnStatistics(DB, TBL, partNames, colNames);
    // Compare ColumnStatisticsDesc
    Assert.assertEquals(1, statsFromDB.size());
    Assert.assertEquals(desc.getLastAnalyzed(), statsFromDB.get(0).getStatsDesc().getLastAnalyzed());
    Assert.assertEquals(DB, statsFromDB.get(0).getStatsDesc().getDbName());
    Assert.assertEquals(TBL, statsFromDB.get(0).getStatsDesc().getTableName());
    Assert.assertFalse(statsFromDB.get(0).getStatsDesc().isIsTblLevel());
    // Compare ColumnStatisticsObj
    Assert.assertEquals(1, statsFromDB.get(0).getStatsObjSize());
    ColumnStatisticsObj objFromDB = statsFromDB.get(0).getStatsObj().get(0);
    ColumnStatisticsData dataFromDB = objFromDB.getStatsData();
    // Compare ColumnStatisticsData
    Assert.assertEquals(ColumnStatisticsData._Fields.DECIMAL_STATS, dataFromDB.getSetField());
    // Compare DecimalColumnStatsData
    DecimalColumnStatsData decimalDataFromDB = dataFromDB.getDecimalStats();
    Assert.assertEquals(decimalData.getHighValue(), decimalDataFromDB.getHighValue());
    Assert.assertEquals(decimalData.getLowValue(), decimalDataFromDB.getLowValue());
    Assert.assertEquals(decimalData.getNumNulls(), decimalDataFromDB.getNumNulls());
    Assert.assertEquals(decimalData.getNumDVs(), decimalDataFromDB.getNumDVs());
  }

  @Test
  public void createTableWithPrimaryKey() throws Exception {
    String tableName = "pktable";
    String pkName = "test_pk";
    String pkColNames[] = { "col0" };
    Table table = createMultiColumnTable(tableName, "int");

    List<SQLPrimaryKey> pk = Arrays.asList(
        new SQLPrimaryKey(DB, tableName, pkColNames[0], 0, pkName, true, false, true));

    store.createTableWithConstraints(table, pk, null);

    pk = store.getPrimaryKeys(DB, tableName);

    Assert.assertNotNull(pk);
    Assert.assertEquals(1, pk.size());
    Assert.assertEquals(DB, pk.get(0).getTable_db());
    Assert.assertEquals(tableName, pk.get(0).getTable_name());
    Assert.assertEquals(pkColNames[0], pk.get(0).getColumn_name());
    Assert.assertEquals(0, pk.get(0).getKey_seq());
    Assert.assertEquals(pkName, pk.get(0).getPk_name());
    Assert.assertTrue(pk.get(0).isEnable_cstr());
    Assert.assertFalse(pk.get(0).isValidate_cstr());
    Assert.assertTrue(pk.get(0).isRely_cstr());

    // Drop the primary key
    store.dropConstraint(DB, tableName, pkName);

    pk = store.getPrimaryKeys(DB, tableName);
    Assert.assertNull(pk);
  }

  @Test
  public void createTableWithForeignKey() throws Exception {
    String tableName = "fktable";
    String pkTable = "pktable";
    String pkName = "test_pk";
    String fkName = "test_fk";
    String fkColNames[] = { "col0" };
    String pkColNames[] = { "pcol0" };
    Table table = createMultiColumnTable(tableName, "int");

    List<SQLForeignKey> fk = Arrays.asList(
        new SQLForeignKey(DB, pkTable, pkColNames[0], DB, tableName, fkColNames[0], 0, 1, 2,
            fkName, pkName, true, false, false));

    store.createTableWithConstraints(table, null, fk);

    fk = store.getForeignKeys(DB, pkTable, DB, tableName);

    Assert.assertNotNull(fk);
    Assert.assertEquals(1, fk.size());
    Assert.assertEquals(DB, fk.get(0).getPktable_db());
    Assert.assertEquals(pkTable, fk.get(0).getPktable_name());
    Assert.assertEquals(pkColNames[0], fk.get(0).getPkcolumn_name());
    Assert.assertEquals(DB, fk.get(0).getFktable_db());
    Assert.assertEquals(tableName, fk.get(0).getFktable_name());
    Assert.assertEquals(fkColNames[0], fk.get(0).getFkcolumn_name());
    Assert.assertEquals(0, fk.get(0).getKey_seq());
    Assert.assertEquals(1, fk.get(0).getUpdate_rule());
    Assert.assertEquals(2, fk.get(0).getDelete_rule());
    Assert.assertEquals(fkName, fk.get(0).getFk_name());
    Assert.assertEquals(pkName, fk.get(0).getPk_name());
    Assert.assertTrue(fk.get(0).isEnable_cstr());
    Assert.assertFalse(fk.get(0).isValidate_cstr());
    Assert.assertFalse(fk.get(0).isRely_cstr());
  }

  // Test that we can add a primary key with multiple columns
  @Test
  public void addMultiColPrimaryKey() throws Exception {
    String tableName = "mcpktable";
    String pkName = "test_pk";
    String pkColNames[] = { "col0", "col1", "col2" };
    Table table = createMultiColumnTable(tableName, "int", "varchar(32)", "decimal(10,2)");

    List<SQLPrimaryKey> pk = Arrays.asList(
        new SQLPrimaryKey(DB, tableName, pkColNames[1], 0, pkName, false, true, true),
        new SQLPrimaryKey(DB, tableName, pkColNames[2], 1, pkName, false, true, true)
    );

    store.createTable(table);
    store.addPrimaryKeys(pk);

    Assert.assertNotNull(pk);
    Assert.assertEquals(2, pk.size());
    SQLPrimaryKey[] sorted = pk.toArray(new SQLPrimaryKey[2]);
    Arrays.sort(sorted, new Comparator<SQLPrimaryKey>() {
      @Override
      public int compare(SQLPrimaryKey o1, SQLPrimaryKey o2) {
        return o1.getColumn_name().compareTo(o2.getColumn_name());
      }
    });
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(DB, sorted[i].getTable_db());
      Assert.assertEquals(tableName, sorted[i].getTable_name());
      Assert.assertEquals(pkColNames[i+1], sorted[i].getColumn_name());
      Assert.assertEquals(i, sorted[i].getKey_seq());
      Assert.assertEquals(pkName, sorted[i].getPk_name());
      Assert.assertFalse(sorted[i].isEnable_cstr());
      Assert.assertTrue(sorted[i].isValidate_cstr());
      Assert.assertTrue(sorted[i].isRely_cstr());
    }

  }

  // Test that we can create a foreign key with multiple columns
  @Test
  public void addMultiColForeignKey() throws Exception {
    String tableName = "mcfktable";
    String pkTable = "pktable";
    String pkName = "test_pk";
    String fkName = "test_fk";
    String fkColNames[] = { "col0", "col1", "col2" };
    String pkColNames[] = { "pcol0", "pcol1" };
    Table table = createMultiColumnTable(tableName, "int", "double", "timestamp");

    List<SQLForeignKey> fk = Arrays.asList(
        new SQLForeignKey(DB, pkTable, pkColNames[0], DB, tableName, fkColNames[1], 0, 1, 2,
            fkName, pkName, true, false, false),
        new SQLForeignKey(DB, pkTable, pkColNames[1], DB, tableName, fkColNames[2], 1, 1, 2,
            fkName, pkName, true, false, false)
        );

    store.createTable(table);
    store.addForeignKeys(fk);

    fk = store.getForeignKeys(DB, pkTable, DB, tableName);

    Assert.assertNotNull(fk);
    Assert.assertEquals(2, fk.size());
    SQLForeignKey[] sorted = fk.toArray(new SQLForeignKey[2]);
    Arrays.sort(sorted, new Comparator<SQLForeignKey>() {
      @Override
      public int compare(SQLForeignKey o1, SQLForeignKey o2) {
        if (o1.getFk_name().equals(o2.getFk_name())) {
          return o1.getFkcolumn_name().compareTo(o2.getFkcolumn_name());
        } else {
          return o1.getFk_name().compareTo(o2.getFk_name());
        }
      }
    });

    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(DB, sorted[i].getPktable_db());
      Assert.assertEquals(pkTable, sorted[i].getPktable_name());
      Assert.assertEquals(pkColNames[i], sorted[i].getPkcolumn_name());
      Assert.assertEquals(DB, sorted[i].getFktable_db());
      Assert.assertEquals(tableName, sorted[i].getFktable_name());
      Assert.assertEquals(fkColNames[i+1], sorted[i].getFkcolumn_name());
      Assert.assertEquals(i, sorted[i].getKey_seq());
      Assert.assertEquals(1, sorted[i].getUpdate_rule());
      Assert.assertEquals(2, sorted[i].getDelete_rule());
      Assert.assertEquals(fkName, sorted[i].getFk_name());
      Assert.assertEquals(pkName, sorted[i].getPk_name());
      Assert.assertTrue(sorted[i].isEnable_cstr());
      Assert.assertFalse(sorted[i].isValidate_cstr());
      Assert.assertFalse(sorted[i].isRely_cstr());
    }

  }

  // Test that we can add 2 foreign keys at once
  @Test
  public void addMultiForeignKeys() throws Exception {
    String tableName = "mcfktable";
    String pkTable = "pktable";
    String pkTable2 = "pktable2";
    String pkName = "test_pk";
    String pkName2 = "test_pk2";
    String fkName = "test_fk";
    String fkName2 = "test_fk2";
    String fkColNames[] = { "col0", "col1", "col2" };
    String pkColNames[] = { "pcol0", "pcol1" };
    String pkColNames2[] = { "p2col0" };
    Table table = createMultiColumnTable(tableName, "int", "double", "timestamp");

    List<SQLForeignKey> fk = Arrays.asList(
        new SQLForeignKey(DB, pkTable, pkColNames[0], DB, tableName, fkColNames[1], 0, 1, 2,
            fkName, pkName, true, false, true),
        new SQLForeignKey(DB, pkTable, pkColNames[1], DB, tableName, fkColNames[2], 1, 1, 2,
            fkName, pkName, true, false, true),
        new SQLForeignKey(DB, pkTable2, pkColNames2[0], DB, tableName, fkColNames[0], 0, 1, 2,
            fkName2, pkName2, true, false, true)
    );

    store.createTable(table);
    store.addForeignKeys(fk);

    fk = store.getForeignKeys(DB, pkTable, DB, tableName);

    Assert.assertNotNull(fk);
    Assert.assertEquals(2, fk.size());
    SQLForeignKey[] sorted = fk.toArray(new SQLForeignKey[2]);
    Arrays.sort(sorted, new Comparator<SQLForeignKey>() {
      @Override
      public int compare(SQLForeignKey o1, SQLForeignKey o2) {
        if (o1.getFk_name().equals(o2.getFk_name())) {
          return o1.getFkcolumn_name().compareTo(o2.getFkcolumn_name());
        } else {
          return o1.getFk_name().compareTo(o2.getFk_name());
        }
      }
    });

    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(DB, sorted[i].getPktable_db());
      Assert.assertEquals(pkTable, sorted[i].getPktable_name());
      Assert.assertEquals(pkColNames[i], sorted[i].getPkcolumn_name());
      Assert.assertEquals(DB, sorted[i].getFktable_db());
      Assert.assertEquals(tableName, sorted[i].getFktable_name());
      Assert.assertEquals(fkColNames[i+1], sorted[i].getFkcolumn_name());
      Assert.assertEquals(i, sorted[i].getKey_seq());
      Assert.assertEquals(1, sorted[i].getUpdate_rule());
      Assert.assertEquals(2, sorted[i].getDelete_rule());
      Assert.assertEquals(fkName, sorted[i].getFk_name());
      Assert.assertEquals(pkName, sorted[i].getPk_name());
      Assert.assertTrue(sorted[i].isEnable_cstr());
      Assert.assertFalse(sorted[i].isValidate_cstr());
      Assert.assertTrue(sorted[i].isRely_cstr());
    }
    fk = store.getForeignKeys(DB, pkTable2, DB, tableName);
    Assert.assertNotNull(fk);
    Assert.assertEquals(1, fk.size());
    Assert.assertEquals(DB, fk.get(0).getPktable_db());
    Assert.assertEquals(pkTable2, fk.get(0).getPktable_name());
    Assert.assertEquals(pkColNames2[0], fk.get(0).getPkcolumn_name());
    Assert.assertEquals(DB, fk.get(0).getFktable_db());
    Assert.assertEquals(tableName, fk.get(0).getFktable_name());
    Assert.assertEquals(fkColNames[0], fk.get(0).getFkcolumn_name());
    Assert.assertEquals(0, fk.get(0).getKey_seq());
    Assert.assertEquals(1, fk.get(0).getUpdate_rule());
    Assert.assertEquals(2, fk.get(0).getDelete_rule());
    Assert.assertEquals(fkName2, fk.get(0).getFk_name());
    Assert.assertEquals(pkName2, fk.get(0).getPk_name());
    Assert.assertTrue(fk.get(0).isEnable_cstr());
    Assert.assertFalse(fk.get(0).isValidate_cstr());
    Assert.assertTrue(fk.get(0).isRely_cstr());

  }

  // Test that we can add a foreign key when one already exists
  @Test
  public void addSecondForeignKeys() throws Exception {
    String tableName = "mcfktable";
    String pkTable = "pktable";
    String pkTable2 = "pktable2";
    String pkName = "test_pk";
    String pkName2 = "test_pk2";
    String fkName = "test_fk";
    String fkName2 = "test_fk2";
    String fkColNames[] = { "col0", "col1", "col2" };
    String pkColNames[] = { "pcol0", "pcol1" };
    String pkColNames2[] = { "p2col0" };
    Table table = createMultiColumnTable(tableName, "int", "double", "timestamp");

    List<SQLForeignKey> fk = Arrays.asList(
        new SQLForeignKey(DB, pkTable, pkColNames[0], DB, tableName, fkColNames[1], 0, 1, 2,
            fkName, pkName, true, false, true),
        new SQLForeignKey(DB, pkTable, pkColNames[1], DB, tableName, fkColNames[2], 1, 1, 2,
            fkName, pkName, true, false, true)
    );

    store.createTable(table);
    store.addForeignKeys(fk);

    fk = Arrays.asList(
        new SQLForeignKey(DB, pkTable2, pkColNames2[0], DB, tableName, fkColNames[0], 0, 1, 2,
            fkName2, pkName2, true, false, true)
    );
    store.addForeignKeys(fk);

    fk = store.getForeignKeys(DB, pkTable, DB, tableName);

    Assert.assertNotNull(fk);
    Assert.assertEquals(2, fk.size());
    SQLForeignKey[] sorted = fk.toArray(new SQLForeignKey[2]);
    Arrays.sort(sorted, new Comparator<SQLForeignKey>() {
      @Override
      public int compare(SQLForeignKey o1, SQLForeignKey o2) {
        if (o1.getFk_name().equals(o2.getFk_name())) {
          return o1.getFkcolumn_name().compareTo(o2.getFkcolumn_name());
        } else {
          return o1.getFk_name().compareTo(o2.getFk_name());
        }
      }
    });

    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(DB, sorted[i].getPktable_db());
      Assert.assertEquals(pkTable, sorted[i].getPktable_name());
      Assert.assertEquals(pkColNames[i], sorted[i].getPkcolumn_name());
      Assert.assertEquals(DB, sorted[i].getFktable_db());
      Assert.assertEquals(tableName, sorted[i].getFktable_name());
      Assert.assertEquals(fkColNames[i+1], sorted[i].getFkcolumn_name());
      Assert.assertEquals(i, sorted[i].getKey_seq());
      Assert.assertEquals(1, sorted[i].getUpdate_rule());
      Assert.assertEquals(2, sorted[i].getDelete_rule());
      Assert.assertEquals(fkName, sorted[i].getFk_name());
      Assert.assertEquals(pkName, sorted[i].getPk_name());
      Assert.assertTrue(sorted[i].isEnable_cstr());
      Assert.assertFalse(sorted[i].isValidate_cstr());
      Assert.assertTrue(sorted[i].isRely_cstr());
    }

    fk = store.getForeignKeys(DB, pkTable2, DB, tableName);
    Assert.assertNotNull(fk);
    Assert.assertEquals(1, fk.size());
    Assert.assertEquals(DB, fk.get(0).getPktable_db());
    Assert.assertEquals(pkTable2, fk.get(0).getPktable_name());
    Assert.assertEquals(pkColNames2[0], fk.get(0).getPkcolumn_name());
    Assert.assertEquals(DB, fk.get(0).getFktable_db());
    Assert.assertEquals(tableName, fk.get(0).getFktable_name());
    Assert.assertEquals(fkColNames[0], fk.get(0).getFkcolumn_name());
    Assert.assertEquals(0, fk.get(0).getKey_seq());
    Assert.assertEquals(1, fk.get(0).getUpdate_rule());
    Assert.assertEquals(2, fk.get(0).getDelete_rule());
    Assert.assertEquals(fkName2, fk.get(0).getFk_name());
    Assert.assertEquals(pkName2, fk.get(0).getPk_name());
    Assert.assertTrue(fk.get(0).isEnable_cstr());
    Assert.assertFalse(fk.get(0).isValidate_cstr());
    Assert.assertTrue(fk.get(0).isRely_cstr());

    // Check that passing null gets all the foreign keys
    fk = store.getForeignKeys(null, null, DB, tableName);
    Assert.assertNotNull(fk);
    Assert.assertEquals(3, fk.size());

    store.dropConstraint(DB, tableName, fkName);

    fk = store.getForeignKeys(DB, pkTable2, DB, tableName);
    Assert.assertNotNull(fk);
    Assert.assertEquals(1, fk.size());
    Assert.assertEquals(DB, fk.get(0).getPktable_db());
    Assert.assertEquals(pkTable2, fk.get(0).getPktable_name());
    Assert.assertEquals(pkColNames2[0], fk.get(0).getPkcolumn_name());
    Assert.assertEquals(DB, fk.get(0).getFktable_db());
    Assert.assertEquals(tableName, fk.get(0).getFktable_name());
    Assert.assertEquals(fkColNames[0], fk.get(0).getFkcolumn_name());
    Assert.assertEquals(0, fk.get(0).getKey_seq());
    Assert.assertEquals(1, fk.get(0).getUpdate_rule());
    Assert.assertEquals(2, fk.get(0).getDelete_rule());
    Assert.assertEquals(fkName2, fk.get(0).getFk_name());
    Assert.assertEquals(pkName2, fk.get(0).getPk_name());
    Assert.assertTrue(fk.get(0).isEnable_cstr());
    Assert.assertFalse(fk.get(0).isValidate_cstr());
    Assert.assertTrue(fk.get(0).isRely_cstr());

    store.dropConstraint(DB, tableName, fkName2);

    fk = store.getForeignKeys(DB, pkTable2, DB, tableName);
    Assert.assertNull(fk);
  }

  // Try adding a primary key when one already exists
  @Test(expected= MetaException.class)
  public void doublePrimaryKey() throws Exception {
    String tableName = "pktable";
    String pkName = "test_pk";
    String pkColNames[] = { "col0" };
    Table table = createMultiColumnTable(tableName, "int");

    List<SQLPrimaryKey> pk = Arrays.asList(
        new SQLPrimaryKey(DB, tableName, pkColNames[0], 0, pkName, true, false, true));

    store.createTableWithConstraints(table, pk, null);

    store.addPrimaryKeys(pk);
  }



  private Table createMockTableAndPartition(String partType, String partVal) throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", partType, ""));
    List<String> vals = new ArrayList<String>();
    vals.add(partVal);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17,
        serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    int currentTime = (int)(System.currentTimeMillis() / 1000);
    Table table = new Table(TBL, DB, "me", currentTime, currentTime, 0, sd, cols,
        emptyParameters, null, null, null);
    store.createTable(table);
    Partition part = new Partition(vals, DB, TBL, currentTime, currentTime, sd,
        emptyParameters);
    store.addPartition(part);
    return table;
  }

  private Table createMockTable(String type) throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", type, ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17,
        serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    int currentTime = (int)(System.currentTimeMillis() / 1000);
    Table table = new Table(TBL, DB, "me", currentTime, currentTime, 0, sd, cols,
        emptyParameters, null, null, null);
    store.createTable(table);
    return table;
  }

  private Table createMultiColumnTable(String tblName, String... types) throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    for (int i = 0; i < types.length; i++) cols.add(new FieldSchema("col" + i, types[i], ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17,
        serde, Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    int currentTime = (int)(System.currentTimeMillis() / 1000);
    Table table = new Table(tblName, DB, "me", currentTime, currentTime, 0, sd, cols,
        emptyParameters, null, null, null);
    store.createTable(table);
    return table;
  }

  /**
   * Returns a dummy table level ColumnStatisticsDesc with default values
   */
  private ColumnStatisticsDesc getMockTblColStatsDesc() {
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(DEFAULT_TIME);
    desc.setDbName(DB);
    desc.setTableName(TBL);
    desc.setIsTblLevel(true);
    return desc;
  }

  /**
   * Returns a dummy partition level ColumnStatisticsDesc
   */
  private ColumnStatisticsDesc getMockPartColStatsDesc(String partKey, String partVal) {
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setLastAnalyzed(DEFAULT_TIME);
    desc.setDbName(DB);
    desc.setTableName(TBL);
    // part1=val1
    desc.setPartName(partKey + PART_KV_SEPARATOR + partVal);
    desc.setIsTblLevel(false);
    return desc;
  }

}
