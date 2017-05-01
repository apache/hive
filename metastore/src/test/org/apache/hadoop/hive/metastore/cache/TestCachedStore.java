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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TestObjectStore.MockPartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCachedStore {

  private CachedStore cachedStore = new CachedStore();

  @Before
  public void setUp() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS, MockPartitionExpressionProxy.class.getName());

    ObjectStore objectStore = new ObjectStore();
    objectStore.setConf(conf);

    cachedStore.setRawStore(objectStore);

    SharedCache.getDatabaseCache().clear();
    SharedCache.getTableCache().clear();
    SharedCache.getPartitionCache().clear();
    SharedCache.getSdCache().clear();
    SharedCache.getPartitionColStatsCache().clear();
  }

  @Test
  public void testSharedStoreDb() {
    Database db1 = new Database();
    Database db2 = new Database();
    Database db3 = new Database();
    Database newDb1 = new Database();
    newDb1.setName("db1");

    SharedCache.addDatabaseToCache("db1", db1);
    SharedCache.addDatabaseToCache("db2", db2);
    SharedCache.addDatabaseToCache("db3", db3);

    Assert.assertEquals(SharedCache.getCachedDatabaseCount(), 3);

    SharedCache.alterDatabaseInCache("db1", newDb1);

    Assert.assertEquals(SharedCache.getCachedDatabaseCount(), 3);

    SharedCache.removeDatabaseFromCache("db2");

    Assert.assertEquals(SharedCache.getCachedDatabaseCount(), 2);

    List<String> dbs = SharedCache.listCachedDatabases();
    Assert.assertEquals(dbs.size(), 2);
    Assert.assertTrue(dbs.contains("db1"));
    Assert.assertTrue(dbs.contains("db3"));
  }

  @Test
  public void testSharedStoreTable() {
    Table tbl1 = new Table();
    StorageDescriptor sd1 = new StorageDescriptor();
    List<FieldSchema> cols1 = new ArrayList<FieldSchema>();
    cols1.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params1 = new HashMap<String, String>();
    params1.put("key", "value");
    sd1.setCols(cols1);
    sd1.setParameters(params1);
    sd1.setLocation("loc1");
    tbl1.setSd(sd1);
    tbl1.setPartitionKeys(new ArrayList<FieldSchema>());

    Table tbl2 = new Table();
    StorageDescriptor sd2 = new StorageDescriptor();
    List<FieldSchema> cols2 = new ArrayList<FieldSchema>();
    cols2.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params2 = new HashMap<String, String>();
    params2.put("key", "value");
    sd2.setCols(cols2);
    sd2.setParameters(params2);
    sd2.setLocation("loc2");
    tbl2.setSd(sd2);
    tbl2.setPartitionKeys(new ArrayList<FieldSchema>());

    Table tbl3 = new Table();
    StorageDescriptor sd3 = new StorageDescriptor();
    List<FieldSchema> cols3 = new ArrayList<FieldSchema>();
    cols3.add(new FieldSchema("col3", "int", ""));
    Map<String, String> params3 = new HashMap<String, String>();
    params3.put("key2", "value2");
    sd3.setCols(cols3);
    sd3.setParameters(params3);
    sd3.setLocation("loc3");
    tbl3.setSd(sd3);
    tbl3.setPartitionKeys(new ArrayList<FieldSchema>());

    Table newTbl1 = new Table();
    newTbl1.setDbName("db2");
    newTbl1.setTableName("tbl1");
    StorageDescriptor newSd1 = new StorageDescriptor();
    List<FieldSchema> newCols1 = new ArrayList<FieldSchema>();
    newCols1.add(new FieldSchema("newcol1", "int", ""));
    Map<String, String> newParams1 = new HashMap<String, String>();
    newParams1.put("key", "value");
    newSd1.setCols(newCols1);
    newSd1.setParameters(params1);
    newSd1.setLocation("loc1");
    newTbl1.setSd(newSd1);
    newTbl1.setPartitionKeys(new ArrayList<FieldSchema>());

    SharedCache.addTableToCache("db1", "tbl1", tbl1);
    SharedCache.addTableToCache("db1", "tbl2", tbl2);
    SharedCache.addTableToCache("db1", "tbl3", tbl3);
    SharedCache.addTableToCache("db2", "tbl1", tbl1);

    Assert.assertEquals(SharedCache.getCachedTableCount(), 4);
    Assert.assertEquals(SharedCache.getSdCache().size(), 2);

    Table t = SharedCache.getTableFromCache("db1", "tbl1");
    Assert.assertEquals(t.getSd().getLocation(), "loc1");

    SharedCache.removeTableFromCache("db1", "tbl1");
    Assert.assertEquals(SharedCache.getCachedTableCount(), 3);
    Assert.assertEquals(SharedCache.getSdCache().size(), 2);

    SharedCache.alterTableInCache("db2", "tbl1", newTbl1);
    Assert.assertEquals(SharedCache.getCachedTableCount(), 3);
    Assert.assertEquals(SharedCache.getSdCache().size(), 3);

    SharedCache.removeTableFromCache("db1", "tbl2");
    Assert.assertEquals(SharedCache.getCachedTableCount(), 2);
    Assert.assertEquals(SharedCache.getSdCache().size(), 2);
  }

  @Test
  public void testSharedStorePartition() {
    Partition part1 = new Partition();
    StorageDescriptor sd1 = new StorageDescriptor();
    List<FieldSchema> cols1 = new ArrayList<FieldSchema>();
    cols1.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params1 = new HashMap<String, String>();
    params1.put("key", "value");
    sd1.setCols(cols1);
    sd1.setParameters(params1);
    sd1.setLocation("loc1");
    part1.setSd(sd1);
    part1.setValues(Arrays.asList("201701"));

    Partition part2 = new Partition();
    StorageDescriptor sd2 = new StorageDescriptor();
    List<FieldSchema> cols2 = new ArrayList<FieldSchema>();
    cols2.add(new FieldSchema("col1", "int", ""));
    Map<String, String> params2 = new HashMap<String, String>();
    params2.put("key", "value");
    sd2.setCols(cols2);
    sd2.setParameters(params2);
    sd2.setLocation("loc2");
    part2.setSd(sd2);
    part2.setValues(Arrays.asList("201702"));

    Partition part3 = new Partition();
    StorageDescriptor sd3 = new StorageDescriptor();
    List<FieldSchema> cols3 = new ArrayList<FieldSchema>();
    cols3.add(new FieldSchema("col3", "int", ""));
    Map<String, String> params3 = new HashMap<String, String>();
    params3.put("key2", "value2");
    sd3.setCols(cols3);
    sd3.setParameters(params3);
    sd3.setLocation("loc3");
    part3.setSd(sd3);
    part3.setValues(Arrays.asList("201703"));

    Partition newPart1 = new Partition();
    newPart1.setDbName("db1");
    newPart1.setTableName("tbl1");
    StorageDescriptor newSd1 = new StorageDescriptor();
    List<FieldSchema> newCols1 = new ArrayList<FieldSchema>();
    newCols1.add(new FieldSchema("newcol1", "int", ""));
    Map<String, String> newParams1 = new HashMap<String, String>();
    newParams1.put("key", "value");
    newSd1.setCols(newCols1);
    newSd1.setParameters(params1);
    newSd1.setLocation("loc1");
    newPart1.setSd(newSd1);
    newPart1.setValues(Arrays.asList("201701"));

    SharedCache.addPartitionToCache("db1", "tbl1", part1);
    SharedCache.addPartitionToCache("db1", "tbl1", part2);
    SharedCache.addPartitionToCache("db1", "tbl1", part3);
    SharedCache.addPartitionToCache("db1", "tbl2", part1);

    Assert.assertEquals(SharedCache.getCachedPartitionCount(), 4);
    Assert.assertEquals(SharedCache.getSdCache().size(), 2);

    Partition t = SharedCache.getPartitionFromCache("db1", "tbl1", Arrays.asList("201701"));
    Assert.assertEquals(t.getSd().getLocation(), "loc1");

    SharedCache.removePartitionFromCache("db1", "tbl2", Arrays.asList("201701"));
    Assert.assertEquals(SharedCache.getCachedPartitionCount(), 3);
    Assert.assertEquals(SharedCache.getSdCache().size(), 2);

    SharedCache.alterPartitionInCache("db1", "tbl1", Arrays.asList("201701"), newPart1);
    Assert.assertEquals(SharedCache.getCachedPartitionCount(), 3);
    Assert.assertEquals(SharedCache.getSdCache().size(), 3);

    SharedCache.removePartitionFromCache("db1", "tbl1", Arrays.asList("201702"));
    Assert.assertEquals(SharedCache.getCachedPartitionCount(), 2);
    Assert.assertEquals(SharedCache.getSdCache().size(), 2);
  }
}
