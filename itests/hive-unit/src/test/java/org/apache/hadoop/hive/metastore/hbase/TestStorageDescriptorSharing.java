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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Integration tests with HBase Mini-cluster for HBaseStore
 */
public class TestStorageDescriptorSharing extends HBaseIntegrationTests {

  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseStoreIntegration.class.getName());

  private MessageDigest md;

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
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

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

    Assert.assertEquals(1, HBaseReadWrite.getInstance().countStorageDescriptor());

    String tableName2 = "differentTable";
    sd = new StorageDescriptor(cols, "file:/tmp", "input2", "output", false, 0,
        serde, null, null, emptyParameters);
    table = new Table(tableName2, "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    store.createTable(table);

    Assert.assertEquals(2, HBaseReadWrite.getInstance().countStorageDescriptor());

    // Drop one of the partitions and make sure it doesn't drop the storage descriptor
    store.dropPartition(dbName, tableName, Arrays.asList(partVals.get(0)));
    Assert.assertEquals(2, HBaseReadWrite.getInstance().countStorageDescriptor());

    // Alter the second table in a few ways to make sure it changes it's descriptor properly
    table = store.getTable(dbName, tableName2);
    byte[] sdHash = HBaseUtils.hashStorageDescriptor(table.getSd(), md);

    // Alter the table without touching the storage descriptor
    table.setLastAccessTime(startTime + 1);
    store.alterTable(dbName, tableName2, table);
    Assert.assertEquals(2, HBaseReadWrite.getInstance().countStorageDescriptor());
    table = store.getTable(dbName, tableName2);
    byte[] alteredHash = HBaseUtils.hashStorageDescriptor(table.getSd(), md);
    Assert.assertArrayEquals(sdHash, alteredHash);

    // Alter the table, changing the storage descriptor
    table.getSd().setOutputFormat("output_changed");
    store.alterTable(dbName, tableName2, table);
    Assert.assertEquals(2, HBaseReadWrite.getInstance().countStorageDescriptor());
    table = store.getTable(dbName, tableName2);
    alteredHash = HBaseUtils.hashStorageDescriptor(table.getSd(), md);
    Assert.assertFalse(Arrays.equals(sdHash, alteredHash));

    // Alter one of the partitions without touching the storage descriptor
    Partition part = store.getPartition(dbName, tableName, Arrays.asList(partVals.get(1)));
    sdHash = HBaseUtils.hashStorageDescriptor(part.getSd(), md);
    part.setLastAccessTime(part.getLastAccessTime() + 1);
    store.alterPartition(dbName, tableName, Arrays.asList(partVals.get(1)), part);
    Assert.assertEquals(2, HBaseReadWrite.getInstance().countStorageDescriptor());
    part = store.getPartition(dbName, tableName, Arrays.asList(partVals.get(1)));
    alteredHash = HBaseUtils.hashStorageDescriptor(part.getSd(), md);
    Assert.assertArrayEquals(sdHash, alteredHash);

    // Alter the partition, changing the storage descriptor
    part.getSd().setOutputFormat("output_changed_some_more");
    store.alterPartition(dbName, tableName, Arrays.asList(partVals.get(1)), part);
    Assert.assertEquals(3, HBaseReadWrite.getInstance().countStorageDescriptor());
    part = store.getPartition(dbName, tableName, Arrays.asList(partVals.get(1)));
    alteredHash = HBaseUtils.hashStorageDescriptor(part.getSd(), md);
    Assert.assertFalse(Arrays.equals(sdHash, alteredHash));

    // Alter multiple partitions without touching the storage descriptors
    List<Partition> parts = store.getPartitions(dbName, tableName, -1);
    sdHash = HBaseUtils.hashStorageDescriptor(parts.get(1).getSd(), md);
    for (int i = 1; i < 3; i++) {
      parts.get(i).setLastAccessTime(97);
    }
    List<List<String>> listPartVals = new ArrayList<List<String>>();
    for (String pv : partVals.subList(1, partVals.size())) {
      listPartVals.add(Arrays.asList(pv));
    }
    store.alterPartitions(dbName, tableName, listPartVals, parts);
    Assert.assertEquals(3, HBaseReadWrite.getInstance().countStorageDescriptor());
    parts = store.getPartitions(dbName, tableName, -1);
    alteredHash = HBaseUtils.hashStorageDescriptor(parts.get(1).getSd(), md);
    Assert.assertArrayEquals(sdHash, alteredHash);

    // Alter multiple partitions changning the storage descriptors
    parts = store.getPartitions(dbName, tableName, -1);
    sdHash = HBaseUtils.hashStorageDescriptor(parts.get(1).getSd(), md);
    for (int i = 1; i < 3; i++) {
      parts.get(i).getSd().setOutputFormat("yet_a_different_of");
    }
    store.alterPartitions(dbName, tableName, listPartVals, parts);
    Assert.assertEquals(4, HBaseReadWrite.getInstance().countStorageDescriptor());
    parts = store.getPartitions(dbName, tableName, -1);
    alteredHash = HBaseUtils.hashStorageDescriptor(parts.get(1).getSd(), md);
    Assert.assertFalse(Arrays.equals(sdHash, alteredHash));

    for (String partVal : partVals.subList(1, partVals.size())) {
      store.dropPartition(dbName, tableName, Arrays.asList(partVal));
    }
    store.dropTable(dbName, tableName);
    store.dropTable(dbName, tableName2);

    Assert.assertEquals(0, HBaseReadWrite.getInstance().countStorageDescriptor());


  }
}
