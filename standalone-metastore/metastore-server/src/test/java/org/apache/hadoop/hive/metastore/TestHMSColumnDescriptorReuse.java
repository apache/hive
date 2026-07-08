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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Derby;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestHMSColumnDescriptorReuse {
  private ObjectStore objectStore = null;
  Configuration conf;

  // Modify to try out with different databases.
  // Keep it on Derby once you commit your change. It makes the test execution faster.
  private final DatabaseRule DB = new Derby(true);

  @Before
  public void setUp() throws Exception {
    DB.before();
    DB.install();

    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, DB.getJdbcUrl());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER, DB.getJdbcDriver());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME, DB.getHiveUser());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PWD, DB.getHivePassword());
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, false);

    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.PARTITION_REUSE_COLUMN_DESCRIPTORS, true);

    MetaStoreTestUtils.setConfForStandloneMode(conf);

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
    Database db = new DatabaseBuilder()
            .setName("default")
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf);
    objectStore.createDatabase(db);
  }

  @After
  public void tearDown() {
    DB.after();
  }

  @Test
  public void testReuseAfterSimpleSchemaEvolution() throws Exception {
    FieldSchema id = new FieldSchema("id", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema fname = new FieldSchema("fname", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema country = new FieldSchema("country", ColumnType.STRING_TYPE_NAME, "");

    Table tbl1 = newTable(Arrays.asList(id, fname), Collections.singletonList(country));
    objectStore.createTable(tbl1);
    objectStore.addPartition(newPart(tbl1, "US"));

    FieldSchema lname = new FieldSchema("lname", ColumnType.STRING_TYPE_NAME, "");
    Table tbl2 = newTable(Arrays.asList(id, fname, lname), Collections.singletonList(country));

    objectStore.alterTable(DEFAULT_CATALOG_NAME, tbl1.getDbName(), tbl1.getTableName(), tbl2, null);
    objectStore.addPartition(newPart(tbl2, "Italy"));

    objectStore.addPartition(newPart(tbl2, "Hungary"));
    objectStore.addPartition(newPart(tbl1, "Ukraine"));

    assertEquals(2, countColumnDescriptors());
  }

  @Test
  public void testAddPartitionAlterAddPartition() throws MetaException, InvalidObjectException {
    FieldSchema id = new FieldSchema("id", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema fname = new FieldSchema("fname", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema country = new FieldSchema("country", ColumnType.STRING_TYPE_NAME, "");

    Table tbl1 = newTable(Arrays.asList(id, fname), Collections.singletonList(country));
    objectStore.createTable(tbl1);
    objectStore.addPartition(newPart(tbl1, "US"));
    objectStore.addPartition(newPart(tbl1, "Greece"));
    FieldSchema lname = new FieldSchema("lname", ColumnType.STRING_TYPE_NAME, "");

    Table tbl2 = newTable(Arrays.asList(id, fname, lname), Collections.singletonList(country));
    objectStore.alterTable(DEFAULT_CATALOG_NAME, tbl1.getDbName(), tbl1.getTableName(), tbl2, null);
    objectStore.addPartition(newPart(tbl2, "Italy"));

    // Mimics replication scenario where we are adding partitions to the "same" table but with a different schema.
    // The tbl1 is using the old storage descriptor so "Germany" and "Belgium" partitions will have the old schema
    // And will lead to "duplicate" entries in "CDS" and "COLUMNS_V2" tables.
    objectStore.addPartition(newPart(tbl1, "Germany"));
    objectStore.addPartition(newPart(tbl1, "Belgium"));

    // On the other hand the addition of a partition to the table with the new schema is successfully using the
    // existing storage/column descriptors
    objectStore.addPartition(newPart(tbl2, "England"));

    assertEquals(2, countColumnDescriptors());
  }

  @Test
  public void testNoReusableColumnDescriptors() throws MetaException, InvalidObjectException {
    FieldSchema id = new FieldSchema("id", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema fname = new FieldSchema("fname", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema country = new FieldSchema("country", ColumnType.STRING_TYPE_NAME, "");

    Table tbl1 = newTable(Arrays.asList(id, fname), Collections.singletonList(country));
    objectStore.createTable(tbl1);
    objectStore.addPartition(newPart(tbl1, "US"));

    FieldSchema lname = new FieldSchema("lname", ColumnType.STRING_TYPE_NAME, "");
    Table tbl2 = newTable(Arrays.asList(id, fname, lname), Collections.singletonList(country));

    objectStore.alterTable(DEFAULT_CATALOG_NAME, tbl1.getDbName(), tbl1.getTableName(), tbl2, null);
    objectStore.addPartition(newPart(tbl2, "Italy"));

    FieldSchema address = new FieldSchema("address", ColumnType.STRING_TYPE_NAME, "");
    Table tbl3 = newTable(Arrays.asList(id, fname, lname, address), Collections.singletonList(country));
    objectStore.alterTable(DEFAULT_CATALOG_NAME, tbl1.getDbName(), tbl1.getTableName(), tbl3, null);
    objectStore.addPartition(newPart(tbl3, "Hungary"));

    assertEquals(3, countColumnDescriptors());
  }

  private int countColumnDescriptors() {
    try(Connection c = TestTxnDbUtil.getConnection(conf)){
      try(ResultSet rs = c.prepareStatement("SELECT COUNT(*) FROM \"CDS\"").executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Table newTable(List<FieldSchema> columns, List<FieldSchema> partCols) {
    int timeSec = (int) System.currentTimeMillis() / 1000;

    StorageDescriptor sd = new StorageDescriptor(columns,
            "/fake/location/person",
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.mapred.MapFileOutputFormat",
            false,
            0,
            new SerDeInfo("SerDeName", "serializationLib", null),
            null,
            null,
            null);

    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "false");

    return
            new Table("person", "default", "owner", timeSec, timeSec, 3, sd, partCols,
                    tableParams, null, null, "MANAGED_TABLE");
  }

  private static Partition newPart(Table tbl, String value) {
    int timeSec = (int) System.currentTimeMillis() / 1000;
    Map<String, String> partitionParams = new HashMap<>();

    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    StorageDescriptor psd = tbl.getSd().deepCopy();
    psd.setLocation(psd.getLocation() + "/" + value);
    Partition p = new Partition(
        Collections.singletonList(value),
        tbl.getDbName(),
        tbl.getTableName(),
        timeSec,
        timeSec,
        psd,
        partitionParams);
    p.setCatName(DEFAULT_CATALOG_NAME);

    return p;
  }

}
