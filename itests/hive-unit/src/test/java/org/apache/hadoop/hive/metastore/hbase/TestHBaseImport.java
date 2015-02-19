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
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
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
 * Test that import from an RDBMS based metastore works
 */
public class TestHBaseImport {

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
  public void doImport() throws Exception {
    RawStore rdbms = new ObjectStore();
    rdbms.setConf(conf);

    String[] dbNames = new String[] {"importdb1", "importdb2"};
    String[] tableNames = new String[] {"nonparttable", "parttable"};
    String[] partVals = new String[] {"na", "emea", "latam", "apac"};
    String[] roles = new String[] {"role1", "role2"};
    int now = (int)System.currentTimeMillis() / 1000;

    for (int i = 0; i < roles.length; i++) {
      rdbms.addRole(roles[i], "me");
    }

    for (int i = 0; i < dbNames.length; i++) {
      rdbms.createDatabase(new Database(dbNames[i], "no description", "file:/tmp", emptyParameters));

      List<FieldSchema> cols = new ArrayList<FieldSchema>();
      cols.add(new FieldSchema("col1", "int", "nocomment"));
      SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
      StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
          serde, null, null, emptyParameters);
      rdbms.createTable(new Table(tableNames[0], dbNames[i], "me", now, now, 0, sd, null,
          emptyParameters, null, null, null));

      List<FieldSchema> partCols = new ArrayList<FieldSchema>();
      partCols.add(new FieldSchema("region", "string", ""));
      rdbms.createTable(new Table(tableNames[1], dbNames[i], "me", now, now, 0, sd, partCols,
          emptyParameters, null, null, null));

      for (int j = 0; j < partVals.length; j++) {
        StorageDescriptor psd = new StorageDescriptor(sd);
        psd.setLocation("file:/tmp/region=" + partVals[j]);
        Partition part = new Partition(Arrays.asList(partVals[j]), dbNames[i], tableNames[1],
            now, now, psd, emptyParameters);
        store.addPartition(part);
      }
    }

    HBaseImport importer = new HBaseImport();
    importer.setConnections(rdbms, store);
    importer.run();

    for (int i = 0; i < roles.length; i++) {
      Role role = store.getRole(roles[i]);
      Assert.assertNotNull(role);
      Assert.assertEquals(roles[i], role.getRoleName());
    }
    // Make sure there aren't any extra roles
    Assert.assertEquals(2, store.listRoleNames().size());

    for (int i = 0; i < dbNames.length; i++) {
      Database db = store.getDatabase(dbNames[i]);
      Assert.assertNotNull(db);
      // check one random value in the db rather than every value
      Assert.assertEquals("file:/tmp", db.getLocationUri());

      Table table = store.getTable(db.getName(), tableNames[0]);
      Assert.assertNotNull(table);
      Assert.assertEquals(now, table.getLastAccessTime());
      Assert.assertEquals("input", table.getSd().getInputFormat());

      table = store.getTable(db.getName(), tableNames[1]);
      Assert.assertNotNull(table);

      for (int j = 0; j < partVals.length; j++) {
        Partition part = store.getPartition(dbNames[i], tableNames[1], Arrays.asList(partVals[j]));
        Assert.assertNotNull(part);
        Assert.assertEquals("file:/tmp/region=" + partVals[j], part.getSd().getLocation());
      }

      Assert.assertEquals(4, store.getPartitions(dbNames[i], tableNames[1], -1).size());
      Assert.assertEquals(2, store.getAllTables(dbNames[i]).size());


    }

    Assert.assertEquals(2, store.getAllDatabases().size());
  }
}
