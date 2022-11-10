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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.Test;

public class TestMetastoreTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(TestMetastoreTransformer.class);
  protected static HiveMetaStoreClient client;
  protected static Configuration conf = null;
  protected static Warehouse warehouse;
  protected static boolean isThriftClient = false;

  @Before
  public void setUp() throws Exception {
    initConf();
    warehouse = new Warehouse(conf);

    // set some values to use for getting conf. vars
    MetastoreConf.setBoolVar(conf, ConfVars.METRICS_ENABLED, true);
    conf.set("datanucleus.autoCreateTables", "false");
    conf.set("hive.in.test", "true");

    MetaStoreTestUtils.setConfForStandloneMode(conf);

    warehouse = new Warehouse(conf);
    client = createClient();
  }

  @After
  public void tearDown() throws Exception {
    client.close();
  }

  protected HiveMetaStoreClient createClient() throws Exception {
    try {
      return new HiveMetaStoreClient(conf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  protected void initConf() {
    if (null == conf) {
      conf = MetastoreConf.newMetastoreConf();
    }
  }

  private static void silentDropDatabase(String dbName) throws TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException | InvalidOperationException | MetaException e) {
      // NOP
    }
  }

  @Test
  public void testAlterTableIsCaseInSensitive() throws Exception {
    String dbName = "alterdb";
    String tblName = "altertbl";

    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);

    String dbLocation = MetastoreConf.getVar(conf, ConfVars.WAREHOUSE_EXTERNAL) + "/_testDB_table_create_";
    String mgdLocation = MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/_testDB_table_create_";
    new DatabaseBuilder().setName(dbName).setLocation(dbLocation).setManagedLocation(mgdLocation).create(client, conf);

    ArrayList<FieldSchema> invCols = new ArrayList<>(2);
    invCols.add(new FieldSchema("n-ame", ColumnType.STRING_TYPE_NAME, ""));
    invCols.add(new FieldSchema("in.come", ColumnType.INT_TYPE_NAME, ""));

    Table tbl = new TableBuilder().setDbName(dbName).setTableName(tblName).setCols(invCols).build(conf);

    client.createTable(tbl);
    tbl = client.getTable(tbl.getDbName(), tbl.getTableName());
    tbl.setTableName(tblName.toUpperCase());

    // expected to execute the operation without any exceptions
    client.alter_table(tbl.getDbName(), tbl.getTableName().toUpperCase(), tbl);

    Table tbl2 = client.getTable(tbl.getDbName(), tbl.getTableName().toLowerCase());
    assertEquals(tbl.getTableName().toLowerCase(), tbl2.getTableName());

  }

  @Test
  public void testLocationBlank() throws Exception {
    Table tbl =
        new TableBuilder().setTableName("locationBlank").setCols(new ArrayList<FieldSchema>()).setLocation("")
            .build(conf);

    // expected to execute the operation without any exceptions
    client.createTable(tbl);

    Table tbl2 = client.getTable(tbl.getDbName(), tbl.getTableName().toLowerCase());
    assertEquals("locationblank", new File(tbl2.getSd().getLocation()).getName());
  }

}
