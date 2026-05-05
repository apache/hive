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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Assertions;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;

/**
 * Tests to verify metastore without acid support.
 */
@Category(MetastoreUnitTest.class)
public class TestNoAcidSupport {
  private static Configuration conf;
  private static HiveMetaStoreClient client;
  private static final String DB_NAME = "TestNoAcidSupport";
  private static final String TABLE_NAME = "t";

  @BeforeClass
  public static void beforeTests() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_SUPPORT_ACID, false);
    client = new HiveMetaStoreClient(conf);
    client.dropDatabase(DB_NAME, true, true, true);
    new DatabaseBuilder().setName(DB_NAME).create(client, conf);
  }

  @AfterClass
  public static void afterTests() throws Exception {
    try {
      client.dropDatabase(DB_NAME, true, true, true);
      client.close();
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
  }

  @After
  public void afterTest() throws TException {
    client.dropTable(DB_NAME, TABLE_NAME);
  }

  @Test
  public void testCreateAcidTable() {
    Exception exception = Assertions.assertThrows(MetaException.class, () -> {
      new TableBuilder().setDbName(DB_NAME).setTableName(TABLE_NAME)
          .addCol("i", ColumnType.INT_TYPE_NAME)
          .setType(TableType.MANAGED_TABLE.name())
          .addTableParam(TABLE_IS_TRANSACTIONAL, "true")
          .create(client, conf);
    });
    Assertions.assertTrue(exception.getMessage().contains("ACID tables are not permitted when the " +
        ConfVars.METASTORE_SUPPORT_ACID.getHiveName() + " property is set to false"));
  }

  @Test
  public void testCreateManagedTableChangeToExternal() throws Exception {
    new TableBuilder().setDbName(DB_NAME).setTableName(TABLE_NAME)
        .addCol("i", ColumnType.INT_TYPE_NAME)
        .setType(TableType.MANAGED_TABLE.name())
        .create(client, conf);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    Assertions.assertEquals(TableType.EXTERNAL_TABLE.name(), t.getTableType());
    Assertions.assertTrue(Boolean.parseBoolean(t.getParameters().get(HiveMetaHook.EXTERNAL)));
  }

  @Test
  public void testCreateExternalTable() throws Exception {
    new TableBuilder().setDbName(DB_NAME).setTableName(TABLE_NAME)
        .addCol("i", ColumnType.INT_TYPE_NAME)
        .setType(TableType.EXTERNAL_TABLE.name())
        .create(client, conf);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    Assertions.assertEquals(TableType.EXTERNAL_TABLE.name(), t.getTableType());
    Assertions.assertTrue(Boolean.parseBoolean(t.getParameters().get(HiveMetaHook.EXTERNAL)));
  }

  @Test
  public void testAlterToManagedTable() throws Exception {
    new TableBuilder().setDbName(DB_NAME).setTableName(TABLE_NAME)
        .addCol("i", ColumnType.INT_TYPE_NAME)
        .setType(TableType.EXTERNAL_TABLE.name())
        .create(client, conf);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    Assertions.assertEquals(TableType.EXTERNAL_TABLE.name(), t.getTableType());
    t.setTableType(TableType.MANAGED_TABLE.name());
    t.getParameters().put(TABLE_IS_TRANSACTIONAL, "true");
    Exception exception = Assertions.assertThrows(MetaException.class, () -> {
      client.alter_table(DB_NAME, TABLE_NAME, t);
    });
    Assertions.assertTrue(exception.getMessage().contains("ACID tables are not permitted when the " +
        ConfVars.METASTORE_SUPPORT_ACID.getHiveName() + " property is set to false"));
  }
}
