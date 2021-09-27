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

import jline.internal.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLForeignKeyBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLPrimaryKeyBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Category(MetastoreUnitTest.class)
public class TestHiveMetastoreClientForGenuineErrors {
  private Configuration conf;
  private IMetaStoreClient msc;
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetastoreClientForGenuineErrors.class);
  private RetryingMetaStoreClient retryingMetaStoreClient;

  @Before
  public void setup() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_FAILURE_RETRIES, 1);
    MetastoreConf.setLongVar(conf, ConfVars.HMS_HANDLER_ATTEMPTS, 1);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    retryingMetaStoreClient = new RetryingMetaStoreClient(conf,
        new Class[] {Configuration.class, HiveMetaHookLoader.class, Boolean.class},
        new Object[] {conf, null, true},
          null , HiveMetaStoreClient.class);
    msc = retryingMetaStoreClient.getProxy(retryingMetaStoreClient);
  }

  @Test
  public void testHiveMetaStoreClientForGenuineErrors() throws TException {
    String tblname_parent = "parent_tbl";
    String colname = "col1";
    String tblname_child = "child_tbl";
    String dbname = "test_db_2311";

    new DatabaseBuilder()
        .setName(dbname)
        .create(msc, conf);

    LOG.warn("DataBase Created");

    new TableBuilder()
        .setTableName(tblname_parent)
        .setType(TableType.MANAGED_TABLE.toString())
        .setDbName(dbname)
        .addCol(colname, ColumnType.STRING_TYPE_NAME)
        .create(msc, conf);

    new TableBuilder()
        .setTableName(tblname_child)
        .setDbName(dbname)
        .addCol(colname,ColumnType.STRING_TYPE_NAME)
        .create(msc,conf);

    LOG.warn("Tables Created");

    List<SQLPrimaryKey> pk =new SQLPrimaryKeyBuilder()
        .setTableName(tblname_parent)
        .setDbName(dbname)
        .addColumn(colname)
        .setConstraintName("const1")
        .setPrimaryKeyName("primarykeyparent").build(conf);

    msc.addPrimaryKey(pk);

    LOG.warn("First Primary Key Attempt Done.");

    List<SQLForeignKey> fks = new SQLForeignKeyBuilder()
        .setDbName(dbname)
        .addColumn(colname)
        .addPkColumn(colname)
        .setTableName(tblname_child)
        .setPkTable(tblname_parent)
        .setPkName("primarykeyparent")
        .setDbName(dbname)
        .setPkDb(dbname)
        .setConstraintName("fks1")
        .build(conf);

    try {
      msc.addForeignKey(fks);
    } catch (Exception e){
      LOG.warn(e.getMessage());
    }
    try {
      msc.addForeignKey(fks);
    } catch (Exception e){
      Log.warn("Total retries made:"+ retryingMetaStoreClient.getRetriesMade());
      LOG.warn(e.getMessage());
    }
    Assert.assertEquals(0, retryingMetaStoreClient.getRetriesMade());
  }
}
