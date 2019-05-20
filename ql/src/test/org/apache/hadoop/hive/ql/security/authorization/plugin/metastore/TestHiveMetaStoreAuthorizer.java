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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
Test HiveMetaStoreAuthorier
 */
public class TestHiveMetaStoreAuthorizer {
  private Configuration conf;
  private HiveMetaStoreClient msc;

  private static final String dbName = "test";
  private static final String tblName = "tmptbl";
  private static final String metaConfVal = "";

  @Before
  public void setUp() throws Exception {

    System.setProperty("hive.metastore.pre.event.listeners",
            "org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer");

    conf = MetastoreConf.newMetastoreConf();

    MetastoreConf.setVar(conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN, metaConfVal);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetastoreConf.setVar(conf, ConfVars.HIVE_AUTHORIZATION_MANAGER, DummyHiveAuthorizerFactory.class.getName());
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);

    msc = new HiveMetaStoreClient(conf);

    msc.dropDatabase(dbName, true, true, true);
  }

  private void validateCreateDb(Database expectedDb, Database actualDb) {
    assertEquals(expectedDb.getName(), actualDb.getName());
    assertEquals(expectedDb.getLocationUri(), actualDb.getLocationUri());
  }

  private void validateTable(Table expectedTable, Table actualTable) {
    assertEquals(expectedTable.getTableName(), actualTable.getTableName());
    assertEquals(expectedTable.getDbName(), actualTable.getDbName());
    assertEquals(expectedTable.getSd().getLocation(), actualTable.getSd().getLocation());
  }

  private void validateCreateTable(Table expectedTable, Table actualTable) {
    validateTable(expectedTable, actualTable);
  }

  @Test
  public void testHiveMetaStoreAuthorizer() throws Exception {
    new DatabaseBuilder()
            .setName(dbName)
            .create(msc, conf);
    PreCreateDatabaseEvent preDbEvent = (PreCreateDatabaseEvent) HiveMetaStoreAuthorizer.getPreEventContext();
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, preDbEvent.getDatabase());

    Table table = new TableBuilder()
            .inDb(db)
            .setTableName(tblName)
            .addCol("a", "string")
            .addPartCol("b", "string")
            .create(msc, conf);
    PreCreateTableEvent preTblEvent = (PreCreateTableEvent)(HiveMetaStoreAuthorizer.getPreEventContext());
    Table tbl = msc.getTable(dbName, tblName);
    validateCreateTable(tbl, preTblEvent.getTable());
  }
}
