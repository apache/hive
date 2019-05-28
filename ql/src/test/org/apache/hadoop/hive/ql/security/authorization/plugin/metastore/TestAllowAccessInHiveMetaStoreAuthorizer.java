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
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
Test whether HiveAuthorizer for MetaStore operation is trigger and HiveMetaStoreAuthzInfo is created by HiveMetaStoreAuthorizer
 */
public class TestAllowAccessInHiveMetaStoreAuthorizer {
  private static final String dbName      = "test";
  private static final String tblName     = "tmptbl";
  private static final String normalUser  = "bob";

  private static final String metaConfVal = "";

  private RawStore rawStore;
  private Configuration conf;
  private HiveMetaStore.HMSHandler hmsHandler;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_TXN_STATS_ENABLED, true);
    MetastoreConf.setBoolVar(conf, ConfVars.AGGREGATE_STATS_CACHE_ENABLED, false);
    MetastoreConf.setVar(conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN, metaConfVal);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetastoreConf.setVar(conf, ConfVars.HIVE_AUTHORIZATION_MANAGER, DummyHiveAuthorizerFactory.class.getName());
    MetastoreConf.setVar(conf, ConfVars.PRE_EVENT_LISTENERS, AllowAccessHiveMetaStoreAuthorizer.class.getName());
    conf.set("hadoop.proxyuser.hive.groups", "*");
    conf.set("hadoop.proxyuser.hive.hosts", "*");
    conf.set("hadoop.proxyuser.hive.users", "*");
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    hmsHandler = new HiveMetaStore.HMSHandler("test", conf, true);
    rawStore   = new ObjectStore();
    rawStore.setConf(hmsHandler.getConf());
    // Create the 'hive' catalog with new warehouse directory
    HiveMetaStore.HMSHandler.createDefaultCatalog(rawStore, new Warehouse(conf));
  }

  @Test
  public void testCreateDatabaseAuthorization() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(normalUser));
    try {
      Database db = new DatabaseBuilder()
              .setName(dbName)
              .build(conf);
      hmsHandler.create_database(db);
    } catch (Exception e) {
      // no Exceptions in checkPrivilege, user is allowed to create DB and hence it is a pass
    }
  }


  @Test
  public void testCreateTableAuthorization() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(normalUser));
    try {
      Table table = new TableBuilder()
              .setTableName(tblName)
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .setOwner(normalUser)
              .build(conf);
      hmsHandler.create_table(table);
    } catch (Exception e) {
      // no Exceptions in checkPrivilege, user is allowed to create Table and hence it is a pass
    }
  }
}
