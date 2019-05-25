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
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.client.builder.*;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
Test whether HiveAuthorizer for MetaStore operation is trigger and HiveMetaStoreAuthzInfo is created by HiveMetaStoreAuthorizer
 */
public class TestHiveMetaStoreAuthorizer {
  private static final String dbName      = "test";
  private static final String tblName     = "tmptbl";
  private static final String viewName    = "tmpview";
  private static final String roleName    = "tmpRole";
  private static final String catalogName = "testCatalog";
  private static final String normalUser  = "bob";
  private static final String superUser   = "hive";

  private static final String metaConfVal = "";

  private RawStore rawStore;
  private Configuration conf;
  private HiveMetaStore.HMSHandler hmsHandler;
  private String[] colType = new String[] {"double", "string"};

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
    MetastoreConf.setVar(conf, ConfVars.PRE_EVENT_LISTENERS, DummyDenyAccessHiveMetaStoreAuthorizer.class.getName());
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
      e.printStackTrace();
      String err = e.getMessage();
      String expected = "Operation type " + PreEventContext.PreEventType.CREATE_DATABASE.name()+ " not allowed for user:" + normalUser;
      assertEquals(expected, err);
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
      String err = e.getMessage();
      String expected = "Operation type " + PreEventContext.PreEventType.CREATE_TABLE + " not allowed for user:" + normalUser;
      assertEquals(expected, err);
    }
  }

  @Test
  public void testCreateViewNormalUserAuthorization() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(normalUser));
    try {
      Table viewObj = new TableBuilder()
              .setTableName(viewName)
              .setType(TableType.VIRTUAL_VIEW.name())
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .setOwner(normalUser)
              .build(conf);
      hmsHandler.create_table(viewObj);
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type CREATE_VIEW not allowed for user:" + normalUser;
      assertEquals(expected, err);
    }
  }

  @Test
  public void testCreateViewSuperUserAuthorization() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(superUser));
    try {
      Table viewObj = new TableBuilder()
              .setTableName(viewName)
              .setType(TableType.VIRTUAL_VIEW.name())
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .build(conf);
      hmsHandler.create_table(viewObj);
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type CREATE_VIEW allowed for user:" + superUser;
      assertEquals(expected, err);
    }
  }

  @Test
  public void testCreateRole() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(normalUser));
    try {
      Role role = new RoleBuilder()
              .setRoleName(roleName)
              .setOwnerName(normalUser)
              .build();
      hmsHandler.create_role(role);
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type " + PreEventContext.PreEventType.AUTHORIZATION_API_CALL.name()+ " not allowed for user:" + normalUser;
      assertEquals(expected, err);
    }
  }

  @Test
  public void testCreateCatalog() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(normalUser));
    try {
      Catalog catalog = new CatalogBuilder()
              .setName(catalogName)
              .setLocation("file:///tmp")
              .build();
      hmsHandler.create_catalog(new CreateCatalogRequest(catalog));
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type " + PreEventContext.PreEventType.CREATE_CATALOG.name()+ " not allowed for user:" + normalUser;
      assertEquals(expected,err);
    }
  }

  @Test
  public void testCreateCatalogSuperUserAuthorization() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(superUser));
    try {
      Catalog catalog = new CatalogBuilder()
              .setName(catalogName)
              .setLocation("file:///tmp")
              .build();
      hmsHandler.create_catalog(new CreateCatalogRequest(catalog));
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type " + PreEventContext.PreEventType.CREATE_CATALOG.name() + " allowed for user:" + superUser;
      assertEquals(expected, err);
    }
  }
}
