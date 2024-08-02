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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.client.builder.*;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.io.File;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
Test whether HiveAuthorizer for MetaStore operation is trigger and HiveMetaStoreAuthzInfo is created by HiveMetaStoreAuthorizer
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHiveMetaStoreAuthorizer {
  private static final String dbName           = "test";
  private static final String tblName          = "tmptbl";
  private static final String viewName         = "tmpview";
  private static final String roleName         = "tmpRole";
  private static final String catalogName      = "testCatalog";
  private static final String dcName           = "testDC";
  private static final String unAuthorizedUser = "bob";
  private static final String authorizedUser   = "sam";
  private static final String superUser        = "hive";

  private static final String metaConfVal = "";

  private static final String TEST_DATA_DIR = new File("file:///testdata").getPath();
  private RawStore rawStore;
  private Configuration conf;
  private HMSHandler hmsHandler;

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
    MetastoreConf.setVar(conf, ConfVars.PRE_EVENT_LISTENERS, HiveMetaStoreAuthorizer.class.getName());
    MetastoreConf.setVar(conf, ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER, HadoopDefaultMetastoreAuthenticator.class.getName() );
    conf.set("hadoop.proxyuser.hive.groups", "*");
    conf.set("hadoop.proxyuser.hive.hosts", "*");
    conf.set("hadoop.proxyuser.hive.users", "*");

    MetaStoreTestUtils.setConfForStandloneMode(conf);

    hmsHandler = new HMSHandler("test", conf);
    hmsHandler.init();
    rawStore   = new ObjectStore();
    rawStore.setConf(hmsHandler.getConf());
    // Create the 'hive' catalog with new warehouse directory
    HMSHandler.createDefaultCatalog(rawStore, new Warehouse(conf));
    try {
      DropDataConnectorRequest dropDcReq = new DropDataConnectorRequest(dcName);
      dropDcReq.setIfNotExists(true);
      dropDcReq.setCheckReferences(true);
      hmsHandler.drop_dataconnector_req(dropDcReq);
      hmsHandler.drop_table(dbName, tblName, true);
      hmsHandler.drop_database(dbName, true, false);
      hmsHandler.drop_catalog(new DropCatalogRequest(catalogName));
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    } catch (Exception e) {
      // NoSuchObjectException will be ignored if the step objects are not there
    }
  }

  @Test
  public void testA_CreateDatabase_unAuthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(unAuthorizedUser));
    try {
      Database db = new DatabaseBuilder()
              .setName(dbName)
              .build(conf);
      hmsHandler.create_database(db);
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type " + HiveOperationType.CREATEDATABASE+ " not allowed for user:" + unAuthorizedUser;
      assertEquals(expected, err);
    }
  }

  @Test
  public void testB_CreateTable_unAuthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(unAuthorizedUser));
    try {
      Table table = new TableBuilder()
              .setTableName(tblName)
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .setOwner(unAuthorizedUser)
              .build(conf);
      hmsHandler.create_table(table);
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type " + HiveOperationType.CREATETABLE + " not allowed for user:" + unAuthorizedUser;
      assertEquals(expected, err);
    }
  }

  @Test
  public void testC_CreateView_anyUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      Table viewObj = new TableBuilder()
              .setTableName(viewName)
              .setType(TableType.VIRTUAL_VIEW.name())
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .setOwner(authorizedUser)
              .build(conf);
      hmsHandler.create_table(viewObj);
      Map<String, String> params = viewObj.getParameters();
      assertTrue(params.containsKey("Authorized"));
      assertTrue("false".equalsIgnoreCase(params.get("Authorized")));
    } catch (Exception e) {
      // no Exceptions for user same as normal user is now allowed CREATE_VIEW operation
    }
  }

  @Test
  public void testC2_AlterView_anyUser() throws Exception{
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      Table viewObj = new TableBuilder()
              .setTableName(viewName)
              .setType(TableType.VIRTUAL_VIEW.name())
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .setOwner(authorizedUser)
              .build(conf);
      hmsHandler.create_table(viewObj);
      viewObj = new TableBuilder()
              .setTableName(viewName)
              .setType(TableType.VIRTUAL_VIEW.name())
              .addCol("dep", ColumnType.STRING_TYPE_NAME)
              .setOwner(authorizedUser)
              .build(conf);
      hmsHandler.alter_table("default", viewName, viewObj);
      Map<String, String> params = viewObj.getParameters();
      assertTrue(params.containsKey("Authorized"));
      assertTrue("false".equalsIgnoreCase(params.get("Authorized")));
    } catch (Exception e) {
      // no Exceptions for user same as normal user is now allowed Alter_VIEW operation
    }
  }

  @Test
  public void testD_CreateView_SuperUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(superUser));
    try {
      Table viewObj = new TableBuilder()
              .setTableName(viewName)
              .setType(TableType.VIRTUAL_VIEW.name())
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .build(conf);
      hmsHandler.create_table(viewObj);
    } catch (Exception e) {
      // no Exceptions for superuser as hive is allowed CREATE_VIEW operation
    }
  }

  @Test
  public void testE_CreateRole__anyUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      Role role = new RoleBuilder()
              .setRoleName(roleName)
              .setOwnerName(authorizedUser)
              .build();
      hmsHandler.create_role(role);
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type " + PreEventContext.PreEventType.AUTHORIZATION_API_CALL.name()+ " not allowed for user:" + authorizedUser;
      assertEquals(expected, err);
    }
  }

  @Test
  public void testF_CreateCatalog_anyUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      Catalog catalog = new CatalogBuilder()
              .setName(catalogName)
              .setLocation(TEST_DATA_DIR)
              .build();
      hmsHandler.create_catalog(new CreateCatalogRequest(catalog));
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type " + PreEventContext.PreEventType.CREATE_CATALOG.name()+ " not allowed for user:" + authorizedUser;
      assertEquals(expected,err);
    }
  }

  @Test
  public void testG_CreateCatalog_SuperUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(superUser));
    try {
      Catalog catalog = new CatalogBuilder()
              .setName(catalogName)
              .setLocation(TEST_DATA_DIR)
              .build();
      hmsHandler.create_catalog(new CreateCatalogRequest(catalog));
    } catch (Exception e) {
      // no Exceptions for superuser as hive is allowed CREATE CATALOG operation
    }
  }


  @Test
  public void testH_CreateDatabase_authorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      Database db = new DatabaseBuilder()
              .setName(dbName)
              .build(conf);
      hmsHandler.create_database(db);
    } catch (Exception e) {
      // No Exception for create database for authorized user
    }
  }

  @Test
  public void testI_CreateTable_authorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      Table table = new TableBuilder()
              .setTableName(tblName)
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .setOwner(authorizedUser)
              .build(conf);
      hmsHandler.create_table(table);
    } catch (Exception e) {
      // No Exception for create table for authorized user
    }
  }

  @Test
  public void testJ_AlterTable_AuthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      Table table = new TableBuilder()
              .setTableName(tblName)
              .addCol("name", ColumnType.STRING_TYPE_NAME)
              .setOwner(authorizedUser)
              .build(conf);
      hmsHandler.create_table(table);

      Table alteredTable = new TableBuilder()
              .addCol("dep", ColumnType.STRING_TYPE_NAME)
              .build(conf);
      hmsHandler.alter_table("default",tblName,alteredTable);
    } catch (Exception e) {
      // No Exception for create table for authorized user
    }
  }

  @Test
  public void testK_DropTable_authorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      hmsHandler.drop_table(dbName,tblName,true);
    } catch (Exception e) {
      // No Exception for create table for authorized user
    }
  }

  @Test
  public void testL_DropDatabase_authorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      hmsHandler.drop_database(dbName,true,true);
    } catch (Exception e) {
      // No Exception for dropDatabase for authorized user
    }
  }

  @Test
  public void testM_DropCatalog_SuperUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(superUser));
    try {
      hmsHandler.drop_catalog(new DropCatalogRequest(catalogName));
    } catch (Exception e) {
      // no Exceptions for superuser as hive is allowed CREATE CATALOG operation
    }
  }

  @Test
  public void testNShowDatabaseAuthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      hmsHandler.get_all_databases();
    } catch (Exception e) {
      // no Exceptions for show database as authorized user.
    }
  }

  @Test
  public void testOShowDatabaseUnauthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(unAuthorizedUser));
    try {
      hmsHandler.get_all_databases();
    } catch (Exception e) {
      String err = e.getMessage();
      if (StringUtils.isNotEmpty(err)) {
        assert(true);
      }
    }
  }

  @Test
  public void testPShowTablesAuthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      hmsHandler.get_all_tables("default");
    } catch (Exception e) {
      // no Exceptions for show tables as authorized user.
    }
  }

  @Test
  public void testQShowTablesUnauthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(unAuthorizedUser));
    try {
      hmsHandler.get_all_tables("default");
    } catch (Exception e) {
      String err = e.getMessage();
      if (StringUtils.isNotEmpty(err)) {
        assert(true);
      }
    }
  }

  @Test
  public void testR_CreateDataConnector_unAuthorizedUser() {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(unAuthorizedUser));
    try {
      DataConnector connector = new DataConnector(dcName, "mysql", "jdbc:mysql://localhost:3306/hive");
      CreateDataConnectorRequest connectorReq = new CreateDataConnectorRequest(connector);
      hmsHandler.create_dataconnector_req(connectorReq);
    } catch (Exception e) {
      String err = e.getMessage();
      String expected = "Operation type " + HiveOperationType.CREATEDATACONNECTOR+ " not allowed for user:" + unAuthorizedUser;
      assertEquals(expected, err);
    }
  }

  @Test
  public void testS_CreateDataConnector_authorizedUser() {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      DataConnector connector = new DataConnector(dcName, "mysql", "jdbc:mysql://localhost:3306/hive");
      CreateDataConnectorRequest connectorReq = new CreateDataConnectorRequest(connector);
      hmsHandler.create_dataconnector_req(connectorReq);
    } catch (Exception e) {
      fail("testS_CreateDataConnector_authorizedUser() failed with " + e);
    }
  }

  @Test
  public void testT_AlterDataConnector_AuthorizedUser() {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      DataConnector connector = new DataConnector(dcName, "mysql", "jdbc:mysql://localhost:3306/hive");
      CreateDataConnectorRequest connectorReq = new CreateDataConnectorRequest(connector);
      hmsHandler.create_dataconnector_req(connectorReq);

      DataConnector newConnector = new DataConnector(dcName, "mysql", "jdbc:mysql://localhost:3308/hive");
      AlterDataConnectorRequest alterReq = new AlterDataConnectorRequest(dcName, newConnector);
      hmsHandler.alter_dataconnector_req(alterReq);
    } catch (Exception e) {
      fail("testT_AlterDataConnector_AuthorizedUser() failed with " + e);
    }
  }

  @Test
  public void testU_DropDataConnector_authorizedUser() {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      DropDataConnectorRequest dropDcReq = new DropDataConnectorRequest(dcName);
      dropDcReq.setIfNotExists(true);
      dropDcReq.setCheckReferences(true);
      hmsHandler.drop_dataconnector_req(dropDcReq);
    } catch (Exception e) {
      fail("testU_DropDataConnector_authorizedUser() failed with " + e);
    }
  }

  @Test
  public void testUnAuthorizedCause() {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(unAuthorizedUser));
    try {
      Database db = new DatabaseBuilder()
              .setName(dbName)
              .build(conf);
      hmsHandler.create_database(db);
    } catch (Exception e) {
      String[] rootCauseStackTrace = ExceptionUtils.getRootCauseStackTrace(e);
      assertTrue(Arrays.stream(rootCauseStackTrace)
              .anyMatch(stack -> stack.contains(DummyHiveAuthorizer.class.getName())));
    }
  }
}
