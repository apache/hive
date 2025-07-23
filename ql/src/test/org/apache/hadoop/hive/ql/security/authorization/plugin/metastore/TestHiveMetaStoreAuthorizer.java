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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.client.builder.*;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.filtercontext.TableFilterContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.util.stream.Collectors;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/*
Test whether HiveAuthorizer for MetaStore operation is trigger and HiveMetaStoreAuthzInfo is created by HiveMetaStoreAuthorizer
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHiveMetaStoreAuthorizer {
  private static final String dbName = "test";
  private static final String tblName = "tmptbl";
  private static final String viewName = "tmpview";
  private static final String roleName = "tmpRole";
  private static final String catalogName = "testCatalog";
  private static final String dcName = "testDC";
  private static final String unAuthorizedUser = "bob";
  static final String authorizedUser = "sam";
  private static final String superUser = "hive";
  private static final String default_db = "default";

  private static final String metaConfVal = "";

  private static final String TEST_DATA_DIR = new File("file:///testdata").getPath();
  private static final List<String> PARTCOL_SCHEMA = Lists.newArrayList("yyyy", "mm", "dd");
  private RawStore rawStore;
  private Configuration conf;
  private HMSHandler hmsHandler;
  private Warehouse wh;

  static HiveAuthorizer mockHiveAuthorizer;
  static final List<String> allowedUsers = Arrays.asList("sam", "rob");

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_TXN_STATS_ENABLED, true);
    MetastoreConf.setBoolVar(conf, ConfVars.AGGREGATE_STATS_CACHE_ENABLED, false);
    MetastoreConf.setVar(conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN, metaConfVal);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetastoreConf.setVar(conf, ConfVars.HIVE_AUTHORIZATION_MANAGER, MockHiveAuthorizerFactory.class.getName());
    MetastoreConf.setVar(conf, ConfVars.PRE_EVENT_LISTENERS, HiveMetaStoreAuthorizer.class.getName());
    MetastoreConf.setVar(conf, ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER, HadoopDefaultMetastoreAuthenticator.class.getName());
    MetastoreConf.setVar(conf, ConfVars.FILTER_HOOK, MockMetaStoreOwnerFilterHook.class.getName());
    conf.set("hadoop.proxyuser.hive.groups", "*");
    conf.set("hadoop.proxyuser.hive.hosts", "*");
    conf.set("hadoop.proxyuser.hive.users", "*");

    MetaStoreTestUtils.setConfForStandloneMode(conf);
    wh = Mockito.spy(new Warehouse(conf));

    hmsHandler = new HMSHandler("test", conf);
    hmsHandler.init(wh);
    rawStore = new ObjectStore();
    rawStore.setConf(hmsHandler.getConf());
    // Create the 'hive' catalog with new warehouse directory
    HMSHandler.createDefaultCatalog(rawStore, new Warehouse(conf));
    try {
      DropDataConnectorRequest dropDcReq = new DropDataConnectorRequest(dcName);
      dropDcReq.setIfNotExists(true);
      dropDcReq.setCheckReferences(true);
      hmsHandler.drop_dataconnector_req(dropDcReq);
      hmsHandler.drop_table("default", tblName, true);
      hmsHandler.drop_database(dbName, true, false);
      hmsHandler.drop_catalog(new DropCatalogRequest(catalogName));
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    } catch (Exception e) {
      // NoSuchObjectException will be ignored if the step objects are not there
    }
    mockHiveAuthorizer = Mockito.mock(HiveAuthorizer.class);
    configureMockAuthorizer();
  }

  /**
   * Configures the mock authorizer to check permissions based on username
   */
  private static void configureMockAuthorizer() throws HiveAuthzPluginException, HiveAccessControlException {
    doAnswer(invocation -> {
      HiveOperationType opType = invocation.getArgument(0);
      String user;
      try {
        user = UserGroupInformation.getLoginUser().getShortUserName();
      } catch (Exception e) {
        throw new HiveAuthzPluginException("Unable to get UserGroupInformation");
      }

      if (!allowedUsers.contains(user) && !user.equals(superUser)) {
        throw new HiveAuthzPluginException("Operation type " + opType + " not allowed for user:" + user);
      }
      return null;
    }).when(mockHiveAuthorizer).checkPrivileges(any(HiveOperationType.class), any(), any(), any(HiveAuthzContext.class));
  }

  /**
   * Factory class that provides MockHiveAuthorizer instance
   */
  public static class MockHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
                                               HiveConf conf, HiveAuthenticationProvider hiveAuthenticator,
                                               HiveAuthzSessionContext ctx) {
      return mockHiveAuthorizer;
    }
  }

  /**
   * Captures and returns the privilege objects passed to the authorizer
   */
  private Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> getHivePrivilegeObjectsFromLastCall()
      throws HiveAuthzPluginException, HiveAccessControlException {
    @SuppressWarnings("unchecked")
    Class<List<HivePrivilegeObject>> class_listPrivObjects = (Class) List.class;
    ArgumentCaptor<List<HivePrivilegeObject>> inputsCapturer = ArgumentCaptor
        .forClass(class_listPrivObjects);
    ArgumentCaptor<List<HivePrivilegeObject>> outputsCapturer = ArgumentCaptor
        .forClass(class_listPrivObjects);

    verify(mockHiveAuthorizer).checkPrivileges(any(HiveOperationType.class),
        inputsCapturer.capture(), outputsCapturer.capture(),
        any(HiveAuthzContext.class));

    return new ImmutablePair<>(inputsCapturer.getValue(), outputsCapturer.getValue());
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
      String expected = "Operation type " + HiveOperationType.CREATEDATABASE + " not allowed for user:" + unAuthorizedUser;
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
  public void testC2_AlterView_anyUser() throws Exception {
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
      String expected = "Operation type " + PreEventContext.PreEventType.AUTHORIZATION_API_CALL.name() + " not allowed for user:" + authorizedUser;
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
      String expected = "Operation type " + PreEventContext.PreEventType.CREATE_CATALOG.name() + " not allowed for user:" + authorizedUser;
      assertEquals(expected, err);
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
          .setOwnerName(authorizedUser)
          .setOwnerType(PrincipalType.USER)
          .build(conf);
      hmsHandler.create_database(db);
      // Verify the mock was called with correct ownership info
      Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectsFromLastCall();
      List<HivePrivilegeObject> outputs = io.getRight();

      assertEquals("Should have one output for create database", 1, outputs.size());
      HivePrivilegeObject dbObj = outputs.get(0);
      assertEquals("Output object should be a database",
          HivePrivilegeObject.HivePrivilegeObjectType.DATABASE,
          dbObj.getType());
    } catch (Exception e) {
      fail("Authorized user should be allowed to create database: " + e.getMessage());
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
      Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectsFromLastCall();
      List<HivePrivilegeObject> outputs = io.getRight();

      List<HivePrivilegeObject> tableOutputs = outputs.stream()
          .filter(o -> o.getType() == HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW)
          .collect(Collectors.toList());

      assertEquals("Should have exactly one table output for create table", 1, tableOutputs.size());
      HivePrivilegeObject tableObj = tableOutputs.get(0);

      assertEquals("Output object should be a table",
          HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
          tableObj.getType());
    } catch (Exception e) {
      fail("Authorized user should be allowed to create table: " + e.getMessage());
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
      hmsHandler.alter_table("default", tblName, alteredTable);
    } catch (Exception e) {
      // No Exception for create table for authorized user
    }
  }

  @Test
  public void testK_DropTable_authorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      hmsHandler.drop_table(dbName, tblName, true);
    } catch (Exception e) {
      // No Exception for create table for authorized user
    }
  }

  @Test
  public void testL_DropDatabase_authorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      hmsHandler.drop_database(dbName, true, true);
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
        assert (true);
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
        assert (true);
      }
    }
  }

  @Test
  public void testGetDatabaseObjects_UnauthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(unAuthorizedUser));
    try {
      Database db = new DatabaseBuilder()
          .setName(dbName)
          .build(conf);
      hmsHandler.create_database(db);
      GetDatabaseObjectsRequest request = new GetDatabaseObjectsRequest();
      request.setCatalogName("hive");
      hmsHandler.get_databases_req(request);
    } catch (Exception e) {
      String err = e.getMessage();
      assertTrue("Exception message should contain operation type",
          err.contains("Operation type") && err.contains("not allowed for user:" + unAuthorizedUser));
    } finally {
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(superUser));
      try {
        hmsHandler.drop_database(dbName, true, false);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
  }

  @Test
  public void testGetDatabaseObjects_AuthorizedUser() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      Database db = new DatabaseBuilder()
          .setName(dbName)
          .setOwnerName(authorizedUser)
          .build(conf);
      hmsHandler.create_database(db);
      GetDatabaseObjectsRequest request = new GetDatabaseObjectsRequest();
      request.setCatalogName("hive");
      GetDatabaseObjectsResponse response = hmsHandler.get_databases_req(request);

      assertNotNull("Response should not be null", response);
      assertNotNull("Databases list should not be null", response.getDatabases());
      assertTrue("Should find the created database",
          response.getDatabases().stream().anyMatch(d -> d.getName().equals(dbName)));
    } finally {
      try {
        hmsHandler.drop_database(dbName, true, false);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
  }

  @Test
  public void testTableFilterContextWithOwnership() throws Exception {
    List<TableMeta> tableMetas = new ArrayList<>();
    TableMeta ownerTableMeta = new TableMeta();
    ownerTableMeta.setCatName("hive");
    ownerTableMeta.setDbName(default_db);
    ownerTableMeta.setTableName("owner_table");
    ownerTableMeta.setOwnerName(authorizedUser);
    ownerTableMeta.setOwnerType(org.apache.hadoop.hive.metastore.api.PrincipalType.USER);
    tableMetas.add(ownerTableMeta);

    TableMeta otherTableMeta = new TableMeta();
    otherTableMeta.setCatName("hive");
    otherTableMeta.setDbName(default_db);
    otherTableMeta.setTableName("other_table");
    otherTableMeta.setOwnerName(unAuthorizedUser);
    otherTableMeta.setOwnerType(org.apache.hadoop.hive.metastore.api.PrincipalType.USER);
    tableMetas.add(otherTableMeta);

    TableFilterContext filterContext = TableFilterContext.createFromTableMetas(default_db, tableMetas);
    List<Table> tables = filterContext.getTables();
    assertEquals("Should have two tables in context", 2, tables.size());

    boolean foundOwnerTable = false;
    boolean foundOtherTable = false;

    for (Table table : tables) {
      if (table.getTableName().equals("owner_table")) {
        foundOwnerTable = true;
        assertEquals("owner_table should have authorized user as owner", authorizedUser, table.getOwner());
      } else if (table.getTableName().equals("other_table")) {
        foundOtherTable = true;
        assertEquals("other_table should have unauthorized user as owner", unAuthorizedUser, table.getOwner());
      }
    }

    assertTrue("owner_table not found in tables", foundOwnerTable);
    assertTrue("other_table not found in tables", foundOtherTable);

    HiveMetaStoreAuthzInfo authzInfo = filterContext.getAuthzContext();
    List<HivePrivilegeObject> privObjects = authzInfo.getInputHObjs();

    assertEquals("Should have two privilege objects", 2, privObjects.size());

    foundOwnerTable = false;
    foundOtherTable = false;

    for (HivePrivilegeObject obj : privObjects) {
      if (obj.getObjectName().equals("owner_table")) {
        foundOwnerTable = true;
        assertEquals("owner_table privilege object should have authorized user as owner",
            authorizedUser, obj.getOwnerName());
      } else if (obj.getObjectName().equals("other_table")) {
        foundOtherTable = true;
        assertEquals("other_table privilege object should have unauthorized user as owner",
            unAuthorizedUser, obj.getOwnerName());
      }
    }

    assertTrue("owner_table not found in privilege objects", foundOwnerTable);
    assertTrue("other_table not found in privilege objects", foundOtherTable);
  }

  @Test
  public void testGetDatabaseObjects_WithPattern() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    String testDb1 = "test_db1";
    String testDb2 = "test_db2";
    String otherDb = "other_db";

    try {
      // Create test databases
      Database db1 = new DatabaseBuilder()
          .setName(testDb1)
          .setOwnerName(authorizedUser)
          .build(conf);
      hmsHandler.create_database(db1);

      Database db2 = new DatabaseBuilder()
          .setName(testDb2)
          .setOwnerName(authorizedUser)
          .build(conf);
      hmsHandler.create_database(db2);

      Database db3 = new DatabaseBuilder()
          .setName(otherDb)
          .setOwnerName(authorizedUser)
          .build(conf);
      hmsHandler.create_database(db3);

      // Fetch database objects with pattern
      GetDatabaseObjectsRequest request = new GetDatabaseObjectsRequest();
      request.setCatalogName("hive");
      request.setPattern("test_*");
      GetDatabaseObjectsResponse response = hmsHandler.get_databases_req(request);

      assertNotNull("Response should not be null", response);
      assertNotNull("Databases list should not be null", response.getDatabases());

      List<String> dbNames = response.getDatabases().stream()
          .map(Database::getName)
          .collect(Collectors.toList());

      assertTrue("Should find test_db1", dbNames.contains(testDb1));
      assertTrue("Should find test_db2", dbNames.contains(testDb2));
      assertFalse("Should not find other_db", dbNames.contains(otherDb));
    } finally {
      try {
        hmsHandler.drop_database(testDb1, true, false);
        hmsHandler.drop_database(testDb2, true, false);
        hmsHandler.drop_database(otherDb, true, false);
      } catch (Exception e) {
        // Ignore cleanup errors
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
      String expected = "Operation type " + HiveOperationType.CREATEDATACONNECTOR + " not allowed for user:" + unAuthorizedUser;
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


  /**
   * Captures and returns the privilege objects for Alter Partition
   */
  private Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> getHivePrivilegeObjectsForAlterPartition()
      throws HiveAuthzPluginException, HiveAccessControlException {
    @SuppressWarnings("unchecked")
    Class<List<HivePrivilegeObject>> class_listPrivObjects = (Class) List.class;
    ArgumentCaptor<List<HivePrivilegeObject>> inputsCapturer = ArgumentCaptor
        .forClass(class_listPrivObjects);
    ArgumentCaptor<List<HivePrivilegeObject>> outputsCapturer = ArgumentCaptor
        .forClass(class_listPrivObjects);

    verify(mockHiveAuthorizer).checkPrivileges(eq(HiveOperationType.ALTERPARTITION_FILEFORMAT),
        inputsCapturer.capture(), outputsCapturer.capture(),
        any(HiveAuthzContext.class));

    return new ImmutablePair<>(inputsCapturer.getValue(), outputsCapturer.getValue());
  }
  @Test
  public void testV_AlterPartition_DFSUriPrivObject() {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      List<List<String>> testValues = createTable4PartColsParts();
      List<Partition> oldParts = hmsHandler.get_partitions(dbName, tblName, (short) -1);
      Partition oldPart = oldParts.get(3);
      Partition newPart = makeTestChangesOnPartition(oldPart);

      hmsHandler.rename_partition(dbName, tblName,oldPart.getValues(),newPart);

      Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectsForAlterPartition();
      List<HivePrivilegeObject> outputs = io.getRight();

      List<HivePrivilegeObject> tableOutputs = outputs.stream()
          .filter(o -> o.getType() == HivePrivilegeObject.HivePrivilegeObjectType.DFS_URI)
          .collect(Collectors.toList());

      assertEquals("Should have one DFS_URI privilege object", 1, tableOutputs.size());
      HivePrivilegeObject DFSUriObj = tableOutputs.get(0);

      assertEquals("DFS_URI should be same as new partition location",
          oldPart.getSd().getLocation()+ "/hh=01", DFSUriObj.getObjectName());
    } catch (Exception e) {
      fail("testV_AlterPartition_DFSUriPrivObject() failed with " + e);
    }
  }

  protected Table createPartitionedTestTable(String dbName, String tableName,
      List<String> partCols, boolean setPartitionLevelPrivilages)
      throws Exception {

    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build(conf);
    hmsHandler.create_database(db);

    TableBuilder builder = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string");

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build(conf);

    hmsHandler.create_table(table);
    return table;
  }

  protected List<List<String>> createTable4PartColsParts() throws
      Exception {
    Table table = createPartitionedTestTable(dbName, tblName, PARTCOL_SCHEMA, false);
    List<List<String>> testValues = Lists.newArrayList(
        Lists.newArrayList("1999", "01", "02"),
        Lists.newArrayList("2009", "02", "10"),
        Lists.newArrayList("2017", "10", "26"),
        Lists.newArrayList("2017", "11", "27"));

    for (List<String> vals : testValues) {
      addPartition(table, vals);
    }

    return testValues;
  }

  protected void addPartition(Table table, List<String> values)
      throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().inTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    hmsHandler.add_partition(partitionBuilder.build(conf));
  }

  protected static Partition makeTestChangesOnPartition(Partition partition) {
    Partition newPart = new Partition(partition);
    newPart.getParameters().put("hmsTestParam001", "testValue001");
    newPart.getSd().setLocation(partition.getSd().getLocation() + "/hh=01");
    newPart.setValues(Lists.newArrayList("2018", "11", "27"));
    return newPart;
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
      // Check if the exception chain contains HiveAuthzPluginException
      Throwable current = e;
      boolean foundAuthzException = false;
      while (current != null) {
        if (current instanceof HiveAuthzPluginException) {
          foundAuthzException = true;
          String expectedErrMsg = "Operation type " + HiveOperationType.CREATEDATABASE + " not allowed for user:" + unAuthorizedUser;
          assertTrue("Expected error message mismatch. Actual: '" + current.getMessage() + "'", current.getMessage().contains(expectedErrMsg));
          break;
        }
        current = current.getCause();
      }

      if (!foundAuthzException) {
        String expectedErrMsg = "Operation type " + HiveOperationType.CREATEDATABASE + " not allowed for user:" + unAuthorizedUser;
        assertTrue("Expected HiveAuthzPluginException in exception chain. Message: '" + e.getMessage() + "'",
            e.getMessage().contains(expectedErrMsg));
      }
    }
  }

  @Test
  public void testDropTableNoTablePathWritePermissionShouldFail() throws Exception {
    UserGroupInformation.setLoginUser(
            UserGroupInformation.createRemoteUser(authorizedUser));

    Table table = new TableBuilder()
        .setTableName(tblName)
        .addCol("name", ColumnType.STRING_TYPE_NAME)
        .setOwner(authorizedUser)
        .build(conf);
    hmsHandler.create_table(table);

    Path tablePath = new Path(table.getSd().getLocation());
    when(wh.isWritable(Mockito.eq(tablePath.getParent()))).thenReturn(true);
    when(wh.isWritable(Mockito.eq(tablePath))).thenReturn(false);

    try {
      hmsHandler.drop_table("default", tblName, true);
    } catch (MetaException e) {
      String expected = "%s metadata not deleted since %s is not writable by %s"
          .formatted("Table", tablePath.toString(), authorizedUser);
      assertEquals(expected, e.getMessage());
    }
  }
}
