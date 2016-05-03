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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc.authorization;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAccessControllerWrapper;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizationValidator;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verify validation of jdbc metadata methods is happening
 */
public class TestJdbcMetadataApiAuth {
  private static MiniHS2 miniHS2 = null;

  /**
   * HiveAuthorizationValidator that allows/disallows actions based on
   * allowActions boolean value
   */
  public static class TestAuthValidator extends SQLStdHiveAuthorizationValidator {

    public static boolean allowActions;
    public static final String DENIED_ERR = "Actions not allowed because of allowActions=false";

    public TestAuthValidator(HiveMetastoreClientFactory metastoreClientFactory, HiveConf conf,
        HiveAuthenticationProvider authenticator,
        SQLStdHiveAccessControllerWrapper privilegeManager, HiveAuthzSessionContext ctx)
        throws HiveAuthzPluginException {
      super(metastoreClientFactory, conf, authenticator, privilegeManager, ctx);
    }

    @Override
    public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
        List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
        throws HiveAuthzPluginException, HiveAccessControlException {
      if (!allowActions) {
        throw new HiveAccessControlException(DENIED_ERR);
      }
    }
  }

  /**
   * Factory that uses TestAuthValidator
   */
  public static class TestAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx)
        throws HiveAuthzPluginException {
      SQLStdHiveAccessControllerWrapper privilegeManager = new SQLStdHiveAccessControllerWrapper(
          metastoreClientFactory, conf, authenticator, ctx);
      return new HiveAuthorizerImpl(privilegeManager, new TestAuthValidator(metastoreClientFactory,
          conf, authenticator, privilegeManager, ctx));
    }
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, TestAuthorizerFactory.class.getName());
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    miniHS2 = new MiniHS2(conf);
    miniHS2.start(new HashMap<String, String>());

    TestAuthValidator.allowActions = true;
    // set up a db and table
    String tableName1 = TestJdbcMetadataApiAuth.class.getSimpleName() + "_tab";
    String dbName1 = TestJdbcMetadataApiAuth.class.getSimpleName() + "_db";
    // create connection as user1
    Connection hs2Conn = getConnection("user1");
    Statement stmt = hs2Conn.createStatement();

    // create table, db
    stmt.execute("create table " + tableName1 + "(i int) ");
    stmt.execute("create database " + dbName1);
    stmt.close();
    hs2Conn.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  /**
   * Call the HS2 metadata api's with authorizer allowing those calls
   * @throws Exception
   */
  @Test
  public void testMetaApiAllowed() throws Exception {
    TestAuthValidator.allowActions = true;

    Connection hs2Conn = getConnection("user1");
    DatabaseMetaData dbmetadata = hs2Conn.getMetaData();
    ResultSet res;

    res = dbmetadata.getCatalogs();
    assertFalse(res.next());

    res = dbmetadata.getSchemas();
    assertTrue(res.next());
    assertTrue(res.next());

    res = dbmetadata.getTypeInfo();
    assertTrue(res.next());

    res = dbmetadata.getTables(null, "default", "t%", null);
    assertTrue(res.next());

    res = dbmetadata.getTableTypes();
    assertTrue(res.next());

    res = dbmetadata.getColumns(null, "default", "nosuchtable", null);
    assertFalse(res.next());

    res = dbmetadata.getFunctions(null, null, "trim");
    assertTrue(res.next());

  }

  /**
   * Call the HS2 metadata api's with authorizer disallowing those calls
   * @throws Exception
   */

  @Test
  public void testMetaApiDisAllowed() throws Exception {
    TestAuthValidator.allowActions = false;

    Connection hs2Conn = getConnection("user1");
    DatabaseMetaData dbmetadata = hs2Conn.getMetaData();

    try {
      dbmetadata.getCatalogs();
      fail("HiveAccessControlException expected");
    } catch (SQLException e) {
      assertErrorContains(e, TestAuthValidator.DENIED_ERR);
    } catch (Exception e) {
      fail("HiveAccessControlException expected");
    }

    try {
      dbmetadata.getSchemas();
      fail("HiveAccessControlException expected");
    } catch (SQLException e) {
      assertErrorContains(e, TestAuthValidator.DENIED_ERR);
    } catch (Exception e) {
      fail("HiveAccessControlException expected");
    }

    try {
      dbmetadata.getTypeInfo();
      fail("HiveAccessControlException expected");
    } catch (SQLException e) {
      assertErrorContains(e, TestAuthValidator.DENIED_ERR);
    } catch (Exception e) {
      fail("HiveAccessControlException expected");
    }

    try {
      dbmetadata.getTables(null, "default", "t%", null);
      fail("HiveAccessControlException expected");
    } catch (SQLException e) {
      assertErrorContains(e, TestAuthValidator.DENIED_ERR);
    } catch (Exception e) {
      fail("HiveAccessControlException expected");
    }

    try {
      dbmetadata.getTableTypes();
      fail("HiveAccessControlException expected");
    } catch (SQLException e) {
      assertErrorContains(e, TestAuthValidator.DENIED_ERR);
    } catch (Exception e) {
      fail("HiveAccessControlException expected");
    }

    try {
      dbmetadata.getColumns(null, "default", "nosuchtable", null);
      fail("HiveAccessControlException expected");
    } catch (SQLException e) {
      assertErrorContains(e, TestAuthValidator.DENIED_ERR);
    } catch (Exception e) {
      fail("HiveAccessControlException expected");
    }

    try {
      dbmetadata.getFunctions(null, null, "trim");
      fail("HiveAccessControlException expected");
    } catch (SQLException e) {
      assertErrorContains(e, TestAuthValidator.DENIED_ERR);
    } catch (Exception e) {
      fail("HiveAccessControlException expected");
    }

  }

  private void assertErrorContains(SQLException e, String deniedErr) {
    if(!e.getMessage().contains(deniedErr)) {
      fail("Exception message [" + e.getMessage() + "] does not contain [" + deniedErr + "]");
    }
  }

  private static Connection getConnection(String userName) throws Exception {
    return DriverManager.getConnection(miniHS2.getJdbcURL(), userName, "bar");
  }

}
