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

package org.apache.hive.service.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hive.testutils.MiniZooKeeperCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationValidator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyChangeListener;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLsImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.DummyHiveAuthorizationValidator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAccessControllerWrapper;
import org.apache.hive.beeline.BeeLine;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test restricted information schema with privilege synchronization
 */
public abstract class InformationSchemaWithPrivilegeTestBase {

  // Group mapping:
  // group_a: user1, user2
  // group_b: user2
  static class FakeGroupAuthenticator extends HadoopDefaultAuthenticator {
    @Override
    public List<String> getGroupNames() {
      List<String> groups = new ArrayList<String>();
      if (getUserName().equals("user1")) {
        groups.add("group_a");
      } else if (getUserName().equals("user2")) {
        groups.add("group_a");
        groups.add("group_b");
      }
      return groups;
    }
  }

  // Privilege matrix:
  //                    user1  user2  group_a  group_b  public
  // testdb1:            S             S
  //   testtable1.*:     SU     S
  //   testtable2.*:                   S
  //   testtable3.*:                                     S
  //   testtable4.*:                            S
  // testdb2:            S
  //   testtable1.key    S
  static class TestHivePolicyProvider implements HivePolicyProvider {
    @Override
    public HiveResourceACLs getResourceACLs(HivePrivilegeObject hiveObject) {
      HiveResourceACLsImpl acls = new HiveResourceACLsImpl();
      if (hiveObject.getType() == HivePrivilegeObjectType.DATABASE) {
        if (hiveObject.getDbname().equals("testdb1")) {
          acls.addUserEntry("user1", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
          acls.addGroupEntry("group_a", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb2")) {
          acls.addUserEntry("user1", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        }
      } else if (hiveObject.getType() == HivePrivilegeObjectType.TABLE_OR_VIEW) {
        if (hiveObject.getDbname().equals("testdb1") && hiveObject.getObjectName().equals("testtable1")) {
          acls.addUserEntry("user1", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
          acls.addUserEntry("user1", HiveResourceACLs.Privilege.UPDATE, HiveResourceACLs.AccessResult.ALLOWED);
          acls.addUserEntry("user2", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb1") && hiveObject.getObjectName().equals("testtable2")) {
          acls.addGroupEntry("group_a", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb1") && hiveObject.getObjectName().equals("testtable3")) {
          acls.addGroupEntry("public", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb1") && hiveObject.getObjectName().equals("testtable4")) {
          acls.addGroupEntry("group_b", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb2") && hiveObject.getObjectName().equals("testtable1")) {
          acls.addUserEntry("user1", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        }
      } else if (hiveObject.getType() == HivePrivilegeObjectType.COLUMN) {
        if (hiveObject.getDbname().equals("testdb1") && hiveObject.getObjectName().equals("testtable1")) {
          acls.addUserEntry("user1", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
          acls.addUserEntry("user2", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb1") && hiveObject.getObjectName().equals("testtable2")) {
          acls.addGroupEntry("group_a", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb1") && hiveObject.getObjectName().equals("testtable3")) {
          acls.addGroupEntry("public", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb1") && hiveObject.getObjectName().equals("testtable4")) {
          acls.addGroupEntry("group_b", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        } else if (hiveObject.getDbname().equals("testdb2") && hiveObject.getObjectName().equals("testtable1")
            && hiveObject.getColumns().get(0).equals("key")) {
          acls.addUserEntry("user1", HiveResourceACLs.Privilege.SELECT, HiveResourceACLs.AccessResult.ALLOWED);
        }
      }
      return acls;
    }

    @Override
    public void registerHivePolicyChangeListener(HivePolicyChangeListener listener) {
      // PolicyChangeListener will be implemented later
    }
  }

  static class HiveAuthorizerImplWithPolicyProvider extends HiveAuthorizerImpl {

    HiveAuthorizerImplWithPolicyProvider(HiveAccessController accessController,
        HiveAuthorizationValidator authValidator) {
      super(accessController, authValidator);
    }

    @Override
    public HivePolicyProvider getHivePolicyProvider() throws HiveAuthzPluginException {
      return new TestHivePolicyProvider();
    }
  }

  static class HiveAuthorizerImplWithNullPolicyProvider extends HiveAuthorizerImpl {

    HiveAuthorizerImplWithNullPolicyProvider(HiveAccessController accessController,
        HiveAuthorizationValidator authValidator) {
      super(accessController, authValidator);
    }

    @Override
    public HivePolicyProvider getHivePolicyProvider() throws HiveAuthzPluginException {
      return null;
    }
  }

  static class TestHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory, HiveConf conf,
        HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) throws HiveAuthzPluginException {
      SQLStdHiveAccessControllerWrapper privilegeManager = new SQLStdHiveAccessControllerWrapper(metastoreClientFactory,
          conf, authenticator, ctx);
      return new HiveAuthorizerImplWithPolicyProvider(privilegeManager,
          new DummyHiveAuthorizationValidator());
    }
  }

  private static final String LOCALHOST_KEY_STORE_NAME = "keystore.jks";
  private static final String TRUST_STORE_NAME = "truststore.jks";
  private static final String KEY_STORE_TRUST_STORE_PASSWORD = "HiveJdbc";
  private static final String KEY_STORE_TRUST_STORE_TYPE = "JKS";

  private static MiniHS2 miniHS2 = null;
  private static MiniZooKeeperCluster zkCluster = null;
  private static Map<String, String> confOverlay;
  private static String hiveSchemaVer;


  public static void setupInternal(boolean zookeeperSSLEnabled) throws Exception {
    File zkDataDir = new File(System.getProperty("test.tmp.dir"));
    zkCluster = new MiniZooKeeperCluster(zookeeperSSLEnabled);
    int zkPort = zkCluster.startup(zkDataDir);

    miniHS2 = new MiniHS2(new HiveConfForTest(InformationSchemaWithPrivilegeTestBase.class));
    confOverlay = new HashMap<String, String>();
    Path workDir = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"));
    confOverlay.put("mapred.local.dir", workDir + File.separator + "TestInformationSchemaWithPrivilege"
        + File.separator + "mapred" + File.separator + "local");
    confOverlay.put("mapred.system.dir", workDir + File.separator + "TestInformationSchemaWithPrivilege"
        + File.separator + "mapred" + File.separator + "system");
    confOverlay.put("mapreduce.jobtracker.staging.root.dir", workDir + File.separator + "TestInformationSchemaWithPrivilege"
        + File.separator + "mapred" + File.separator + "staging");
    confOverlay.put("mapred.temp.dir", workDir + File.separator + "TestInformationSchemaWithPrivilege"
        + File.separator + "mapred" + File.separator + "temp");
    confOverlay.put(ConfVars.HIVE_PRIVILEGE_SYNCHRONIZER_INTERVAL.varname, "1");
    confOverlay.put(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY.varname, "true");
    confOverlay.put(ConfVars.HIVE_AUTHORIZATION_MANAGER.varname, TestHiveAuthorizerFactory.class.getName());
    confOverlay.put(ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, "localhost");
    confOverlay.put(ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.varname, Integer.toString(zkPort));
    confOverlay.put(MetastoreConf.ConfVars.AUTO_CREATE_ALL.getVarname(), "true");
    confOverlay.put(ConfVars.HIVE_AUTHENTICATOR_MANAGER.varname, FakeGroupAuthenticator.class.getName());
    confOverlay.put(ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, "true");
    confOverlay.put(ConfVars.HIVE_PRIVILEGE_SYNCHRONIZER.varname, "true");
    confOverlay.put(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST.varname, ".*");

    if(zookeeperSSLEnabled) {
      String dataFileDir = !System.getProperty("test.data.files", "").isEmpty() ?
          System.getProperty("test.data.files") :
          (new HiveConfForTest(InformationSchemaWithPrivilegeTestBase.class)).get("test.data.files").replace('\\', '/').replace("c:", "");
      confOverlay.put(ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_LOCATION.varname,
          dataFileDir + File.separator + LOCALHOST_KEY_STORE_NAME);
      confOverlay.put(ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_PASSWORD.varname,
          KEY_STORE_TRUST_STORE_PASSWORD);
      confOverlay.put(ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_TYPE.varname,
          KEY_STORE_TRUST_STORE_TYPE);
      confOverlay.put(ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION.varname,
          dataFileDir + File.separator + TRUST_STORE_NAME);
      confOverlay.put(ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD.varname,
          KEY_STORE_TRUST_STORE_PASSWORD);
      confOverlay.put(ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_TYPE.varname,
          KEY_STORE_TRUST_STORE_TYPE);
      confOverlay.put(ConfVars.HIVE_ZOOKEEPER_SSL_ENABLE.varname, "true");
    }
    miniHS2.start(confOverlay);

    hiveSchemaVer = MetaStoreSchemaInfoFactory.get(miniHS2.getServerConf()).getHiveSchemaVersion();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (miniHS2 != null) {
      miniHS2.stop();
    }
    if (zkCluster != null) {
      zkCluster.shutdown();
    }
  }

  @Test
  public void test() throws Exception {

    String db1Name = "testdb1";
    String db2Name = "testdb2";
    String table1Name = "testtable1";
    String table2Name = "testtable2";
    String table3Name = "testtable3";
    String table4Name = "testtable4";
    CLIServiceClient serviceClient = miniHS2.getServiceClient();
    SessionHandle sessHandle = serviceClient.openSession("hive_test_user", "");
    serviceClient.executeStatement(sessHandle, "DROP DATABASE IF EXISTS " + db1Name + " CASCADE", confOverlay);
    serviceClient.executeStatement(sessHandle, "CREATE DATABASE " + db1Name, confOverlay);
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS " + db1Name + "." + table1Name, confOverlay);
    serviceClient.executeStatement(sessHandle,
        "CREATE TABLE " + db1Name + "." + table1Name + "(key string, value double)", confOverlay);
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS " + db1Name + "." + table2Name, confOverlay);
    serviceClient.executeStatement(sessHandle,
        "CREATE TABLE " + db1Name + "." + table2Name + "(key string, value double)", confOverlay);
    serviceClient.executeStatement(sessHandle, "DROP VIEW IF EXISTS " + db1Name + "." + table3Name, confOverlay);
    serviceClient.executeStatement(sessHandle,
        "CREATE VIEW " + db1Name + "." + table3Name + " AS SELECT * FROM " + db1Name + "." + table1Name, confOverlay);
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS " + db1Name + "." + table4Name, confOverlay);
    serviceClient.executeStatement(sessHandle,
        "CREATE TABLE " + db1Name + "." + table4Name + "(key string, value double) PARTITIONED BY (p string)",
        confOverlay);

    serviceClient.executeStatement(sessHandle, "DROP DATABASE IF EXISTS " + db2Name + " CASCADE", confOverlay);
    serviceClient.executeStatement(sessHandle, "CREATE DATABASE " + db2Name, confOverlay);
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS " + db2Name + "." + table1Name, confOverlay);
    serviceClient.executeStatement(sessHandle,
        "CREATE TABLE " + db2Name + "." + table1Name + "(key string, value double)", confOverlay);

    // Just to trigger auto creation of needed metastore tables
    serviceClient.executeStatement(sessHandle, "SHOW GRANT USER hive_test_user ON ALL", confOverlay);
    serviceClient.closeSession(sessHandle);

    List<String> baseArgs = new ArrayList<String>();
    baseArgs.add("-d");
    baseArgs.add(BeeLine.BEELINE_DEFAULT_JDBC_DRIVER);
    baseArgs.add("-u");
    baseArgs.add(miniHS2.getBaseJdbcURL());
    baseArgs.add("-n");
    baseArgs.add("hive_test_user");

    List<String> args = new ArrayList<String>(baseArgs);
    args.add("-f");
    args.add("../../standalone-metastore/metastore-server/src/main/sql/hive/hive-schema-" + hiveSchemaVer + ".hive.sql");
    BeeLine beeLine = new BeeLine();
    int result = beeLine.begin(args.toArray(new String[] {}), null);
    beeLine.close();
    Assert.assertEquals(result, 0);

    boolean containsDb1 = false;
    boolean containsDb2 = false;
    boolean containsDb1Table1 = false;
    boolean containsDb1Table2 = false;
    boolean containsDb1Table3 = false;
    boolean containsDb1Table4 = false;
    boolean containsDb2Table1 = false;
    boolean containsDb1Table1SelectPriv = false;
    boolean containsDb1Table1UpdatePriv = false;
    boolean containsDb1Table2SelectPriv = false;
    boolean containsDb1Table3SelectPriv = false;
    boolean containsDb1Table4SelectPriv = false;
    boolean containsDb2Table1SelectPriv = false;
    boolean containsDb1Table1Key = false;
    boolean containsDb1Table1Value = false;
    boolean containsDb1Table2Key = false;
    boolean containsDb1Table2Value = false;
    boolean containsDb1Table3Key = false;
    boolean containsDb1Table3Value = false;
    boolean containsDb1Table4Key = false;
    boolean containsDb1Table4Value = false;
    boolean containsDb1Table4P = false;
    boolean containsDb2Table1Key = false;

    // We shall have enough time to synchronize privileges during loading
    // information schema

    // User1 privileges:
    // testdb1:            S
    //   testtable1.*:     SU
    //   testtable2.*:     S
    //   testtable3.*:     S
    //   testtable4.*:
    // testdb2:            S
    //   testtable1.*:     S
    sessHandle = serviceClient.openSession("user1", "");
    OperationHandle opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.SCHEMATA",
        confOverlay);
    RowSet rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 2);
    Iterator<Object[]> iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[1].equals(db1Name)) {
        containsDb1 = true;
      } else if (cols[1].equals(db2Name)) {
        containsDb2 = true;
      }
    }
    Assert.assertTrue(containsDb1 && containsDb2);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.TABLES", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 4);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[1].equals(db1Name) && cols[2].equals(table1Name)) {
        containsDb1Table1 = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table2Name)) {
        containsDb1Table2 = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table3Name)) {
        containsDb1Table3 = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table4Name)) {
        containsDb1Table4 = true;
      } else if (cols[1].equals(db2Name) && cols[2].equals(table1Name)) {
        containsDb2Table1 = true;
      }
    }
    Assert.assertTrue(
        containsDb1Table1 && containsDb1Table2 && containsDb1Table3 && !containsDb1Table4 && containsDb2Table1);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.VIEWS", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 1);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[1].equals(db1Name) && cols[2].equals(table3Name)) {
        containsDb1Table3 = true;
      } else {
        containsDb1Table3 = false;
      }
    }
    Assert.assertTrue(containsDb1Table3);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.TABLE_PRIVILEGES",
        confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 5);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[3].equals(db1Name) && cols[4].equals(table1Name) && cols[5].equals("SELECT")) {
        containsDb1Table1SelectPriv = true;
      }
      if (cols[3].equals(db1Name) && cols[4].equals(table1Name) && cols[5].equals("UPDATE")) {
        containsDb1Table1UpdatePriv = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table2Name) && cols[5].equals("SELECT")) {
        containsDb1Table2SelectPriv = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table3Name) && cols[5].equals("SELECT")) {
        containsDb1Table3SelectPriv = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table4Name) && cols[5].equals("SELECT")) {
        containsDb1Table4SelectPriv = true;
      } else if (cols[3].equals(db2Name) && cols[4].equals(table1Name) && cols[5].equals("SELECT")) {
        containsDb2Table1SelectPriv = true;
      }
    }
    Assert.assertTrue(containsDb1Table1SelectPriv && containsDb1Table1UpdatePriv && containsDb1Table2SelectPriv
        && containsDb1Table3SelectPriv && !containsDb1Table4SelectPriv && containsDb2Table1SelectPriv);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.COLUMNS", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 7);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[1].equals(db1Name) && cols[2].equals(table1Name) && cols[3].equals("key")) {
        containsDb1Table1Key = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table1Name) && cols[3].equals("value")) {
        containsDb1Table1Value = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table2Name) && cols[3].equals("key")) {
        containsDb1Table2Key = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table2Name) && cols[3].equals("value")) {
        containsDb1Table2Value = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table3Name) && cols[3].equals("key")) {
        containsDb1Table3Key = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table3Name) && cols[3].equals("value")) {
        containsDb1Table3Value = true;
      } else if (cols[1].equals(db2Name) && cols[2].equals(table1Name) && cols[3].equals("key")) {
        containsDb2Table1Key = true;
      }
    }
    Assert.assertTrue(containsDb1Table1Key && containsDb1Table1Value && containsDb1Table2Key && containsDb1Table2Value
        && containsDb1Table3Key && containsDb1Table3Value && containsDb2Table1Key);

    containsDb1Table1Key = false;
    containsDb1Table1Value = false;
    containsDb1Table2Key = false;
    containsDb1Table2Value = false;
    containsDb1Table3Key = false;
    containsDb1Table3Value = false;
    containsDb2Table1Key = false;
    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.COLUMN_PRIVILEGES",
        confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 7);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[3].equals(db1Name) && cols[4].equals(table1Name) && cols[5].equals("key")) {
        containsDb1Table1Key = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table1Name) && cols[5].equals("value")) {
        containsDb1Table1Value = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table2Name) && cols[5].equals("key")) {
        containsDb1Table2Key = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table2Name) && cols[5].equals("value")) {
        containsDb1Table2Value = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table3Name) && cols[5].equals("key")) {
        containsDb1Table3Key = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table3Name) && cols[5].equals("value")) {
        containsDb1Table3Value = true;
      } else if (cols[3].equals(db2Name) && cols[4].equals(table1Name) && cols[5].equals("key")) {
        containsDb2Table1Key = true;
      }
    }
    Assert.assertTrue(containsDb1Table1Key && containsDb1Table1Value && containsDb1Table2Key && containsDb1Table2Value
        && containsDb1Table3Key && containsDb1Table3Value && containsDb2Table1Key);
    serviceClient.closeSession(sessHandle);

    // User2 privileges:
    // testdb1: S
    // testtable1.*: S
    // testtable2.*: S
    // testtable3.*: S
    // testtable4.*: S
    // testdb2:
    // testtable1.*:
    sessHandle = serviceClient.openSession("user2", "");
    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.SCHEMATA", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 1);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[1].equals(db1Name)) {
        containsDb1 = true;
      }
    }
    Assert.assertTrue(containsDb1);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.TABLES", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 4);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[1].equals(db1Name) && cols[2].equals(table1Name)) {
        containsDb1Table1 = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table2Name)) {
        containsDb1Table2 = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table3Name)) {
        containsDb1Table3 = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table4Name)) {
        containsDb1Table4 = true;
      }
    }
    Assert.assertTrue(containsDb1Table1 && containsDb1Table2 && containsDb1Table3 && containsDb1Table4);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.VIEWS", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 1);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[1].equals(db1Name) && cols[2].equals(table3Name)) {
        containsDb1Table3 = true;
      } else {
        containsDb1Table3 = false;
      }
    }
    Assert.assertTrue(containsDb1Table3);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.TABLE_PRIVILEGES",
        confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 4);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[3].equals(db1Name) && cols[4].equals(table1Name) && cols[5].equals("SELECT")) {
        containsDb1Table1SelectPriv = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table2Name) && cols[5].equals("SELECT")) {
        containsDb1Table2SelectPriv = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table3Name) && cols[5].equals("SELECT")) {
        containsDb1Table3SelectPriv = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table4Name) && cols[5].equals("SELECT")) {
        containsDb1Table4SelectPriv = true;
      }
    }
    Assert.assertTrue(containsDb1Table1SelectPriv && containsDb1Table2SelectPriv && containsDb1Table3SelectPriv
        && containsDb1Table4SelectPriv);

    // db1.testtable3.p should also be in COLUMNS, will fix in separate ticket
    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.COLUMNS", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 8);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[1].equals(db1Name) && cols[2].equals(table1Name) && cols[3].equals("key")) {
        containsDb1Table1Key = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table1Name) && cols[3].equals("value")) {
        containsDb1Table1Value = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table2Name) && cols[3].equals("key")) {
        containsDb1Table2Key = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table2Name) && cols[3].equals("value")) {
        containsDb1Table2Value = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table3Name) && cols[3].equals("key")) {
        containsDb1Table3Key = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table3Name) && cols[3].equals("value")) {
        containsDb1Table3Value = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table4Name) && cols[3].equals("key")) {
        containsDb1Table4Key = true;
      } else if (cols[1].equals(db1Name) && cols[2].equals(table4Name) && cols[3].equals("value")) {
        containsDb1Table4Value = true;
      }
    }
    Assert.assertTrue(containsDb1Table1Key && containsDb1Table1Value && containsDb1Table2Key && containsDb1Table2Value
        && containsDb1Table3Key && containsDb1Table3Value && containsDb1Table4Key && containsDb1Table4Value);

    containsDb1Table1Key = false;
    containsDb1Table1Value = false;
    containsDb1Table2Key = false;
    containsDb1Table2Value = false;
    containsDb1Table3Key = false;
    containsDb1Table3Value = false;
    containsDb1Table4Key = false;
    containsDb1Table4Value = false;
    containsDb1Table4P = false;
    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.COLUMN_PRIVILEGES",
        confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 9);
    iter = rowSet.iterator();
    while (iter.hasNext()) {
      Object[] cols = iter.next();
      if (cols[3].equals(db1Name) && cols[4].equals(table1Name) && cols[5].equals("key")) {
        containsDb1Table1Key = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table1Name) && cols[5].equals("value")) {
        containsDb1Table1Value = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table2Name) && cols[5].equals("key")) {
        containsDb1Table2Key = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table2Name) && cols[5].equals("value")) {
        containsDb1Table2Value = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table3Name) && cols[5].equals("key")) {
        containsDb1Table3Key = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table3Name) && cols[5].equals("value")) {
        containsDb1Table3Value = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table4Name) && cols[5].equals("key")) {
        containsDb1Table4Key = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table4Name) && cols[5].equals("value")) {
        containsDb1Table4Value = true;
      } else if (cols[3].equals(db1Name) && cols[4].equals(table4Name) && cols[5].equals("p")) {
        containsDb1Table4P = true;
      }
    }
    Assert.assertTrue(containsDb1Table1Key && containsDb1Table1Value && containsDb1Table2Key && containsDb1Table2Value
        && containsDb1Table3Key && containsDb1Table3Value && containsDb1Table4Key && containsDb1Table4Value
        && containsDb1Table4P);
    serviceClient.closeSession(sessHandle);

    // Revert hive.server2.restrict_information_schema to false
    miniHS2.getHiveConf().set(ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, "false");

    sessHandle = serviceClient.openSession("user1", "");

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.SCHEMATA", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 5);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.TABLES", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertTrue(rowSet.numRows() > 50);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.TABLE_PRIVILEGES",
        confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertTrue(rowSet.numRows() > 200);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.COLUMNS", confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertTrue(rowSet.numRows() > 350);

    opHandle = serviceClient.executeStatement(sessHandle, "select * from INFORMATION_SCHEMA.COLUMN_PRIVILEGES",
        confOverlay);
    rowSet = serviceClient.fetchResults(opHandle);
    Assert.assertEquals(rowSet.numRows(), 12);
  }
}
