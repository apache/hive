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

package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

/**
 * Tests the {@link HivePrivilegeObject} inputs passed to {@link HiveAuthorizer#checkPrivileges}
 * for view queries over partitioned base tables.
 */
public class TestViewPartitionPrivilegeObjects {

  protected static HiveConf conf;
  protected static Driver driver;
  static HiveAuthorizer mockedAuthorizer;

  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      TestViewPartitionPrivilegeObjects.mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      return TestViewPartitionPrivilegeObjects.mockedAuthorizer;
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hive"));
    conf = new HiveConfForTest(TestViewPartitionPrivilegeObjects.class);
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.setVar(ConfVars.HIVE_TXN_MANAGER, DbTxnManager.class.getName());
    conf.setVar(ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    conf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    conf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "mr");

    TestTxnDbUtil.prepDb(conf);
    SessionState.start(conf);
    driver = new Driver(conf);

    runCmd("CREATE DATABASE IF NOT EXISTS datadb");
    runCmd("CREATE TABLE IF NOT EXISTS datadb.t1 (i INT) PARTITIONED BY (dept STRING)");
    runCmd("ALTER TABLE datadb.t1 ADD IF NOT EXISTS PARTITION (dept='a')");
    runCmd("CREATE DATABASE IF NOT EXISTS viewdb");
    runCmd("CREATE VIEW IF NOT EXISTS viewdb.v1 AS SELECT * FROM datadb.t1");
    // Target table used by INSERT OVERWRITE tests — non-partitioned to keep DML simple
    runCmd("CREATE TABLE IF NOT EXISTS datadb.insert_target (i INT, dept STRING)");
  }

  @Before
  public void resetMock() {
    if (mockedAuthorizer != null) {
      reset(mockedAuthorizer);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    runCmd("DROP VIEW IF EXISTS viewdb.v1");
    runCmd("DROP TABLE IF EXISTS datadb.t1");
    runCmd("DROP TABLE IF EXISTS datadb.insert_target");
    runCmd("DROP DATABASE IF EXISTS viewdb");
    runCmd("DROP DATABASE IF EXISTS datadb");
    driver.close();
  }

  @Test
  public void testViewSelectNoPartitionPrivObj() throws Exception {
    conf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    SessionState.get().setConf(conf);

    HiveAuthenticationProvider user1Auth = Mockito.mock(HiveAuthenticationProvider.class);
    Mockito.when(user1Auth.getUserName()).thenReturn("user1");
    SessionState.get().setAuthenticator(user1Auth);

    driver.compile("SELECT * FROM viewdb.v1", true);

    List<HivePrivilegeObject> inputs = getInputPrivObjects();

    Assert.assertTrue("Expected a TABLE_OR_VIEW object for the view",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW
                && "v1".equalsIgnoreCase(h.getObjectName())
                && "viewdb".equalsIgnoreCase(h.getDbname())));

    Assert.assertFalse("HIVE-29628: view query must not send a PARTITION object on the base table",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.PARTITION
                && "t1".equalsIgnoreCase(h.getObjectName())
                && "datadb".equalsIgnoreCase(h.getDbname())));

    Assert.assertFalse("HIVE-29628: view query must not send a base-table TABLE_OR_VIEW object",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW
                && "t1".equalsIgnoreCase(h.getObjectName())
                && "datadb".equalsIgnoreCase(h.getDbname())));
  }

  /**
   * Direct reads on a partitioned table must still emit a PARTITION privilege object
   * so table/partition policies (e.g. Ranger) can be enforced.
   */
  @Test
  public void testDirectTableSelect() throws Exception {
    conf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    SessionState.get().setConf(conf);

    driver.compile("SELECT * FROM datadb.t1", true);

    List<HivePrivilegeObject> inputs = getInputPrivObjects();

    Assert.assertTrue("Expected a PARTITION privilege object for direct table access",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.PARTITION
                && "t1".equalsIgnoreCase(h.getObjectName())
                && "datadb".equalsIgnoreCase(h.getDbname())));
  }

  /**
   * When a view over a partitioned table is the READ SOURCE of an INSERT OVERWRITE statement,
   * {@code checkPrivileges} inputs must contain only {@code TABLE_OR_VIEW viewdb/v1}.
   */
  @Test
  public void testInsertOverwriteFromView() throws Exception {
    conf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    SessionState.get().setConf(conf);

    driver.compile("INSERT OVERWRITE TABLE datadb.insert_target SELECT * FROM viewdb.v1", true);

    List<HivePrivilegeObject> inputs = getInputPrivObjects();

    Assert.assertTrue("Expected TABLE_OR_VIEW privilege object for view source in INSERT",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW
                && "v1".equalsIgnoreCase(h.getObjectName())
                && "viewdb".equalsIgnoreCase(h.getDbname())));

    Assert.assertFalse(
        "INSERT from view must not send a PARTITION privilege object on the base table",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.PARTITION
                && "t1".equalsIgnoreCase(h.getObjectName())
                && "datadb".equalsIgnoreCase(h.getDbname())));
  }

  @SuppressWarnings("unchecked")
  private List<HivePrivilegeObject> getInputPrivObjects()
      throws HiveAuthzPluginException, HiveAccessControlException {
    Class<List<HivePrivilegeObject>> cls = (Class) List.class;
    ArgumentCaptor<List<HivePrivilegeObject>> inputsCapturer = ArgumentCaptor.forClass(cls);
    ArgumentCaptor<List<HivePrivilegeObject>> outputsCapturer = ArgumentCaptor.forClass(cls);

    verify(mockedAuthorizer, atLeastOnce()).checkPrivileges(
        any(HiveOperationType.class),
        inputsCapturer.capture(),
        outputsCapturer.capture(),
        any(HiveAuthzContext.class));

    List<List<HivePrivilegeObject>> all = inputsCapturer.getAllValues();
    return all.get(all.size() - 1);
  }

  private static void runCmd(String cmd) throws Exception {
    driver.run(cmd);
  }
}
