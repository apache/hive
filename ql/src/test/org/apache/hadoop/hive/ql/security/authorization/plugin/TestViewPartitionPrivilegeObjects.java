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
 * for view queries over partitioned base tables (HIVE-29628).
 */
public class TestViewPartitionPrivilegeObjects {

  static final String DATA_DB = "datadb";
  static final String VIEW_DB = "viewdb";
  static final String BASE_TABLE = "t1";
  static final String VIEW_NAME = "v1";

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
    conf.setVar(ConfVars.HIVE_MAPRED_MODE, "nonstrict");
    conf.setVar(ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    conf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    conf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "mr");

    TestTxnDbUtil.prepDb(conf);
    SessionState.start(conf);
    driver = new Driver(conf);

    runCmd("CREATE DATABASE IF NOT EXISTS " + DATA_DB);
    runCmd("CREATE TABLE IF NOT EXISTS " + DATA_DB + "." + BASE_TABLE
        + " (i INT) PARTITIONED BY (dept STRING)");
    runCmd("ALTER TABLE " + DATA_DB + "." + BASE_TABLE + " ADD IF NOT EXISTS PARTITION (dept='a')");
    runCmd("CREATE DATABASE IF NOT EXISTS " + VIEW_DB);
    runCmd("CREATE VIEW IF NOT EXISTS " + VIEW_DB + "." + VIEW_NAME
        + " AS SELECT * FROM " + DATA_DB + "." + BASE_TABLE);
  }

  @Before
  public void resetMock() {
    if (mockedAuthorizer != null) {
      reset(mockedAuthorizer);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    runCmd("DROP VIEW IF EXISTS " + VIEW_DB + "." + VIEW_NAME);
    runCmd("DROP TABLE IF EXISTS " + DATA_DB + "." + BASE_TABLE);
    runCmd("DROP DATABASE IF EXISTS " + VIEW_DB);
    runCmd("DROP DATABASE IF EXISTS " + DATA_DB);
    driver.close();
  }

  /**
   * Mirrors {@code authorization_view_without_base_select_priv.q} with
   * {@code hive.fetch.task.conversion=none}: a view-only user must not produce a
   * PARTITION privilege object on the underlying base table.
   */
  @Test
  public void testViewSelectNoBaseTablePartitionPrivObj() throws Exception {
    conf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    SessionState.get().setConf(conf);

    HiveAuthenticationProvider user1Auth = Mockito.mock(HiveAuthenticationProvider.class);
    Mockito.when(user1Auth.getUserName()).thenReturn("user1");
    SessionState.get().setAuthenticator(user1Auth);

    driver.compile("SELECT * FROM " + VIEW_DB + "." + VIEW_NAME, true);

    List<HivePrivilegeObject> inputs = getInputPrivObjects();

    Assert.assertTrue("Expected a TABLE_OR_VIEW object for the view",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW
                && VIEW_NAME.equalsIgnoreCase(h.getObjectName())
                && VIEW_DB.equalsIgnoreCase(h.getDbname())));

    Assert.assertFalse("View query must not send a PARTITION object on the base table",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.PARTITION
                && BASE_TABLE.equalsIgnoreCase(h.getObjectName())
                && DATA_DB.equalsIgnoreCase(h.getDbname())));

    Assert.assertFalse("View query must not send a base-table TABLE_OR_VIEW object",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW
                && BASE_TABLE.equalsIgnoreCase(h.getObjectName())
                && DATA_DB.equalsIgnoreCase(h.getDbname())));
  }

  /**
   * Direct reads on a partitioned table must still emit a PARTITION privilege object
   * so table/partition policies (e.g. Ranger) can be enforced.
   */
  @Test
  public void testDirectTableSelectHasPartitionPrivObj() throws Exception {
    conf.setVar(ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    SessionState.get().setConf(conf);

    driver.compile("SELECT * FROM " + DATA_DB + "." + BASE_TABLE, true);

    List<HivePrivilegeObject> inputs = getInputPrivObjects();

    Assert.assertTrue("Expected a PARTITION privilege object for direct table access",
        inputs.stream().anyMatch(h ->
            h.getType() == HivePrivilegeObject.HivePrivilegeObjectType.PARTITION
                && BASE_TABLE.equalsIgnoreCase(h.getObjectName())
                && DATA_DB.equalsIgnoreCase(h.getDbname())));
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
