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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

/**
 * Test HiveAuthorizer api invocation.
 */
public class TestHivePrivilegeObjectOwnerNameAndType {
  protected static HiveConf conf;
  protected static Driver driver;
  private static final String TABLE_NAME = TestHivePrivilegeObjectOwnerNameAndType.class.getSimpleName() + "Table";
  static HiveAuthorizer mockedAuthorizer;

  /**
   * This factory creates a mocked HiveAuthorizer class. Use the mocked class to
   * capture the argument passed to it in the test case.
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      TestHivePrivilegeObjectOwnerNameAndType.mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      return TestHivePrivilegeObjectOwnerNameAndType.mockedAuthorizer;
    }

  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hive"));
    conf = new HiveConf();

    // Turn on mocked authorization
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    //conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.setVar(ConfVars.HIVE_TXN_MANAGER, DbTxnManager.class.getName());
    conf.setVar(ConfVars.HIVE_MAPRED_MODE, "nonstrict");
    conf.setVar(ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");

    TestTxnDbUtil.prepDb(conf);
    SessionState.start(conf);
    driver = new Driver(conf);
    runCmd("create table " + TABLE_NAME + " (i int, j int, k string) partitioned by (city string, `date` string) ");
  }

  private static void runCmd(String cmd) throws Exception {
    driver.run(cmd);
  }

  @AfterClass
  public static void afterTests() throws Exception {
    // Drop the tables when we're done.  This makes the test work inside an IDE
    runCmd("drop table if exists " + TABLE_NAME);
    driver.close();
  }

  @Test
  public void testOwnerNames() throws Exception {
    reset(mockedAuthorizer);
    driver.compile("create table default.t1 (name string)", true);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    boolean containsDBOwnerName = false;
    boolean containsTblOwnerName = false;
    for (HivePrivilegeObject hpo : io.getLeft()) {
      if (hpo.getType() == HivePrivilegeObject.HivePrivilegeObjectType.DATABASE && hpo.getOwnerName() != null) {
        containsDBOwnerName = true;
      }
      if (hpo.getType() == HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW && hpo.getOwnerName() != null) {
        containsTblOwnerName = true;
      }
    }
    for (HivePrivilegeObject hpo : io.getRight()) {
      if (hpo.getType() == HivePrivilegeObject.HivePrivilegeObjectType.DATABASE && hpo.getOwnerName() != null) {
        containsDBOwnerName = true;
      }
      if (hpo.getType() == HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW && hpo.getOwnerName() != null) {
        containsTblOwnerName = true;
      }
    }
    if (!containsTblOwnerName || !containsDBOwnerName) {
      String errorMessage = "Ownername is not present in HivePrivilegeObject";
      throw new HiveAuthzPluginException(errorMessage);
    }
  }

  @Test
  public void testOwnerType() throws Exception {
    reset(mockedAuthorizer);
    driver.compile("create table default.t1 (name string)", true);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    boolean containsOwnerType = false;
    for (HivePrivilegeObject hpo : io.getLeft()) {
      if (hpo.getOwnerType() != null) {
        containsOwnerType = true;
      }
    }
    for (HivePrivilegeObject hpo : io.getRight()) {
      if (hpo.getOwnerType() != null) {
        containsOwnerType = true;
      }
    }
    Assert.assertTrue(containsOwnerType);
  }

  @Test
  public void testActionTypeForPartitionedTable() throws Exception {
    runCmd("CREATE EXTERNAL TABLE Part (eid int, name int) PARTITIONED BY (position int, dept int, sal int)");
    reset(mockedAuthorizer);
    runCmd("insert overwrite table part partition(position=2,DEPT,SAL) select 2,2,2,2");
    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> hpoList = io.getValue();
    Assert.assertFalse(hpoList.isEmpty());
    for (HivePrivilegeObject hpo : hpoList) {
      Assert.assertEquals(hpo.getActionType(), HivePrivilegeObject.HivePrivObjectActionType.INSERT_OVERWRITE);
    }
  }

  /**
   * Test to check, if only single instance of Hive Privilege object is created,
   * during bulk insert into a partitioned table.
   */
  @Test
  public void testSingleInstanceOfHPOForPartitionedTable() throws Exception {
    reset(mockedAuthorizer);
    runCmd("insert overwrite table part partition(position=2,DEPT,SAL)" +
            " select 2,2,2,2" +
            " union all" +
            " select 1,2,3,4" +
            " union all" +
            " select 3,4,5,6");
    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> hpoList = io.getValue();
    Assert.assertEquals(1, hpoList.size());
  }

  /**
   * @return pair with left value as inputs and right value as outputs,
   *  passed in current call to authorizer.checkPrivileges
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  private Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> getHivePrivilegeObjectInputs()
      throws HiveAuthzPluginException, HiveAccessControlException {
    // Create argument capturer
    // a class variable cast to this generic of generic class
    Class<List<HivePrivilegeObject>> classListPrivObjects = (Class) List.class;
    ArgumentCaptor<List<HivePrivilegeObject>> inputsCapturer = ArgumentCaptor.forClass(classListPrivObjects);
    ArgumentCaptor<List<HivePrivilegeObject>> outputsCapturer = ArgumentCaptor.forClass(classListPrivObjects);

    verify(mockedAuthorizer)
        .checkPrivileges(any(HiveOperationType.class), inputsCapturer.capture(), outputsCapturer.capture(),
            any(HiveAuthzContext.class));

    return new ImmutablePair<List<HivePrivilegeObject>, List<HivePrivilegeObject>>(
        inputsCapturer.getValue(), outputsCapturer.getValue());
  }

}
