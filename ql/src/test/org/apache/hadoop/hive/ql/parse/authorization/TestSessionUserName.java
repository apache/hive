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
package org.apache.hadoop.hive.ql.parse.authorization;

import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestSessionUserName {

  @Before
  public void setup() throws Exception {
    //clear the username
    HiveAuthorizerStoringUserNameFactory.username = null;
  }

  /**
   * Test if the authorization factory gets the username provided by
   * the authenticator, if SesstionState is created without username
   * @throws Exception
   */
  @Test
  public void testSessionDefaultUser() throws Exception {
    SessionState ss = new SessionState(getAuthV2HiveConf());
    setupDataNucleusFreeHive(ss.getConf());
    SessionState.start(ss);

    Assert.assertEquals("check username", ss.getAuthenticator().getUserName(),
        HiveAuthorizerStoringUserNameFactory.username);
  }

  /**
   * Test if the authorization factory gets the username set in the SessionState constructor
   * @throws Exception
   */
  @Test
  public void testSessionConstructorUser() throws Exception {
    final String USER_NAME = "authtestuser";
    SessionState ss = new SessionState(getAuthV2HiveConf(), USER_NAME);
    setupDataNucleusFreeHive(ss.getConf());
    SessionState.start(ss);
    ss.getAuthenticator();

    Assert.assertEquals("check username", USER_NAME,
        HiveAuthorizerStoringUserNameFactory.username);
  }

  /**
   * Test that the groupNames are retrieved properly from UGI
   * @throws Exception
   */
  @Test
  public void testSessionGetGroupNames() throws Exception {
    final String testUser = "authtestuser";
    final List<String> testGroups = Arrays.asList("group1", "group2");
    UserGroupInformation.createUserForTesting(testUser, testGroups.toArray(new String[0]));

    SessionState ss = new SessionState(getAuthV2HiveConf(), testUser);
    setupDataNucleusFreeHive(ss.getConf());
    assertEquals("check groups", testGroups, ss.getAuthenticator().getGroupNames());
  }

  /**
   * Test that the groupNames returned is null, when the user name is null. The user name is null
   * in the case of embedded HS2 and we assert that we don't throw an NPE in that case.
   * @throws Exception
   */
  @Test
  public void testSessionNullUser() throws Exception {
    SessionState ss = new SessionState(getAuthV2HiveConf(), null);
    setupDataNucleusFreeHive(ss.getConf());
    SessionState.start(ss);

    assertNull("getGroupNames when userName == null", ss.getAuthenticator().getGroupNames());
  }

  /**
   * Get a mocked Hive object that does not create a real meta store client object
   * This gets rid of the datanucleus initializtion which makes it easier
   * to run test from IDEs
   * @param hiveConf
   * @throws MetaException
   *
   */
  private void setupDataNucleusFreeHive(HiveConf hiveConf) throws MetaException {
    Hive db = Mockito.mock(Hive.class);
    Mockito.when(db.getMSC()).thenReturn(null);
    Mockito.when(db.getConf()).thenReturn(hiveConf);
    Hive.set(db);
  }


  /**
   * @return HiveConf with authorization V2 enabled with a dummy authorization factory
   * that captures the given user name
   */
  private HiveConf getAuthV2HiveConf() {
    HiveConf conf = new HiveConfForTest(getClass());
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        HiveAuthorizerStoringUserNameFactory.class.getName());
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        SessionStateUserAuthenticator.class.getName());
    return conf;
  }

  /**
   * dummy hive authorizer that stores the user name
   */
  static class HiveAuthorizerStoringUserNameFactory implements HiveAuthorizerFactory{
    static String username;

    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      username = authenticator.getUserName();
      HiveAccessController acontroller = Mockito.mock(HiveAccessController.class);
      return new HiveAuthorizerImpl(acontroller, null);
    }

  }
}
