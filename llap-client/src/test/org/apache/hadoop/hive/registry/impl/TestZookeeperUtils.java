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

package org.apache.hadoop.hive.registry.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * ZookeeperUtils test suite.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestZookeeperUtils {

  private Configuration conf;
  private UserGroupInformation ugi;

  MockedStatic<UserGroupInformation> userGroupInformationMockedStatic;

  @Before
  public void setup() {
    conf = new Configuration();
    userGroupInformationMockedStatic = Mockito.mockStatic(UserGroupInformation.class);
    userGroupInformationMockedStatic.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
    ugi = Mockito.mock(UserGroupInformation.class);
    UserGroupInformation.setLoginUser(ugi);
  }

  @After
  public void teardown() {
    userGroupInformationMockedStatic.close();
  }

  /**
   * Secure scenario, invoked e.g. from within HS2 or LLAP daemon process, kinit'ed inside proc.
   */
  @Test
  public void testHadoopAuthKerberosFromKeytabAndZookeeperUseKerberos() {
    Assert.assertTrue(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS));
    Assert.assertTrue(ZookeeperUtils.isKerberosEnabled(conf));
  }

  /**
   * Secure scenario, invoked e.g. from within HS2 or LLAP status process, kinit'ed in parent proc.
   */
  @Test
  public void testHadoopAuthKerberosFromTicketAndZookeeperUseKerberos() {
    Assert.assertTrue(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS));
    Assert.assertTrue(ZookeeperUtils.isKerberosEnabled(conf));
  }

  /**
   * Secure scenario, invoked e.g. from within Tez AM process.
   */
  @Test
  public void testHadoopAuthKerberosNoLoginAndZookeeperUseKerberos() {
    Assert.assertTrue(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS));
    Assert.assertTrue(ZookeeperUtils.isKerberosEnabled(conf));
  }

  /**
   * Unsecure scenario.
   */
  @Test
  public void testHadoopAuthSimpleAndZookeeperUseKerberos() {
    userGroupInformationMockedStatic.when(UserGroupInformation::isSecurityEnabled).thenReturn(false);
    Assert.assertTrue(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS));
    Assert.assertFalse(ZookeeperUtils.isKerberosEnabled(conf));
  }

  /**
   * Secure scenario with hive.zookeeper.kerberos.enabled=false.
   */
  @Test
  public void testHadoopAuthKerberosAndZookeeperNoKerberos(){
    conf.setBoolean(HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS.varname, false);
    Assert.assertFalse(ZookeeperUtils.isKerberosEnabled(conf));
  }

}
