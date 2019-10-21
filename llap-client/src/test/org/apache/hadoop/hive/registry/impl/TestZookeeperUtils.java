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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * ZookeeperUtils test suite.
 */
public class TestZookeeperUtils {

  private Configuration conf;
  private UserGroupInformation ugi;

  @Before
  public void setup() {
    conf = new Configuration();
    ugi = Mockito.mock(UserGroupInformation.class);
    UserGroupInformation.setLoginUser(ugi);
  }

  @Test
  public void testHadoopAuthKerberosAndZookeeperUseKerberos() {
    Mockito.when(ugi.isFromKeytab()).thenReturn(true);
    Assert.assertTrue(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS));
    Assert.assertTrue(ZookeeperUtils.isKerberosEnabled(conf));
  }

  @Test
  public void testHadoopAuthKerberosAndZookeeperNoKerberos(){
    Mockito.when(ugi.isFromKeytab()).thenReturn(true);
    conf.setBoolean(HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS.varname, false);
    Assert.assertFalse(ZookeeperUtils.isKerberosEnabled(conf));
  }

  @Test
  public void testHadoopAuthSimpleAndZookeeperKerberos(){
    Mockito.when(ugi.isFromKeytab()).thenReturn(false);
    conf.setBoolean(HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS.varname, false);
    Assert.assertFalse(ZookeeperUtils.isKerberosEnabled(conf));
  }
}
