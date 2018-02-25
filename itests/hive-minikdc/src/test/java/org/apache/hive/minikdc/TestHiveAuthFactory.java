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
package org.apache.hive.minikdc;

import org.junit.Assert;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestHiveAuthFactory {
  private static HiveConf hiveConf;
  private static MiniHiveKdc miniHiveKdc = null;

  @BeforeClass
  public static void setUp() throws Exception {
    hiveConf = new HiveConf();
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc(hiveConf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  /**
   * Verify that delegation token manager is started with no exception for MemoryTokenStore
   * @throws Exception
   */
  @Test
  public void testStartTokenManagerForMemoryTokenStore() throws Exception {
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthConstants.AuthTypes.KERBEROS.getAuthName());
    String principalName = miniHiveKdc.getFullHiveServicePrincipal();
    System.out.println("Principal: " + principalName);

    hiveConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL, principalName);
    String keyTabFile = miniHiveKdc.getKeyTabFile(miniHiveKdc.getHiveServicePrincipal());
    System.out.println("keyTabFile: " + keyTabFile);
    Assert.assertNotNull(keyTabFile);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB, keyTabFile);

    HiveAuthFactory authFactory = new HiveAuthFactory(hiveConf);
    Assert.assertNotNull(authFactory);
    Assert.assertEquals("org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge$Server$TUGIAssumingTransportFactory",
        authFactory.getAuthTransFactory().getClass().getName());
  }

  /**
   * Verify that delegation token manager is started with no exception for DBTokenStore
   * @throws Exception
   */
  @Test
  public void testStartTokenManagerForDBTokenStore() throws Exception {
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthConstants.AuthTypes.KERBEROS.getAuthName());
    String principalName = miniHiveKdc.getFullHiveServicePrincipal();
    System.out.println("Principal: " + principalName);

    hiveConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL, principalName);
    String keyTabFile = miniHiveKdc.getKeyTabFile(miniHiveKdc.getHiveServicePrincipal());
    System.out.println("keyTabFile: " + keyTabFile);
    Assert.assertNotNull(keyTabFile);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB, keyTabFile);

    hiveConf.setVar(ConfVars.METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS, "org.apache.hadoop.hive.metastore.security.DBTokenStore");

    HiveAuthFactory authFactory = new HiveAuthFactory(hiveConf);
    Assert.assertNotNull(authFactory);
    Assert.assertEquals("org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge$Server$TUGIAssumingTransportFactory",
        authFactory.getAuthTransFactory().getClass().getName());
  }
}
