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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.cli.SemanticAnalysis;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test HCatAuthUtil
 */
public class TestHCatAuthUtil {

  public static class DummyV2AuthorizerFactory implements HiveAuthorizerFactory {

    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider hiveAuthenticator, HiveAuthzSessionContext ctx)
        throws HiveAuthzPluginException {
      return Mockito.mock(HiveAuthorizer.class);
    }
  }

  /**
   * Test with auth enabled and StorageBasedAuthorizationProvider
   */
  @Test
  public void authEnabledV1Auth() throws Exception {
    HiveConf hcatConf = new HiveConf(this.getClass());
    hcatConf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    hcatConf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, StorageBasedAuthorizationProvider.class.getName());
    SessionState.start(hcatConf);
    assertTrue("hcat auth should be enabled", HCatAuthUtil.isAuthorizationEnabled(hcatConf));
  }

  /**
   * Test with auth enabled and v2 auth
   */
  @Test
  public void authEnabledV2Auth() throws Exception {
    HiveConf hcatConf = new HiveConf(this.getClass());
    hcatConf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    hcatConf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, DummyV2AuthorizerFactory.class.getName());
    SessionState.start(hcatConf);
    assertFalse("hcat auth should be disabled", HCatAuthUtil.isAuthorizationEnabled(hcatConf));
  }

  /**
   * Test with auth disabled
   */
  @Test
  public void authDisabled() throws Exception {
    HiveConf hcatConf = new HiveConf(this.getClass());
    hcatConf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, false);
    SessionState.start(hcatConf);
    assertFalse("hcat auth should be disabled", HCatAuthUtil.isAuthorizationEnabled(hcatConf));
  }
}
