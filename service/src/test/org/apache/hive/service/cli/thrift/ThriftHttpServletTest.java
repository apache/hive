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
package org.apache.hive.service.cli.thrift;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.hive.service.auth.ldap.HttpEmptyAuthenticationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;

/**
 * ThriftHttpServletTest.
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftHttpServletTest {
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  private ThriftHttpServlet thriftHttpServlet;

  @Before
  public void setUp() throws Exception {
    HiveConf hiveConf = new HiveConf();
    String authType = HiveAuthConstants.AuthTypes.KERBEROS.toString();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, authType);
    thriftHttpServlet = new ThriftHttpServlet(null, null, null, null, null, hiveConf);
  }

  @Test
  public void testMissingAuthorizationHeader() throws Exception {
    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);
    Mockito.when(httpServletRequest.getHeader(HttpAuthUtils.AUTHORIZATION)).thenReturn(null);

    exceptionRule.expect(HttpEmptyAuthenticationException.class);
    exceptionRule.expectMessage("Authorization header received " +
            "from the client is empty.");
    thriftHttpServlet.doKerberosAuth(httpServletRequest);
  }

  @Test
  public void testEmptyAuthorizationHeader() throws Exception {
    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);
    Mockito.when(httpServletRequest.getHeader(HttpAuthUtils.AUTHORIZATION)).thenReturn("");

    exceptionRule.expect(HttpEmptyAuthenticationException.class);
    exceptionRule.expectMessage("Authorization header received " +
        "from the client is empty.");
    thriftHttpServlet.doKerberosAuth(httpServletRequest);
  }
}
