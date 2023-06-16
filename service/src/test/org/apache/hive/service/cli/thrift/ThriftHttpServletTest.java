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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;

/**
 * ThriftHttpServletTest.
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftHttpServletTest {
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  private ThriftHttpServlet thriftHttpServlet;

  private HiveConf hiveConf = new HiveConf();

  @Before
  public void setUp() throws Exception {
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

  @Test public void testApproveOnFilters() throws Exception {
    // No Filtering and no headers in request
    Assert.assertTrue(thriftHttpServlet.approveOnFilter(Mockito.mock(HttpServletRequest.class),
        Mockito.mock(HttpServletResponse.class)));

    testApproveOnFiltersBase(HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname,
        ThriftHttpServlet.X_XSRF_HEADER, true);
    testApproveOnFiltersBase(HiveConf.ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname, ThriftHttpServlet.X_CSRF_TOKEN,
        true);

    // Filter flag and header did not match
    testApproveOnFiltersBase(HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname, ThriftHttpServlet.X_CSRF_TOKEN,
        false);
    testApproveOnFiltersBase(HiveConf.ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname,
        ThriftHttpServlet.X_XSRF_HEADER, false);
  }

  private void testApproveOnFiltersBase(String filterName, String headerName, boolean assertion) throws Exception {
    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse httpServletResponse = Mockito.mock(HttpServletResponse.class);
    PrintWriter writer = Mockito.mock(PrintWriter.class);
    Mockito.when(httpServletResponse.getWriter()).thenReturn(writer);
    hiveConf = new HiveConf();
    thriftHttpServlet = new ThriftHttpServlet(null, null, null, null, null, hiveConf);

    // Filtering is enabled, but header is not sent
    hiveConf.setBoolean(filterName, true);
    thriftHttpServlet = new ThriftHttpServlet(null, null, null, null, null, hiveConf);
    Assert.assertFalse(thriftHttpServlet.approveOnFilter(httpServletRequest, httpServletResponse));

    // header sent and filtering enabled
    Mockito.when(httpServletRequest.getHeader(headerName)).thenReturn("value");
    thriftHttpServlet = new ThriftHttpServlet(null, null, null, null, null, hiveConf);
    Assert.assertEquals(thriftHttpServlet.approveOnFilter(httpServletRequest, httpServletResponse), assertion);

    // header sent but filtering not enabled
    hiveConf.setBoolean(filterName, false);
    if (filterName.equals(HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname)) {
      hiveConf.setBoolean(HiveConf.ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname, true);
    } else {
      hiveConf.setBoolean(HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname, true);
    }
    thriftHttpServlet = new ThriftHttpServlet(null, null, null, null, null, hiveConf);
    Assert.assertEquals(thriftHttpServlet.approveOnFilter(httpServletRequest, httpServletResponse), !assertion);
  }

}
