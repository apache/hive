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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestThriftHttpFilter {

  @Parameterized.Parameter
  public String filterName;

  @Parameterized.Parameter(1)
  public boolean isFilterEnabled;

  @Parameterized.Parameter(2)
  public String headerName;

  @Parameterized.Parameter(3)
  public boolean isHeaderSent;

  @Parameterized.Parameter(4)
  public int invokeFilterChaining;
  private HiveConf hiveConf;
  private HttpServletRequest httpServletRequest;
  private HttpServletResponse httpServletResponse;
  private FilterChain filterChain;
  private FilterConfig filterConfig;

  @Parameterized.Parameters(name = "{index}: filterName={0},isFilteringEnabled={1},headerName={2},isHeaderSent={3},invokeFilterChaining={4}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname, true, ThriftHttpFilter.X_XSRF_HEADER, true, 1 },
        { HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname, true, ThriftHttpFilter.X_XSRF_HEADER, false, 0 },
        { HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname, false, ThriftHttpFilter.X_XSRF_HEADER, true, 1 },
        { HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname, false, ThriftHttpFilter.X_XSRF_HEADER, false, 1 },
        { HiveConf.ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname, true, ThriftHttpFilter.X_CSRF_TOKEN, true, 1 },
        { HiveConf.ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname, true, ThriftHttpFilter.X_CSRF_TOKEN, false, 0 },
        { HiveConf.ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname, false, ThriftHttpFilter.X_CSRF_TOKEN, true, 1 },
        { HiveConf.ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname, false, ThriftHttpFilter.X_CSRF_TOKEN, false, 1 }
    });
  }

  @Before
  public void setup() throws IOException {
    hiveConf = new HiveConf();
    httpServletRequest = Mockito.mock(HttpServletRequest.class);
    httpServletResponse = Mockito.mock(HttpServletResponse.class);
    filterChain = Mockito.mock(FilterChain.class);
    filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(httpServletRequest.getRequestURI()).thenReturn("/cliservice");
    PrintWriter printWriter = Mockito.mock(PrintWriter.class);
    Mockito.when(httpServletResponse.getWriter()).thenReturn(printWriter);
  }

  private void executeFilter() throws ServletException, IOException {
    ThriftHttpFilter thriftHttpFilter = new ThriftHttpFilter(hiveConf);
    thriftHttpFilter.init(filterConfig);
    thriftHttpFilter.doFilter(httpServletRequest, httpServletResponse, filterChain);
    thriftHttpFilter.destroy();
  }

  private void initializeFilter() {
    hiveConf.setBoolean(filterName, isFilterEnabled);
    if (isHeaderSent) {
      Mockito.when(httpServletRequest.getHeader(headerName)).thenReturn(headerName);
    } else {
      Mockito.when(httpServletRequest.getHeader(headerName)).thenReturn(null);
    }
  }

  @Test
  public void testFilter() throws ServletException, IOException {
    initializeFilter();
    executeFilter();
    Mockito.verify(filterChain, Mockito.times(invokeFilterChaining)).doFilter(httpServletRequest, httpServletResponse);
  }
}
