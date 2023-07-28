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
import org.apache.hadoop.hive.shims.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class ThriftHttpFilter implements Filter {

  public static final Logger LOG = LoggerFactory.getLogger(ThriftHttpFilter.class.getName());
  protected static final String X_CSRF_TOKEN = "X-CSRF-TOKEN";
  protected static final String X_XSRF_HEADER = "X-XSRF-HEADER";

  protected static final String ERROR_MESSAGE = "Request did not have valid XSRF header/CSRF token, rejecting.";

  protected HiveConf hiveConf;

  public ThriftHttpFilter(HiveConf hiveConf) {
    super();
    this.hiveConf = hiveConf;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // As of now no action is needed in this method
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
      throws IOException, ServletException {
    boolean xsrfFlag = this.hiveConf.getBoolean(HiveConf.ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname, false);
    boolean csrfFlag = this.hiveConf.getBoolean(HiveConf.ConfVars.HIVE_SERVER2_CSRF_FILTER_ENABLED.varname, false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Is {} filtering Enabled : {}", X_CSRF_TOKEN, csrfFlag);
      LOG.debug("Is {} filtering Enabled : {}", X_XSRF_HEADER, xsrfFlag);
    }
    if ((!xsrfFlag && !csrfFlag) ||
        (csrfFlag && Utils.doXsrfFilter(request, response, null, X_CSRF_TOKEN)) ||
        (xsrfFlag && Utils.doXsrfFilter(request, response, null, X_XSRF_HEADER))) {
      filterChain.doFilter(request, response);
    } else {
      LOG.warn(ERROR_MESSAGE);
      ((HttpServletResponse)response).sendError(HttpServletResponse.SC_FORBIDDEN, ERROR_MESSAGE);
    }
  }

  @Override
  public void destroy() {
    // As of now no action is needed in this method
  }
}
