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

package org.apache.hive.service.servlet;

import org.apache.hive.service.auth.ldap.LdapAuthService;
import org.apache.hive.service.server.HiveServer2;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This Java filter demonstrates how to intercept the request
 * and transform the response to implement authentication feature.
 * for the website's back-end.
 *
 * @author www.codejava.net
 */
public class LDAPAuthenticationFilter implements Filter {

  private static final String LOGIN_FORM_URI = "loginForm.jsp";
  private static final String LOGIN_SERVLET_URI = "login";
  private final LdapAuthService ldapAuthService;

  public LDAPAuthenticationFilter(LdapAuthService ldapAuthService) {
    this.ldapAuthService = ldapAuthService;
  }

  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    HttpServletRequest httpRequest = (HttpServletRequest) request;
    String requestURI = httpRequest.getRequestURI();

    boolean isLoginFormRequest = requestURI.endsWith(LOGIN_FORM_URI);
    boolean isLoginServletRequest = requestURI.endsWith(LOGIN_SERVLET_URI);
    boolean isLoggedIn = ldapAuthService.authorize(httpRequest, (HttpServletResponse) response);

    if (isLoggedIn && (isLoginFormRequest || isLoginServletRequest)) {
      // User is already logged in, and is trying to login again; forward to the main homepage
      RequestDispatcher dispatcher = request.getRequestDispatcher(HiveServer2.HS2_WEBUI_ROOT_URI);
      dispatcher.forward(request, response);
    } else if (isLoggedIn || isLoginFormRequest || isLoginServletRequest) {
      // Continues the filter chain
      chain.doFilter(request, response);
    } else {
      // User is not logged in, so authentication is required; forwards to the login page
      RequestDispatcher dispatcher = request.getRequestDispatcher(LOGIN_FORM_URI);
      dispatcher.forward(request, response);
    }
  }

  public void destroy() {
    // A default filter destroy method
  }

  public void init(FilterConfig fConfig) throws ServletException {
    // A default filter init method
  }
}