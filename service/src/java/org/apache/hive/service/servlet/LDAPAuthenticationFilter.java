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
import org.eclipse.jetty.http.HttpMethod;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LDAPAuthenticationFilter implements Filter {

  public static final String LOGIN_FORM_URI = "/loginForm.jsp";
  public static final String LOGIN_SERVLET_URI = "/login";
  private final LdapAuthService ldapAuthService;

  public LDAPAuthenticationFilter(LdapAuthService ldapAuthService) {
    this.ldapAuthService = ldapAuthService;
  }

  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    boolean isLoggedIn = ldapAuthService.authenticate(httpRequest, (HttpServletResponse) response);
    boolean forwardRequest = isLoginRequest(httpRequest) == isLoggedIn;
    if (forwardRequest) {
      ServletContext rootContext = request.getServletContext().getContext("/");
      // if the request is trying to login, forward to the main homepage in case
      // the user has already logged in, otherwise the login page.
      String forwardUri = isLoggedIn ? HiveServer2.HS2_WEBUI_ROOT_URI : LOGIN_FORM_URI;
      RequestDispatcher dispatcher = rootContext.getRequestDispatcher(forwardUri);
      dispatcher.forward(request, response);
    } else {
      chain.doFilter(request, response);
    }
  }

  public boolean isLoginRequest(HttpServletRequest request) {
    String method = request.getMethod();
    String servletPath = request.getServletPath();
    return LOGIN_FORM_URI.equals(servletPath) ||
        HttpMethod.POST.name().equalsIgnoreCase(method) && LOGIN_SERVLET_URI.equals(servletPath);
  }

  public void destroy() {
    // A default filter destroy method
  }

  public void init(FilterConfig fConfig) throws ServletException {
    // A default filter init method
  }
}