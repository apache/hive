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

import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.Optional;

public class LoginServlet extends HttpServlet {
  public static final String LOGIN_FAILURE_MESSAGE = "login_failure_message";
  private final LdapAuthService ldapAuthService;

  public LoginServlet(LdapAuthService ldapAuthService) {
    this.ldapAuthService = ldapAuthService;
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response) {
    boolean success = ldapAuthService.authenticate(request, response);
    PrintWriter out = null;
    try {
      out = response.getWriter();
      final RequestDispatcher dispatcher;
      if (success) {
        dispatcher = request.getRequestDispatcher(HiveServer2.HS2_WEBUI_ROOT_URI);
      } else {
        request.setAttribute(LOGIN_FAILURE_MESSAGE, "Invalid username or password");
        dispatcher = request.getRequestDispatcher(LDAPAuthenticationFilter.LOGIN_FORM_URI);
      }
      dispatcher.forward(request, response);
    } catch (Exception e) {
      if (out == null) {
        return;
      }
      Integer statusCode = (Integer) request.getAttribute("javax.servlet.error.status_code");
      String servletName = Optional.ofNullable((String) request.getAttribute("javax.servlet.error.servlet_name")).orElse("Unknown");
      String requestUri = Optional.ofNullable((String) request.getAttribute("javax.servlet.error.request_uri")).orElse("Unknown");

      response.setContentType("text/html");
      out.write("<html><head><title>Exception/Error Details</title></head><body>");
      out.write("<h3>Error Details</h3>");
      out.write("<strong>Status Code</strong>:" + statusCode + "<br>");
      out.write("<ul><li>Servlet Name:" + servletName + "</li>");
      out.write("<li>Requested URI:" + requestUri + "</li>");
      out.write("<li>Exception Name:" + e.getClass().getName() + "</li>");
      out.write("<li>Exception Message:" + e.getMessage() + "</li>");
      out.write("</ul><br><br></body></html>");
    }
  }
}
