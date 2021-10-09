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

package org.apache.hive.jdbc.saml;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hive.jdbc.saml.IJdbcBrowserClient.HiveJdbcBrowserServerResponse;
import org.apache.hive.service.auth.saml.HiveSamlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpBrowserClientServlet extends HttpServlet {
  private final HiveJdbcBrowserClient browserClient;
  private static final Logger LOG = LoggerFactory.getLogger(
      HttpBrowserClientServlet.class);
  HttpBrowserClientServlet(HiveJdbcBrowserClient browserClient) {
    this.browserClient = browserClient;
  }

  @Override
  protected void doPost(
      HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    LOG.info("Received request from {}:{} for browserClient at port {}",
        req.getRemoteAddr(), req.getRemotePort(), browserClient.toString());
    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    String token = req.getParameter(HiveSamlUtils.TOKEN_KEY);
    String msg = req.getParameter(HiveSamlUtils.MESSAGE_KEY);
    boolean status = Boolean.parseBoolean(req.getParameter(HiveSamlUtils.STATUS_KEY));
    HiveJdbcBrowserServerResponse response = new HiveJdbcBrowserServerResponse(status,
        msg, token);
    if (response.isSuccessful()) {
      resp.getWriter().write("Successfully authenticated. You may close this window.");
    } else {
      resp.getWriter().write(
          "Authentication failed. Please check server logs for details. "
              + "You may close this window.");
    }
    resp.getWriter().flush();
    browserClient.addServerResponse(response);
  }
}
