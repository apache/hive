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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hive.http.HttpConstants;
import org.apache.hive.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Returns "true" if this HS2 instance is leader else "false".
 * hadoop.security.instrumentation.requires.admin should be set to true and current user has to be in admin ACLS
 * for accessing this endpoint.
 */
public class HS2LeadershipStatus extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(HS2LeadershipStatus.class);

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    // admin check -
    // allows when hadoop.security.instrumentation.requires.admin is set to false
    // when hadoop.security.instrumentation.requires.admin is set to true, checks if hadoop.security.authorization
    // is true and if the logged in user (via PAM or SPNEGO + kerberos) is in hive.users.in.admin.role list
    final ServletContext context = getServletContext();
    if (!HttpServer.isInstrumentationAccessAllowed(context, request, response)) {
      LOG.warn("Unauthorized to perform GET action. remoteUser: {}", request.getRemoteUser());
      return;
    }

    setResponseHeaders(response);

    ServletContext ctx = getServletContext();
    AtomicBoolean isLeader = (AtomicBoolean) ctx.getAttribute("hs2.isLeader");
    LOG.info("Returning isLeader: {}", isLeader);
    ObjectMapper mapper = new ObjectMapper();
    mapper.writerWithDefaultPrettyPrinter().writeValue(response.getWriter(), isLeader);
    response.setStatus(HttpServletResponse.SC_OK);
    response.flushBuffer();
  }

  protected void setResponseHeaders(final HttpServletResponse response) {
    response.setContentType(HttpConstants.CONTENT_TYPE_JSON);
    response.setHeader(HttpConstants.ACCESS_CONTROL_ALLOW_METHODS,
        HttpConstants.METHOD_GET + "," + HttpConstants.METHOD_DELETE);
    response.setHeader(HttpConstants.ACCESS_CONTROL_ALLOW_ORIGIN, HttpConstants.WILDCARD);
  }
}
