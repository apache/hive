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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hive.http.HttpServer;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extends read-only HS2LeadershipStatus servlet to provide a DELETE method for triggering failover.
 * Invoking a "DELETE" method on this endpoint will trigger a failover if this instance is a leader.
 * hadoop.security.instrumentation.requires.admin should be set to true and current user has to be in admin ACLS
 * for accessing this endpoint.
 */
public class HS2LeadershipManager extends HS2LeadershipStatus {
  private static final Logger LOG = LoggerFactory.getLogger(HS2LeadershipManager.class);

  private class FailoverResponse {
    private boolean success;
    private String message;

    FailoverResponse() {
    }

    public boolean isSuccess() {
      return success;
    }

    public void setSuccess(final boolean success) {
      this.success = success;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(final String message) {
      this.message = message;
    }
  }

  @Override
  public void doDelete(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
    // strict admin check -
    // allows ONLY if hadoop.security.instrumentation.requires.admin is set to true
    // when hadoop.security.instrumentation.requires.admin is set to true, checks if hadoop.security.authorization
    // is true and if the logged in user (via PAM or SPNEGO + kerberos) is in hive.users.in.admin.role list
    final ServletContext context = getServletContext();
    if (!HttpServer.isInstrumentationAccessAllowedStrict(context, request, response)) {
      LOG.warn("Unauthorized to perform DELETE action. remoteUser: {}", request.getRemoteUser());
      return;
    }

    setResponseHeaders(response);

    LOG.info("DELETE handler invoked for failover..");
    ObjectMapper mapper = new ObjectMapper();
    FailoverResponse failoverResponse = new FailoverResponse();
    AtomicBoolean isLeader = (AtomicBoolean) context.getAttribute("hs2.isLeader");
    if (!isLeader.get()) {
      String msg = "Cannot failover an instance that is not a leader";
      LOG.info(msg);
      failoverResponse.setSuccess(false);
      failoverResponse.setMessage(msg);
      mapper.writerWithDefaultPrettyPrinter().writeValue(response.getWriter(), failoverResponse);
      response.setStatus(HttpServletResponse.SC_FORBIDDEN);
      return;
    }

    HiveServer2.FailoverHandlerCallback failoverHandler = (HiveServer2.FailoverHandlerCallback) context
      .getAttribute("hs2.failover.callback");
    try {
      String msg = "Failover successful!";
      LOG.info(msg);
      failoverHandler.failover();
      failoverResponse.setSuccess(true);
      failoverResponse.setMessage(msg);
      mapper.writerWithDefaultPrettyPrinter().writeValue(response.getWriter(), failoverResponse);
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      String errMsg = "Cannot perform failover of HS2 instance. err: " + e.getMessage();
      LOG.error(errMsg, e);
      failoverResponse.setSuccess(false);
      failoverResponse.setMessage(errMsg);
      mapper.writerWithDefaultPrettyPrinter().writeValue(response.getWriter(), failoverResponse);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
}
