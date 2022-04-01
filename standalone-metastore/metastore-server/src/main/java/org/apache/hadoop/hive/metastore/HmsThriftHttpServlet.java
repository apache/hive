/* * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;

public class HmsThriftHttpServlet extends TServlet {

  private static final Logger LOG = LoggerFactory
      .getLogger(HmsThriftHttpServlet.class);

  private static final String X_USER = MetaStoreUtils.USER_NAME_HTTP_HEADER;

  private final boolean isSecurityEnabled;

  public HmsThriftHttpServlet(TProcessor processor,
      TProtocolFactory inProtocolFactory, TProtocolFactory outProtocolFactory) {
    super(processor, inProtocolFactory, outProtocolFactory);
    // This should ideally be reveiving an instance of the Configuration which is used for the check
    isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
  }

  public HmsThriftHttpServlet(TProcessor processor,
      TProtocolFactory protocolFactory) {
    super(processor, protocolFactory);
    isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
  }

  @Override
  protected void doPost(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {

    Enumeration<String> headerNames = request.getHeaderNames();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Logging headers in request");
      while (headerNames.hasMoreElements()) {
        String headerName = headerNames.nextElement();
        LOG.debug("Header: [{}], Value: [{}]", headerName,
            request.getHeader(headerName));
      }
    }
    String userFromHeader = request.getHeader(X_USER);
    if (userFromHeader == null || userFromHeader.isEmpty()) {
      LOG.error("No user header: {} found", X_USER);
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
          "User Header missing");
      return;
    }

    // TODO: These should ideally be in some kind of a Cache with Weak referencse.
    // If HMS were to set up some kind of a session, this would go into the session by having
    // this filter work with a custom Processor / or set the username into the session
    // as is done for HS2.
    // In case of HMS, it looks like each request is independent, and there is no session
    // information, so the UGI needs to be set up in the Connection layer itself.
    UserGroupInformation clientUgi;
    // Temporary, and useless for now. Here only to allow this to work on an otherwise kerberized
    // server.
    if (isSecurityEnabled) {
      LOG.info("Creating proxy user for: {}", userFromHeader);
      clientUgi = UserGroupInformation.createProxyUser(userFromHeader, UserGroupInformation.getLoginUser());
    } else {
      LOG.info("Creating remote user for: {}", userFromHeader);
      clientUgi = UserGroupInformation.createRemoteUser(userFromHeader);
    }


    PrivilegedExceptionAction<Void> action = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        HmsThriftHttpServlet.super.doPost(request, response);
        return null;
      }
    };

    try {
      clientUgi.doAs(action);
    } catch (InterruptedException | RuntimeException e) {
      // TODO: Exception handling likely needs to be better, so that the client
      // can make better sense of what has gone wrong. Lookup what this looks like
      // in the default thrift binary interface.
      LOG.info("Exception while processing call", e);
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "C1C User Header missing");
    }
  }
}