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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;
import java.util.Optional;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;
import org.apache.hadoop.hive.metastore.auth.jwt.JWTValidator;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.pac4j.core.context.JEEContext;
import org.pac4j.core.credentials.TokenCredentials;
import org.pac4j.core.credentials.extractor.BearerAuthExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;

/*
Servlet class used by HiveMetastore server when running in HTTP mode.
If JWT auth is enabled, then the servlet is also responsible for validating
JWTs sent in the Authorization header in HTTP request.
 */
public class HmsThriftHttpServlet extends TServlet {
  private static final Logger LOG = LoggerFactory
      .getLogger(HmsThriftHttpServlet.class);
  private static final String X_USER = MetaStoreUtils.USER_NAME_HTTP_HEADER;
  private final boolean isSecurityEnabled;
  private final boolean jwtAuthEnabled;
  private JWTValidator jwtValidator = null;
  private Configuration conf;

  public HmsThriftHttpServlet(TProcessor processor,
      TProtocolFactory protocolFactory, Configuration conf) {
    super(processor, protocolFactory);
    this.conf = conf;
    isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    if (MetastoreConf.getVar(conf,
        ConfVars.THRIFT_METASTORE_AUTHENTICATION).equalsIgnoreCase("jwt")) {
      jwtAuthEnabled = true;
    } else {
      jwtAuthEnabled = false;
    }
  }

  public void init() throws ServletException {
    super.init();
    if (jwtAuthEnabled) {
      try {
        jwtValidator = new JWTValidator(this.conf);
      } catch (Exception e) {
        throw new ServletException("Failed to initialize HmsThriftHttpServlet."
            + " Error: " + e);
      }
    }
  }

  @Override
  protected void doPost(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Logging headers in doPost request");
      Enumeration<String> headerNames = request.getHeaderNames();
      while (headerNames.hasMoreElements()) {
        String headerName = headerNames.nextElement();
        LOG.debug("Header: [{}], Value: [{}]", headerName,
            request.getHeader(headerName));
      }
    }
    try {
      String userFromHeader = extractUserName(request, response);
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
        LOG.error("Exception when executing http request as user: " + clientUgi.getUserName(),
            e);
        throw new ServletException(e);
      }
    } catch (HttpAuthenticationException e) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      response.getWriter().println("Authentication error: " + e.getMessage());
      // Also log the error message on server side
      LOG.error("Authentication error: ", e);
    }
  }
  private String extractUserName(HttpServletRequest request, HttpServletResponse response)
      throws HttpAuthenticationException {
    if (!jwtAuthEnabled) {
      String userFromHeader = request.getHeader(X_USER);
      if (userFromHeader == null || userFromHeader.isEmpty()) {
        throw new HttpAuthenticationException("User header " + X_USER + " missing in request");
      }
      return userFromHeader;
    }
    String signedJwt = extractBearerToken(request, response);
    if (signedJwt == null) {
      throw new HttpAuthenticationException("Couldn't find bearer token in the auth header in the request");
    }
    String user;
    try {
      user = jwtValidator.validateJWTAndExtractUser(signedJwt);
      Preconditions.checkNotNull(user, "JWT needs to contain the user name as subject");
      Preconditions.checkState(!user.isEmpty(), "User name should not be empty in JWT");
      LOG.info("Successfully validated and extracted user name {} from JWT in Auth "
          + "header in the request", user);
    } catch (Exception e) {
      throw new HttpAuthenticationException("Failed to validate JWT from Bearer token in "
          + "Authentication header", e);
    }
    return user;
  }

  /**
   * Extracts the bearer authorization header from the request. If there is no bearer
   * authorization token, returns null.
   */
  private String extractBearerToken(HttpServletRequest request,
      HttpServletResponse response) {
    BearerAuthExtractor extractor = new BearerAuthExtractor();
    Optional<TokenCredentials> tokenCredentials = extractor.extract(new JEEContext(
        request, response));
    return tokenCredentials.map(TokenCredentials::getToken).orElse(null);
  }
}
