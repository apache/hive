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

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;
import java.io.Closeable;
import java.net.URI;
import org.apache.hive.service.auth.saml.HiveSamlUtils;

/**
 * Main interface which exposes the methods to do a browser based SSO flow from
 * a JDBC connection.
 */
public interface IJdbcBrowserClient extends Closeable {

  /**
   * Start a webserver and bind to a port number as configued.
   * @throws HiveJdbcBrowserException
   */
  void startListening() throws HiveJdbcBrowserException;

  /**
   * Execute the browser actions to complete the SSO workflow. This method assumes
   * that the {@link #init(JdbcBrowserClientContext)} method has been called already
   * to initialize the state needed for doing the browser based flow.
   * @throws HiveJdbcBrowserException in case of any errors to instantiate or execute
   * browser flow.
   */
  void doBrowserSSO() throws HiveJdbcBrowserException;

  /**
   * Initializes the browser client context. The client context contains a client
   * identifier which must be used to set the http header with key
   * {@link HiveSamlUtils#SSO_CLIENT_IDENTIFIER}.
   */
  void init(JdbcBrowserClientContext context);

  /**
   * Gets the port on this localhost where this browser client is listening on.
   */
  Integer getPort();

  /**
   * Returns the {@link HiveJdbcBrowserServerResponse} as received from the server
   * on the port where this browser client is listening on.
   */
  HiveJdbcBrowserServerResponse getServerResponse();

  /**
   * Gets the client identifier to be used to set in the http header for the requests
   * from this browser client.
   */
  String getClientIdentifier();

  /**
   * Util class for encapsulating all the initialization context for the BrowserClient.
   */
  class JdbcBrowserClientContext {
    private final URI ssoUri;
    private final String clientIdentifier;
    JdbcBrowserClientContext(URI ssoUrl, String clientIdentifier) {
      this.ssoUri = Preconditions.checkNotNull(ssoUrl);
      this.clientIdentifier = Preconditions.checkNotNull(clientIdentifier);
    }

    public URI getSsoUri() {
      return ssoUri;
    }

    public String getClientIdentifier() {
      return clientIdentifier;
    }
  }

  class HiveJdbcBrowserException extends Exception {
    HiveJdbcBrowserException(String msg, Throwable ex) {
      super(msg, ex);
    }

    HiveJdbcBrowserException(String msg) {
      super(msg);
    }

    HiveJdbcBrowserException(Throwable e) {
      super(e);
    }
  }

  /**
   * Util class which can be used to parse the response received from the server.
   */
  @Immutable
  class HiveJdbcBrowserServerResponse {
    private final String msg;
    private final boolean status;
    private final String token;

    public HiveJdbcBrowserServerResponse(boolean status, String msg, String token) {
      this.status = status;
      this.msg = msg;
      this.token = token;
    }

    public String getMsg() {
      return msg;
    }

    public boolean isSuccessful() {
      return status;
    }

    public String getToken() {
      return token;
    }

    /**
     * A response is valid if the status is true with a non-empty token or
     * if status is false
     */
    public boolean isValid() {
      return !status || (token != null && !token.isEmpty());
    }
  }
}
