/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.http.security;

import org.apache.hadoop.hive.conf.HiveConf;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.AuthenticationState;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import javax.security.sasl.AuthenticationException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import net.sf.jpam.Pam;

/*

  This class authenticates HS2 web UI via PAM. To authenticate use

   * httpGet with header name "Authorization"
   * and header value "Basic authB64Code"

    where  authB64Code is Base64 string for "login:password"
 */

public class PamAuthenticator extends LoginAuthenticator {
  private final String pamServiceNames;

  public PamAuthenticator(HiveConf conf) throws AuthenticationException {
    super();
    pamServiceNames = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PAM_SERVICES);
    if (pamServiceNames == null || pamServiceNames.trim().isEmpty()) {
      throw new AuthenticationException("No PAM services are set.");
    }
  }

  @Override
  public String getAuthenticationType() {
    return "pam";
  }

  @Override
  public AuthenticationState validateRequest(Request request, Response response, Callback callback)
      throws ServerAuthException {
    String credentials = request.getHeaders().get(HttpHeader.AUTHORIZATION);

    try {
      if (credentials != null) {
        int space = credentials.indexOf(' ');
        if (space > 0) {
          String method = credentials.substring(0, space);
          if ("basic".equalsIgnoreCase(method)) {
            credentials = credentials.substring(space + 1);
            credentials = new String(Base64.getDecoder().decode(credentials), StandardCharsets.ISO_8859_1);
            int i = credentials.indexOf(':');
            if (i > 0) {
              String username = credentials.substring(0, i);
              String password = credentials.substring(i + 1);

              UserIdentity user = login(username, password, request, response);
              if (user != null) {
                return new UserAuthenticationSucceeded(getAuthenticationType(), user);
              }
            }
          }
        }
      }

      response.getHeaders().put(HttpHeader.WWW_AUTHENTICATE,
          "basic realm=\"" + getLoginService().getName() + '"');
      Response.writeError(request, response, callback, 401);
      return AuthenticationState.CHALLENGE;
    } catch (Exception e) {
      throw new ServerAuthException(e);
    }
  }

  protected UserIdentity login(String username, String password, Request request, Response response)
      throws AuthenticationException {
    if (authenticate(username, password)) {
      return new PamUserIdentity(username);
    }
    return null;
  }

  private boolean authenticate(String user, String password) throws AuthenticationException {
    String[] pamServices = pamServiceNames.split(",");
    String errorMsg = "Error authenticating with the PAM service: ";
    for (String pamService : pamServices) {
      try {
        Pam pam = new Pam(pamService);
        if (!pam.authenticateSuccessful(user, password)) {
          return false;
        }
      } catch (Throwable e) {
        // Catch the exception caused by missing jpam.so which otherwise would
        // crashes the thread and causes the client hanging rather than notifying
        // the client nicely
        throw new AuthenticationException(errorMsg + pamService, e);
      }
    }
    return true;
  }
}
