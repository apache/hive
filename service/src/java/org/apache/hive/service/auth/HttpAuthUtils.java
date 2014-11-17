/**
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

package org.apache.hive.service.auth;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * Utility functions for HTTP mode authentication.
 */
public final class HttpAuthUtils {
  public static final String WWW_AUTHENTICATE = "WWW-Authenticate";
  public static final String AUTHORIZATION = "Authorization";
  public static final String BASIC = "Basic";
  public static final String NEGOTIATE = "Negotiate";

  /**
   * @return Stringified Base64 encoded kerberosAuthHeader on success
   * @throws Exception
   */
  public static String getKerberosServiceTicket(String principal, String host,
      String serverHttpUrl, boolean assumeSubject) throws Exception {
    String serverPrincipal =
        ShimLoader.getHadoopThriftAuthBridge().getServerPrincipal(principal, host);
    if (assumeSubject) {
      // With this option, we're assuming that the external application,
      // using the JDBC driver has done a JAAS kerberos login already
      AccessControlContext context = AccessController.getContext();
      Subject subject = Subject.getSubject(context);
      if (subject == null) {
        throw new Exception("The Subject is not set");
      }
      return Subject.doAs(subject, new HttpKerberosClientAction(serverPrincipal, serverHttpUrl));
    } else {
      // JAAS login from ticket cache to setup the client UserGroupInformation
      UserGroupInformation clientUGI =
          ShimLoader.getHadoopThriftAuthBridge().getCurrentUGIWithConf("kerberos");
      return clientUGI.doAs(new HttpKerberosClientAction(serverPrincipal, serverHttpUrl));
    }
  }

  private HttpAuthUtils() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * We'll create an instance of this class within a doAs block so that the client's TGT credentials
   * can be read from the Subject
   */
  public static class HttpKerberosClientAction implements PrivilegedExceptionAction<String> {
    public static final String HTTP_RESPONSE = "HTTP_RESPONSE";
    public static final String SERVER_HTTP_URL = "SERVER_HTTP_URL";
    private final String serverPrincipal;
    private final String serverHttpUrl;
    private final Base64 base64codec;
    private final HttpContext httpContext;

    public HttpKerberosClientAction(String serverPrincipal, String serverHttpUrl) {
      this.serverPrincipal = serverPrincipal;
      this.serverHttpUrl = serverHttpUrl;
      base64codec = new Base64(0);
      httpContext = new BasicHttpContext();
      httpContext.setAttribute(SERVER_HTTP_URL, serverHttpUrl);
    }

    @Override
    public String run() throws Exception {
      // This Oid for Kerberos GSS-API mechanism.
      Oid mechOid = new Oid("1.2.840.113554.1.2.2");
      // Oid for kerberos principal name
      Oid krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1");
      GSSManager manager = GSSManager.getInstance();
      // GSS name for server
      GSSName serverName = manager.createName(serverPrincipal, krb5PrincipalOid);
      // Create a GSSContext for authentication with the service.
      // We're passing client credentials as null since we want them to be read from the Subject.
      GSSContext gssContext =
          manager.createContext(serverName, mechOid, null, GSSContext.DEFAULT_LIFETIME);
      gssContext.requestMutualAuth(false);
      // Establish context
      byte[] inToken = new byte[0];
      byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
      gssContext.dispose();
      // Base64 encoded and stringified token for server
      return new String(base64codec.encode(outToken));
    }
  }
}
