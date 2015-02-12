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

package org.apache.hive.service.cli.thrift;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.auth.AuthenticationProviderFactory;
import org.apache.hive.service.auth.AuthenticationProviderFactory.AuthMethods;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.hive.service.auth.HttpAuthenticationException;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 *
 * ThriftHttpServlet
 *
 */
public class ThriftHttpServlet extends TServlet {

  private static final long serialVersionUID = 1L;
  public static final Log LOG = LogFactory.getLog(ThriftHttpServlet.class.getName());
  private final String authType;
  private final UserGroupInformation serviceUGI;
  private final UserGroupInformation httpUGI;

  public ThriftHttpServlet(TProcessor processor, TProtocolFactory protocolFactory,
      String authType, UserGroupInformation serviceUGI, UserGroupInformation httpUGI) {
    super(processor, protocolFactory);
    this.authType = authType;
    this.serviceUGI = serviceUGI;
    this.httpUGI = httpUGI;
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String clientUserName;
    String clientIpAddress;
    try {
      // For a kerberos setup
      if(isKerberosAuthMode(authType)) {
        clientUserName = doKerberosAuth(request);
        String doAsQueryParam = getDoAsQueryParam(request.getQueryString());
        if (doAsQueryParam != null) {
          SessionManager.setProxyUserName(doAsQueryParam);
        }
      }
      else {
        clientUserName = doPasswdAuth(request, authType);
      }
      LOG.debug("Client username: " + clientUserName);
      // Set the thread local username to be used for doAs if true
      SessionManager.setUserName(clientUserName);

      clientIpAddress = request.getRemoteAddr();
      LOG.debug("Client IP Address: " + clientIpAddress);
      // Set the thread local ip address
      SessionManager.setIpAddress(clientIpAddress);

      super.doPost(request, response);
    }
    catch (HttpAuthenticationException e) {
      LOG.error("Error: ", e);
      // Send a 401 to the client
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      if(isKerberosAuthMode(authType)) {
        response.addHeader(HttpAuthUtils.WWW_AUTHENTICATE, HttpAuthUtils.NEGOTIATE);
      }
      response.getWriter().println("Authentication Error: " + e.getMessage());
    }
    finally {
      // Clear the thread locals
      SessionManager.clearUserName();
      SessionManager.clearIpAddress();
      SessionManager.clearProxyUserName();
    }
  }

  /**
   * Do the LDAP/PAM authentication
   * @param request
   * @param authType
   * @throws HttpAuthenticationException
   */
  private String doPasswdAuth(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String userName = getUsername(request, authType);
    // No-op when authType is NOSASL
    if (!authType.equalsIgnoreCase(HiveAuthFactory.AuthTypes.NOSASL.toString())) {
      try {
        AuthMethods authMethod = AuthMethods.getValidAuthMethod(authType);
        PasswdAuthenticationProvider provider =
            AuthenticationProviderFactory.getAuthenticationProvider(authMethod);
        provider.Authenticate(userName, getPassword(request, authType));

      } catch (Exception e) {
        throw new HttpAuthenticationException(e);
      }
    }
    return userName;
  }

  /**
   * Do the GSS-API kerberos authentication.
   * We already have a logged in subject in the form of serviceUGI,
   * which GSS-API will extract information from.
   * In case of a SPNego request we use the httpUGI,
   * for the authenticating service tickets.
   * @param request
   * @return
   * @throws HttpAuthenticationException
   */
  private String doKerberosAuth(HttpServletRequest request)
      throws HttpAuthenticationException {
    // Try authenticating with the http/_HOST principal
    if (httpUGI != null) {
      try {
        return httpUGI.doAs(new HttpKerberosServerAction(request, httpUGI));
      } catch (Exception e) {
        LOG.info("Failed to authenticate with http/_HOST kerberos principal, " +
            "trying with hive/_HOST kerberos principal");
      }
    }
    // Now try with hive/_HOST principal
    try {
      return serviceUGI.doAs(new HttpKerberosServerAction(request, serviceUGI));
    } catch (Exception e) {
      LOG.error("Failed to authenticate with hive/_HOST kerberos principal");
      throw new HttpAuthenticationException(e);
    }

  }

  class HttpKerberosServerAction implements PrivilegedExceptionAction<String> {
    HttpServletRequest request;
    UserGroupInformation serviceUGI;

    HttpKerberosServerAction(HttpServletRequest request,
        UserGroupInformation serviceUGI) {
      this.request = request;
      this.serviceUGI = serviceUGI;
    }

    @Override
    public String run() throws HttpAuthenticationException {
      // Get own Kerberos credentials for accepting connection
      GSSManager manager = GSSManager.getInstance();
      GSSContext gssContext = null;
      String serverPrincipal = getPrincipalWithoutRealm(
          serviceUGI.getUserName());
      try {
        // This Oid for Kerberos GSS-API mechanism.
        Oid kerberosMechOid = new Oid("1.2.840.113554.1.2.2");
        // Oid for SPNego GSS-API mechanism.
        Oid spnegoMechOid = new Oid("1.3.6.1.5.5.2");
        // Oid for kerberos principal name
        Oid krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1");

        // GSS name for server
        GSSName serverName = manager.createName(serverPrincipal, krb5PrincipalOid);

        // GSS credentials for server
        GSSCredential serverCreds = manager.createCredential(serverName,
            GSSCredential.DEFAULT_LIFETIME,
            new Oid[]{kerberosMechOid, spnegoMechOid},
            GSSCredential.ACCEPT_ONLY);

        // Create a GSS context
        gssContext = manager.createContext(serverCreds);
        // Get service ticket from the authorization header
        String serviceTicketBase64 = getAuthHeader(request, authType);
        byte[] inToken = Base64.decodeBase64(serviceTicketBase64.getBytes());
        gssContext.acceptSecContext(inToken, 0, inToken.length);
        // Authenticate or deny based on its context completion
        if (!gssContext.isEstablished()) {
          throw new HttpAuthenticationException("Kerberos authentication failed: " +
              "unable to establish context with the service ticket " +
              "provided by the client.");
        }
        else {
          return getPrincipalWithoutRealmAndHost(gssContext.getSrcName().toString());
        }
      }
      catch (GSSException e) {
        throw new HttpAuthenticationException("Kerberos authentication failed: ", e);
      }
      finally {
        if (gssContext != null) {
          try {
            gssContext.dispose();
          } catch (GSSException e) {
            // No-op
          }
        }
      }
    }

    private String getPrincipalWithoutRealm(String fullPrincipal)
        throws HttpAuthenticationException {
      KerberosNameShim fullKerberosName;
      try {
        fullKerberosName = ShimLoader.getHadoopShims().getKerberosNameShim(fullPrincipal);
      } catch (IOException e) {
        throw new HttpAuthenticationException(e);
      }
      String serviceName = fullKerberosName.getServiceName();
      String hostName = fullKerberosName.getHostName();
      String principalWithoutRealm = serviceName;
      if (hostName != null) {
        principalWithoutRealm = serviceName + "/" + hostName;
      }
      return principalWithoutRealm;
    }

    private String getPrincipalWithoutRealmAndHost(String fullPrincipal)
        throws HttpAuthenticationException {
      KerberosNameShim fullKerberosName;
      try {
        fullKerberosName = ShimLoader.getHadoopShims().getKerberosNameShim(fullPrincipal);
      } catch (IOException e) {
        throw new HttpAuthenticationException(e);
      }
      return fullKerberosName.getServiceName();
    }
  }

  private String getUsername(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String creds[] = getAuthHeaderTokens(request, authType);
    // Username must be present
    if (creds[0] == null || creds[0].isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain username.");
    }
    return creds[0];
  }

  private String getPassword(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String creds[] = getAuthHeaderTokens(request, authType);
    // Password must be present
    if (creds[1] == null || creds[1].isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain username.");
    }
    return creds[1];
  }

  private String[] getAuthHeaderTokens(HttpServletRequest request,
      String authType) throws HttpAuthenticationException {
    String authHeaderBase64 = getAuthHeader(request, authType);
    String authHeaderString = StringUtils.newStringUtf8(
        Base64.decodeBase64(authHeaderBase64.getBytes()));
    String[] creds = authHeaderString.split(":");
    return creds;
  }

  /**
   * Returns the base64 encoded auth header payload
   * @param request
   * @param authType
   * @return
   * @throws HttpAuthenticationException
   */
  private String getAuthHeader(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String authHeader = request.getHeader(HttpAuthUtils.AUTHORIZATION);
    // Each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client is empty.");
    }

    String authHeaderBase64String;
    int beginIndex;
    if (isKerberosAuthMode(authType)) {
      beginIndex = (HttpAuthUtils.NEGOTIATE + " ").length();
    }
    else {
      beginIndex = (HttpAuthUtils.BASIC + " ").length();
    }
    authHeaderBase64String = authHeader.substring(beginIndex);
    // Authorization header must have a payload
    if (authHeaderBase64String == null || authHeaderBase64String.isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain any data.");
    }
    return authHeaderBase64String;
  }

  private boolean isKerberosAuthMode(String authType) {
    return authType.equalsIgnoreCase(HiveAuthFactory.AuthTypes.KERBEROS.toString());
  }

  private static String getDoAsQueryParam(String queryString) {
    if (queryString == null) {
      return null;
    }
    Map<String, String[]> params = javax.servlet.http.HttpUtils.parseQueryString( queryString );
    Set<String> keySet = params.keySet();
    for (String key: keySet) {
      if (key.equalsIgnoreCase("doAs")) {
        return params.get(key)[0];
      }
    }
    return null;
  }

}


