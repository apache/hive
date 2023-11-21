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

package org.apache.hive.service.cli.thrift;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.NewCookie;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.CookieSigner;
import org.apache.hive.service.auth.AuthType;
import org.apache.hive.service.auth.AuthenticationProviderFactory;
import org.apache.hive.service.auth.AuthenticationProviderFactory.AuthMethods;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.hive.service.auth.HttpAuthenticationException;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.auth.jwt.JWTValidator;
import org.apache.hive.service.auth.ldap.HttpEmptyAuthenticationException;
import org.apache.hive.service.auth.saml.HiveSaml2Client;
import org.apache.hive.service.auth.saml.HiveSamlRelayStateStore;
import org.apache.hive.service.auth.saml.HiveSamlUtils;
import org.apache.hive.service.auth.saml.HttpSamlAuthenticationException;
import org.apache.hive.service.auth.saml.HiveSamlAuthTokenGenerator;
import org.apache.hive.service.cli.HiveSQLException;
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
import org.pac4j.core.context.JEEContext;
import org.pac4j.core.credentials.TokenCredentials;
import org.pac4j.core.credentials.extractor.BearerAuthExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * ThriftHttpServlet
 *
 */
public class ThriftHttpServlet extends TServlet {

  private static final long serialVersionUID = 1L;
  public static final Logger LOG = LoggerFactory.getLogger(ThriftHttpServlet.class.getName());
  private final AuthType authType;
  private final UserGroupInformation serviceUGI;
  private final UserGroupInformation httpUGI;
  private final HiveConf hiveConf;

  // Class members for cookie based authentication.
  private CookieSigner signer;
  public static final String AUTH_COOKIE = "hive.server2.auth";
  private static final SecureRandom RAN = new SecureRandom();
  private boolean isCookieAuthEnabled;
  private String cookieDomain;
  private String cookiePath;
  private int cookieMaxAge;
  private boolean isCookieSecure;
  private boolean isHttpOnlyCookie;
  private final HiveAuthFactory hiveAuthFactory;
  public static final String HIVE_DELEGATION_TOKEN_HEADER =  "X-Hive-Delegation-Token";
  private static final String X_FORWARDED_FOR = "X-Forwarded-For";
  private JWTValidator jwtValidator;

  public ThriftHttpServlet(TProcessor processor, TProtocolFactory protocolFactory,
      UserGroupInformation serviceUGI, UserGroupInformation httpUGI,
      HiveAuthFactory hiveAuthFactory, HiveConf hiveConf) throws Exception {
    super(processor, protocolFactory);
    this.hiveConf = hiveConf;
    this.authType = AuthType.authTypeFromConf(hiveConf, true);
    this.serviceUGI = serviceUGI;
    this.httpUGI = httpUGI;
    this.hiveAuthFactory = hiveAuthFactory;
    this.isCookieAuthEnabled = hiveConf.getBoolVar(
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_AUTH_ENABLED);
    // Initialize the cookie based authentication related variables.
    if (isCookieAuthEnabled) {
      // Generate the signer with secret.
      String secret = Long.toString(RAN.nextLong());
      LOG.debug("Using the random number as the secret for cookie generation " + secret);
      this.signer = new CookieSigner(secret.getBytes());
      this.cookieMaxAge = (int) hiveConf.getTimeVar(
        ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE, TimeUnit.SECONDS);
      this.cookieDomain = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_DOMAIN);
      this.cookiePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_PATH);
      // always send secure cookies for SSL mode
      this.isCookieSecure = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL);
      this.isHttpOnlyCookie = hiveConf.getBoolVar(
        ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_IS_HTTPONLY);
    }
    if (this.authType.isEnabled(HiveAuthConstants.AuthTypes.JWT)) {
      this.jwtValidator = new JWTValidator(hiveConf);
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String clientUserName = null;
    String clientIpAddress;
    boolean requireNewCookie = false;
    logTrackingHeaderIfAny(request);

    try {

      clientIpAddress = request.getRemoteAddr();
      LOG.debug("Client IP Address: " + clientIpAddress);
      // If the cookie based authentication is already enabled, parse the
      // request and validate the request cookies.
      if (isCookieAuthEnabled) {
        clientUserName = validateCookie(request);
        requireNewCookie = (clientUserName == null);
        if (requireNewCookie) {
          LOG.info("Could not validate cookie sent, will try to generate a new cookie");
        }
      }

      // Set the thread local ip address
      SessionManager.setIpAddress(clientIpAddress);

      // get forwarded hosts address
      String forwarded_for = request.getHeader(X_FORWARDED_FOR);
      if (forwarded_for != null) {
        LOG.debug("{}:{}", X_FORWARDED_FOR, forwarded_for);
        List<String> forwardedAddresses = Arrays.asList(forwarded_for.split(","));
        SessionManager.setForwardedAddresses(forwardedAddresses);
      } else {
        SessionManager.setForwardedAddresses(Collections.emptyList());
      }

      // If the cookie based authentication is not enabled or the request does not have a valid
      // cookie, use authentication depending on the server setup.
      if (clientUserName == null) {
        String trustedDomain = HiveConf.getVar(hiveConf, ConfVars.HIVE_SERVER2_TRUSTED_DOMAIN).trim();
        final boolean useXff = HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_SERVER2_TRUSTED_DOMAIN_USE_XFF_HEADER);
        if (useXff && !trustedDomain.isEmpty() &&
          SessionManager.getForwardedAddresses() != null && !SessionManager.getForwardedAddresses().isEmpty()) {
          // general format of XFF header is 'X-Forwarded-For: client, proxy1, proxy2' where left most being the client
          clientIpAddress = SessionManager.getForwardedAddresses().get(0);
          LOG.info("Trusted domain authN is enabled. clientIp from X-Forwarded-For header: {}", clientIpAddress);
        }
        // Skip authentication if the connection is from the trusted domain, if specified.
        // getRemoteHost may or may not return the FQDN of the remote host depending upon the
        // HTTP server configuration. So, force a reverse DNS lookup.
        String remoteHostName =
                InetAddress.getByName(clientIpAddress).getCanonicalHostName();
        if (!trustedDomain.isEmpty() &&
                PlainSaslHelper.isHostFromTrustedDomain(remoteHostName, trustedDomain)) {
          LOG.info("No authentication performed because the connecting host " + remoteHostName +
                  " is from the trusted domain " + trustedDomain);
          // In order to skip authentication, we use auth type NOSASL to be consistent with the
          // HiveAuthFactory defaults. In HTTP mode, it will also get us the user name from the
          // HTTP request header.
          clientUserName = doPasswdAuth(request, HiveAuthConstants.AuthTypes.NOSASL.getAuthName());
        } else {
          // For a kerberos setup
          if (isAuthTypeEnabled(request, HiveAuthConstants.AuthTypes.KERBEROS)) {
            String delegationToken = request.getHeader(HIVE_DELEGATION_TOKEN_HEADER);
            // Each http request must have an Authorization header
            if ((delegationToken != null) && (!delegationToken.isEmpty())) {
              clientUserName = doTokenAuth(request);
            } else {
              clientUserName = doKerberosAuth(request);
            }
          } else if (isAuthTypeEnabled(request, HiveAuthConstants.AuthTypes.JWT)) {
            clientUserName = validateJWT(request, response);
          } else if (isAuthTypeEnabled(request, HiveAuthConstants.AuthTypes.SAML)) {
            // check if this request needs a SAML redirect
            String authHeader = request.getHeader(HttpAuthUtils.AUTHORIZATION);
            if ((authHeader == null || authHeader.isEmpty()) && needsRedirect(request, response)) {
              doSamlRedirect(request, response);
              return;
            } else if(authHeader.toLowerCase().startsWith(HttpAuthUtils.BASIC.toLowerCase())) {
              // fall back to password based authentication if the header starts with Basic
              clientUserName = doPasswdAuth(request, authType.getPasswordBasedAuthStr());
            } else {
              // redirect is not needed. Do SAML auth.
              clientUserName = doSamlAuth(request, response);
            }
          } else {
            String proxyHeader = HiveConf.getVar(hiveConf, ConfVars.HIVE_SERVER2_TRUSTED_PROXY_TRUSTHEADER).trim();
            if (!proxyHeader.equals("") && request.getHeader(proxyHeader) != null) { //Trusted header is present, which means the user is already authorized.
              clientUserName = getUsername(request);
            } else {
              // For password based authentication
              clientUserName = doPasswdAuth(request, authType.getPasswordBasedAuthStr());
            }
          }
        }
      }
      assert (clientUserName != null);
      LOG.debug("Client username: " + clientUserName);

      // Set the thread local username to be used for doAs if true
      SessionManager.setUserName(clientUserName);

      // find proxy user if any from query param
      String doAsQueryParam = getDoAsQueryParam(request.getQueryString());
      if (doAsQueryParam != null) {
        SessionManager.setProxyUserName(doAsQueryParam);
      }


      // Generate new cookie and add it to the response
      if (requireNewCookie &&
          !authType.isEnabled(HiveAuthConstants.AuthTypes.NOSASL)) {
        String cookieToken = HttpAuthUtils.createCookieToken(clientUserName);
        Cookie hs2Cookie = createCookie(signer.signCookie(cookieToken));

        if (isHttpOnlyCookie) {
          response.setHeader("SET-COOKIE", getHttpOnlyCookieHeader(hs2Cookie));
        } else {
          response.addCookie(hs2Cookie);
        }
        LOG.info("Cookie added for clientUserName " + clientUserName);
      }
      super.doPost(request, response);
    } catch (HttpAuthenticationException e) {
      // Ignore HttpEmptyAuthenticationException, it is normal for knox
      // to send a request with empty header
      if (!(e instanceof HttpEmptyAuthenticationException)) {
        LOG.error("Error: ", e);
      }
      // Wait until all the data is received and then respond with 401
      if (request.getContentLength() < 0) {
        try {
          ByteStreams.skipFully(request.getInputStream(), Integer.MAX_VALUE);
        } catch (EOFException ex) {
          LOG.info(ex.getMessage());
        }
      }
      // Send a 401 to the client
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      if (e instanceof HttpEmptyAuthenticationException &&
          authType.isEnabled(HiveAuthConstants.AuthTypes.KERBEROS)) {
        response.addHeader(HttpAuthUtils.WWW_AUTHENTICATE, HttpAuthUtils.NEGOTIATE);
      } else {
        try {
          LOG.error("Login attempt is failed for user : " +
              getUsername(request) + ". Error Messsage :" + e.getMessage());
        } catch (Exception ex) {
          // Ignore Exception
        }
      }
      response.getWriter().println("Authentication Error: " + e.getMessage());
    } finally {
      // Clear the thread locals
      SessionManager.clearUserName();
      SessionManager.clearIpAddress();
      SessionManager.clearProxyUserName();
      SessionManager.clearForwardedAddresses();
    }
  }

  private void logTrackingHeaderIfAny(HttpServletRequest request) {
    if (request.getHeader(Constants.HTTP_HEADER_REQUEST_TRACK) != null) {
      String requestTrackHeader = request.getHeader(Constants.HTTP_HEADER_REQUEST_TRACK);
      LOG.info("{}:{}", Constants.HTTP_HEADER_REQUEST_TRACK, requestTrackHeader);
    }
  }

  private String validateJWT(HttpServletRequest request, HttpServletResponse response)
      throws HttpAuthenticationException {
    Preconditions.checkState(jwtValidator != null, "JWT validator should have been set");
    String signedJwt = extractBearerToken(request, response);
    String user = null;
    try {
      user = jwtValidator.validateJWTAndExtractUser(signedJwt);
      Preconditions.checkNotNull(user, "JWT needs to contain the user name as subject");
      Preconditions.checkState(!user.isEmpty(), "User name should not be empty");
      LOG.info("JWT verification successful for user {}", user);
    } catch (Exception e) {
      LOG.error("JWT verification failed", e);
      throw new HttpAuthenticationException(e);
    }
    return user;
  }

  /**
   * A request needs redirect if it does not have a bearer token and it contains a valid
   * response port in its header.
   */
  private boolean needsRedirect(HttpServletRequest request,
      HttpServletResponse response) {
    String token = extractBearerToken(request, response);
    // if there is a bearer token; we use to authenticate else we look for
    // the response port header just to make sure we are not generating
    // SAML requests for any random http post requests.
    if (token != null) {
      return false;
    }
    try {
      HiveSamlUtils.validateSamlResponsePort(request);
      return true;
    } catch (HttpSamlAuthenticationException e) {
      LOG.debug("Response port could not be validated: " + e.getMessage());
    }
    return false;
  }

  /**
   * Generate a SAML Authentication request using HTTP-Redirect binding.
   */
  private void doSamlRedirect(HttpServletRequest request, HttpServletResponse response)
      throws HttpSamlAuthenticationException {
    HiveSaml2Client.get(hiveConf).setRedirect(request, response);
  }

  /**
   * This method validates the bearer token in the request. If the token is not or if the
   * token is not valid it throws a {@link HttpSamlAuthenticationException}. A token is
   * valid only if all the following conditions are met.
   * 1. Token signature is valid.
   * 2. Token is not expired.
   * 3. Token maps to a relayState which has a matching client identifier in the request.
   */
  private String doSamlAuth(HttpServletRequest request, HttpServletResponse response)
      throws HttpAuthenticationException {
    String token = extractBearerToken(request, response);
    if (token == null) {
      throw new HttpSamlAuthenticationException("Token not found.");
    }
    String clientIdentifier = request.getHeader(HiveSamlUtils.SSO_CLIENT_IDENTIFIER);
    if (clientIdentifier == null) {
      throw new HttpSamlAuthenticationException("Client identifier not found.");
    }
    String user = HiveSamlAuthTokenGenerator.get(hiveConf).validate(token);
    LOG.info("Successfully validated the token for user {}", user);
    // token is valid; now confirm if the client identifier matches with the relay state.
    Map<String, String> keyValues = new HashMap<>();
    if (HiveSamlAuthTokenGenerator.parse(token, keyValues)) {
      String relayStateKey = keyValues.get(HiveSamlAuthTokenGenerator.RELAY_STATE);
      if (!HiveSamlRelayStateStore.get()
          .validateClientIdentifier(relayStateKey, clientIdentifier)) {
        throw new HttpSamlAuthenticationException(
            "Client identifier could not be validated");
      }
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

  /**
   * Retrieves the client name from cookieString. If the cookie does not
   * correspond to a valid client, the function returns null.
   * @param cookies HTTP Request cookies.
   * @return Client Username if cookieString has a HS2 Generated cookie that is currently valid.
   * Else, returns null.
   */
  private String getClientNameFromCookie(Cookie[] cookies) {
    // Current Cookie Name, Current Cookie Value
    String currName, currValue;

    // Following is the main loop which iterates through all the cookies send by the client.
    // The HS2 generated cookies are of the format hive.server2.auth=<value>
    // A cookie which is identified as a hiveserver2 generated cookie is validated
    // by calling signer.verifyAndExtract(). If the validation passes, send the
    // username for which the cookie is validated to the caller. If no client side
    // cookie passes the validation, return null to the caller.
    for (Cookie currCookie : cookies) {
      // Get the cookie name
      currName = currCookie.getName();
      if (!currName.equals(AUTH_COOKIE)) {
        // Not a HS2 generated cookie, continue.
        continue;
      }
      // If we reached here, we have match for HS2 generated cookie
      currValue = currCookie.getValue();
      // Validate the value.
      try {
        currValue = signer.verifyAndExtract(currValue);
      } catch (IllegalArgumentException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Invalid cookie", e);
        }
        currValue = null;
      }
      // Retrieve the user name, do the final validation step.
      if (currValue != null) {
        String userName = HttpAuthUtils.getUserNameFromCookieToken(currValue);

        if (userName == null) {
          LOG.warn("Invalid cookie token " + currValue);
          continue;
        }
        //We have found a valid cookie in the client request.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Validated the cookie for user " + userName);
        }
        return userName;
      }
    }
    // No valid HS2 generated cookies found, return null
    return null;
  }

  /**
   * Convert cookie array to human readable cookie string
   * @param cookies Cookie Array
   * @return String containing all the cookies separated by a newline character.
   * Each cookie is of the format [key]=[value]
   */
  private String toCookieStr(Cookie[] cookies) {
    StringBuilder cookieStr = new StringBuilder();

    for (Cookie c : cookies) {
      cookieStr.append(c.getName()).append('=').append(c.getValue()).append(" ;\n");
    }
    return cookieStr.toString();
  }

  /**
   * Validate the request cookie. This function iterates over the request cookie headers
   * and finds a cookie that represents a valid client/server session. If it finds one, it
   * returns the client name associated with the session. Else, it returns null.
   * @param request The HTTP Servlet Request send by the client
   * @return Client Username if the request has valid HS2 cookie, else returns null
   */
  private String validateCookie(HttpServletRequest request) {
    // Find all the valid cookies associated with the request.
    Cookie[] cookies = request.getCookies();

    if (cookies == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No valid cookies associated with the request " + request);
      }
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received cookies: " + toCookieStr(cookies));
    }
    return getClientNameFromCookie(cookies);
  }

  /**
   * Generate a server side cookie given the cookie value as the input.
   * @param str Input string token.
   * @return The generated cookie.
   */
  private Cookie createCookie(String str) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cookie name = " + AUTH_COOKIE + " value = " + str);
    }
    Cookie cookie = new Cookie(AUTH_COOKIE, str);

    cookie.setMaxAge(cookieMaxAge);
    if (cookieDomain != null) {
      cookie.setDomain(cookieDomain);
    }
    if (cookiePath != null) {
      cookie.setPath(cookiePath);
    }
    cookie.setSecure(isCookieSecure);
    return cookie;
  }

  /**
   * Generate httponly cookie from HS2 cookie
   * @param cookie HS2 generated cookie
   * @return The httponly cookie
   */
  private static String getHttpOnlyCookieHeader(Cookie cookie) {
    NewCookie newCookie = new NewCookie(cookie.getName(), cookie.getValue(),
      cookie.getPath(), cookie.getDomain(), cookie.getVersion(),
      cookie.getComment(), cookie.getMaxAge(), cookie.getSecure());
    return newCookie + "; HttpOnly";
  }

  /**
   * Do the LDAP/PAM authentication
   * @param request request to authenticate
   * @param authType type of authentication
   * @throws HttpAuthenticationException on error authenticating end user
   */
  private String doPasswdAuth(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String userName = getUsername(request);
    // No-op when authType is NOSASL
    if (!authType.toLowerCase().contains(HiveAuthConstants.AuthTypes.NOSASL.toString().toLowerCase())) {
      try {
        AuthMethods authMethod = AuthMethods.getValidAuthMethod(authType);
        PasswdAuthenticationProvider provider =
            AuthenticationProviderFactory.getAuthenticationProvider(authMethod, hiveConf);
        provider.Authenticate(userName, getPassword(request));
      } catch (Exception e) {
        throw new HttpAuthenticationException(e);
      }
    }
    return userName;
  }

  private String doTokenAuth(HttpServletRequest request)
      throws HttpAuthenticationException {
    String tokenStr = request.getHeader(HIVE_DELEGATION_TOKEN_HEADER);
    try {
      return hiveAuthFactory.verifyDelegationToken(tokenStr);
    } catch (HiveSQLException e) {
      throw new HttpAuthenticationException(e);
    }
  }

  /**
   * Do the GSS-API kerberos authentication.
   * We already have a logged in subject in the form of serviceUGI,
   * which GSS-API will extract information from.
   * In case of a SPNego request we use the httpUGI,
   * for the authenticating service tickets.
   * @param request Request to act on
   * @return client principal name
   * @throws HttpAuthenticationException on error authenticating the user
   */
  @VisibleForTesting
  String doKerberosAuth(HttpServletRequest request)
      throws HttpAuthenticationException {
    // Each http request must have an Authorization header
    // Check before trying to do kerberos authentication twice
    getAuthHeader(request);

    // Try authenticating with the HTTP/_HOST principal
    if (httpUGI != null) {
      try {
        return httpUGI.doAs(new HttpKerberosServerAction(request, httpUGI));
      } catch (Exception e) {
        LOG.info("Failed to authenticate with HTTP/_HOST kerberos principal, " +
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
        String serviceTicketBase64 = getAuthHeader(request);
        byte[] inToken = Base64.getDecoder().decode(serviceTicketBase64);
        gssContext.acceptSecContext(inToken, 0, inToken.length);
        // Authenticate or deny based on its context completion
        if (!gssContext.isEstablished()) {
          throw new HttpAuthenticationException("Kerberos authentication failed: " +
              "unable to establish context with the service ticket " +
              "provided by the client.");
        } else {
          return getPrincipalWithoutRealmAndHost(gssContext.getSrcName().toString());
        }
      } catch (GSSException e) {
        if (gssContext != null) {
          try {
            LOG.error("Login attempt is failed for user : " +
                getPrincipalWithoutRealmAndHost(gssContext.getSrcName().toString()) +
                ". Error Messsage :" + e.getMessage());
          } catch (Exception ex) {
            // Ignore Exception
          }
        }
        throw new HttpAuthenticationException("Kerberos authentication failed: ", e);
      } finally {
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
        return fullKerberosName.getShortName();
      } catch (IOException e) {
        throw new HttpAuthenticationException(e);
      }
    }
  }

  private String getUsername(HttpServletRequest request)
      throws HttpAuthenticationException {
    String authHeaderDecodedString = getAuthHeaderDecodedString(request);
    String[] creds = authHeaderDecodedString.split(":", 2);
    // Username must be present
    if (creds[0] == null || creds[0].isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain username.");
    }
    return creds[0];
  }

  private String getPassword(HttpServletRequest request)
      throws HttpAuthenticationException {
    String authHeaderDecodedString = getAuthHeaderDecodedString(request);
    String[] creds = authHeaderDecodedString.split(":", 2);
    // Password must be present
    if (creds.length < 2 || creds[1] == null || creds[1].isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain password.");
    }
    return creds[1];
  }

  private String getAuthHeaderDecodedString(HttpServletRequest request) throws HttpAuthenticationException {
    String authHeaderBase64Str = getAuthHeader(request);
    try {
      return new String(Base64.getDecoder().decode(authHeaderBase64Str), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      // Auth header content is not base64 encoded
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client does not contain base64 encoded data", e);
    }
  }

  /**
   * Returns the base64 encoded auth header payload.
   * @param request request to interrogate
   * @return base64 encoded auth header payload
   * @throws HttpAuthenticationException exception if header is missing or empty
   */
  private String getAuthHeader(HttpServletRequest request)
      throws HttpAuthenticationException {
    String authHeader = request.getHeader(HttpAuthUtils.AUTHORIZATION);
    // Each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty()) {
      throw new HttpEmptyAuthenticationException("Authorization header received " +
          "from the client is empty.");
    }

    LOG.debug("HTTP Auth Header [{}]", authHeader);

    String[] parts = authHeader.split(" ");

    // Assume the Base-64 string is always the last thing in the header
    String authHeaderBase64String = parts[parts.length - 1];

    // Authorization header must have a payload
    if (authHeaderBase64String.isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain any data.");
    }
    return authHeaderBase64String;
  }

  @VisibleForTesting
  public boolean isAuthTypeEnabled(HttpServletRequest request,
      HiveAuthConstants.AuthTypes authType) {

    if (hasAuthScheme(request, HttpAuthUtils.BASIC) && this.authType.isPasswordBasedAuth(authType)
        && this.authType.isEnabled(authType)) {
      // The "Authorization: Basic" scheme indicates password-based authentication.
      return true;
    }
    if (this.authType.isEnabled(HiveAuthConstants.AuthTypes.KERBEROS) &&
       authType == HiveAuthConstants.AuthTypes.KERBEROS &&
       (StringUtils.isNotEmpty(request.getHeader(HIVE_DELEGATION_TOKEN_HEADER)) ||
       hasAuthScheme(request, HttpAuthUtils.NEGOTIATE))) {
      // Hive delegation token or "Authorization: Negotiate" scheme indicates Kerberos
      // authentication.
      return true;
    }
    if (this.authType.isEnabled(HiveAuthConstants.AuthTypes.SAML) &&
        authType == HiveAuthConstants.AuthTypes.SAML &&
        (StringUtils.isNotEmpty(request.getHeader(HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT)) ||
        hasAuthScheme(request, HttpAuthUtils.BEARER) && !hasJWT(request))) {
      // X-Hive-Token-Response-Port header or "Authorization: Bearer <SAML TOKEN>" scheme
      // indicates SAML authentication.
      return true;
    }
    if (this.authType.isEnabled(HiveAuthConstants.AuthTypes.JWT) &&
        authType == HiveAuthConstants.AuthTypes.JWT &&
        hasAuthScheme(request, HttpAuthUtils.BEARER) && hasJWT(request)) {
      // The "Authorization: Bearer <any.JWT.token>" scheme indicates JWT authentication.
      return true;
    }
    return false;
  }

  private boolean hasJWT(HttpServletRequest request) {
    String authHeaderString;
    try {
      authHeaderString = getAuthHeader(request);
    } catch (HttpAuthenticationException e) {
      return false;
    }
    // Assume JWT consists of three parts separated by dots
    String[] jwt = authHeaderString.split("\\.");
    return jwt.length == 3;
  }

  private boolean hasAuthScheme(HttpServletRequest request, String authScheme) {
    String authHeader = request.getHeader(HttpAuthUtils.AUTHORIZATION);
    if (authHeader == null || authHeader.isEmpty()) {
      return false;
    }

    LOG.debug("HTTP Auth Header [{}]", authHeader);

    String[] parts = authHeader.trim().split(" ");
    if (parts.length < 2) {
      return false;
    }

    return parts[0].equalsIgnoreCase(authScheme.toLowerCase());
  }

  private static String getDoAsQueryParam(String queryString) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("URL query string:" + queryString);
    }
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


