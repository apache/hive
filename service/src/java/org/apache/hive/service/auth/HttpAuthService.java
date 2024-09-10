/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.auth;

import org.apache.hive.service.CookieSigner;
import org.apache.hive.service.auth.ldap.HttpEmptyAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.NewCookie;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

public class HttpAuthService {
  private static final Logger LOG = LoggerFactory.getLogger(HttpAuthService.class);
  public static final String USERNAME_REQUEST_PARAM_NAME = "username";
  public static final String PASSWORD_REQUEST_PARAM_NAME = "password";
  private static final SecureRandom RAN = new SecureRandom();
  public static final String HIVE_SERVER2_WEBUI_AUTH_COOKIE_NAME = "hive.server2.webui.auth";
  private final CookieSigner signer;
  private final String cookieDomain;
  private final String cookiePath;
  private final int cookieMaxAge;
  private final boolean isCookieSecure;
  private final String authCookieName;
  
  public HttpAuthService(String cookieDomain, String cookiePath, int cookieMaxAge, boolean isCookieSecure, 
      String authCookieName) {
    String secret = Long.toString(RAN.nextLong());
    this.signer = new CookieSigner(secret.getBytes());
    this.cookieMaxAge = cookieMaxAge;
    this.cookieDomain = cookieDomain;
    this.cookiePath = cookiePath;
    this.authCookieName = authCookieName;
    this.isCookieSecure = isCookieSecure;
  }

  public Cookie signAndCreateCookie(String cookieToken) {
    return createCookie(signer.signCookie(cookieToken));
  }

  /**
   * Generate a server side cookie given the cookie value as the input.
   * @param str Signed input string token.
   * @return The generated cookie.
   */
  public Cookie createCookie(String str) {
    Cookie cookie = new Cookie(authCookieName, str);
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
   * Validate the request cookie. This function iterates over the request cookie headers
   * and finds a cookie that represents a valid client/server session. If it finds one, it
   * returns the client name associated with the session. Else, it returns null.
   * @param request The HTTP Servlet Request send by the client
   * @return Client Username if the request has valid HS2 cookie, else returns null
   * @throws UnsupportedEncodingException in case of wrong encoding
   */
  public String validateCookie(HttpServletRequest request) throws UnsupportedEncodingException {
    // Find all the valid cookies associated with the request.
    Cookie[] cookies = request.getCookies();
    if (cookies == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No valid cookies associated with the request {}", request);
      }
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received cookies: {}", toCookieStr(cookies));
    }
    return getClientNameFromCookie(cookies);
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
    String currName;
    String currValue;

    // Following is the main loop which iterates through all the cookies sent by the client.
    // The HS2 generated cookies are of the format hive.server2.auth=<value>
    // A cookie which is identified as a hiveserver2 generated cookie is validated
    // by calling signer.verifyAndExtract(). If the validation passes, send the
    // username for which the cookie is validated to the caller. If no client side
    // cookie passes the validation, return null to the caller.
    for (Cookie currCookie : cookies) {
      // Get the cookie name
      currName = currCookie.getName();
      if (!currName.equals(authCookieName)) {
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
          LOG.warn("Invalid cookie token {}", currValue);
          continue;
        }
        //We have found a valid cookie in the client request.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Validated the cookie for user {}", userName);
        }
        return userName;
      }
    }
    // No valid HS2 generated cookies found, return null
    return null;
  }

  public String verifyAndExtract(String signedValue) {
    return signer.verifyAndExtract(signedValue); 
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
   * Retrieves username from http request
   * @param request The HTTP Servlet Request send by the client
   * @return String containing username
   */
  public String getUsername(HttpServletRequest request) throws HttpAuthenticationException {
    // If the request contains username parameter, return its value
    String usernameParam = request.getParameter(USERNAME_REQUEST_PARAM_NAME);
    if (usernameParam != null && !usernameParam.isEmpty()) {
      return usernameParam;
    }
    // If the request contains username parameter, return its value
    String authHeaderDecodedString = getAuthHeaderDecodedString(request);
    String[] credentials = authHeaderDecodedString.split(":", 2);
    if (credentials[0] == null || credentials[0].isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received from the client does not contain username.");
    }
    return credentials[0];
  }

  /**
   * Retrieves password from http request
   * @param request The HTTP Servlet Request send by the client
   * @return String containing password
   */
  public String getPassword(HttpServletRequest request) throws HttpAuthenticationException {
    // If the request contains password parameter, return its value
    String passwordParam = request.getParameter(PASSWORD_REQUEST_PARAM_NAME);
    if (passwordParam != null && !passwordParam.isEmpty()) {
      return passwordParam;
    }
    // If the request contains password parameter, return its value
    String authHeaderDecodedString = getAuthHeaderDecodedString(request);
    String[] credentials = authHeaderDecodedString.split(":", 2);
    if (credentials.length < 2 || credentials[1] == null || credentials[1].isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received from the client does not contain password.");
    }
    return credentials[1];
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
  public String getAuthHeader(HttpServletRequest request)
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

  /**
   * Generate httponly cookie from HS2 cookie
   * @param cookie HS2 generated cookie
   * @return The httponly cookie
   */
  public static String getHttpOnlyCookieHeader(Cookie cookie) {
    NewCookie newCookie = new NewCookie(cookie.getName(), cookie.getValue(),
        cookie.getPath(), cookie.getDomain(), cookie.getVersion(),
        cookie.getComment(), cookie.getMaxAge(), cookie.getSecure());
    return newCookie + "; HttpOnly";
  }
}