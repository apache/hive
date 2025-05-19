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

package org.apache.hive.service.auth.ldap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.auth.AuthenticationProviderFactory;
import org.apache.hive.service.auth.HttpAuthService;
import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.hive.service.auth.HttpAuthenticationException;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

public class LdapAuthService extends HttpAuthService {
  private static final Logger LOG = LoggerFactory.getLogger(LdapAuthService.class);
  private final PasswdAuthenticationProvider authProvider;
  
  public LdapAuthService(HiveConf hiveConf, PasswdAuthenticationProvider provider) {
    super(
        hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_HTTP_COOKIE_DOMAIN),
        hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_HTTP_COOKIE_PATH),
        (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_HTTP_COOKIE_MAX_AGE, TimeUnit.SECONDS),
        hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL),
        HIVE_SERVER2_WEBUI_AUTH_COOKIE_NAME);
    if (provider != null) {
      this.authProvider = provider;
    } else {
      try {
        this.authProvider = AuthenticationProviderFactory
            .getAuthenticationProvider(AuthenticationProviderFactory.AuthMethods.LDAP, hiveConf);
        // always send secure cookies for SSL mode
      } catch (AuthenticationException e) {
        throw new ServiceException(e);
      }
    }
  }
  
  public boolean authenticate(HttpServletRequest request, HttpServletResponse response) {
    try {
      String clientUserName = validateCookie(request);
      if (clientUserName == null) {
        clientUserName = getUsername(request);
        authProvider.authenticate(clientUserName, getPassword(request));

        String cookieToken = HttpAuthUtils.createCookieToken(clientUserName);
        Cookie hs2Cookie = signAndCreateCookie(cookieToken);

        response.addCookie(hs2Cookie);
      }
    } catch (HttpAuthenticationException | AuthenticationException | UnsupportedEncodingException e) {
      LOG.debug("Error in authenticating HTTP request", e);
      return false;
    }
    return true;
  }
}