/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.druid.security;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.http.client.AbstractHttpClient;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.HttpResponseHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

import java.net.CookieManager;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This is a slightly modified version of kerberos module borrowed from druid project
 * Couple of reasons behind the copy/modification instead of mvn dependency.
 *  1/ Need to remove the authentication step since it not required
 *  2/ To avoid some un-needed transitive dependencies that can clash on the classpath like jetty-XX.
 */
public class KerberosHttpClient extends AbstractHttpClient
{
  protected static final org.slf4j.Logger log = LoggerFactory.getLogger(KerberosHttpClient.class);
  private final HttpClient delegate;
  private final CookieManager cookieManager;

  public KerberosHttpClient(HttpClient delegate)
  {
    this.delegate = delegate;
    this.cookieManager = new CookieManager();
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
          Request request, HttpResponseHandler<Intermediate, Final> httpResponseHandler, Duration duration
  )
  {
    final SettableFuture<Final> retVal = SettableFuture.create();
    inner_go(request, httpResponseHandler, duration, retVal);
    return retVal;
  }


  private <Intermediate, Final> void inner_go(
          final Request request,
          final HttpResponseHandler<Intermediate, Final> httpResponseHandler,
          final Duration duration,
          final SettableFuture<Final> future
  )
  {
    try {
      final String host = request.getUrl().getHost();
      final URI uri = request.getUrl().toURI();

      /* Cookies Manager is used to cache cookie returned by service.
       The goal us to avoid doing KDC requests for every request.*/

      Map<String, List<String>> cookieMap = cookieManager.get(uri, Collections.<String, List<String>>emptyMap());
      for (Map.Entry<String, List<String>> entry : cookieMap.entrySet()) {
        request.addHeaderValues(entry.getKey(), entry.getValue());
      }
      final boolean should_retry_on_unauthorized_response;

      if (DruidKerberosUtil.needToSendCredentials(cookieManager.getCookieStore(), uri)) {
        // No Cookies for requested URI, authenticate user and add authentication header
        log.debug("No Auth Cookie found for URI{}. Existing Cookies{} Authenticating... ", uri,
            cookieManager.getCookieStore().getCookies()
        );
        // Assuming that a valid UGI with kerberos cred is created by HS2 or LLAP
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        currentUser.checkTGTAndReloginFromKeytab();
        log.debug("The user credential is {}", currentUser);
        String challenge = currentUser.doAs(new PrivilegedExceptionAction<String>() {
          @Override public String run() throws Exception {
            return DruidKerberosUtil.kerberosChallenge(host);
          }
        });
        request.setHeader(HttpHeaders.Names.AUTHORIZATION, "Negotiate " + challenge);
        /* no reason to retry if the challenge ticket is not valid. */
        should_retry_on_unauthorized_response = false;
      } else {
        /* In this branch we had already a cookie that did expire
        therefore we need to resend a valid Kerberos challenge*/
        log.debug("Found Auth Cookie found for URI {} cookie {}", uri,
            DruidKerberosUtil.getAuthCookie(cookieManager.getCookieStore(), uri).toString()
        );
        should_retry_on_unauthorized_response = true;
      }

      ListenableFuture<RetryResponseHolder<Final>> internalFuture = delegate.go(request, new RetryIfUnauthorizedResponseHandler<Intermediate, Final>(
          new ResponseCookieHandler(request.getUrl().toURI(), cookieManager, httpResponseHandler
              )), duration
      );

      RetryResponseHolder<Final> responseHolder = internalFuture.get();

      if (should_retry_on_unauthorized_response && responseHolder.shouldRetry()) {
        log.debug("Preparing for Retry boolean {} and result {}, object{} ",
            should_retry_on_unauthorized_response, responseHolder.shouldRetry(), responseHolder.getObj()
        );
        // remove Auth cookie
        DruidKerberosUtil.removeAuthCookie(cookieManager.getCookieStore(), uri);
        // clear existing cookie
        request.setHeader("Cookie", "");
        inner_go(request.copy(), httpResponseHandler, duration, future);

      } else {
        future.set(responseHolder.getObj());
      }
    }
    catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }


}
