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
import java.net.URI;

import org.apache.hc.client5.http.impl.DefaultRedirectStrategy;
import org.apache.hc.client5.http.protocol.RedirectStrategy;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ProtocolException;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hive.jdbc.saml.IJdbcBrowserClient.JdbcBrowserClientContext;
import org.apache.hive.service.auth.saml.HiveSamlUtils;

/**
 * This is an implementation of {@link RedirectStrategy} to intercept the HTTP redirect
 * response received from the server in a browser mode auth flow. This is mainly used
 * to get the redirect URL from the HTTP redirect response since HttpThrift client does
 * not expose such information when the server throws a HTTP 302 redirect as response.
 * The redirect URL is then used to initialize the {@link IJdbcBrowserClient} so that it
 * can do the browser based SSO.
 */
public class HiveJdbcSamlRedirectStrategy extends DefaultRedirectStrategy {
  private final IJdbcBrowserClient browserClient;

  public HiveJdbcSamlRedirectStrategy(IJdbcBrowserClient browserClient) {
    this.browserClient = Preconditions.checkNotNull(browserClient);
  }

  @Override
  public boolean isRedirected(
      final HttpRequest request,
      final HttpResponse response,
      final HttpContext context) throws ProtocolException {
    int status = response.getCode();
    if (status == HttpStatus.SC_MOVED_TEMPORARILY || status == HttpStatus.SC_SEE_OTHER) {
      // Only the HS2-originated SAML redirect carries the SSO_CLIENT_IDENTIFIER header.
      // When we see it, capture the redirect location + identifier for the browser
      // client, then return false so httpclient5 does NOT transparently follow the
      // redirect. Letting the 302 propagate back to THttpClient is what allows
      // HiveConnection.isSamlRedirect() to detect it and drive the browser SSO flow.
      // For any other 302/303 (intermediate IDP redirects, unrelated traffic) fall
      // through to the superclass's default handling.
      Header clientIdentifier = response
          .getFirstHeader(HiveSamlUtils.SSO_CLIENT_IDENTIFIER);
      if (clientIdentifier != null) {
        URI locationUri;
        try {
          locationUri = getLocationURI(request, response, context);
        } catch (HttpException e) {
          throw new ProtocolException(e.getMessage(), e);
        }
        IJdbcBrowserClient.JdbcBrowserClientContext browserClientContext = new JdbcBrowserClientContext(
            locationUri, clientIdentifier.getValue());
        browserClient.init(browserClientContext);
        return false;
      }
    }
    return super.isRedirected(request, response, context);
  }

  @Override
  public URI getLocationURI(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException {
    // add our own check to super-call
    return checkSsoUri(super.getLocationURI(request, response, context));
  }

  /**
   * Checks that the URI used to redirect SSO is valid.
   * @param uri the uri to validate
   * @return the uri
   * @throws ProtocolException if uri is null or not http(s) or not absolute
   */
  static URI checkSsoUri(URI uri) throws ProtocolException {
    if (uri == null) {
      throw new ProtocolException("SSO Url is null");
    }
    final String scheme = uri.getScheme();
    // require https or https and absolute
    final boolean valid = ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                          && uri.isAbsolute();
    if (!valid) {
      throw new ProtocolException("SSO Url "+uri.toString()+ "is invalid");
    }
    return uri;
  }
}
