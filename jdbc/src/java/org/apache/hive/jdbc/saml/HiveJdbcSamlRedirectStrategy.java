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
import org.apache.hive.jdbc.saml.IJdbcBrowserClient.JdbcBrowserClientContext;
import org.apache.hive.service.auth.saml.HiveSamlUtils;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.protocol.HttpContext;

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
    int status = response.getStatusLine().getStatusCode();
    if (status == HttpStatus.SC_MOVED_TEMPORARILY || status == HttpStatus.SC_SEE_OTHER) {
      URI locationUri = getLocationURI(request, response, context);
      Header clientIdentifier = response
          .getFirstHeader(HiveSamlUtils.SSO_CLIENT_IDENTIFIER);
      IJdbcBrowserClient.JdbcBrowserClientContext browserClientContext = new JdbcBrowserClientContext(
          locationUri, clientIdentifier.getValue());
      browserClient.init(browserClientContext);
    }
    return super.isRedirected(request, response, context);
  }

  @Override
  public URI getLocationURI(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
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
