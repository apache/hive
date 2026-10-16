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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HMSCatalogAdapter.Route;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Original @ <a href="https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/test/java/org/apache/iceberg/rest/RESTCatalogServlet.java">RESTCatalogServlet.java</a>
 * The RESTCatalogServlet provides a servlet implementation used in combination with a
 * RESTCatalogAdaptor to proxy the REST Spec to any Catalog implementation.
 */
public class HMSCatalogServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogServlet.class);
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String APPLICATION_JSON = "application/json";
  
  private final HMSCatalogAdapter restCatalogAdapter;
  private final Map<String, String> responseHeaders =
      ImmutableMap.of(CONTENT_TYPE, APPLICATION_JSON);

  /**
   * Creates a servlet that dispatches Iceberg REST Catalog requests to {@code restCatalogAdapter}.
   *
   * @param restCatalogAdapter the adapter used to execute each request
   */
  public HMSCatalogServlet(HMSCatalogAdapter restCatalogAdapter) {
    this.restCatalogAdapter = restCatalogAdapter;
  }

  @Override
  public String getServletName() {
    return "Iceberg REST Catalog";
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    execute(ServletRequestContext.from(request), response);
  }

  @Override
  protected void doHead(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    execute(ServletRequestContext.from(request), response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    execute(ServletRequestContext.from(request), response);
  }

  @Override
  protected void doDelete(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    execute(ServletRequestContext.from(request), response);
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) {
    try {
      ServletRequestContext context = ServletRequestContext.from(request);
      execute(context, response);
    } catch (RuntimeException | IOException e) {
      LOG.error("Error processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  protected void execute(ServletRequestContext context, HttpServletResponse response)
      throws IOException {
    response.setStatus(HttpServletResponse.SC_OK);
    responseHeaders.forEach(response::setHeader);

    final Optional<ErrorResponse> error = context.error();
    if (error.isPresent()) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      RESTObjectMapper.mapper().writeValue(response.getWriter(), error.get());
      return;
    }

    try {
      Object responseBody =
          restCatalogAdapter.execute(
              context.method(),
              context.path(),
              context.headers(),
              context.queryParams(),
              context.body(),
              response);

      if (responseBody != null) {
        RESTObjectMapper.mapper().writeValue(response.getWriter(), responseBody);
      }
    } catch (RESTException e) {
      // A RESTException is thrown by HMSCatalogAdapter.execute() after the error handler has
      // already written the correct HTTP status and body to the response (e.g. 404, 403).
      // It is not an unexpected server failure, so log at DEBUG to avoid flooding the console.
      LOG.debug("REST request resulted in a client error (already handled): {}", e.getMessage());
    } catch (RuntimeException | IOException e) {
      // Genuine unexpected server error – log the full stack trace.
      LOG.error("Error processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOG.error("Unexpected exception when processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Package-private (non-private compared to upstream RESTCatalogServlet) so HMSCatalogAdapter
   * can reuse it to set 204 No Content without duplicating the route list.
   */
  static boolean shouldReturn204(Route route) {
    return route == Route.NAMESPACE_EXISTS
        || route == Route.TABLE_EXISTS
        || route == Route.VIEW_EXISTS
        || route == Route.DROP_NAMESPACE
        || route == Route.DROP_TABLE
        || route == Route.DROP_VIEW
        || route == Route.RENAME_TABLE
        || route == Route.RENAME_VIEW
        // TODO: Enable once server-side scan planning (Route.CANCEL_PLAN_TABLE_SCAN) is implemented
        // || route == Route.CANCEL_PLAN_TABLE_SCAN
        || route == Route.REPORT_METRICS
        || route == Route.COMMIT_TRANSACTION;
  }

  @Override
  public void destroy() {
    super.destroy();
    restCatalogAdapter.close();
  }

  /**
   * Parsed view of a servlet request, or the {@link ErrorResponse} to return instead if parsing
   * the request failed.
   */
  public static class ServletRequestContext {
    private HTTPMethod method;
    private Route route;
    private String path;
    private Map<String, String> headers = Map.of();
    private Map<String, String> queryParams;
    private Object body;

    private ErrorResponse errorResponse;

    private ServletRequestContext(ErrorResponse errorResponse) {
      this.errorResponse = errorResponse;
    }

    private ServletRequestContext(
        HTTPMethod method,
        Route route,
        String path,
        Map<String, String> headers,
        Map<String, String> queryParams,
        Object body) {
      this.method = method;
      this.route = route;
      this.path = path;
      this.headers = headers;
      this.queryParams = queryParams;
      this.body = body;
    }

    static ServletRequestContext from(HttpServletRequest request) throws IOException {
      HTTPMethod method = HTTPMethod.valueOf(request.getMethod());
      // path = uri - context-path + servlet-path + /
      String path = request.getPathInfo();
      if (path == null) {
        path = request.getRequestURI().substring(
            request.getContextPath().length() + request.getServletPath().length());
      }
      // remove leading /
      path = path.substring(1);
      Pair<Route, Map<String, String>> routeContext = Route.from(method, path);

      if (routeContext == null) {
        return new ServletRequestContext(
            ErrorResponse.builder()
                .responseCode(400)
                .withType("BadRequestException")
                .withMessage(String.format("No route for request: %s %s", method, path))
                .build());
      }

      Route route = routeContext.first();
      Object requestBody = null;
      if (route.requestClass() != null) {
        try {
          requestBody = RESTObjectMapper.mapper().readValue(request.getReader(), route.requestClass());
        } catch (Exception e) {
          return new ServletRequestContext(ErrorResponse.builder()
              .responseCode(400)
              .withType(e.getClass().getSimpleName())
              .withMessage(e.getMessage())
              .build());
        }
      }

      Map<String, String> headers =
          request.getHeaderNames() != null
              ? Collections.list(request.getHeaderNames()).stream()
                  .collect(
                      Collectors.toMap(
                          Function.identity(),
                          request::getHeader,
                          (v1, v2) -> v2,
                          () -> Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER)))
              : Map.of();

      Map<String, String> queryParams =
          request.getParameterMap().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()[0]));

      return new ServletRequestContext(method, route, path, headers, queryParams, requestBody);
    }

    public HTTPMethod method() {
      return method;
    }

    public Route route() {
      return route;
    }

    /**
     * The request path, relative to the servlet, with the leading '/' removed.
     * @return the request path
     */
    public String path() {
      return path;
    }

    public Map<String, String> headers() {
      return headers;
    }

    /**
     * The request's query parameters, keyed by parameter name.
     * @return the query parameters
     */
    public Map<String, String> queryParams() {
      return queryParams;
    }

    /**
     * The deserialized request body, or {@code null} if the route expects no request body.
     * @return the request body
     */
    public Object body() {
      return body;
    }

    /**
     * The error to return instead of dispatching the request, if parsing the request failed.
     * @return the error, if any
     */
    public Optional<ErrorResponse> error() {
      return Optional.ofNullable(errorResponse);
    }
  }
}
