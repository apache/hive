/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import com.codahale.metrics.Counter;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Original @ https://github.com/apache/iceberg/blob/1.6.x/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java
 * Adaptor class to translate REST requests into {@link Catalog} API calls.
 */
public class HMSCatalogAdapter implements RESTClient {
  /**  The metric names prefix. */
  static final String HMS_METRIC_PREFIX = "hmscatalog.";
  private static final Splitter SLASH = Splitter.on('/');

  private static final Map<Class<? extends Exception>, Integer> EXCEPTION_ERROR_CODES =
      ImmutableMap.<Class<? extends Exception>, Integer>builder()
          .put(IllegalArgumentException.class, 400)
          .put(ValidationException.class, 400)
          .put(NamespaceNotEmptyException.class, 409)
          .put(NotAuthorizedException.class, 401)
          .put(ForbiddenException.class, 403)
          .put(NoSuchNamespaceException.class, 404)
          .put(NoSuchTableException.class, 404)
          .put(NoSuchViewException.class, 404)
          .put(NoSuchIcebergTableException.class, 404)
          .put(UnsupportedOperationException.class, 406)
          .put(AlreadyExistsException.class, 409)
          .put(CommitFailedException.class, 409)
          .put(UnprocessableEntityException.class, 422)
          .put(CommitStateUnknownException.class, 500)
          .buildOrThrow();

  private static final String URN_OAUTH_TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange";
  private static final String URN_OAUTH_ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token";
  private static final String GRANT_TYPE = "grant_type";
  private static final String CLIENT_CREDENTIALS = "client_credentials";
  private static final String BEARER = "Bearer";
  private static final String CLIENT_ID = "client_id";
  private static final String ACTOR_TOKEN = "actor_token";
  private static final String SUBJECT_TOKEN = "subject_token";
  private static final String VIEWS_PATH = "v1/namespaces/{namespace}/views/{name}";
  private static final String TABLES_PATH = "v1/namespaces/{namespace}/tables/{table}";

  private final Catalog catalog;
  private final SupportsNamespaces asNamespaceCatalog;
  private final ViewCatalog asViewCatalog;


  public HMSCatalogAdapter(HiveCatalog catalog) {
    this.catalog = catalog;
    this.asNamespaceCatalog = catalog;
    this.asViewCatalog = catalog;
  }

  enum HTTPMethod {
    GET,
    HEAD,
    POST,
    DELETE
  }

  enum Route {
    TOKENS(HTTPMethod.POST, "v1/oauth/tokens",
            null, OAuthTokenResponse.class),
    SEPARATE_AUTH_TOKENS_URI(HTTPMethod.POST, "https://auth-server.com/token",
            null, OAuthTokenResponse.class),
    CONFIG(HTTPMethod.GET, "v1/config",
            null, ConfigResponse.class),
    LIST_NAMESPACES(HTTPMethod.GET, "v1/namespaces",
            null, ListNamespacesResponse.class),
    CREATE_NAMESPACE(HTTPMethod.POST, "v1/namespaces",
            CreateNamespaceRequest.class, CreateNamespaceResponse.class),
    LOAD_NAMESPACE(HTTPMethod.GET, "v1/namespaces/{namespace}",
            null, GetNamespaceResponse.class),
    DROP_NAMESPACE(HTTPMethod.DELETE, "v1/namespaces/{namespace}"),
    UPDATE_NAMESPACE(HTTPMethod.POST, "v1/namespaces/{namespace}/properties",
            UpdateNamespacePropertiesRequest.class, UpdateNamespacePropertiesResponse.class),
    LIST_TABLES(HTTPMethod.GET, "v1/namespaces/{namespace}/tables",
            null, ListTablesResponse.class),
    CREATE_TABLE(HTTPMethod.POST, "v1/namespaces/{namespace}/tables",
            CreateTableRequest.class, LoadTableResponse.class),
    LOAD_TABLE(HTTPMethod.GET, TABLES_PATH,
            null, LoadTableResponse.class),
    REGISTER_TABLE(HTTPMethod.POST, "v1/namespaces/{namespace}/register",
            RegisterTableRequest.class, LoadTableResponse.class),
    UPDATE_TABLE(HTTPMethod.POST, TABLES_PATH,
            UpdateTableRequest.class, LoadTableResponse.class),
    DROP_TABLE(HTTPMethod.DELETE, TABLES_PATH),
    RENAME_TABLE(HTTPMethod.POST, "v1/tables/rename",
            RenameTableRequest.class, null),
    REPORT_METRICS(HTTPMethod.POST, "v1/namespaces/{namespace}/tables/{table}/metrics",
            ReportMetricsRequest.class, null),
    COMMIT_TRANSACTION(HTTPMethod.POST, "v1/transactions/commit",
            CommitTransactionRequest.class, null),
    LIST_VIEWS(HTTPMethod.GET, "v1/namespaces/{namespace}/views",
            null, ListTablesResponse.class),
    LOAD_VIEW(HTTPMethod.GET, VIEWS_PATH,
            null, LoadViewResponse.class),
    CREATE_VIEW(HTTPMethod.POST, "v1/namespaces/{namespace}/views",
            CreateViewRequest.class, LoadViewResponse.class),
    UPDATE_VIEW(HTTPMethod.POST, VIEWS_PATH,
            UpdateTableRequest.class, LoadViewResponse.class),
    RENAME_VIEW(HTTPMethod.POST, "v1/views/rename",
            RenameTableRequest.class, null),
    DROP_VIEW(HTTPMethod.DELETE, VIEWS_PATH);

    private final HTTPMethod method;
    private final int requiredLength;
    private final Map<Integer, String> requirements;
    private final Map<Integer, String> variables;
    private final Class<? extends RESTRequest> requestClass;
    private final Class<? extends RESTResponse> responseClass;

    /**
     * An exception safe way of getting a route by name.
     *
     * @param name the route name
     * @return the route instance or null if it could not be found
     */
    static Route byName(String name) {
      try {
        return valueOf(name.toUpperCase());
      } catch (IllegalArgumentException xill) {
        return null;
      }
    }

    Route(HTTPMethod method, String pattern) {
      this(method, pattern, null, null);
    }

    Route(HTTPMethod method, String pattern,
        Class<? extends RESTRequest> requestClass,
        Class<? extends RESTResponse> responseClass
    ) {
      this.method = method;
      // parse the pattern into requirements and variables
      List<String> parts = SLASH.splitToList(pattern);
      ImmutableMap.Builder<Integer, String> requirementsBuilder = ImmutableMap.builder();
      ImmutableMap.Builder<Integer, String> variablesBuilder = ImmutableMap.builder();
      for (int pos = 0; pos < parts.size(); pos += 1) {
        String part = parts.get(pos);
        if (part.startsWith("{") && part.endsWith("}")) {
          variablesBuilder.put(pos, part.substring(1, part.length() - 1));
        } else {
          requirementsBuilder.put(pos, part);
        }
      }
      this.requestClass = requestClass;
      this.responseClass = responseClass;
      this.requiredLength = parts.size();
      this.requirements = requirementsBuilder.build();
      this.variables = variablesBuilder.build();
    }

    private boolean matches(HTTPMethod requestMethod, List<String> requestPath) {
      return method == requestMethod &&
          requiredLength == requestPath.size() &&
          requirements.entrySet().stream()
              .allMatch(
                  requirement ->
                      requirement
                          .getValue()
                          .equalsIgnoreCase(requestPath.get(requirement.getKey())));
    }

    private Map<String, String> variables(List<String> requestPath) {
      ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
      variables.forEach((key, value) -> vars.put(value, requestPath.get(key)));
      return vars.build();
    }

    public static Pair<Route, Map<String, String>> from(HTTPMethod method, String path) {
      List<String> parts = SLASH.splitToList(path);
      for (Route candidate : Route.values()) {
        if (candidate.matches(method, parts)) {
          return Pair.of(candidate, candidate.variables(parts));
        }
      }

      return null;
    }

    public Class<? extends RESTRequest> requestClass() {
      return requestClass;
    }

    public Class<? extends RESTResponse> responseClass() {
      return responseClass;
    }
  }

  /**
   * @param route a route/api-call name
   * @return the metric counter name for the api-call
   */
  static String hmsCatalogMetricCount(String route) {
    return HMS_METRIC_PREFIX + route.toLowerCase() + ".count";
  }

  /**
   * @param apis an optional list of known api call names
   * @return the list of metric names for the HMSCatalog class
   */
  public static List<String> getMetricNames(String... apis) {
    final List<Route> routes;
    if (apis != null && apis.length > 0) {
      routes = Arrays.stream(apis)
          .map(HMSCatalogAdapter.Route::byName)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    } else {
      routes = Arrays.asList(HMSCatalogAdapter.Route.values());
    }
    final List<String> metricNames = new ArrayList<>(routes.size());
    for (HMSCatalogAdapter.Route route : routes) {
      metricNames.add(hmsCatalogMetricCount(route.name()));
    }
    return metricNames;
  }

  private ConfigResponse config() {
    return castResponse(ConfigResponse.class, ConfigResponse.builder().build());
  }

  private OAuthTokenResponse tokens(Object body) {
    @SuppressWarnings("unchecked")
    Map<String, String> request = (Map<String, String>) castRequest(Map.class, body);
    String grantType = request.get(GRANT_TYPE);
    switch (grantType) {
      case CLIENT_CREDENTIALS:
        return OAuthTokenResponse.builder()
            .withToken("client-credentials-token:sub=" + request.get(CLIENT_ID))
            .withIssuedTokenType(URN_OAUTH_ACCESS_TOKEN)
            .withTokenType(BEARER)
            .build();

      case URN_OAUTH_TOKEN_EXCHANGE:
        String actor = request.get(ACTOR_TOKEN);
        String token =
            String.format(
                "token-exchange-token:sub=%s%s",
                request.get(SUBJECT_TOKEN), actor != null ? ",act=" + actor : "");
        return OAuthTokenResponse.builder()
            .withToken(token)
            .withIssuedTokenType(URN_OAUTH_ACCESS_TOKEN)
            .withTokenType(BEARER)
            .build();

      default:
        throw new UnsupportedOperationException("Unsupported grant_type: " + grantType);
    }
  }

  private ListNamespacesResponse listNamespaces(Map<String, String> vars) {
    Namespace namespace;
    if (vars.containsKey("parent")) {
      namespace = Namespace.of(RESTUtil.NAMESPACE_SPLITTER.splitToStream(vars.get("parent")).toArray(String[]::new));
    } else {
      namespace = Namespace.empty();
    }
    return castResponse(ListNamespacesResponse.class, CatalogHandlers.listNamespaces(asNamespaceCatalog, namespace));
  }

  private CreateNamespaceResponse createNamespace(Object body) {
    CreateNamespaceRequest request = castRequest(CreateNamespaceRequest.class, body);
    return castResponse(
        CreateNamespaceResponse.class, CatalogHandlers.createNamespace(asNamespaceCatalog, request));
  }

  private GetNamespaceResponse loadNamespace(Map<String, String> vars) {
    Namespace namespace = namespaceFromPathVars(vars);
    return castResponse(GetNamespaceResponse.class, CatalogHandlers.loadNamespace(asNamespaceCatalog, namespace));
  }

  private RESTResponse dropNamespace(Map<String, String> vars) {
    CatalogHandlers.dropNamespace(asNamespaceCatalog, namespaceFromPathVars(vars));
    return null;
  }

  private UpdateNamespacePropertiesResponse updateNamespace(Map<String, String> vars, Object body) {
    Namespace namespace = namespaceFromPathVars(vars);
    UpdateNamespacePropertiesRequest request = castRequest(UpdateNamespacePropertiesRequest.class, body);
    return castResponse(UpdateNamespacePropertiesResponse.class,
        CatalogHandlers.updateNamespaceProperties(asNamespaceCatalog, namespace, request));
  }

  private ListTablesResponse listTables(Map<String, String> vars) {
    Namespace namespace = namespaceFromPathVars(vars);
    return castResponse(ListTablesResponse.class, CatalogHandlers.listTables(catalog, namespace));
  }

  private LoadTableResponse createTable(Map<String, String> vars, Object body) {
    final Class<LoadTableResponse> responseType = LoadTableResponse.class;
    Namespace namespace = namespaceFromPathVars(vars);
    CreateTableRequest request = castRequest(CreateTableRequest.class, body);
    request.validate();
    if (request.stageCreate()) {
      return castResponse(responseType, CatalogHandlers.stageTableCreate(catalog, namespace, request));
    } else {
      return castResponse(responseType, CatalogHandlers.createTable(catalog, namespace, request));
    }
  }

  private RESTResponse dropTable(Map<String, String> vars) {
    if (PropertyUtil.propertyAsBoolean(vars, "purgeRequested", false)) {
      CatalogHandlers.purgeTable(catalog, identFromPathVars(vars));
    } else {
      CatalogHandlers.dropTable(catalog, identFromPathVars(vars));
    }
    return null;
  }

  private LoadTableResponse loadTable(Map<String, String> vars) {
    TableIdentifier ident = identFromPathVars(vars);
    return castResponse(LoadTableResponse.class, CatalogHandlers.loadTable(catalog, ident));
  }

  private LoadTableResponse registerTable(Map<String, String> vars, Object body) {
      Namespace namespace = namespaceFromPathVars(vars);
      RegisterTableRequest request = castRequest(RegisterTableRequest.class, body);
      return castResponse(LoadTableResponse.class, CatalogHandlers.registerTable(catalog, namespace, request));
  }

  private LoadTableResponse updateTable(Map<String, String> vars, Object body) {
    TableIdentifier ident = identFromPathVars(vars);
    UpdateTableRequest request = castRequest(UpdateTableRequest.class, body);
    return castResponse(LoadTableResponse.class, CatalogHandlers.updateTable(catalog, ident, request));
  }

  private RESTResponse renameTable(Object body) {
    RenameTableRequest request = castRequest(RenameTableRequest.class, body);
    CatalogHandlers.renameTable(catalog, request);
    return null;
  }

  private RESTResponse reportMetrics(Object body) {
    // nothing to do here other than checking that we're getting the correct request
    castRequest(ReportMetricsRequest.class, body);
    return null;
  }

  private RESTResponse commitTransaction(Object body) {
    CommitTransactionRequest request = castRequest(CommitTransactionRequest.class, body);
    commitTransaction(catalog, request);
    return null;
  }

  private ListTablesResponse listViews(Map<String, String> vars) {
    Namespace namespace = namespaceFromPathVars(vars);
    String pageToken = PropertyUtil.propertyAsString(vars, "pageToken", null);
    String pageSize = PropertyUtil.propertyAsString(vars, "pageSize", null);
    if (pageSize != null) {
      return castResponse(ListTablesResponse.class,
          CatalogHandlers.listViews(asViewCatalog, namespace, pageToken, pageSize));
    } else {
      return castResponse(ListTablesResponse.class, CatalogHandlers.listViews(asViewCatalog, namespace));
    }
  }

  private LoadViewResponse createView(Map<String, String> vars, Object body) {
    Namespace namespace = namespaceFromPathVars(vars);
    CreateViewRequest request = castRequest(CreateViewRequest.class, body);
    return castResponse(LoadViewResponse.class, CatalogHandlers.createView(asViewCatalog, namespace, request));
  }

  private LoadViewResponse loadView(Map<String, String> vars) {
    TableIdentifier ident = identFromPathVars(vars);
    return castResponse(LoadViewResponse.class, CatalogHandlers.loadView(asViewCatalog, ident));
  }

  private LoadViewResponse updateView(Map<String, String> vars, Object body) {
    TableIdentifier ident = identFromPathVars(vars);
    UpdateTableRequest request = castRequest(UpdateTableRequest.class, body);
    return castResponse(LoadViewResponse.class, CatalogHandlers.updateView(asViewCatalog, ident, request));
  }

  private RESTResponse renameView(Object body) {
    RenameTableRequest request = castRequest(RenameTableRequest.class, body);
    CatalogHandlers.renameView(asViewCatalog, request);
    return null;
  }

  private RESTResponse dropView(Map<String, String> vars) {
    CatalogHandlers.dropView(asViewCatalog, identFromPathVars(vars));
    return null;
  }

  /**
   * This is a very simplistic approach that only validates the requirements for each table and does
   * not do any other conflict detection. Therefore, it does not guarantee true transactional
   * atomicity, which is left to the implementation details of a REST server.
   */
  private static void commitTransaction(Catalog catalog, CommitTransactionRequest request) {
    List<Transaction> transactions = Lists.newArrayList();

    for (UpdateTableRequest tableChange : request.tableChanges()) {
      Table table = catalog.loadTable(tableChange.identifier());
      if (table instanceof BaseTable) {
        Transaction transaction =
            Transactions.newTransaction(
                tableChange.identifier().toString(), ((BaseTable) table).operations());
        transactions.add(transaction);

        BaseTransaction.TransactionTable txTable =
            (BaseTransaction.TransactionTable) transaction.table();

        // this performs validations and makes temporary commits that are in-memory
        CatalogHandlers.commit(txTable.operations(), tableChange);
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }
    // only commit if validations passed previously
    transactions.forEach(Transaction::commitTransaction);
  }
  
  @SuppressWarnings({"MethodLength", "unchecked"})
  private <T extends RESTResponse> T handleRequest(
      Route route, Map<String, String> vars, Object body) {
    // update HMS catalog route counter metric
    final String metricName = hmsCatalogMetricCount(route.name());
    Counter counter = Metrics.getOrCreateCounter(metricName);
    if (counter != null) {
      counter.inc();
    }
    switch (route) {
      case TOKENS:
        return (T) tokens(body);

      case CONFIG:
        return (T) config();

      case LIST_NAMESPACES:
        return (T) listNamespaces(vars);

      case CREATE_NAMESPACE:
        return (T) createNamespace(body);

      case LOAD_NAMESPACE:
        return (T) loadNamespace(vars);

      case DROP_NAMESPACE:
        return (T) dropNamespace(vars);

      case UPDATE_NAMESPACE:
        return (T) updateNamespace(vars, body);

      case LIST_TABLES:
        return (T) listTables(vars);

      case CREATE_TABLE:
        return (T) createTable(vars, body);

      case DROP_TABLE:
        return (T) dropTable(vars);

      case LOAD_TABLE:
        return (T) loadTable(vars);

      case REGISTER_TABLE:
        return (T) registerTable(vars, body);

      case UPDATE_TABLE:
        return (T) updateTable(vars, body);

      case RENAME_TABLE:
        return (T) renameTable(body);

      case REPORT_METRICS:
        return (T) reportMetrics(body);

      case COMMIT_TRANSACTION:
        return (T) commitTransaction(body);
        
      case LIST_VIEWS:
        return (T) listViews(vars);

      case CREATE_VIEW:
          return (T) createView(vars, body);

      case LOAD_VIEW:
        return (T) loadView(vars);

      case UPDATE_VIEW:
        return (T) updateView(vars, body);
        
      case RENAME_VIEW:
        return (T) renameView(vars);
        
      case DROP_VIEW:
        return (T) dropView(vars);

      default:
    }
    return null;
  }


  <T extends RESTResponse> T execute(
      HTTPMethod method,
      String path,
      Map<String, String> queryParams,
      Object body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    ErrorResponse.Builder errorBuilder = ErrorResponse.builder();
    Pair<Route, Map<String, String>> routeAndVars = Route.from(method, path);
    if (routeAndVars != null) {
      try {
        ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
        if (queryParams != null) {
          vars.putAll(queryParams);
        }
        vars.putAll(routeAndVars.second());
        return handleRequest(routeAndVars.first(), vars.build(), body);
      } catch (RuntimeException e) {
        configureResponseFromException(e, errorBuilder);
      }
    } else {
      errorBuilder
          .responseCode(400)
          .withType("BadRequestException")
          .withMessage(String.format("No route for request: %s %s", method, path));
    }
    ErrorResponse error = errorBuilder.build();
    errorHandler.accept(error);
    // if the error handler doesn't throw an exception, throw a generic one
    throw new RESTException("Unhandled error: %s", error);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.DELETE, path, null, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.DELETE, path, queryParams, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.POST, path, null, body, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.GET, path, queryParams, null, responseType, headers, errorHandler);
  }

  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    execute(HTTPMethod.HEAD, path, null, null, null, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(HTTPMethod.POST, path, null, formData, responseType, headers, errorHandler);
  }

  @Override
  public void close() {
    // The caller is responsible for closing the underlying catalog backing this REST catalog.
  }

  private static class BadResponseType extends RuntimeException {
    private BadResponseType(Class<?> responseType, Object response) {
      super(
          String.format("Invalid response object, not a %s: %s", responseType.getName(), response));
    }
  }

  private static class BadRequestType extends RuntimeException {
    private BadRequestType(Class<?> requestType, Object request) {
      super(String.format("Invalid request object, not a %s: %s", requestType.getName(), request));
    }
  }

  public static <T> T castRequest(Class<T> requestType, Object request) {
    if (requestType.isInstance(request)) {
      return requestType.cast(request);
    }
    throw new BadRequestType(requestType, request);
  }

  public static <T extends RESTResponse> T castResponse(Class<T> responseType, Object response) {
    if (responseType.isInstance(response)) {
      return responseType.cast(response);
    }
    throw new BadResponseType(responseType, response);
  }

  public static void configureResponseFromException(
      Exception exc, ErrorResponse.Builder errorBuilder) {
    errorBuilder
        .responseCode(EXCEPTION_ERROR_CODES.getOrDefault(exc.getClass(), 500))
        .withType(exc.getClass().getSimpleName())
        .withMessage(exc.getMessage())
        .withStackTrace(exc);
  }

  private static Namespace namespaceFromPathVars(Map<String, String> pathVars) {
    return RESTUtil.decodeNamespace(pathVars.get("namespace"));
  }

  private static TableIdentifier identFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("table")));
  }
}
