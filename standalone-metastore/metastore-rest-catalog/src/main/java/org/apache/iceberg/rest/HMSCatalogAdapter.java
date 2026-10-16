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

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import javax.servlet.http.HttpServletResponse;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchIcebergViewException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.metrics.IcebergMetricsReporter;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
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
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Original @ <a href="https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java">RESTCatalogAdapter.java</a>
 * Adaptor class to translate REST requests into {@link Catalog} API calls.
 */
public class HMSCatalogAdapter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogAdapter.class);
  private static final Splitter SLASH = Splitter.on('/').omitEmptyStrings();
  private static final Splitter COMMA = Splitter.on(',').trimResults().omitEmptyStrings();

  private static final String PAGE_TOKEN = "pageToken";
  private static final String PAGE_SIZE = "pageSize";
  private static final String PARENT = "parent";

  private static final String PREFIX_VAR = "prefix";
  private static final String PREFIX_PLACEHOLDER = "{" + PREFIX_VAR + "}";

  /** Index of the first prefix segment, right after "v1". */
  private static final int PREFIX_START = 1;

  private static final String ETAG = "ETag";
  private static final String IF_NONE_MATCH = "If-None-Match";
  private static final Map<String, String> DEFAULT_SNAPSHOT_QUERY_PARAMS =
      ImmutableMap.of(
          RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER,
          RESTCatalogProperties.SNAPSHOT_LOADING_MODE_DEFAULT.name().toLowerCase(Locale.ROOT));
  private static final Map<String, String> REFS_SNAPSHOT_QUERY_PARAMS =
      ImmutableMap.of(
          RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER,
          RESTCatalogProperties.SnapshotMode.REFS.name().toLowerCase(Locale.ROOT));

  private static final Map<Class<? extends Exception>, Integer> EXCEPTION_ERROR_CODES =
      ImmutableMap.<Class<? extends Exception>, Integer>builder()
          .put(IllegalArgumentException.class, 400)
          .put(ValidationException.class, 400)
          .put(NamespaceNotEmptyException.class, 409)
          .put(NotAuthorizedException.class, 401)
          .put(ForbiddenException.class, 403)
          .put(NoSuchNamespaceException.class, 404)
          .put(NoSuchTableException.class, 404)
          .put(NotFoundException.class, 404)
          .put(NoSuchViewException.class, 404)
          .put(NoSuchIcebergTableException.class, 404)
          .put(NoSuchIcebergViewException.class, 404)
          .put(UnsupportedOperationException.class, 406)
          .put(AlreadyExistsException.class, 409)
          .put(CommitFailedException.class, 409)
          .put(UnprocessableEntityException.class, 422)
          .put(CommitStateUnknownException.class, 500)
          .buildOrThrow();

  private final String catalogName;
  private final Catalog catalog;
  private final SupportsNamespaces asNamespaceCatalog;
  private final ViewCatalog asViewCatalog;
  private final IcebergAuthorizer icebergAuthorizer;
  private final List<IcebergMetricsReporter> metricsReporters;
  private final Clock clock = Clock.systemUTC();

  public HMSCatalogAdapter(String catalogName, Catalog catalog, IcebergAuthorizer icebergAuthorizer,
      List<IcebergMetricsReporter> metricsReporters) {
    Preconditions.checkArgument(catalog instanceof SupportsNamespaces);
    Preconditions.checkArgument(catalog instanceof ViewCatalog);
    this.catalogName = catalogName;
    this.catalog = catalog;
    this.asNamespaceCatalog = (SupportsNamespaces) catalog;
    this.asViewCatalog = (ViewCatalog) catalog;
    this.icebergAuthorizer = icebergAuthorizer;
    this.metricsReporters = metricsReporters;
  }

  enum Route {
    CONFIG(HTTPMethod.GET, ResourcePaths.config(), null),
    LIST_NAMESPACES(HTTPMethod.GET, ResourcePaths.V1_NAMESPACES, null),
    CREATE_NAMESPACE(HTTPMethod.POST, ResourcePaths.V1_NAMESPACES, CreateNamespaceRequest.class),
    NAMESPACE_EXISTS(HTTPMethod.HEAD, ResourcePaths.V1_NAMESPACE),
    LOAD_NAMESPACE(HTTPMethod.GET, ResourcePaths.V1_NAMESPACE, null),
    DROP_NAMESPACE(HTTPMethod.DELETE, ResourcePaths.V1_NAMESPACE),
    UPDATE_NAMESPACE(HTTPMethod.POST, ResourcePaths.V1_NAMESPACE_PROPERTIES, UpdateNamespacePropertiesRequest.class),
    LIST_TABLES(HTTPMethod.GET, ResourcePaths.V1_TABLES, null),
    CREATE_TABLE(HTTPMethod.POST, ResourcePaths.V1_TABLES, CreateTableRequest.class),
    TABLE_EXISTS(HTTPMethod.HEAD, ResourcePaths.V1_TABLE),
    LOAD_TABLE(HTTPMethod.GET, ResourcePaths.V1_TABLE, null),
    REGISTER_TABLE(HTTPMethod.POST, ResourcePaths.V1_TABLE_REGISTER, RegisterTableRequest.class),
    UPDATE_TABLE(HTTPMethod.POST, ResourcePaths.V1_TABLE, UpdateTableRequest.class),
    DROP_TABLE(HTTPMethod.DELETE, ResourcePaths.V1_TABLE),
    RENAME_TABLE(HTTPMethod.POST, ResourcePaths.V1_TABLE_RENAME, RenameTableRequest.class),
    REPORT_METRICS(HTTPMethod.POST, ResourcePaths.V1_TABLE_METRICS, ReportMetricsRequest.class),
    COMMIT_TRANSACTION(HTTPMethod.POST, ResourcePaths.V1_TRANSACTIONS_COMMIT, CommitTransactionRequest.class),
    LIST_VIEWS(HTTPMethod.GET, ResourcePaths.V1_VIEWS, null),
    VIEW_EXISTS(HTTPMethod.HEAD, ResourcePaths.V1_VIEW),
    LOAD_VIEW(HTTPMethod.GET, ResourcePaths.V1_VIEW, null),
    CREATE_VIEW(HTTPMethod.POST, ResourcePaths.V1_VIEWS, CreateViewRequest.class),
    UPDATE_VIEW(HTTPMethod.POST, ResourcePaths.V1_VIEW, UpdateTableRequest.class),
    RENAME_VIEW(HTTPMethod.POST, ResourcePaths.V1_VIEW_RENAME, RenameTableRequest.class),
    DROP_VIEW(HTTPMethod.DELETE, ResourcePaths.V1_VIEW),
    REGISTER_VIEW(HTTPMethod.POST, ResourcePaths.V1_VIEW_REGISTER, RegisterViewRequest.class);

    private final HTTPMethod method;
    private final int requiredLength;
    private final Map<Integer, String> requirements;
    private final Map<Integer, String> variables;
    private final Class<? extends RESTRequest> requestClass;
    private final String pathTemplate;
    private final boolean acceptsPrefix;

    Route(HTTPMethod method, String pattern) {
      this(method, pattern, null);
    }

    Route(
        HTTPMethod method,
        String pattern,
        Class<? extends RESTRequest> requestClass) {
      this.method = method;
      this.pathTemplate = pattern;

      List<String> segments = SLASH.splitToList(pattern);
      this.acceptsPrefix = segments.contains(PREFIX_PLACEHOLDER);

      // parse the pattern into requirements and variables
      List<String> parts = segments.stream().filter(s -> !PREFIX_PLACEHOLDER.equals(s)).toList();
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
      this.requiredLength = parts.size();
      this.requirements = requirementsBuilder.build();
      this.variables = variablesBuilder.build();
    }

    /** Number of extra segments in the request, i.e. the prefix length. */
    private int prefixLength(List<String> requestPath) {
      return requestPath.size() - requiredLength;
    }

    /** Maps a template index to a request-path index, skipping prefix segments. */
    private int mappedIndex(int templateIndex, int prefixLength) {
      return templateIndex < PREFIX_START ? templateIndex : templateIndex + prefixLength;
    }

    private boolean matches(HTTPMethod requestMethod, List<String> requestPath) {
      if (method != requestMethod) {
        return false;
      }

      // A multi-segment prefix like "catalogs/my_catalog" gives prefixLength == 2
      int prefixLength = prefixLength(requestPath);

      // If the path is too short, or too long but the route doesn't support a prefix, reject.
      if (prefixLength < 0 || (prefixLength > 0 && !acceptsPrefix)) {
        return false;
      }

      for (Map.Entry<Integer, String> requirement : requirements.entrySet()) {
        String actual = requestPath.get(mappedIndex(requirement.getKey(), prefixLength));
        if (!requirement.getValue().equalsIgnoreCase(actual)) {
          return false;
        }
      }
      return true;
    }

    private Map<String, String> variables(List<String> requestPath) {
      int prefixLength = prefixLength(requestPath);

      ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
      for (Map.Entry<Integer, String> var : variables.entrySet()) {
        vars.put(var.getValue(), requestPath.get(mappedIndex(var.getKey(), prefixLength)));
      }

      if (prefixLength > 0) {
        // Clients insert the configured prefix verbatim, so "catalogs/sales" arrives
        // as two segments; rejoin them. An encoded %2F inside a prefix is not preserved.
        String prefix = String.join("/",
            requestPath.subList(PREFIX_START, PREFIX_START + prefixLength));
        // The HMS backend serves a single HiveCatalog and does not scope by prefix yet.
        LOG.debug("Ignoring request prefix '{}' for route {}", prefix, this);
        vars.put(PREFIX_VAR, prefix);
      }
      return vars.build();
    }

    public static Pair<Route, Map<String, String>> from(HTTPMethod method, String path) {
      List<String> parts = SLASH.splitToList(path);
      Route best = null;
      for (Route candidate : Route.values()) {
        if (candidate.matches(method, parts)
            && (best == null || candidate.prefixLength(parts) < best.prefixLength(parts))) {
          best = candidate;
        }
      }
      return best == null ? null : Pair.of(best, best.variables(parts));
    }

    public Class<? extends RESTRequest> requestClass() {
      return requestClass;
    }
  }

  private ConfigResponse config() {
    final List<Endpoint> endpoints = Arrays.stream(Route.values())
        .map(r -> Endpoint.create(r.method.name(), r.pathTemplate)).toList();
    return ConfigResponse.builder().withEndpoints(endpoints).build();
  }

  /**
   * Paging parameters of a list request. Without pageSize the listing is unpaged, expressed as a
   * single unbounded first page. This relies on CatalogHandlers.paginate treating a null token as
   * the first page and returning a null next-page-token once the list is exhausted.
   */
  private record PageRequest(String token, String size) {
    private static final PageRequest UNPAGED =
        new PageRequest(null, String.valueOf(Integer.MAX_VALUE));

    static PageRequest from(Map<String, String> vars) {
      String size = vars.get(PAGE_SIZE);
      if (size == null) {
        return UNPAGED; // token ignored, as upstream; keeping it would overflow token + MAX_VALUE
      }
      Preconditions.checkArgument(NumberUtils.toInt(size, 0) > 0,
          "Invalid %s: %s, must be a positive integer", PAGE_SIZE, size);
      return new PageRequest(vars.get(PAGE_TOKEN), size);
    }
  }

  private ListNamespacesResponse listNamespaces(Map<String, String> vars) {
    Namespace parent = vars.containsKey(PARENT)
        ? RESTUtil.namespaceFromQueryParam(vars.get(PARENT))
        : Namespace.empty();
    PageRequest page = PageRequest.from(vars);
    return CatalogHandlers.listNamespaces(asNamespaceCatalog, parent, page.token(), page.size());
  }

  private CreateNamespaceResponse createNamespace(Object body) {
    CreateNamespaceRequest request = castRequest(CreateNamespaceRequest.class, body);
    return CatalogHandlers.createNamespace(asNamespaceCatalog, request);
  }

  private RESTResponse namespaceExists(Map<String, String> vars) {
    Namespace namespace = namespaceFromPathVars(vars);
    CatalogHandlers.namespaceExists(asNamespaceCatalog, namespace);
    return null;
  }

  private GetNamespaceResponse loadNamespace(Map<String, String> vars) {
    Namespace namespace = namespaceFromPathVars(vars);
    return CatalogHandlers.loadNamespace(asNamespaceCatalog, namespace);
  }

  private RESTResponse dropNamespace(Map<String, String> vars) {
    CatalogHandlers.dropNamespace(asNamespaceCatalog, namespaceFromPathVars(vars));
    return null;
  }

  private UpdateNamespacePropertiesResponse updateNamespace(Map<String, String> vars, Object body) {
    Namespace namespace = namespaceFromPathVars(vars);
    UpdateNamespacePropertiesRequest request =
        castRequest(UpdateNamespacePropertiesRequest.class, body);
    return CatalogHandlers.updateNamespaceProperties(asNamespaceCatalog, namespace, request);
  }

  private ListTablesResponse listTables(Map<String, String> vars) {
    Namespace namespace = namespaceFromPathVars(vars);
    PageRequest page = PageRequest.from(vars);
    return CatalogHandlers.listTables(catalog, namespace, page.token(), page.size());
  }

  private LoadTableResponse createTable(
      Map<String, String> vars, Object body, Map<String, String> responseHeaders) {
    Namespace namespace = namespaceFromPathVars(vars);
    CreateTableRequest request = castRequest(CreateTableRequest.class, body);
    request.validate();
    final LoadTableResponse loadTableResponse;
    if (request.stageCreate()) {
      Map<String, String> namespaceMetadata = asNamespaceCatalog.loadNamespaceMetadata(namespace);
      icebergAuthorizer.validateStageCreateTable(catalogName, namespace, namespaceMetadata, request);
      loadTableResponse = CatalogHandlers.stageTableCreate(catalog, namespace, request);
    } else {
      loadTableResponse = CatalogHandlers.createTable(catalog, namespace, request);
    }
    attachETagHeader(loadTableResponse, responseHeaders);
    return loadTableResponse;
  }

  private RESTResponse dropTable(Map<String, String> vars) {
    TableIdentifier ident = identFromPathVars(vars);
    if (PropertyUtil.propertyAsBoolean(vars, "purgeRequested", false)) {
      String location = catalog.loadTable(ident).location();
      icebergAuthorizer.validateDropTablePurge(catalogName, ident, location);
      CatalogHandlers.purgeTable(catalog, ident);
    } else {
      CatalogHandlers.dropTable(catalog, ident);
    }
    return null;
  }

  private RESTResponse tableExists(Map<String, String> vars) {
    TableIdentifier ident = identFromPathVars(vars);
    CatalogHandlers.tableExists(catalog, ident);
    return null;
  }

  private LoadTableResponse loadTable(
      Map<String, String> vars,
      Map<String, String> queryParams,
      Map<String, String> headers,
      Map<String, String> responseHeaders) {
    TableIdentifier ident = identFromPathVars(vars);
    String snapshots = vars.get(RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER);
    RESTCatalogProperties.SnapshotMode mode = RESTCatalogProperties.SNAPSHOT_LOADING_MODE_DEFAULT;
    if (snapshots != null) {
      mode = RESTCatalogProperties.SnapshotMode.valueOf(snapshots.toUpperCase(Locale.ROOT));
    }
    LoadTableResponse loadTableResponse = CatalogHandlers.loadTable(catalog, ident, mode);
    if (loadTableResponse != null && loadTableResponse.metadataLocation() != null) {
      String eTag =
          ETagProvider.of(
              loadTableResponse.metadataLocation(),
              effectiveSnapshotQueryParams(queryParams, mode));
      responseHeaders.put(ETAG, eTag);
      if (matchesETag(headers.get(IF_NONE_MATCH), eTag)) {
        return null;
      }
    }
    return loadTableResponse;
  }

  private LoadTableResponse registerTable(
      Map<String, String> vars, Object body, Map<String, String> responseHeaders) {
    Namespace namespace = namespaceFromPathVars(vars);
    RegisterTableRequest request = castRequest(RegisterTableRequest.class, body);
    request.validate();
    Map<String, String> namespaceMetadata = asNamespaceCatalog.loadNamespaceMetadata(namespace);
    icebergAuthorizer.validateRegisterTable(catalogName, namespace, namespaceMetadata, request);
    LoadTableResponse loadTableResponse = CatalogHandlers.registerTable(catalog, namespace, request);
    attachETagHeader(loadTableResponse, responseHeaders);
    return loadTableResponse;
  }

  private LoadTableResponse updateTable(
      Map<String, String> vars, Object body, Map<String, String> responseHeaders) {
    TableIdentifier ident = identFromPathVars(vars);
    UpdateTableRequest request = castRequest(UpdateTableRequest.class, body);
    LoadTableResponse loadTableResponse = CatalogHandlers.updateTable(catalog, ident, request);
    attachETagHeader(loadTableResponse, responseHeaders);
    return loadTableResponse;
  }

  private RESTResponse renameTable(Object body) {
    RenameTableRequest request = castRequest(RenameTableRequest.class, body);
    CatalogHandlers.renameTable(catalog, request);
    return null;
  }

  private RESTResponse reportMetrics(Map<String, String> vars, Object body) {
    final TableIdentifier ident = identFromPathVars(vars);
    final var report = castRequest(ReportMetricsRequest.class, body).report();
    final var receivedAt = clock.instant();
    metricsReporters.forEach(reporter -> reporter.report(catalogName, ident, report, receivedAt));
    return null;
  }

  private RESTResponse commitTransaction(Object body) {
    CommitTransactionRequest request = castRequest(CommitTransactionRequest.class, body);
    commitTransaction(catalog, request);
    return null;
  }

  private ListTablesResponse listViews(Map<String, String> vars) {
    Namespace namespace = namespaceFromPathVars(vars);
    PageRequest page = PageRequest.from(vars);
    return CatalogHandlers.listViews(asViewCatalog, namespace, page.token(), page.size());
  }

  private LoadViewResponse createView(Map<String, String> vars, Object body) {
    Namespace namespace = namespaceFromPathVars(vars);
    CreateViewRequest request = castRequest(CreateViewRequest.class, body);
    return CatalogHandlers.createView(asViewCatalog, namespace, request);
  }

  private RESTResponse viewExists(Map<String, String> vars) {
    TableIdentifier ident = viewIdentFromPathVars(vars);
    CatalogHandlers.viewExists(asViewCatalog, ident);
    return null;
  }

  private LoadViewResponse loadView(Map<String, String> vars) {
    TableIdentifier ident = viewIdentFromPathVars(vars);
    return CatalogHandlers.loadView(asViewCatalog, ident);
  }

  private LoadViewResponse updateView(Map<String, String> vars, Object body) {
    TableIdentifier ident = viewIdentFromPathVars(vars);
    UpdateTableRequest request = castRequest(UpdateTableRequest.class, body);
    return CatalogHandlers.updateView(asViewCatalog, ident, request);
  }

  private RESTResponse renameView(Object body) {
    RenameTableRequest request = castRequest(RenameTableRequest.class, body);
    CatalogHandlers.renameView(asViewCatalog, request);
    return null;
  }

  private RESTResponse dropView(Map<String, String> vars) {
    CatalogHandlers.dropView(asViewCatalog, viewIdentFromPathVars(vars));
    return null;
  }

  private LoadViewResponse registerView(Map<String, String> vars, Object body) {
    Namespace namespace = namespaceFromPathVars(vars);
    RegisterViewRequest request = castRequest(RegisterViewRequest.class, body);
    return CatalogHandlers.registerView(asViewCatalog, namespace, request);
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

  @SuppressWarnings({"unchecked"})
  private <T extends RESTResponse> T handleRequest(
      Route route,
      Map<String, String> vars,
      Map<String, String> queryParams,
      Map<String, String> headers,
      Object body,
      Map<String, String> responseHeaders) {
    return (T) switch (route) {
      case CONFIG -> config();
      case LIST_NAMESPACES -> listNamespaces(vars);
      case CREATE_NAMESPACE -> createNamespace(body);
      case NAMESPACE_EXISTS -> namespaceExists(vars);
      case LOAD_NAMESPACE -> loadNamespace(vars);
      case DROP_NAMESPACE -> dropNamespace(vars);
      case UPDATE_NAMESPACE -> updateNamespace(vars, body);
      case LIST_TABLES -> listTables(vars);
      case CREATE_TABLE -> createTable(vars, body, responseHeaders);
      case DROP_TABLE -> dropTable(vars);
      case TABLE_EXISTS -> tableExists(vars);
      case LOAD_TABLE -> loadTable(vars, queryParams, headers, responseHeaders);
      case REGISTER_TABLE -> registerTable(vars, body, responseHeaders);
      case UPDATE_TABLE -> updateTable(vars, body, responseHeaders);
      case RENAME_TABLE -> renameTable(body);
      case REPORT_METRICS -> reportMetrics(vars, body);
      case COMMIT_TRANSACTION -> commitTransaction(body);
      case LIST_VIEWS -> listViews(vars);
      case CREATE_VIEW -> createView(vars, body);
      case VIEW_EXISTS -> viewExists(vars);
      case LOAD_VIEW -> loadView(vars);
      case UPDATE_VIEW -> updateView(vars, body);
      case RENAME_VIEW -> renameView(body);
      case DROP_VIEW -> dropView(vars);
      case REGISTER_VIEW -> registerView(vars, body);
    };
  }

  <T extends RESTResponse> T execute(
      HTTPMethod method,
      String path,
      Map<String, String> queryParams,
      Object body,
      HttpServletResponse response) throws IOException {
    return execute(method, path, Map.of(), queryParams, body, response);
  }

  <T extends RESTResponse> T execute(
      HTTPMethod method,
      String path,
      Map<String, String> headers,
      Map<String, String> queryParams,
      Object body,
      HttpServletResponse response) throws IOException {
    ErrorResponse.Builder errorBuilder = ErrorResponse.builder();
    Pair<Route, Map<String, String>> routeAndVars = Route.from(method, path);
    if (routeAndVars != null) {
      try {
        ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
        if (queryParams != null) {
          vars.putAll(queryParams);
        }
        vars.putAll(routeAndVars.second());
        Route route = routeAndVars.first();
        Map<String, String> responseHeaders = Maps.newHashMap();
        T result =
            handleRequest(
                route,
                vars.build(),
                queryParams,
                toCaseInsensitiveHeaders(headers),
                body,
                responseHeaders);
        if (response != null) {
          responseHeaders.forEach(response::setHeader);
          if (result == null) {
            if (route == Route.LOAD_TABLE) {
              response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            } else if (HMSCatalogServlet.shouldReturn204(route)) {
              response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            }
          }
        }
        return result;
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
    response.setStatus(error.code());
    RESTObjectMapper.mapper().writeValue(response.getWriter(), error);
    return null;
  }

  /**
   * Returns a case-insensitive copy of the incoming request headers map, or the map itself if
   * already case-insensitive, ensuring RFC 9110 compliant case-insensitive HTTP header lookups.
   *
   * @param headers the incoming request headers map
   * @return a case-insensitive headers map, or an empty map if null or empty
   */
  private static Map<String, String> toCaseInsensitiveHeaders(Map<String, String> headers) {
    if (headers == null || headers.isEmpty()) {
      return Map.of();
    }
    if (headers instanceof SortedMap<?, ?> sorted
        && sorted.comparator() == String.CASE_INSENSITIVE_ORDER) {
      return headers;
    }
    Map<String, String> caseInsensitive = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    caseInsensitive.putAll(headers);
    return caseInsensitive;
  }

  /**
   * Attaches an RFC 9110 compliant ETag header computed from the metadata location and the default
   * snapshot loading query parameters to the response headers map.
   *
   * @param loadTableResponse the table response containing metadata location
   * @param responseHeaders the response headers map to attach the ETag header to
   */
  private static void attachETagHeader(
      LoadTableResponse loadTableResponse, Map<String, String> responseHeaders) {
    Optional.ofNullable(loadTableResponse)
        .map(LoadTableResponse::metadataLocation)
        .ifPresent(loc -> responseHeaders.put(
            ETAG, ETagProvider.of(loc, DEFAULT_SNAPSHOT_QUERY_PARAMS)));
  }

  /**
   * Normalizes query parameters for table loading by ensuring the active {@link
   * RESTCatalogProperties.SnapshotMode} is explicitly reflected, returning pre-allocated immutable
   * defaults when possible to minimize allocations.
   *
   * @param queryParams the incoming query parameters
   * @param mode the resolved snapshot mode
   * @return normalized query parameters including the snapshot mode parameter
   */
  private static Map<String, String> effectiveSnapshotQueryParams(
      Map<String, String> queryParams, RESTCatalogProperties.SnapshotMode mode) {
    if (queryParams == null
        || queryParams.isEmpty()
        || (queryParams.size() == 1
            && queryParams.containsKey(RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER))) {
      return switch (mode) {
        case ALL -> DEFAULT_SNAPSHOT_QUERY_PARAMS;
        case REFS -> REFS_SNAPSHOT_QUERY_PARAMS;
      };
    }
    Map<String, String> normalized = Maps.newHashMapWithExpectedSize(queryParams.size() + 1);
    normalized.putAll(queryParams);
    normalized.put(
        RESTCatalogProperties.SNAPSHOTS_QUERY_PARAMETER, mode.name().toLowerCase(Locale.ROOT));
    return normalized;
  }

  /**
   * Evaluates an HTTP {@code If-None-Match} header against the current ETag according to RFC 9110
   * §13.1.2. Supports wildcard {@code "*"} as well as comma-delimited lists of multiple ETags.
   *
   * @param ifNoneMatchHeader the incoming If-None-Match header value, if any
   * @param currentETag the computed ETag of the current table state
   * @return true if the header matches the current ETag or contains "*", false otherwise
   */
  private static boolean matchesETag(String ifNoneMatchHeader, String currentETag) {
    if (ifNoneMatchHeader == null || currentETag == null) {
      return false;
    }
    if ("*".equals(ifNoneMatchHeader) || ifNoneMatchHeader.equals(currentETag)) {
      return true;
    }
    for (String candidate : COMMA.split(ifNoneMatchHeader)) {
      if ("*".equals(candidate) || candidate.equals(currentETag)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() {
    // The caller is responsible for closing the underlying catalog backing this REST catalog.
    for (IcebergMetricsReporter reporter : metricsReporters) {
      try {
        reporter.close();
      } catch (IOException e) {
        LOG.error("Failed to close metrics reporter: {}", reporter, e);
      }
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


  public static void configureResponseFromException(
      Exception exc, ErrorResponse.Builder errorBuilder) {
    var errorCode = EXCEPTION_ERROR_CODES.getOrDefault(exc.getClass(), 500);
    errorBuilder.responseCode(errorCode).withType(exc.getClass().getSimpleName()).withMessage(exc.getMessage());
    // avoid exposing stack traces for client errors but include them for server errors to aid debugging
    if (errorCode >= 500) {
      LOG.error("A server error happened while processing REST request", exc);
      errorBuilder.withStackTrace(exc);
    } else {
      LOG.info("A client error happened while processing REST request: {}", exc.getMessage());
    }
  }

  private static Namespace namespaceFromPathVars(Map<String, String> pathVars) {
    return RESTUtil.decodeNamespace(pathVars.get("namespace"), "%1F");
  }

  private static TableIdentifier identFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("table")));
  }

  private static TableIdentifier viewIdentFromPathVars(Map<String, String> pathVars) {
    return TableIdentifier.of(
        namespaceFromPathVars(pathVars), RESTUtil.decodeString(pathVars.get("view")));
  }
}
