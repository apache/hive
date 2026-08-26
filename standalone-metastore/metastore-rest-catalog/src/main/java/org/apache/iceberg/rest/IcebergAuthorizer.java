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

import static org.apache.iceberg.hive.HiveCatalog.HMS_DB_OWNER;
import static org.apache.iceberg.hive.HiveCatalog.HMS_DB_OWNER_TYPE;
import static org.apache.iceberg.hive.HiveCatalog.HMS_TABLE_OWNER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.hive.HiveHadoopUtil;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs authorization checks for Iceberg REST Catalog operations that do not reach Hive Metastore.
 *
 * <p>Most catalog operations eventually call Hive Metastore and are authorized there. Some operations, such as
 * stage-create, return metadata without creating a metastore object, so the corresponding metastore authorization
 * hooks are not invoked. This class performs the required checks before those operations are processed.
 */
class IcebergAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergAuthorizer.class);

  @VisibleForTesting
  final Supplier<HiveAuthorizer> authorizerSupplier;

  IcebergAuthorizer(Configuration conf) {
    final var classes = MetastoreConf.getTrimmedStringsVar(conf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS);
    if (classes.length == 0) {
      LOG.info("No pre-event listeners configured, skipping authorization checks");
      this.authorizerSupplier = () -> null;
      return;
    }
    if (Arrays.stream(classes).noneMatch(HiveMetaStoreAuthorizer.class.getName()::equals)) {
      throw new IllegalArgumentException(
          "HiveMetaStoreAuthorizer is required when pre-event listeners are configured, but %s is configured"
              .formatted(Arrays.toString(classes)));
    }

    // Building a HiveAuthorizer is dominated by identity-independent work: HiveConf.cloneConf (a full
    // Configuration deep-copy) and the reflective factory/authenticator lookups. Since read
    // authorization now runs on every table cache hit, do that work once per thread and keep only the
    // identity-sensitive steps per call. This mirrors HiveMetaStoreAuthorizer, which caches the
    // authenticator in a ThreadLocal and refreshes it via setConf on every event; we additionally cache
    // the cloned conf and factory (which it rebuilds per event) since they carry no identity.
    //
    // Per-thread caching is required for correctness, not just speed: the default authenticator
    // (HadoopDefaultAuthenticator) resolves and caches the user name at setConf time from the current
    // UserGroupInformation. Jetty worker threads are pooled and reused across end users, so the toolkit
    // must be thread-confined and setConf must be re-run each call to bind the current request's identity.
    // A single shared authorizer would pin authorization to whichever user built it first.
    final ThreadLocal<AuthorizerToolkit> toolkit = ThreadLocal.withInitial(() -> {
      try {
        final var hiveConf = HiveConf.cloneConf(conf);
        final var authorizerFactory = HiveUtils.getAuthorizerFactory(hiveConf,
            HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);
        final var authenticator = HiveUtils.getAuthenticator(hiveConf,
            HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
        return new AuthorizerToolkit(hiveConf, authorizerFactory, authenticator);
      } catch (HiveException e) {
        throw new IllegalStateException("Failed to initialize Hive authorizer for Iceberg REST Catalog", e);
      }
    });

    this.authorizerSupplier = () -> newRequestAuthorizer(toolkit.get());
  }

  /**
   * Builds a request-scoped {@link HiveAuthorizer} from the calling thread's cached {@link
   * AuthorizerToolkit}. Rebinds the authenticator to the current request's UGI via {@code setConf}
   * and rebuilds the authorizer on every call (rather than caching it), matching {@link
   * HiveMetaStoreAuthorizer#createHiveMetaStoreAuthorizer()}, so no authorizer implementation can
   * retain a stale identity across a pooled thread's successive requests.
   *
   * @param kit the calling thread's identity-independent building blocks
   * @return an authorizer bound to the current request's identity
   * @throws IllegalStateException if the authorization plugin fails to initialize
   */
  private static HiveAuthorizer newRequestAuthorizer(AuthorizerToolkit kit) {
    try {
      kit.authenticator.setConf(kit.hiveConf);
      final var authzContextBuilder = new HiveAuthzSessionContext.Builder();
      authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVEMETASTORE);
      authzContextBuilder.setSessionString("IcebergRESTCatalog");
      return kit.authorizerFactory.createHiveAuthorizer(
          new HiveMetastoreClientFactoryImpl(kit.hiveConf), kit.hiveConf, kit.authenticator,
          authzContextBuilder.build());
    } catch (HiveException e) {
      throw new IllegalStateException("Failed to initialize Hive authorizer for Iceberg REST Catalog", e);
    }
  }

  /**
   * Per-thread, identity-independent building blocks for a {@link HiveAuthorizer}. Cached in a
   * {@link ThreadLocal} so the expensive {@link HiveConf} clone and reflective factory/authenticator
   * lookups run once per thread; the identity-sensitive {@code setConf}/{@code createHiveAuthorizer}
   * steps still run on every authorization call.
   */
  private record AuthorizerToolkit(HiveConf hiveConf,
                                   HiveAuthorizerFactory authorizerFactory,
                                   HiveAuthenticationProvider authenticator) {}

  @VisibleForTesting
  IcebergAuthorizer(Supplier<HiveAuthorizer> authorizerSupplier) {
    this.authorizerSupplier = authorizerSupplier;
  }

  /**
   * Enforces authorization similar to [CreateTableEvent]. Checking the DFS_URI privilege for the location is critical;
   * without it, Credential Vending becomes a ticket service that allows end users to access arbitrary locations.
   * Checking DATABASE or TABLE_OR_VIEW privileges are nice to have. Without them, end users would notice missing
   * privileges after writing data files.
   *
   * @param catalogName the Hive catalog name
   * @param namespace the Iceberg namespace
   * @param namespaceMetadata the Iceberg namespace metadata
   * @param request the create table request
   * @throws ForbiddenException if the user does not have the required privileges
   * @throws IllegalStateException if the authorization plugin fails
   */
  void validateStageCreateTable(String catalogName, Namespace namespace, Map<String, String> namespaceMetadata,
      CreateTableRequest request) {
    Preconditions.checkArgument(request.stageCreate(), "Only stage create requests are supported");
    Preconditions.checkArgument(namespace.levels().length == 1, "Hive does not support multi-level namespaces");
    var databaseName = namespace.level(0);
    var authorizer = authorizerSupplier.get();
    if (authorizer == null) {
      LOG.info("No pre-event listener is configured, skipping stage-create authorization");
      return;
    }

    List<HivePrivilegeObject> inputs = request.location() == null
        ? Collections.emptyList()
        : Collections.singletonList(new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DFS_URI,
            request.location()));
    final String currentUser = HiveHadoopUtil.currentUser();
    String databaseOwnerName = currentUser;
    final PrincipalType databaseOwnerType;
    if (namespaceMetadata.get(HMS_DB_OWNER) == null) {
      databaseOwnerType = PrincipalType.USER;
    } else {
      databaseOwnerName = namespaceMetadata.get(HMS_DB_OWNER);
      var rawOwnerType = namespaceMetadata.get(HMS_DB_OWNER_TYPE);
      databaseOwnerType = rawOwnerType == null ? null : PrincipalType.valueOf(rawOwnerType);
    }
    final String tableOwnerName = request.properties().getOrDefault(HMS_TABLE_OWNER, currentUser);
    List<HivePrivilegeObject> outputs = List.of(
        new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DATABASE, catalogName, databaseName, null,
            null, null, HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, null,
            databaseOwnerName, databaseOwnerType),
        new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW, catalogName, databaseName,
            request.name(), null, null, HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, null,
            tableOwnerName, PrincipalType.USER)
    );

    check(authorizer, HiveOperationType.CREATETABLE, inputs, outputs, "create table " + request.name());
  }

  /**
   * Authorizes loading a table. Invoked by {@code HMSCachingCatalog} for reads served from the
   * cache: a cache hit never reaches Hive Metastore, so its read cannot be authorized by the HMS
   * pre-event listener (a cache miss reloads through HMS and is authorized there). Mirrors the
   * {@code QUERY} check {@code ReadTableEvent} performs on a metastore {@code get_table}.
   *
   * @param catalogName the Hive catalog name
   * @param identifier the table identifier (a metadata-table identifier is checked against its
   *                   base table)
   * @throws ForbiddenException if the user does not have the required privileges
   * @throws IllegalStateException if the authorization plugin fails
   */
  void authorizeLoadTable(String catalogName, TableIdentifier identifier) {
    var base = baseTableIdentifier(identifier);
    check(HiveOperationType.QUERY, List.of(tableOrView(catalogName, base)), List.of(), "select");
  }

  /**
   * Filters a table listing down to the entries the user may see, mirroring how Hive filters a
   * {@code SHOW TABLES} result. Hive Metastore performs no pre-event authorization for
   * {@code get_tables}, so without this filter a user would see tables they cannot read. A user
   * with no privileges receives an empty list rather than an error.
   *
   * @param catalogName the Hive catalog name
   * @param identifiers the full listing to filter
   * @return the subset the user is allowed to see, sorted by name
   * @throws IllegalStateException if the authorization plugin fails
   */
  List<TableIdentifier> filterTables(String catalogName, List<TableIdentifier> identifiers) {
    return filterTableOrViews(catalogName, identifiers, "show tables");
  }

  /**
   * Filters a view listing down to the entries the user may see. See {@link #filterTables}.
   *
   * @param catalogName the Hive catalog name
   * @param identifiers the full listing to filter
   * @return the subset the user is allowed to see, sorted by name
   * @throws IllegalStateException if the authorization plugin fails
   */
  List<TableIdentifier> filterViews(String catalogName, List<TableIdentifier> identifiers) {
    return filterTableOrViews(catalogName, identifiers, "show views");
  }

  private List<TableIdentifier> filterTableOrViews(String catalogName, List<TableIdentifier> identifiers,
      String commandString) {
    var authorizer = authorizerSupplier.get();
    if (authorizer == null) {
      return identifiers;
    }
    List<HivePrivilegeObject> objects = new ArrayList<>(identifiers.size());
    for (TableIdentifier identifier : identifiers) {
      objects.add(tableOrView(catalogName, identifier));
    }
    List<HivePrivilegeObject> allowed = filterListCmd(authorizer, objects, commandString);
    if (allowed.isEmpty()) {
      return Collections.emptyList();
    }
    List<TableIdentifier> result = new ArrayList<>(allowed.size());
    for (HivePrivilegeObject object : allowed) {
      result.add(TableIdentifier.of(object.getDbname(), object.getObjectName()));
    }
    result.sort(Comparator.comparing(TableIdentifier::name));
    return result;
  }

  /**
   * Filters a namespace listing down to the databases the user may see, mirroring how Hive filters
   * a {@code SHOW DATABASES} result. Hive Metastore performs no pre-event authorization for
   * {@code get_databases}, so without this filter a user would see databases they cannot access.
   * Multi-level namespaces cannot map to a Hive database and are dropped rather than failing the
   * whole listing.
   *
   * @param catalogName the Hive catalog name
   * @param namespaces the full listing to filter
   * @return the subset the user is allowed to see, sorted by name
   * @throws IllegalStateException if the authorization plugin fails
   */
  List<Namespace> filterNamespaces(String catalogName, List<Namespace> namespaces) {
    var authorizer = authorizerSupplier.get();
    if (authorizer == null) {
      return namespaces;
    }
    // Only single-level namespaces map to a Hive database; multi-level ones are dropped.
    List<HivePrivilegeObject> objects = new ArrayList<>(namespaces.size());
    for (Namespace namespace : namespaces) {
      if (namespace.levels().length == 1) {
        objects.add(database(catalogName, namespace));
      }
    }
    List<HivePrivilegeObject> allowed = filterListCmd(authorizer, objects, "show databases");
    if (allowed.isEmpty()) {
      return Collections.emptyList();
    }
    List<Namespace> result = new ArrayList<>(allowed.size());
    for (HivePrivilegeObject object : allowed) {
      result.add(Namespace.of(object.getDbname()));
    }
    result.sort(Comparator.comparing(namespace -> namespace.level(0)));
    return result;
  }

  private List<HivePrivilegeObject> filterListCmd(HiveAuthorizer authorizer, List<HivePrivilegeObject> objects,
      String commandString) {
    var builder = new HiveAuthzContext.Builder();
    builder.setCommandString(commandString);
    try {
      List<HivePrivilegeObject> allowed = authorizer.filterListCmdObjects(objects, builder.build());
      return allowed == null ? List.of() : allowed;
    } catch (HiveAccessControlException e) {
      throw new ForbiddenException(e, e.getMessage());
    } catch (HiveAuthzPluginException e) {
      throw new IllegalStateException("Failed to filter " + commandString + " results", e);
    }
  }

  /**
   * Normalizes a metadata-table identifier ({@code db.table.<type>}) to its base table
   * ({@code db.table}) so access cannot be granted through a metadata-table name. Non-metadata
   * identifiers are returned unchanged.
   */
  private static TableIdentifier baseTableIdentifier(TableIdentifier identifier) {
    String[] levels = identifier.namespace().levels();
    // A metadata-table identifier is db.table.<type>, so its namespace always carries at least the
    // parent database and table. A single-level namespace whose name happens to match a metadata
    // type is a real table, not a metadata table, and must be left untouched.
    if (levels.length >= 2 && MetadataTableType.from(identifier.name()) != null) {
      return TableIdentifier.of(levels);
    }
    return identifier;
  }

  private static HivePrivilegeObject tableOrView(String catalogName, TableIdentifier identifier) {
    return new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW, catalogName,
        identifier.namespace().level(0), identifier.name());
  }

  private static HivePrivilegeObject database(String catalogName, Namespace namespace) {
    Preconditions.checkArgument(namespace.levels().length == 1, "Hive does not support multi-level namespaces");
    return new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DATABASE, catalogName,
        namespace.level(0), (String) null);
  }

  private void check(HiveOperationType operation, List<HivePrivilegeObject> inputs,
      List<HivePrivilegeObject> outputs, String commandString) {
    check(authorizerSupplier.get(), operation, inputs, outputs, commandString);
  }

  private void check(HiveAuthorizer authorizer, HiveOperationType operation, List<HivePrivilegeObject> inputs,
      List<HivePrivilegeObject> outputs, String commandString) {
    if (authorizer == null) {
      LOG.debug("No pre-event listener is configured, skipping {} authorization", operation);
      return;
    }
    var builder = new HiveAuthzContext.Builder();
    builder.setCommandString(commandString);
    try {
      authorizer.checkPrivileges(operation, inputs, outputs, builder.build());
    } catch (HiveAccessControlException e) {
      throw new ForbiddenException(e, e.getMessage());
    } catch (HiveAuthzPluginException e) {
      throw new IllegalStateException("Failed to check privileges for " + operation, e);
    }
  }
}
