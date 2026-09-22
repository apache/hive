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

package org.apache.iceberg.rest;

import static org.apache.iceberg.hive.HiveCatalog.HMS_DB_OWNER;
import static org.apache.iceberg.hive.HiveCatalog.HMS_DB_OWNER_TYPE;
import static org.apache.iceberg.hive.HiveCatalog.HMS_TABLE_OWNER;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.hive.HiveHadoopUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
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

  private final Configuration conf;

  IcebergAuthorizer(Configuration conf) {
    this.conf = conf;
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

    this.authorizerSupplier = () -> {
      try {
        final var hiveConf = HiveConf.cloneConf(conf);
        final var authorizerFactory = HiveUtils.getAuthorizerFactory(hiveConf,
            HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);

        final var authenticator = HiveUtils.getAuthenticator(hiveConf,
            HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
        authenticator.setConf(hiveConf);

        final var authzContextBuilder = new HiveAuthzSessionContext.Builder();
        authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVEMETASTORE);
        authzContextBuilder.setSessionString("IcebergRESTCatalog");
        return authorizerFactory.createHiveAuthorizer(
            new HiveMetastoreClientFactoryImpl(hiveConf), hiveConf, authenticator, authzContextBuilder.build());
      } catch (HiveException e) {
        throw new IllegalStateException("Failed to initialize Hive authorizer for Iceberg REST Catalog", e);
      }
    };
  }

  @VisibleForTesting
  IcebergAuthorizer(Supplier<HiveAuthorizer> authorizerSupplier) {
    this(authorizerSupplier, new Configuration(false));
  }

  @VisibleForTesting
  IcebergAuthorizer(Supplier<HiveAuthorizer> authorizerSupplier, Configuration conf) {
    this.authorizerSupplier = authorizerSupplier;
    this.conf = conf;
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

    var builder = new HiveAuthzContext.Builder();
    builder.setCommandString("create table " + request.name());
    try {
      authorizer.checkPrivileges(HiveOperationType.CREATETABLE, inputs, outputs, builder.build());
    } catch (HiveAccessControlException e) {
      throw new ForbiddenException(e, e.getMessage());
    } catch (HiveAuthzPluginException e) {
      throw new IllegalStateException("Failed to check privileges stage-create", e);
    }
  }

  /**
   * Enforces authorization for REGISTER_TABLE. The request's {@code metadataLocation} is fetched with the
   * catalog's shared, service-level {@link FileIO}, so both that location and the {@code location()} embedded in
   * the metadata file it points to (which becomes the table's HMS {@code StorageDescriptor.location}, and is what
   * a later purge trusts as its deletion root, see {@link #validateDropTablePurge}) must be authorized. Otherwise
   * REGISTER_TABLE is an arbitrary-file-read primitive that returns any metadata file's contents to the caller.
   *
   * <p>When no {@code HiveAuthorizer} is configured, falls back to requiring both locations to be contained in
   * the namespace's external or managed root, since there is no policy to otherwise decide whether the caller may
   * read an arbitrary location with service credentials.
   *
   * @param catalogName the Hive catalog name
   * @param namespace the Iceberg namespace
   * @param namespaceMetadata the Iceberg namespace metadata
   * @param request the register table request
   * @param io the {@link FileIO} used to read the metadata file
   * @throws ForbiddenException if a location is not authorized, or not contained in the namespace
   * @throws IllegalStateException if the authorization plugin fails
   */
  void validateRegisterTable(String catalogName, Namespace namespace, Map<String, String> namespaceMetadata,
      RegisterTableRequest request, FileIO io) {
    Preconditions.checkArgument(namespace.levels().length == 1, "Hive does not support multi-level namespaces");
    var databaseName = namespace.level(0);
    var commandString = "register table " + request.name();
    checkLocationAuthorized(catalogName, databaseName, namespaceMetadata, request.metadataLocation(), commandString);

    var metadata = TableMetadataParser.read(io, request.metadataLocation());
    checkLocationAuthorized(catalogName, databaseName, namespaceMetadata, metadata.location(), commandString);
  }

  /**
   * Enforces authorization for DROP_TABLE with {@code purge=true}. Purge deletes every file referenced by the
   * table's current metadata using the catalog's shared, service-level {@link FileIO}, so the location must be
   * authorized like any other DFS_URI access.
   *
   * <p>Unlike {@link #validateRegisterTable}, there is no namespace-containment fallback here: the structural
   * fence in {@code HiveCatalog.dropTable} already restricts purge deletions to files under the table's own
   * location regardless of whether a {@code HiveAuthorizer} is configured, so a deployment without one relies on
   * that fence rather than this check.
   *
   * @param catalogName the Hive catalog name
   * @param identifier the table identifier being dropped
   * @param location the table's current location
   * @throws ForbiddenException if the location is not authorized
   * @throws IllegalStateException if the authorization plugin fails
   */
  void validateDropTablePurge(String catalogName, TableIdentifier identifier, String location) {
    var authorizer = authorizerSupplier.get();
    if (authorizer == null) {
      LOG.info("No pre-event listener is configured for catalog {}, skipping drop-table-purge authorization for {}",
          catalogName, identifier);
      return;
    }

    var inputs = Collections.singletonList(
        new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DFS_URI, location));
    var builder = new HiveAuthzContext.Builder();
    builder.setCommandString("drop table " + identifier.name());
    try {
      authorizer.checkPrivileges(HiveOperationType.DROPTABLE, inputs, Collections.emptyList(), builder.build());
    } catch (HiveAccessControlException e) {
      throw new ForbiddenException(e, e.getMessage());
    } catch (HiveAuthzPluginException e) {
      throw new IllegalStateException("Failed to check privileges drop-table-purge", e);
    }
  }

  private void checkLocationAuthorized(String catalogName, String databaseName,
      Map<String, String> namespaceMetadata, String location, String commandString) {
    var authorizer = authorizerSupplier.get();
    if (authorizer == null) {
      LOG.info("No pre-event listener is configured for catalog {}, falling back to namespace containment for {}",
          catalogName, location);
      checkContainedInNamespace(databaseName, namespaceMetadata, location);
      return;
    }

    var inputs = Collections.singletonList(
        new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DFS_URI, location));
    var builder = new HiveAuthzContext.Builder();
    builder.setCommandString(commandString);
    try {
      authorizer.checkPrivileges(HiveOperationType.CREATETABLE, inputs, Collections.emptyList(), builder.build());
    } catch (HiveAccessControlException e) {
      throw new ForbiddenException(e, e.getMessage());
    } catch (HiveAuthzPluginException e) {
      throw new IllegalStateException("Failed to check privileges for " + commandString, e);
    }
  }

  private void checkContainedInNamespace(String databaseName, Map<String, String> namespaceMetadata,
      String location) {
    var externalRoot = namespaceMetadata.get("location");
    if (externalRoot != null && isContained(externalRoot, location)) {
      return;
    }
    if (isContained(managedNamespaceLocation(databaseName), location)) {
      return;
    }
    throw new ForbiddenException(
        "Location %s is not authorized and is not contained in namespace %s", location, databaseName);
  }

  private String managedNamespaceLocation(String databaseName) {
    var warehouseLocation = conf.get(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname);
    Preconditions.checkNotNull(warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    if (warehouseLocation.endsWith("/")) {
      warehouseLocation = warehouseLocation.substring(0, warehouseLocation.length() - 1);
    }
    return String.format("%s/%s.db", warehouseLocation, databaseName);
  }

  /**
   * Checks whether {@code candidate} resolves under {@code root}. Both are resolved via {@link Path#toUri()} and
   * {@link java.net.URI#normalize()}, which -- unlike {@link Path}'s own normalization -- actually collapses
   * {@code .}/{@code ..} segments; a plain string-prefix comparison on unnormalized paths would let a location
   * like {@code root/../../elsewhere} pass a naive check while actually resolving outside {@code root}.
   */
  private static boolean isContained(String root, String candidate) {
    var normalizedRoot = normalize(root);
    var normalizedCandidate = normalize(candidate);
    return normalizedCandidate.equals(normalizedRoot)
        || normalizedCandidate.startsWith(normalizedRoot.endsWith("/") ? normalizedRoot : normalizedRoot + "/");
  }

  private static String normalize(String location) {
    return new Path(location).toUri().normalize().toString();
  }
}
