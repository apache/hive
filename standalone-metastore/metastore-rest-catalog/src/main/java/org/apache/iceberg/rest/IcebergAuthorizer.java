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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.credential.StorageOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer;
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
   * Translates Hive privileges into storage operations.
   *
   * @param catalogName the catalog name
   * @param identifier the table identifier
   * @param columnNames the column names
   * @return a set of acceptable storage operations
   * @throws IllegalStateException if the authorization plugin fails
   */
  Set<StorageOperation> resolveAllowedStorageOperations(String catalogName, TableIdentifier identifier,
      List<String> columnNames) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1);
    final var database = identifier.namespace().level(0);
    final var table = identifier.name();

    final var authorizer = authorizerSupplier.get();
    if (authorizer == null) {
      LOG.info("No pre-event listener is configured, skipping credential-vending authorization");
      return EnumSet.allOf(StorageOperation.class);
    }
    if (!isReadable(authorizer, catalogName, database, table, columnNames)) {
      // Can we accept the write-only user?
      // I guess no because the write operation requires to read the current metadata.
      return EnumSet.noneOf(StorageOperation.class);
    }
    return isWritable(authorizer, catalogName, database, table)
        ? EnumSet.allOf(StorageOperation.class)
        : EnumSet.of(StorageOperation.LIST, StorageOperation.READ);
  }

  /**
   * Apply a similar permission check to the following event.
   * {@link org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.events.ReadTableEvent}
   */
  private boolean isReadable(HiveAuthorizer authorizer, String catalog, String database, String table,
      List<String> columns) {
    // We may add the owner and owner type in the future. It requires an extra metastore request.
    final var readPrivileges = Collections.singletonList(
        new HivePrivilegeObject(
            HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
            catalog,
            database,
            table,
            null,
            columns
        )
    );
    final var builder = new HiveAuthzContext.Builder();
    builder.setCommandString("read");
    final var context = builder.build();
    if (!isAllowed(authorizer, readPrivileges, Collections.emptyList(), context)) {
      return false;
    }

    if (!authorizer.needTransform()) {
      return true;
    }

    final List<HivePrivilegeObject> rewritePrivileges;
    try {
      rewritePrivileges = authorizer.applyRowFilterAndColumnMasking(context, readPrivileges);
    } catch (SemanticException e) {
      LOG.error("Failed to confirm the row filter and column masking", e);
      return false;
    }

    if (CollectionUtils.isNotEmpty(rewritePrivileges)) {
      LOG.info("The current Iceberg REST API can't enforce fine-grained access control");
      return false;
    }

    return true;
  }

  // Check if the user has the INSERT INTO permission
  private boolean isWritable(HiveAuthorizer authorizer, String catalog, String database, String table) {
    final var object = new HivePrivilegeObject(
        HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
        catalog,
        database,
        table,
        null,
        null,
        HivePrivilegeObject.HivePrivObjectActionType.INSERT,
        null
    );
    final var builder = new HiveAuthzContext.Builder();
    builder.setCommandString("write");
    final var context = builder.build();
    return isAllowed(authorizer, Collections.emptyList(), Collections.singletonList(object), context);
  }

  private boolean isAllowed(HiveAuthorizer authorizer, List<HivePrivilegeObject> input,
      List<HivePrivilegeObject> output, HiveAuthzContext context) {
    try {
      authorizer.checkPrivileges(HiveOperationType.QUERY, input, output, context);
      return true;
    } catch (HiveAccessControlException e) {
      return false;
    } catch (HiveAuthzPluginException e) {
      throw new IllegalStateException("Failed to check privileges for Iceberg credential vending", e);
    }
  }
}
