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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.credential.CompositeVendedCredentialProvider;
import org.apache.hadoop.hive.metastore.credential.StorageAccessRequest;
import org.apache.hadoop.hive.metastore.credential.StorageOperation;
import org.apache.hadoop.hive.metastore.credential.VendedCredentialProvider;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * This class provides vended credentials for Iceberg.
 */
public class IcebergVendedCredentialProvider {
  private final String catalog;
  private final VendedCredentialProvider vendedCredentialProvider;
  private final Supplier<HiveAuthorizer> authorizerSupplier;

  public IcebergVendedCredentialProvider(Configuration conf) {
    this.catalog = MetaStoreUtils.getDefaultCatalog(conf);
    this.vendedCredentialProvider = new CompositeVendedCredentialProvider(conf);
    this.authorizerSupplier = () -> {
      try {
        final var hiveConf = HiveConf.cloneConf(conf);
        final var authorizerFactory = HiveUtils.getAuthorizerFactory(hiveConf,
            HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);
        if (authorizerFactory == null) {
          throw new IllegalStateException("Iceberg's Credential Vending requires Hive's Authorization Manager");
        }

        final var authenticator = HiveUtils.getAuthenticator(hiveConf,
            HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
        authenticator.setConf(hiveConf);

        final var authzContextBuilder = new HiveAuthzSessionContext.Builder();
        authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVEMETASTORE);
        authzContextBuilder.setSessionString("IcebergRESTCatalog");
        return authorizerFactory.createHiveAuthorizer(
            new HiveMetastoreClientFactoryImpl(hiveConf), hiveConf, authenticator, authzContextBuilder.build());
      } catch (HiveException e) {
        throw new IllegalStateException("Failed to initialize Hive authorizer for Iceberg credential vending", e);
      }
    };
  }

  @VisibleForTesting
  IcebergVendedCredentialProvider(String catalog, VendedCredentialProvider vendedCredentialProvider,
      Supplier<HiveAuthorizer> authorizerSupplier) {
    this.catalog = catalog;
    this.vendedCredentialProvider = vendedCredentialProvider;
    this.authorizerSupplier = authorizerSupplier;
  }

  /**
   * Vends credentials for the given table identifier.
   *
   * @param identifier the table identifier
   * @param location the table location
   * @return the vended credentials
   */
  public List<Credential> vend(TableIdentifier identifier, String location) {
    final String username;
    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    final var allowedOperations = resolveAllowedOperations(identifier);
    if (allowedOperations.isEmpty()) {
      return Collections.emptyList();
    }
    final var request = new StorageAccessRequest(new Path(location), allowedOperations);
    return vendedCredentialProvider.vend(username, Collections.singletonList(request)).stream()
        .map(credential -> ImmutableCredential.builder()
            .prefix(credential.prefix().toString()).config(credential.credentials()).build())
        .map(x -> (Credential) x)
        .toList();
  }

  private Set<StorageOperation> resolveAllowedOperations(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1);
    final var database = identifier.namespace().level(0);
    final var table = identifier.name();

    final var authorizer = authorizerSupplier.get();
    final var allowedOperations = EnumSet.noneOf(StorageOperation.class);
    if (isReadable(authorizer, database, table)) {
      allowedOperations.add(StorageOperation.LIST);
      allowedOperations.add(StorageOperation.READ);
    }
    if (isWritable(authorizer, database, table)) {
      allowedOperations.add(StorageOperation.CREATE);
      allowedOperations.add(StorageOperation.DELETE);
    }

    return allowedOperations;
  }

  // Check if the user has the SELECT permission
  private boolean isReadable(HiveAuthorizer authorizer, String database, String table) {
    final var object = new HivePrivilegeObject(
        HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
        catalog,
        database,
        table
    );
    return isAllowed(authorizer, Collections.singletonList(object), Collections.emptyList());
  }

  // Check if the user has the INSERT INTO permission
  private boolean isWritable(HiveAuthorizer authorizer, String database, String table) {
    final var object = new HivePrivilegeObject(
        HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
        catalog,
        database,
        table,
        null,
        null,
        HivePrivilegeObject.HivePrivObjectActionType.INSERT,
        null);
    return isAllowed(authorizer, Collections.emptyList(), Collections.singletonList(object));
  }

  private boolean isAllowed(HiveAuthorizer authorizer, List<HivePrivilegeObject> input,
      List<HivePrivilegeObject> output) {
    var context = new HiveAuthzContext.Builder().build();
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
