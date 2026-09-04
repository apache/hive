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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.credential.CompositeVendedCredentialProvider;
import org.apache.hadoop.hive.metastore.credential.StorageAccessRequest;
import org.apache.hadoop.hive.metastore.credential.VendedCredentialProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class provides vended credentials for Iceberg.
 */
class IcebergVendedCredentialProvider {
  private final IcebergAuthorizer authorizer;
  private final VendedCredentialProvider vendedCredentialProvider;

  IcebergVendedCredentialProvider(IcebergAuthorizer authorizer, Configuration conf) {
    this(authorizer, new CompositeVendedCredentialProvider(conf));
  }

  IcebergVendedCredentialProvider(IcebergAuthorizer authorizer, VendedCredentialProvider vendedCredentialProvider) {
    this.authorizer = authorizer;
    this.vendedCredentialProvider = vendedCredentialProvider;
  }

  /**
   * Vends credentials for the given table identifier.
   *
   * @param identifier the table identifier
   * @param metadata the table metadata
   * @return the vended credentials
   */
  public List<Credential> vend(String catalogName, TableIdentifier identifier, TableMetadata metadata) {
    final String username;
    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // We enable Credential Vending only when the user has access to the all columns.
    final var columnNames = metadata.schemas().stream()
        .flatMap(schema -> schema.columns().stream())
        .map(Types.NestedField::name)
        .distinct()
        .sorted()
        .collect(Collectors.toList());
    final var allowedOperations = authorizer.resolveAllowedStorageOperations(catalogName, identifier, columnNames);
    if (allowedOperations.isEmpty()) {
      return Collections.emptyList();
    }
    // Custom locations via write.metadata.path and write.data.path in the future are not supported yet.
    // We may need to authorize write.metadata.path and write.data.path in TBLPROPERTIES in the same way as LOCATION
    // if we support vended credentials for the custom paths. As of today, we support only the pure LOCATION that is
    // authorized by Ranger. I guess we also need to update IcebergAuthorizer#validateStageCreateTable when we support
    // the custom paths.
    // Related CVE: https://polaris.apache.org/community/security-advisories/cve-2026-42812/
    final var request = new StorageAccessRequest(new Path(metadata.location()), allowedOperations);
    return vendedCredentialProvider.vend(username, Collections.singletonList(request)).stream()
        .map(credential -> ImmutableCredential.builder()
            .prefix(credential.prefix().toString()).config(credential.credentials()).build())
        .map(x -> (Credential) x)
        .toList();
  }
}
