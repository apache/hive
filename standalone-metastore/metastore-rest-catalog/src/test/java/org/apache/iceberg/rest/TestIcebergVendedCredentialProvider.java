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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.credential.StorageAccessRequest;
import org.apache.hadoop.hive.metastore.credential.StorageOperation;
import org.apache.hadoop.hive.metastore.credential.VendedCredentialProvider;
import org.apache.hadoop.hive.metastore.credential.VendedStorageCredential;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.security.PrivilegedAction;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Category(MetastoreUnitTest.class)
public class TestIcebergVendedCredentialProvider {
  private static final String CATALOG = "catalog";
  private static final String DATABASE = "database";
  private static final String TABLE = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DATABASE, TABLE);

  @Test
  public void testVend() {
    var authorizer = Mockito.mock(IcebergAuthorizer.class);
    var operations = EnumSet.of(StorageOperation.LIST, StorageOperation.READ);
    var schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "to_be_deleted", Types.StringType.get()),
        Types.NestedField.required(3, "to_be_renamed", Types.BinaryType.get())
    );
    var updaetdSchema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(3, "renamed", Types.BinaryType.get())
    );
    var columns = List.of("id", "renamed", "to_be_deleted", "to_be_renamed");
    Mockito.when(authorizer.resolveAllowedStorageOperations(CATALOG, TABLE_IDENTIFIER, columns)).thenReturn(operations);

    var path = new Path("s3a://bucket/path");
    var requests = List.of(new StorageAccessRequest(path, operations));
    var credential = List.of(new VendedStorageCredential(path, Map.of("key", "k1"), Instant.MAX));

    var username = "writable";
    var delegate = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(delegate.vend(username, requests)).thenReturn(credential);

    var provider = new IcebergVendedCredentialProvider(authorizer, delegate);
    var metadata = TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        path.toString(),
        Map.of()
    ).updateSchema(updaetdSchema);
    var result = UserGroupInformation.createRemoteUser(username).doAs((PrivilegedAction<List<Credential>>) () ->
        provider.vend(CATALOG, TableIdentifier.of(DATABASE, TABLE), metadata));
    var expected = ImmutableCredential.builder().prefix(path.toString()).config(Map.of("key", "k1")).build();
    Assert.assertEquals(List.of(expected), result);

    Mockito.verify(authorizer).resolveAllowedStorageOperations(CATALOG, TABLE_IDENTIFIER, columns);
    Mockito.verifyNoMoreInteractions(authorizer);
    Mockito.verify(delegate).vend(username, requests);
    Mockito.verifyNoMoreInteractions(delegate);
  }

  @Test
  public void testVendWithoutPrivileges() {
    var authorizer = Mockito.mock(IcebergAuthorizer.class);
    var schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    var columns = List.of("id");
    Mockito.when(authorizer.resolveAllowedStorageOperations(CATALOG, TABLE_IDENTIFIER, columns)).thenReturn(Set.of());

    var username = "denied";
    var delegate = Mockito.mock(VendedCredentialProvider.class);

    var provider = new IcebergVendedCredentialProvider(authorizer, delegate);
    var metadata = TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        "s3a://bucket/path",
        Map.of()
    );
    var result = UserGroupInformation.createRemoteUser(username).doAs((PrivilegedAction<List<Credential>>) () ->
        provider.vend(CATALOG, TableIdentifier.of(DATABASE, TABLE), metadata));

    Assert.assertEquals(List.of(), result);
    Mockito.verify(authorizer).resolveAllowedStorageOperations(CATALOG, TABLE_IDENTIFIER, columns);
    Mockito.verifyNoMoreInteractions(authorizer);
    Mockito.verifyNoInteractions(delegate);
  }
}
