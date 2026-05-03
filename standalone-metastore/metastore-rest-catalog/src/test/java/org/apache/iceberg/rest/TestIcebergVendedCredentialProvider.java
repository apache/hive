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
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.security.PrivilegedAction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@Category(MetastoreUnitTest.class)
public class TestIcebergVendedCredentialProvider {
  private static final String CATALOG = "catalog";
  private static final String DATABASE = "database";
  private static final String TABLE = "tbl";
  private static final HivePrivilegeObject INPUT_OBJECT = new HivePrivilegeObject(
      HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW, CATALOG, DATABASE, TABLE
  );
  private static final HivePrivilegeObject OUTPUT_OBJECT = new HivePrivilegeObject(
      HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW, CATALOG, DATABASE, TABLE,
      null,
      null,
      HivePrivilegeObject.HivePrivObjectActionType.INSERT,
      null
  );

  private static void assertPrivilegeObjects(List<HivePrivilegeObject> expected, List<?> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    var actualObjects = new ArrayList<HivePrivilegeObject>(actual.size());
    actual.forEach(object -> actualObjects.add((HivePrivilegeObject) object));
    for (int index = 0; index < expected.size(); index++) {
      assertPrivilegeObject(expected.get(index), actualObjects.get(index));
    }
  }

  private static void assertPrivilegeObject(HivePrivilegeObject expected, HivePrivilegeObject actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getCatName(), actual.getCatName());
    Assert.assertEquals(expected.getDbname(), actual.getDbname());
    Assert.assertEquals(expected.getObjectName(), actual.getObjectName());
    Assert.assertEquals(expected.getActionType(), actual.getActionType());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testVendWithWritableUser() throws HiveAccessControlException, HiveAuthzPluginException {
    var authorizer = Mockito.mock(HiveAuthorizer.class);
    var delegate = Mockito.mock(VendedCredentialProvider.class);
    var username = "writable";
    var operations = EnumSet.of(StorageOperation.LIST, StorageOperation.READ, StorageOperation.CREATE,
        StorageOperation.DELETE);
    var path = new Path("s3a://bucket/path");
    var requests = List.of(new StorageAccessRequest(path, operations));
    var credential = List.of(new VendedStorageCredential(path, Map.of("key", "k1"), Instant.MAX));
    Mockito.when(delegate.vend(username, requests)).thenReturn(credential);
    var provider = new IcebergVendedCredentialProvider(CATALOG, delegate, () -> authorizer);
    var result = UserGroupInformation.createRemoteUser(username).doAs((PrivilegedAction<List<Credential>>) () ->
        provider.vend(TableIdentifier.of(DATABASE, TABLE), path.toString()));
    var expected = ImmutableCredential.builder().prefix(path.toString()).config(Map.of("key", "k1")).build();
    Assert.assertEquals(List.of(expected), result);

    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(authorizer, Mockito.times(2)).checkPrivileges(
        eq(HiveOperationType.QUERY),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    assertPrivilegeObjects(List.of(INPUT_OBJECT), inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), inputCaptor.getAllValues().getLast());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(OUTPUT_OBJECT), outputCaptor.getAllValues().getLast());
    Mockito.verifyNoMoreInteractions(authorizer);
    Mockito.verify(delegate).vend(username, requests);
    Mockito.verifyNoMoreInteractions(delegate);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testVendWithReadOnlyUser() throws HiveAccessControlException, HiveAuthzPluginException {
    var authorizer = Mockito.mock(HiveAuthorizer.class);
    var delegate = Mockito.mock(VendedCredentialProvider.class);
    var username = "readonly";
    var operations = EnumSet.of(StorageOperation.LIST, StorageOperation.READ);
    var path = new Path("s3a://bucket/path");
    var requests = List.of(new StorageAccessRequest(path, operations));
    var credential = List.of(new VendedStorageCredential(path, Map.of("key", "k1"), Instant.MAX));
    Mockito.when(delegate.vend(username, requests)).thenReturn(credential);
    Mockito.doAnswer(invocation -> {
      if (!((List<?>) invocation.getArgument(2)).isEmpty()) {
        throw new HiveAccessControlException("write denied");
      }
      return null;
    }).when(authorizer).checkPrivileges(any(), any(), any(), any());

    var provider = new IcebergVendedCredentialProvider(CATALOG, delegate, () -> authorizer);
    var result = UserGroupInformation.createRemoteUser(username).doAs((PrivilegedAction<List<Credential>>) () ->
        provider.vend(TableIdentifier.of(DATABASE, TABLE), path.toString()));
    var expected = ImmutableCredential.builder().prefix(path.toString()).config(Map.of("key", "k1")).build();
    Assert.assertEquals(List.of(expected), result);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(authorizer, Mockito.times(2)).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assert.assertEquals(List.of(HiveOperationType.QUERY, HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(List.of(INPUT_OBJECT), inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), inputCaptor.getAllValues().getLast());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(OUTPUT_OBJECT), outputCaptor.getAllValues().getLast());
    Mockito.verifyNoMoreInteractions(authorizer);
    Mockito.verify(delegate).vend(username, requests);
    Mockito.verifyNoMoreInteractions(delegate);
  }

  @Test
  public void testVendWithoutPrivileges() throws HiveAccessControlException, HiveAuthzPluginException {
    var authorizer = Mockito.mock(HiveAuthorizer.class);
    var delegate = Mockito.mock(VendedCredentialProvider.class);
    var username = "denied";
    Mockito.doThrow(new HiveAccessControlException("denied"))
        .when(authorizer).checkPrivileges(any(), any(), any(), any());

    var provider = new IcebergVendedCredentialProvider(CATALOG, delegate, () -> authorizer);
    var result = UserGroupInformation.createRemoteUser(username).doAs((PrivilegedAction<List<Credential>>) () ->
        provider.vend(TableIdentifier.of(DATABASE, TABLE), "s3a://bucket/path"));

    Assert.assertEquals(List.of(), result);
    Mockito.verify(authorizer, Mockito.times(2)).checkPrivileges(any(), any(), any(), any());
    Mockito.verifyNoMoreInteractions(authorizer);
    Mockito.verifyNoInteractions(delegate);
  }
}
