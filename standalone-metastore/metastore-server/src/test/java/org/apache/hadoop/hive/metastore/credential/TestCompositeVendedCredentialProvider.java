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

package org.apache.hadoop.hive.metastore.credential;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Category(MetastoreUnitTest.class)
public class TestCompositeVendedCredentialProvider {
  @Test
  public void testVendRoutesRequestsToSupportingProviders() {
    var firstPath = new Path("s3://bucket-a/warehouse/table-a");
    var firstRequest = new StorageAccessRequest(firstPath, EnumSet.of(StorageOperation.READ));
    var secondPath = new Path("s3://bucket-b/warehouse/table-b");
    var secondRequest = new StorageAccessRequest(secondPath, EnumSet.of(StorageOperation.READ));
    var thirdPath = new Path("s3://bucket-a/warehouse/table-c");
    var thirdRequest = new StorageAccessRequest(thirdPath, EnumSet.of(StorageOperation.READ));
    var requests = List.of(firstRequest, secondRequest, thirdRequest);
    var expiration = Instant.MAX;

    var firstProvider = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(firstProvider.supports(firstRequest)).thenReturn(true);
    Mockito.when(firstProvider.supports(secondRequest)).thenReturn(false);
    Mockito.when(firstProvider.supports(thirdRequest)).thenReturn(true);
    var firstCredentials = List.of(
        new VendedStorageCredential(firstPath, Map.of("provider", "first"), expiration),
        new VendedStorageCredential(thirdPath, Map.of("provider", "first"), expiration)
    );
    Mockito.when(firstProvider.vend("alice", List.of(firstRequest, thirdRequest))).thenReturn(firstCredentials);

    var secondProvider = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(secondProvider.supports(secondRequest)).thenReturn(true);
    var secondCredentials = List.of(new VendedStorageCredential(secondPath, Map.of("provider", "first"), expiration));
    Mockito.when(secondProvider.vend("alice", List.of(secondRequest))).thenReturn(secondCredentials);

    var provider = new CompositeVendedCredentialProvider(List.of(firstProvider, secondProvider));

    var credentials = provider.vend("alice", requests);
    var expected = Stream.concat(firstCredentials.stream(), secondCredentials.stream()).toList();
    Assert.assertEquals(expected, credentials);

    Mockito.verify(firstProvider).supports(firstRequest);
    Mockito.verify(firstProvider).supports(secondRequest);
    Mockito.verify(firstProvider).supports(thirdRequest);
    Mockito.verify(firstProvider).vend("alice", List.of(firstRequest, thirdRequest));
    Mockito.verify(secondProvider).supports(secondRequest);
    Mockito.verify(secondProvider).vend("alice", List.of(secondRequest));
    Mockito.verifyNoMoreInteractions(firstProvider, secondProvider);
  }

  @Test
  public void testVendUsesFirstSupportingProvider() {
    var path = new Path("s3://bucket-a/warehouse/table-a");
    var request = new StorageAccessRequest(path, EnumSet.of(StorageOperation.READ));
    var requests = List.of(request);

    var firstProvider = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(firstProvider.supports(request)).thenReturn(true);
    var expected = List.of(new VendedStorageCredential(path, Map.of("provider", "first"), Instant.MAX));
    Mockito.when(firstProvider.vend("alice", requests)).thenReturn(expected);

    var secondProvider = Mockito.mock(VendedCredentialProvider.class);

    var provider = new CompositeVendedCredentialProvider(List.of(firstProvider, secondProvider));

    var credentials = provider.vend("alice", requests);
    Assert.assertEquals(expected, credentials);

    Mockito.verify(firstProvider).supports(request);
    Mockito.verify(firstProvider).vend("alice", requests);
    Mockito.verifyNoInteractions(secondProvider);
  }

  @Test
  public void testVendFallsBackToNoopProviderWhenNoProviderSupportsRequest() {
    var request = new StorageAccessRequest(
        new Path("s3://bucket-a/warehouse/table-a"),
        EnumSet.of(StorageOperation.READ));
    var requests = List.of(request);

    var firstProvider = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(firstProvider.supports(request)).thenReturn(false);

    var secondProvider = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(secondProvider.supports(request)).thenReturn(false);

    var provider = new CompositeVendedCredentialProvider(List.of(firstProvider, secondProvider));

    var credentials = provider.vend("alice", requests);

    Assert.assertTrue(credentials.isEmpty());
    Mockito.verify(firstProvider).supports(request);
    Mockito.verify(secondProvider).supports(request);
    Mockito.verifyNoMoreInteractions(firstProvider, secondProvider);
  }
}
