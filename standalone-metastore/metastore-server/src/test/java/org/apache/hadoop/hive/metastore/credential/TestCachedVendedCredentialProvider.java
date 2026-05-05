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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

@Category(MetastoreUnitTest.class)
public class TestCachedVendedCredentialProvider {
  @Test
  public void testSupportsDelegatesToWrappedProvider() {
    var request = new StorageAccessRequest(
        new Path("s3://bucket-a/warehouse/table"),
        EnumSet.of(StorageOperation.READ));
    var delegate = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(delegate.supports(request)).thenReturn(true);
    var provider = new CachedVendedCredentialProvider(
        delegate,
        100,
        Duration.ofMinutes(30),
        Clock.systemUTC());

    Assert.assertTrue(provider.supports(request));
    Mockito.verify(delegate).supports(request);
  }

  @Test
  public void testVend() {
    var now = Instant.parse("2026-04-26T12:00:00Z");
    var path = new Path("s3://bucket-a/warehouse/table");
    var request = new StorageAccessRequest(path, EnumSet.of(StorageOperation.READ));
    var requests = List.of(request);
    var response = List.of(new VendedStorageCredential(path, Map.of("token", "first"),
        now.plus(Duration.ofMinutes(20))));
    var delegate = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(delegate.vend("alice", requests))
        .thenReturn(response)
        .thenThrow(new AssertionError("The second request should be served from cache"));
    var provider = new CachedVendedCredentialProvider(
        delegate,
        100,
        Duration.ofMinutes(30),
        Clock.fixed(now, ZoneOffset.UTC));

    var first = provider.vend("alice", requests);
    var second = provider.vend("alice", requests);
    Assert.assertSame(first, second);

    Mockito.verify(delegate, Mockito.times(1)).vend("alice", requests);
    Mockito.verifyNoMoreInteractions(delegate);
  }

  @Test
  public void testVendWithDifferentPrincipals() {
    var now = Instant.parse("2026-04-26T12:00:00Z");
    var path = new Path("s3://bucket-a/warehouse/table");
    var request = new StorageAccessRequest(path, EnumSet.of(StorageOperation.READ));
    var requests = List.of(request);
    var response = List.of(new VendedStorageCredential(path, Map.of("token", "first"),
        now.plus(Duration.ofMinutes(20))));
    var delegate = Mockito.mock(VendedCredentialProvider.class);
    Mockito.when(delegate.vend("alice", requests))
        .thenReturn(response)
        .thenThrow(new AssertionError("The second request should be served from cache"));
    Mockito.when(delegate.vend("bob", requests))
        .thenReturn(response)
        .thenThrow(new AssertionError("The second request should be served from cache"));
    var provider = new CachedVendedCredentialProvider(
        delegate,
        100,
        Duration.ofMinutes(30),
        Clock.fixed(now, ZoneOffset.UTC));

    var alice = provider.vend("alice", requests);
    var bob = provider.vend("bob", requests);
    Assert.assertSame(alice, bob);

    Mockito.verify(delegate, Mockito.times(1)).vend("alice", requests);
    Mockito.verify(delegate, Mockito.times(1)).vend("bob", requests);
    Mockito.verifyNoMoreInteractions(delegate);
  }
}
