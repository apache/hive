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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jetbrains.annotations.NotNull;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

/**
 * A VendedCredentialProvider that caches the results of the delegated provider.
 */
public class CachedVendedCredentialProvider implements VendedCredentialProvider {
  private record CacheKey(String username, List<StorageAccessRequest> accessRequests) {}

  private final VendedCredentialProvider delegate;
  private final Cache<CacheKey, List<VendedStorageCredential>> cache;

  public CachedVendedCredentialProvider(VendedCredentialProvider delegate, long maxSize, Duration maxCacheDuration,
      Clock clock) {
    this.delegate = delegate;
    this.cache = Caffeine.newBuilder().maximumSize(maxSize).expireAfter(
        new Expiry<CacheKey, List<VendedStorageCredential>>() {
          private long calculateExpiration(List<VendedStorageCredential> credentials) {
            var now = clock.instant();
            // Choose the minimal one / 2 in case there is clock-skew
            var expiredIn = credentials.stream().map(VendedStorageCredential::expiredAt)
                .map(expiredAt -> Duration.between(now, expiredAt).dividedBy(2)).min(Duration::compareTo);
            return expiredIn.map(duration -> Math.min(duration.toNanos(), maxCacheDuration.toNanos()))
                .orElseGet(maxCacheDuration::toNanos);
          }

          @Override
          public long expireAfterCreate(@NotNull CachedVendedCredentialProvider.CacheKey key,
              @NotNull List<VendedStorageCredential> value, long currentTime) {
            return calculateExpiration(value);
          }

          @Override
          public long expireAfterUpdate(@NotNull CachedVendedCredentialProvider.CacheKey key,
              @NotNull List<VendedStorageCredential> value, long currentTime, @NonNegative long currentDuration) {
            return calculateExpiration(value);
          }

          @Override
          public long expireAfterRead(@NotNull CachedVendedCredentialProvider.CacheKey key,
              @NotNull List<VendedStorageCredential> value, long currentTime, @NonNegative long currentDuration) {
            return currentDuration;
          }
        }).build();
  }

  @Override
  public boolean supports(StorageAccessRequest request) {
    return delegate.supports(request);
  }

  @Override
  public List<VendedStorageCredential> vend(String username, List<StorageAccessRequest> accessRequests) {
    return cache.get(new CacheKey(username, accessRequests), k -> delegate.vend(username, accessRequests));
  }
}
