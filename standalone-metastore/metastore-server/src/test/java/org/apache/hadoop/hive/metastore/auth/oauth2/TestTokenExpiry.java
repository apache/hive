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

package org.apache.hadoop.hive.metastore.auth.oauth2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.nimbusds.oauth2.sdk.TokenIntrospectionSuccessResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.auth.oauth2.TokenIntrospectionAuthenticator.TokenExpiry;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestTokenExpiry {
  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
  private static final Duration MAX_EXPIRATION = Duration.ofMinutes(10);
  private static final TokenExpiry EXPIRY = new TokenExpiry(MAX_EXPIRATION, CLOCK);

  private static TokenIntrospectionSuccessResponse createResponse(Instant expirationTime) {
    return new TokenIntrospectionSuccessResponse.Builder(true)
        .expirationTime(Date.from(expirationTime))
        .build();
  }

  @Test
  public void testExpireAfterCreateWithFutureToken() {
    TokenIntrospectionSuccessResponse response = createResponse(NOW.plus(Duration.ofMinutes(5)));
    long nanos = EXPIRY.expireAfterCreate("token1", response, 0L);

    Assert.assertTrue("Expiry duration for valid future token must be positive", nanos > 0);
    Assert.assertEquals(Duration.ofMinutes(5).toNanos(), nanos);
  }

  @Test
  public void testCacheRetainsValidToken() {
    Cache<String, TokenIntrospectionSuccessResponse> cache =
        Caffeine.newBuilder().expireAfter(EXPIRY).build();

    TokenIntrospectionSuccessResponse response = createResponse(NOW.plus(Duration.ofMinutes(5)));
    cache.put("token1", response);

    Assert.assertSame(
        "Valid token must be cached and not expired immediately",
        response,
        cache.getIfPresent("token1"));
  }

  @Test
  public void testCacheHitBypassesLoader() {
    Cache<String, TokenIntrospectionSuccessResponse> cache =
        Caffeine.newBuilder().expireAfter(EXPIRY).build();

    TokenIntrospectionSuccessResponse response = createResponse(NOW.plus(Duration.ofMinutes(5)));
    cache.put("token1", response);

    AtomicBoolean loaderCalled = new AtomicBoolean(false);
    TokenIntrospectionSuccessResponse cached =
        cache.get(
            "token1",
            k -> {
              loaderCalled.set(true);
              return createResponse(NOW.plus(Duration.ofMinutes(10)));
            });

    Assert.assertSame("Cache hit must return existing instance", response, cached);
    Assert.assertFalse("Cache loader must not be invoked on hit", loaderCalled.get());
  }

  @Test
  public void testExpireAfterCreateCapsAtMaxExpiration() {
    TokenIntrospectionSuccessResponse response = createResponse(NOW.plus(Duration.ofHours(1)));
    long nanos = EXPIRY.expireAfterCreate("token1", response, 0L);

    Assert.assertEquals("Expiry must be capped at maxExpiration", MAX_EXPIRATION.toNanos(), nanos);
  }

  @Test
  public void testExpireAfterCreateWithExpiredToken() {
    TokenIntrospectionSuccessResponse response = createResponse(NOW.minus(Duration.ofMinutes(1)));
    long nanos = EXPIRY.expireAfterCreate("token1", response, 0L);

    Assert.assertEquals("Expired token must return 0 nanos duration", 0L, nanos);
  }

  @Test
  public void testCacheEvictsExpiredTokenImmediately() {
    Cache<String, TokenIntrospectionSuccessResponse> cache =
        Caffeine.newBuilder().expireAfter(EXPIRY).build();

    TokenIntrospectionSuccessResponse response = createResponse(NOW.minus(Duration.ofMinutes(1)));
    cache.put("expiredToken", response);

    Assert.assertNull(
        "Expired token must not be retained in cache", cache.getIfPresent("expiredToken"));
  }

  @Test
  public void testExpireAfterUpdate() {
    TokenIntrospectionSuccessResponse response = createResponse(NOW.plus(Duration.ofMinutes(3)));
    long nanos = EXPIRY.expireAfterUpdate("token1", response, 0L, Duration.ofMinutes(5).toNanos());

    Assert.assertTrue("Expiry duration on update must be positive", nanos > 0);
    Assert.assertEquals(Duration.ofMinutes(3).toNanos(), nanos);
  }

  @Test
  public void testExpireAfterReadPreservesDuration() {
    TokenIntrospectionSuccessResponse response = createResponse(NOW.plus(Duration.ofMinutes(5)));
    long currentDuration = Duration.ofMinutes(4).toNanos();
    long nanos = EXPIRY.expireAfterRead("token1", response, 0L, currentDuration);

    Assert.assertEquals(
        "expireAfterRead must preserve the current duration", currentDuration, nanos);
  }
}
