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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CompositeVendedCredentialProvider implements VendedCredentialProvider {
  private static final class FallbackVendedCredentialProvider implements VendedCredentialProvider {
    @Override
    public boolean supports(StorageAccessRequest request) {
      return true;
    }

    @Override
    public List<VendedStorageCredential> vend(String username, List<StorageAccessRequest> accessRequests) {
      return List.of();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(CompositeVendedCredentialProvider.class);
  private static final String PROVIDERS_KEY_PREFIX =
      MetastoreConf.ConfVars.CATALOG_VENDED_CREDENTIALS_PROVIDERS.getVarname();
  private static final String CLASS_KEY = "class";
  private static final String CACHE_MAX_SIZE_KEY = "cache.max-size";
  private static final String CACHE_MAX_DURATION_KEY = "cache.max-duration";
  private static final Duration DEFAULT_MAX_CACHE_DURATION = Duration.ofMinutes(30);
  private static final VendedCredentialProvider FALLBACK_PROVIDER = new FallbackVendedCredentialProvider();

  private final List<VendedCredentialProvider> providers;

  private static VendedCredentialProvider create(Configuration conf, String providerId) {
    final var providerConfigKeyPrefix = "%s.%s.".formatted(PROVIDERS_KEY_PREFIX, providerId);
    final var classKey = providerConfigKeyPrefix + CLASS_KEY;
    final var clazz = conf.getClass(classKey, null, VendedCredentialProvider.class);
    if (clazz == null) {
      throw new IllegalArgumentException(
          "No vended credential provider class configured for provider ID: " + providerId);
    }

    final VendedCredentialProvider provider;
    try {
      final var constructor = clazz.getDeclaredConstructor(String.class, Configuration.class);
      provider = constructor.newInstance(providerConfigKeyPrefix, conf);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("Failed to instantiate vended credential provider: " + clazz.getName(), e);
    }

    final var maxCacheSize = conf.getInt(providerConfigKeyPrefix + CACHE_MAX_SIZE_KEY, 0);
    if (maxCacheSize <= 0) {
      LOG.info("Created VendedCredentialProvider, {}, without cache", provider);
      return provider;
    }

    final var maxCacheDuration = Duration.ofNanos(
        conf.getTimeDuration(providerConfigKeyPrefix + CACHE_MAX_DURATION_KEY,
        DEFAULT_MAX_CACHE_DURATION.toNanos(), TimeUnit.NANOSECONDS));
    LOG.info("Created VendedCredentialProvider, {}, with caching (capacity={}, duration={}) ", provider, maxCacheSize,
        maxCacheDuration);
    return new CachedVendedCredentialProvider(provider, maxCacheSize, maxCacheDuration, Clock.systemUTC());
  }

  public CompositeVendedCredentialProvider(Configuration conf) {
    this(
        Arrays
            .stream(
                MetastoreConf.getTrimmedStringsVar(conf, MetastoreConf.ConfVars.CATALOG_VENDED_CREDENTIALS_PROVIDERS))
            .filter(providerId -> !providerId.isEmpty())
            .map(providerId -> create(conf, providerId))
            .toList()
    );
  }

  @VisibleForTesting
  CompositeVendedCredentialProvider(List<VendedCredentialProvider> providers) {
    this.providers = providers;
  }

  @Override
  public boolean supports(StorageAccessRequest request) {
    return true;
  }

  private VendedCredentialProvider providerFor(StorageAccessRequest request) {
    return providers.stream()
        .filter(provider -> provider.supports(request))
        .findFirst()
        .orElse(FALLBACK_PROVIDER);
  }

  @Override
  public List<VendedStorageCredential> vend(String username, List<StorageAccessRequest> accessRequests) {
    final var requestsByProvider = accessRequests.stream()
        .collect(Collectors.groupingBy(this::providerFor, LinkedHashMap::new, Collectors.toList()));
    return requestsByProvider.entrySet().stream()
        .flatMap(entry -> entry.getKey().vend(username, entry.getValue()).stream())
        .toList();
  }
}
