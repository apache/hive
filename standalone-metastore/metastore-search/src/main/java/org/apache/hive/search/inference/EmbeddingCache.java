/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.inference;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.config.InferenceConfig;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.Hashing;

/** In-memory LRU cache of document embeddings keyed by model, task, and text hash. */
public final class EmbeddingCache {
  private final Cache<CacheKey, float[]> cache;
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();

  private EmbeddingCache(Cache<CacheKey, float[]> cache) {
    this.cache = cache;
  }

  public static EmbeddingCache create(Configuration configuration) {
    InferenceConfig inference = new InferenceConfig(configuration);
    if (!inference.isEmbeddingCacheEnabled()) {
      return disabled();
    }
    return new EmbeddingCache(CacheBuilder.newBuilder()
        .maximumSize(inference.getEmbeddingCacheMaxEntries())
        .build());
  }

  public static EmbeddingCache disabled() {
    return new EmbeddingCache(null);
  }

  public Optional<float[]> get(String modelName, EmbedModel.TaskType task, String text) {
    if (cache == null) {
      misses.incrementAndGet();
      return Optional.empty();
    }
    CacheKey key = CacheKey.of(modelName, task, text);
    float[] cached = cache.getIfPresent(key);
    if (cached == null) {
      misses.incrementAndGet();
      return Optional.empty();
    }
    hits.incrementAndGet();
    return Optional.of(Arrays.copyOf(cached, cached.length));
  }

  public void put(String modelName, EmbedModel.TaskType task, String text, float[] embedding) {
    if (cache == null) {
      return;
    }
    cache.put(CacheKey.of(modelName, task, text), Arrays.copyOf(embedding, embedding.length));
  }

  public long hits() {
    return hits.get();
  }

  public long misses() {
    return misses.get();
  }

  public boolean enabled() {
    return cache != null;
  }

  private record CacheKey(String modelName, EmbedModel.TaskType task, long textHash,
                          String text) {
    static CacheKey of(String modelName, EmbedModel.TaskType task, String text) {
      long hash = Hashing.murmur3_128()
          .hashString(text, StandardCharsets.UTF_8)
          .asLong();
      return new CacheKey(modelName, task, hash, text);
    }
  }
}
