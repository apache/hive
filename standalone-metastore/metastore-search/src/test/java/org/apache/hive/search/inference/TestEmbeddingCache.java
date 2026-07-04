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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.config.InferenceConfig;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestEmbeddingCache {

  @Test
  public void getReturnsCopyOfStoredVector() {
    EmbeddingCache cache = EmbeddingCache.create(enabledCacheConf());
    float[] embedding = {0.1f, 0.2f, 0.3f};
    cache.put("model-a", EmbedModel.TaskType.DOCUMENT, "sales orders", embedding);

    float[] cached = cache.get("model-a", EmbedModel.TaskType.DOCUMENT, "sales orders").orElseThrow();
    assertArrayEquals(embedding, cached, 0.001f);
    cached[0] = 9f;
    float[] again = cache.get("model-a", EmbedModel.TaskType.DOCUMENT, "sales orders").orElseThrow();
    assertEquals(0.1f, again[0], 0.001f);
  }

  @Test
  public void cacheIsScopedByModelAndTask() {
    EmbeddingCache cache = EmbeddingCache.create(enabledCacheConf());
    float[] doc = {1f};
    float[] query = {2f};
    cache.put("model-a", EmbedModel.TaskType.DOCUMENT, "text", doc);
    cache.put("model-a", EmbedModel.TaskType.QUERY, "text", query);
    cache.put("model-b", EmbedModel.TaskType.DOCUMENT, "text", new float[] {3f});

    assertArrayEquals(doc, cache.get("model-a", EmbedModel.TaskType.DOCUMENT, "text").orElseThrow(), 0.001f);
    assertArrayEquals(query, cache.get("model-a", EmbedModel.TaskType.QUERY, "text").orElseThrow(), 0.001f);
    assertArrayEquals(new float[] {3f},
        cache.get("model-b", EmbedModel.TaskType.DOCUMENT, "text").orElseThrow(), 0.001f);
  }

  @Test
  public void disabledCacheAlwaysMisses() {
    EmbeddingCache cache = EmbeddingCache.disabled();
    cache.put("model-a", EmbedModel.TaskType.DOCUMENT, "text", new float[] {1f});
    assertFalse(cache.get("model-a", EmbedModel.TaskType.DOCUMENT, "text").isPresent());
    assertEquals(1, cache.misses());
  }

  @Test
  public void tracksHitsAndMisses() {
    EmbeddingCache cache = EmbeddingCache.create(enabledCacheConf());
    cache.get("model-a", EmbedModel.TaskType.DOCUMENT, "missing");

    cache.put("model-a", EmbedModel.TaskType.DOCUMENT, "sales", new float[] {1f});
    assertTrue(cache.get("model-a", EmbedModel.TaskType.DOCUMENT, "sales").isPresent());
    assertEquals(1, cache.hits());
    assertEquals(1, cache.misses());
  }

  private static Configuration enabledCacheConf() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(InferenceConfig.EMBEDDING_CACHE_ENABLED, true);
    conf.setInt(InferenceConfig.EMBEDDING_CACHE_MAX_ENTRIES, 1000);
    return conf;
  }
}
