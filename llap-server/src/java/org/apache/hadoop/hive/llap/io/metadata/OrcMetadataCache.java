/**
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

package org.apache.hadoop.hive.llap.io.metadata;

import java.io.IOException;

import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * TODO#: doc ORC-specific metadata cache.
 * TODO: should be merged with main cache somehow if we find this takes too much memory
 */
public class OrcMetadataCache {
  private static final int DEFAULT_CACHE_ACCESS_CONCURRENCY = 10;
  private static final int DEFAULT_MAX_FILE_ENTRIES = 1000;
  private static final int DEFAULT_MAX_STRIPE_ENTRIES = 10000;
  private static Cache<Long, OrcFileMetadata> METADATA;
  private static Cache<OrcBatchKey, OrcStripeMetadata> STRIPE_METADATA;
  private static OrcMetadataCache instance = new OrcMetadataCache();
  private OrcMetadataCache() {}

  static {
    METADATA = CacheBuilder.newBuilder()
        .concurrencyLevel(DEFAULT_CACHE_ACCESS_CONCURRENCY)
        .maximumSize(DEFAULT_MAX_FILE_ENTRIES)
        .build();
    STRIPE_METADATA = CacheBuilder.newBuilder()
        .concurrencyLevel(DEFAULT_CACHE_ACCESS_CONCURRENCY)
        .maximumSize(DEFAULT_MAX_STRIPE_ENTRIES)
        .build();
  }

  public static OrcMetadataCache getInstance() {
    return instance;
  }

  public void putFileMetadata(long fileId, OrcFileMetadata metaData) {
    METADATA.put(fileId, metaData);
  }

  public void putStripeMetadata(OrcBatchKey stripeKey, OrcStripeMetadata metaData) {
    STRIPE_METADATA.put(stripeKey, metaData);
  }

  public OrcStripeMetadata getStripeMetadata(OrcBatchKey stripeKey) throws IOException {
    return STRIPE_METADATA.getIfPresent(stripeKey);
  }

  public OrcFileMetadata getFileMetadata(long fileId) throws IOException {
    return METADATA.getIfPresent(fileId);
  }
}
