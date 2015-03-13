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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.llap.cache.EvictionListener;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheMemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;

public class OrcMetadataCache implements EvictionListener {
  private final ConcurrentHashMap<Long, OrcFileMetadata> metadata =
      new ConcurrentHashMap<Long, OrcFileMetadata>();
  private final ConcurrentHashMap<OrcBatchKey, OrcStripeMetadata> stripeMetadata =
      new ConcurrentHashMap<OrcBatchKey, OrcStripeMetadata>();
  private final LowLevelCacheMemoryManager memoryManager;
  private final LowLevelCachePolicy policy;

  public OrcMetadataCache(LowLevelCacheMemoryManager memoryManager, LowLevelCachePolicy policy) {
    this.memoryManager = memoryManager;
    this.policy = policy;
  }

  public void putFileMetadata(OrcFileMetadata metaData) {
    memoryManager.reserveMemory(metaData.getMemoryUsage(), false);
    policy.cache(metaData, Priority.HIGH);
    policy.notifyUnlock(metaData); // See OrcFileMetadata, it is always unlocked.
    metadata.put(metaData.getFileId(), metaData);
  }

  public void putStripeMetadata(OrcStripeMetadata metaData) {
    memoryManager.reserveMemory(metaData.getMemoryUsage(), false);
    policy.cache(metaData, Priority.HIGH);
    policy.notifyUnlock(metaData); // See OrcStripeMetadata, it is always unlocked.
    stripeMetadata.put(metaData.getKey(), metaData);
  }

  public OrcStripeMetadata getStripeMetadata(OrcBatchKey stripeKey) throws IOException {
    return stripeMetadata.get(stripeKey);
  }

  public OrcFileMetadata getFileMetadata(long fileId) throws IOException {
    return metadata.get(fileId);
  }

  public void notifyEvicted(OrcFileMetadata buffer) {
    metadata.remove(buffer.getFileId());
    // See OrcFileMetadata - we don't clear the object, it will be GCed when released by users.
  }

  public void notifyEvicted(OrcStripeMetadata buffer) {
    stripeMetadata.remove(buffer.getKey());
    // See OrcStripeMetadata - we don't clear the object, it will be GCed when released by users.
  }

  @Override
  public void notifyEvicted(LlapCacheableBuffer buffer) {
    if (buffer instanceof OrcStripeMetadata) {
      notifyEvicted((OrcStripeMetadata)buffer);
    } else {
      assert buffer instanceof OrcFileMetadata;
      notifyEvicted((OrcFileMetadata)buffer);
    }
  }
}
