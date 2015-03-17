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
import org.apache.hadoop.hive.llap.cache.MemoryManager;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;

public class OrcMetadataCache implements EvictionListener {
  private final ConcurrentHashMap<Long, OrcFileMetadata> metadata =
      new ConcurrentHashMap<Long, OrcFileMetadata>();
  private final ConcurrentHashMap<OrcBatchKey, OrcStripeMetadata> stripeMetadata =
      new ConcurrentHashMap<OrcBatchKey, OrcStripeMetadata>();
  private final MemoryManager memoryManager;
  private final LowLevelCachePolicy policy;

  public OrcMetadataCache(MemoryManager memoryManager, LowLevelCachePolicy policy) {
    this.memoryManager = memoryManager;
    this.policy = policy;
  }

  public OrcFileMetadata putFileMetadata(OrcFileMetadata metaData) {
    long memUsage = metaData.getMemoryUsage();
    memoryManager.reserveMemory(memUsage, false);
    OrcFileMetadata val = metadata.putIfAbsent(metaData.getFileId(), metaData);
    // See OrcFileMetadata; it is always unlocked, so we just "touch" it here to simulate use.
    if (val == null) {
      val = metaData;
      policy.cache(val, Priority.HIGH);
    } else {
      memoryManager.releaseMemory(memUsage);
      policy.notifyLock(val);
    }
    policy.notifyUnlock(val);
    return val;
  }

  public OrcStripeMetadata putStripeMetadata(OrcStripeMetadata metaData) {
    long memUsage = metaData.getMemoryUsage();
    memoryManager.reserveMemory(memUsage, false);
    OrcStripeMetadata val = stripeMetadata.putIfAbsent(metaData.getKey(), metaData);
    // See OrcStripeMetadata; it is always unlocked, so we just "touch" it here to simulate use.
    if (val == null) {
      val = metaData;
      policy.cache(val, Priority.HIGH);
    } else {
      memoryManager.releaseMemory(memUsage);
      policy.notifyLock(val);
    }
    policy.notifyUnlock(val);
    return val;
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
