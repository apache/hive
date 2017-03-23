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

import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.MemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;

public class OrcMetadataCache {
  private final ConcurrentHashMap<Object, OrcFileMetadata> metadata = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<OrcBatchKey, OrcStripeMetadata> stripeMetadata =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Object, OrcFileEstimateErrors> estimateErrors;
  private final MemoryManager memoryManager;
  private final LowLevelCachePolicy policy;

  public OrcMetadataCache(MemoryManager memoryManager, LowLevelCachePolicy policy,
      boolean useEstimateCache) {
    this.memoryManager = memoryManager;
    this.policy = policy;
    this.estimateErrors = useEstimateCache
        ? new ConcurrentHashMap<Object, OrcFileEstimateErrors>() : null;
  }

  public OrcFileMetadata putFileMetadata(OrcFileMetadata metaData) {
    long memUsage = metaData.getMemoryUsage();
    memoryManager.reserveMemory(memUsage);
    OrcFileMetadata val = metadata.putIfAbsent(metaData.getFileKey(), metaData);
    // See OrcFileMetadata; it is always unlocked, so we just "touch" it here to simulate use.
    return touchOnPut(metaData, val, memUsage);
  }

  public OrcStripeMetadata putStripeMetadata(OrcStripeMetadata metaData) {
    long memUsage = metaData.getMemoryUsage();
    memoryManager.reserveMemory(memUsage);
    OrcStripeMetadata val = stripeMetadata.putIfAbsent(metaData.getKey(), metaData);
    // See OrcStripeMetadata; it is always unlocked, so we just "touch" it here to simulate use.
    return touchOnPut(metaData, val, memUsage);
  }

  private <T extends LlapCacheableBuffer> T touchOnPut(T newVal, T oldVal, long memUsage) {
    if (oldVal == null) {
      oldVal = newVal;
      policy.cache(oldVal, Priority.HIGH);
    } else {
      memoryManager.releaseMemory(memUsage);
      policy.notifyLock(oldVal);
    }
    policy.notifyUnlock(oldVal);
    return oldVal;
  }


  public void putIncompleteCbs(Object fileKey, DiskRange[] ranges, long baseOffset) {
    if (estimateErrors == null) return;
    OrcFileEstimateErrors errorData = estimateErrors.get(fileKey);
    boolean isNew = false;
    // We should technically update memory usage if updating the old object, but we don't do it
    // for now; there is no mechanism to properly notify the cache policy/etc. wrt parallel evicts.
    if (errorData == null) {
      errorData = new OrcFileEstimateErrors(fileKey);
      for (DiskRange range : ranges) {
        errorData.addError(range.getOffset(), range.getLength(), baseOffset);
      }
      long memUsage = errorData.estimateMemoryUsage();
      memoryManager.reserveMemory(memUsage);
      OrcFileEstimateErrors old = estimateErrors.putIfAbsent(fileKey, errorData);
      if (old != null) {
        errorData = old;
        memoryManager.releaseMemory(memUsage);
        policy.notifyLock(errorData);
      } else {
        isNew = true;
        policy.cache(errorData, Priority.NORMAL);
      }
    }
    if (!isNew) {
      for (DiskRange range : ranges) {
        errorData.addError(range.getOffset(), range.getLength(), baseOffset);
      }
    }
    policy.notifyUnlock(errorData);
  }

  public OrcStripeMetadata getStripeMetadata(OrcBatchKey stripeKey) throws IOException {
    return touchOnGet(stripeMetadata.get(stripeKey));
  }

  public OrcFileMetadata getFileMetadata(Object fileKey) throws IOException {
    return touchOnGet(metadata.get(fileKey));
  }

  private <T extends LlapCacheableBuffer> T touchOnGet(T result) {
    if (result != null) {
      policy.notifyLock(result);
      policy.notifyUnlock(result); // Never locked for eviction; Java object.
    }
    return result;
  }

  public DiskRangeList getIncompleteCbs(Object fileKey, DiskRangeList ranges, long baseOffset,
      DiskRangeListFactory factory, BooleanRef gotAllData) {
    if (estimateErrors == null) return ranges;
    OrcFileEstimateErrors errors = estimateErrors.get(fileKey);
    if (errors == null) return ranges;
    return errors.getIncompleteCbs(ranges, baseOffset, factory, gotAllData);
  }

  public void notifyEvicted(OrcFileMetadata buffer) {
    metadata.remove(buffer.getFileKey());
    // See OrcFileMetadata - we don't clear the object, it will be GCed when released by users.
  }

  public void notifyEvicted(OrcStripeMetadata buffer) {
    stripeMetadata.remove(buffer.getKey());
    // See OrcStripeMetadata - we don't clear the object, it will be GCed when released by users.
  }

  public void notifyEvicted(OrcFileEstimateErrors buffer) {
    estimateErrors.remove(buffer.getFileKey());
  }
}
