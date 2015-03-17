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
package org.apache.hadoop.hive.llap.cache;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.junit.Test;

public class TestOrcMetadataCache {
  private static final Log LOG = LogFactory.getLog(TestOrcMetadataCache.class);

  private static class DummyCachePolicy implements LowLevelCachePolicy {
    public DummyCachePolicy() {
    }

    public void cache(LlapCacheableBuffer buffer, Priority pri) {
    }

    public void notifyLock(LlapCacheableBuffer buffer) {
    }

    public void notifyUnlock(LlapCacheableBuffer buffer) {
    }

    public long evictSomeBlocks(long memoryToReserve) {
      return memoryToReserve;
    }

    public void setEvictionListener(EvictionListener listener) {
    }
  }

  private static class DummyMemoryManager implements MemoryManager {
    int allocs = 0;

    @Override
    public boolean reserveMemory(long memoryToReserve, boolean waitForEviction) {
      ++allocs;
      return true;
    }

    @Override
    public void releaseMemory(long memUsage) {
      --allocs;
    }
  }

  @Test
  public void testGetPut() throws Exception {
    DummyMemoryManager mm = new DummyMemoryManager();
    DummyCachePolicy cp = new DummyCachePolicy();
    OrcMetadataCache cache = new OrcMetadataCache(mm, cp);
    OrcFileMetadata ofm1 = OrcFileMetadata.createDummy(1), ofm2 = OrcFileMetadata.createDummy(2);
    assertSame(ofm1, cache.putFileMetadata(ofm1));
    assertEquals(1, mm.allocs);
    assertSame(ofm2, cache.putFileMetadata(ofm2));
    assertEquals(2, mm.allocs);
    assertSame(ofm1, cache.getFileMetadata(1));
    assertSame(ofm2, cache.getFileMetadata(2));
    OrcFileMetadata ofm3 = OrcFileMetadata.createDummy(1);
    assertSame(ofm1, cache.putFileMetadata(ofm3));
    assertEquals(2, mm.allocs);
    assertSame(ofm1, cache.getFileMetadata(1));

    OrcStripeMetadata osm1 = OrcStripeMetadata.createDummy(1), osm2 = OrcStripeMetadata.createDummy(2);
    assertSame(osm1, cache.putStripeMetadata(osm1));
    assertEquals(3, mm.allocs);
    assertSame(osm2, cache.putStripeMetadata(osm2));
    assertEquals(4, mm.allocs);
    assertSame(osm1, cache.getStripeMetadata(osm1.getKey()));
    assertSame(osm2, cache.getStripeMetadata(osm2.getKey()));
    OrcStripeMetadata osm3 = OrcStripeMetadata.createDummy(1);
    assertSame(osm1, cache.putStripeMetadata(osm3));
    assertEquals(4, mm.allocs);
    assertSame(osm1, cache.getStripeMetadata(osm3.getKey()));
  }
}
