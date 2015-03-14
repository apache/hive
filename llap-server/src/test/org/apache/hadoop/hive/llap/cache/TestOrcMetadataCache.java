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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListCreateHelper;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.CacheChunk;
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

    @Override
    public boolean reserveMemory(long memoryToReserve, boolean waitForEviction) {
      return true; // TODO: record?
    }
  }

  @Test
  public void testGetPut() {
    DummyMemoryManager mm = new DummyMemoryManager();
    DummyCachePolicy cp = new DummyCachePolicy();
    OrcMetadataCache cache = new OrcMetadataCache(mm, cp);
    // TODO#: implement
  }
  /*
    assertNull(cache.putFileData(fn1, drs(1, 2), fbs(fakes, 0, 1), 0, Priority.NORMAL));
    assertNull(cache.putFileData(fn2, drs(1, 2), fbs(fakes, 2, 3), 0, Priority.NORMAL));
    verifyCacheGet(cache, fn1, 1, 3, fakes[0], fakes[1]);
    verifyCacheGet(cache, fn2, 1, 3, fakes[2], fakes[3]);
    verifyCacheGet(cache, fn1, 2, 4, fakes[1], dr(3, 4));
    LlapMemoryBuffer[] bufsDiff = fbs(fakes, 4, 5);
    long[] mask = cache.putFileData(fn1, drs(3, 1), bufsDiff, 0, Priority.NORMAL);
    assertEquals(1, mask.length);
    assertEquals(2, mask[0]); // 2nd bit set - element 2 was already in cache.
    assertSame(fakes[0], bufsDiff[1]); // Should have been replaced
    verifyCacheGet(cache, fn1, 1, 4, fakes[0], fakes[1], fakes[4]);
  }

  private void verifyCacheGet(LowLevelCacheImpl cache, long fileId, Object... stuff) {
    DiskRangeListCreateHelper list = new DiskRangeListCreateHelper();
    DiskRangeList iter = null;
    int intCount = 0, lastInt = -1;
    int resultCount = stuff.length;
    for (Object obj : stuff) {
      if (obj instanceof Integer) {
        --resultCount;
        assertTrue(intCount >= 0);
        if (intCount == 0) {
          lastInt = (Integer)obj;
          intCount = 1;
        } else {
          list.addOrMerge(lastInt, (Integer)obj, true, true);
          intCount = 0;
        }
        continue;
      } else if (intCount >= 0) {
        assertTrue(intCount == 0);
        intCount = -1;
        iter = cache.getFileData(fileId, list.get(), 0);
        assertEquals(resultCount, iter.listSize());
      }
      assertTrue(iter != null);
      if (obj instanceof LlapMemoryBuffer) {
        assertTrue(iter instanceof CacheChunk);
        assertSame(obj, ((CacheChunk)iter).buffer);
      } else {
        assertTrue(iter.equals(obj));
      }
      iter = iter.next;
    }
  }

  */
}
