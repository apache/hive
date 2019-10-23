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

package org.apache.hadoop.hive.llap.cache;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer.INVALIDATE_FAILED;
import static org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer.INVALIDATE_OK;

@RunWith(Parameterized.class)
public class ClockCachePolicyTest {
  private static final int BUFFER_SIZE = 16;
  private static final long SEED = 9999;
  public static final Predicate<Integer> IS_EVEN = x -> x % 2 == 0;
  public static final Function<Integer, Integer> MOD_3 = x -> x % 3;
  private final int numPages;

  ClockCachePolicyTest.EvictionTracker defaultEvictionListener = new ClockCachePolicyTest.EvictionTracker();

  public ClockCachePolicyTest(int numElements) {
    this.numPages = numElements;
  }

  @Parameterized.Parameters
  public static Collection collectioNumbers() {
    return Arrays.asList(new Object[][] {
        { 0 },
        { 1 },
        { 2 },
        { 5 },
        { 10009 },
        { 20002 }
    });
  }

  @Test public void testCacheCalls() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages);
    Collections.shuffle(access, new Random(SEED));
    ClockCachePolicy policy = new ClockCachePolicy();
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    //expect that clock hand is the first element and last element will be clockHand.prev
    LlapCacheableBuffer currentHand = policy.getClockHand();
    LlapCacheableBuffer lastElement = numPages == 0 ? null : currentHand.prev;
    if (numPages == 0) {
      // no reason to test for this case
      return;
    }
    int i = 0;
    while (currentHand != lastElement) {
      Assert.assertEquals(String.format("Buffers at iteration [%d] not matching", i), currentHand, access.get(i));
      currentHand = currentHand.next;
      i++;
    }
    Assert.assertEquals(lastElement, access.size() == 0 ? null : access.get(i));
    Assert.assertEquals(i, numPages - 1);

    // going counter clock wise
    i = access.size() - 1;
    currentHand = policy.getClockHand().prev;
    lastElement = currentHand.next;
    while (currentHand != lastElement) {
      Assert.assertEquals(String.format("Buffers at iteration [%d] not matching", i), currentHand, access.get(i));
      currentHand = currentHand.prev;
      i--;
    }
    Assert.assertEquals(lastElement, access.get(i));
    Assert.assertEquals(i, 0);
  }

  @Test(timeout = 6000L) public void testEvictionOneByOne() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages);
    Collections.shuffle(access, new Random(SEED));
    ClockCachePolicy policy = new ClockCachePolicy(5);
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    // set the flag for buffer with even index
    for (int i = 0; i < access.size(); i++) {
      if (i % 2 == 0 ) {
        // touch some buffers.
        policy.notifyLock(access.get(i));
      }
    }

    while (policy.getClockHand() != null) {
      // should not over evict
      Assert.assertEquals(BUFFER_SIZE, policy.evictSomeBlocks(BUFFER_SIZE));
    }
    Assert.assertEquals(defaultEvictionListener.evicted.size(), numPages);
    // check the order of the eviction
    for (int i = 0; i < access.size() / 2; i++) {
      Assert.assertEquals(defaultEvictionListener.evicted.get(i), access.get(i * 2 + 1));
      Assert.assertEquals(defaultEvictionListener.evicted.get(i +  access.size() / 2), access.get(i * 2));
    }

  }

  @Test public void testEvictionWithLockedBuffers() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages, IS_EVEN);
    Collections.shuffle(access, new Random(SEED));
    ClockCachePolicy policy = new ClockCachePolicy(5);
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    List<LlapCacheableBuffer>
        unlockedBuffers =
        access.stream()
            .filter(llapCacheableBuffer -> llapCacheableBuffer.invalidate() == INVALIDATE_OK)
            .collect(Collectors.toList());
    Assert.assertEquals(unlockedBuffers.stream().mapToLong(LlapCacheableBuffer::getMemoryUsage).sum(), policy.purge());
  }


  @Test public void testEvictionWithLockedBuffersAndInvalid() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffersWithFn(numPages, MOD_3);
    Collections.shuffle(access, new Random(SEED));
    ClockCachePolicy policy = new ClockCachePolicy(5);
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    List<LlapCacheableBuffer>
        invalidateOkBuffers =
        access.stream()
            .filter(llapCacheableBuffer -> llapCacheableBuffer.invalidate() == INVALIDATE_OK)
            .collect(Collectors.toList());

    List<LlapCacheableBuffer>
        lockedBuffer =
        access.stream()
            .filter(llapCacheableBuffer -> llapCacheableBuffer.invalidate() == INVALIDATE_FAILED)
            .collect(Collectors.toList());

    Assert.assertEquals(invalidateOkBuffers.stream().mapToLong(LlapCacheableBuffer::getMemoryUsage).sum(), policy.purge());
    List<LlapCacheableBuffer> actualLocked = Lists.newArrayList(policy.getIterator());
    Assert.assertEquals(lockedBuffer, actualLocked);
  }

  @Test public void testPurgeWhenAllUnlocked() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages);
    Collections.shuffle(access, new Random(SEED));
    ClockCachePolicy policy = new ClockCachePolicy();
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    policy.purge();
    Assert.assertEquals(defaultEvictionListener.evicted.size(), numPages);
    for (int i = 0; i < access.size(); i++) {
      Assert.assertEquals(defaultEvictionListener.evicted.get(i), access.get(i));
    }
  }


  @Test public void testPurgeWhenAllLocked() {
    defaultEvictionListener.evicted.clear();
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages, i -> false);
    Collections.shuffle(access, new Random(SEED));
    ClockCachePolicy policy = new ClockCachePolicy();
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    Assert.assertEquals(0, policy.purge());
  }

  @NotNull public static List<LlapCacheableBuffer> getLlapCacheableBuffers(int numPages) {
    return getLlapCacheableBuffers(numPages, i -> true);
  }

  @NotNull public static List<LlapCacheableBuffer> getLlapCacheableBuffers(int numPages,
      Predicate<Integer> invalidatePredicate) {
    return IntStream.range(0, numPages)
        .mapToObj(i -> new TestLlapCacheableBuffer(BUFFER_SIZE,
            invalidatePredicate.test(i) ? INVALIDATE_OK : INVALIDATE_FAILED,
            String.valueOf(i)))
        .collect(Collectors.toList());
  }

  @NotNull public static List<LlapCacheableBuffer> getLlapCacheableBuffersWithFn(int numPages,
      Function<Integer, Integer> invalidatePredicate) {
    return IntStream.range(0, numPages)
        .mapToObj(i -> new TestLlapCacheableBuffer(BUFFER_SIZE,
            invalidatePredicate.apply(i),
            String.valueOf(i)))
        .collect(Collectors.toList());
  }

  public void testSimpleSequentialAccess() {
    int numPages = 1001;
    ClockCachePolicyTest.EvictionTracker evictionListener = new ClockCachePolicyTest.EvictionTracker();
    List<LlapCacheableBuffer>
        access =
        IntStream.range(0, numPages)
            .mapToObj(i -> new ClockCachePolicyTest.TestLlapCacheableBuffer(BUFFER_SIZE,
                i % 2 == 0 ? INVALIDATE_OK : INVALIDATE_FAILED,
                String.valueOf(i)))
            .collect(Collectors.toList());
    Collections.shuffle(access, ThreadLocalRandom.current());
    ClockCachePolicy policy = new ClockCachePolicy();
    policy.setEvictionListener(evictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));
    Assert.assertEquals(policy.getClockHand().prev, access.get(access.size() - 1));
    Assert.assertEquals(policy.getClockHand(), access.size() > 1 ? access.get(0) : null);
    Collections.shuffle(access, ThreadLocalRandom.current());
    access.forEach(b -> {
      policy.notifyLock(b);
      policy.notifyUnlock(b);
    });
    //Assert.assertEquals(policy.getClockHand().prev, access.get(access.size() - 1));
    //Assert.assertEquals(policy.getClockHand(), access.size() > 1 ? access.get(0) : null);
    Assert.assertEquals(access.stream()
        .filter(b -> b.invalidate() == INVALIDATE_OK)
        .mapToLong(LlapCacheableBuffer::getMemoryUsage)
        .sum(), policy.purge());
    Assert.assertArrayEquals(access.stream().filter(b -> b.invalidate() == INVALIDATE_OK).toArray(),
        evictionListener.evicted.toArray());
    Assert.assertNull(policy.getClockHand());
    // re add more stuff to the cache.
    access.forEach(policy::notifyUnlock);
    Assert.assertEquals(policy.getClockHand(), access.get(access.size() - 1));
    Assert.assertEquals(policy.getClockHand(), access.size() > 1 ? access.get(0) : null);
    //test eviction of some blocks
    long targetSize = BUFFER_SIZE * 5;
    evictionListener.evicted.clear();
    long evicted = policy.evictSomeBlocks(targetSize);
    Assert.assertEquals(targetSize, evicted);
    Assert.assertEquals(evicted,
        evictionListener.evicted.stream().mapToLong(LlapCacheableBuffer::getMemoryUsage).sum());
  }

  private static class TestLlapCacheableBuffer extends LlapAllocatorBuffer {
    private final long size;
    private int state;
    private final String tag;

    private TestLlapCacheableBuffer(long size, int state, String tag) {
      this.size = size;
      this.state = state;
      this.tag = tag;
    }

    @Override public int invalidate() {
      return state;
    }

    @Override public long getMemoryUsage() {
      return size;
    }

    @Override public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
      evictionDispatcher.notifyEvicted(this);
    }



    @Override public CacheTag getTag() {
      return CacheTag.build(tag);
    }

    @Override public boolean isLocked() {
      return state != INVALIDATE_OK;
    }
  }

  private class EvictionTracker implements EvictionListener {
    public List<LlapCacheableBuffer> evicted = new ArrayList<>();

    @Override public void notifyEvicted(LlapCacheableBuffer buffer) {
      evicted.add(buffer);
    }
  }

  @Test
  public void testCyclicProperty(){
    List<LlapCacheableBuffer> access = getLlapCacheableBuffers(numPages);
    Collections.shuffle(access, new Random(SEED));
    ClockCachePolicy policy = new ClockCachePolicy();
    policy.setEvictionListener(defaultEvictionListener);
    access.forEach(b -> policy.cache(b, LowLevelCache.Priority.NORMAL));

    LlapCacheableBuffer root = policy.getClockHand();
    LlapCacheableBuffer clockHand = root;

    if (numPages == 0 ) {
      Assert.assertNull(clockHand);
      return;// base case empty circle;
    }
    int i = 0;
    do {
      Assert.assertNotNull(clockHand.next);
      Assert.assertEquals(clockHand.next.prev, clockHand);
      clockHand = clockHand.next;
      i++;
    } while (clockHand != root);
    Assert.assertEquals("Clock entries size doesn't match", i, numPages);
  }
}