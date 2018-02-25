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

package org.apache.hadoop.hive.registry.common;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A synchronization utility that allows a thread to take a mutual exclusion lock on a specific slot which is for a given Key K.
 * <p>
 * Each instance of this class can have multiple slots and one can take a lock on a specific slot, execute statements and unlock.
 * So, there can be contention for each slot but no contention among different slots.
 * <p>
 * Locking and unlocking a slot using {@link SlotSynchronizer} can be done using try/finally block like below.
 * <pre>
 * class Service {
 *   private final SlotSynchronizer&lt;K&gt; slotSynchronizer = new SlotSynchronizer&lt;&gt;();
 *   // ...
 *
 *   public void invoke() {
 *     K key = ...
 *     Lock lock = slotSynchronizer.runInSlot(key)
 *     try {
 *       //  ... block of code
 *     } finally {
 *         lock.unlock();
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * There is a way to run a block of code wrapped in {@link Runnable} with {@link #runInSlot(Object, Runnable)} like below.
 * <p>
 * <pre>
 * class Service {
 *   private final SlotSynchronizer&lt;K&gt; slotSynchronizer = new SlotSynchronizer&lt;&gt;();
 *   // ...
 *
 *   public void invoke() {
 *     K key = ...
 *     slotSynchronizer.runInSlot(key, new Runnable() {
 *       public void run() {
 *          // ... block of code
 *       }
 *     }
 *   }
 * }
 * </pre>
 *
 * @param <K> the type of keys for a slot
 */
public class SlotSynchronizer<K> {
    private static final Logger LOG = LoggerFactory.getLogger(SlotSynchronizer.class);

    private final ConcurrentHashMap<K, Lock> locks = new ConcurrentHashMap<>();

    public final class Lock {
        private final K k;
        private AtomicInteger count = new AtomicInteger();
        private ReentrantLock reentrantLock = new ReentrantLock();

        public Lock(K k) {
            Preconditions.checkNotNull(k, "Key k must not be null");
            this.k = k;
        }

        private void lock() {
            count.incrementAndGet();
            reentrantLock.lock();
        }

        /**
         * Unlocks this lock for respective slot if the current thread holds this lock.
         *
         * Current thread should have hold this lock earlier with { SlotSynchronizer#lockSlot(K k)} for key {@code k}.
         *
         * @throws IllegalStateException if the current thread does not hold this lock.
         */
        public void unlock() {
            if (!reentrantLock.isHeldByCurrentThread()) {
                String msg = String.format("Current thread [%s] does not hold the lock, unlock should have been called by " +
                                                   "the thread which invoked lock earlier.", Thread.currentThread());
                LOG.error(msg);
                throw new IllegalStateException(msg);
            }

            count.decrementAndGet();

            // remove this slot if it's count is zero.
            if(count.get() == 0) {
                locks.remove(k, this);
            }

            reentrantLock.unlock();
        }
    }

    /**
     * Returns the lock for the given slot {@code k} after taking a lock for the current thread if it available.
     *
     * If the lock is held by another thread then the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired.
     *
     * @param k slot key for which lock to be taken.
     */
    public Lock lockSlot(K k) {
        Preconditions.checkNotNull(k, "Key k must not be null");

        while (true) {
            Lock newLock = new Lock(k);
            Lock lock = locks.putIfAbsent(k, newLock);
            if (lock == null) {
                lock = newLock;
            }

            // wait to acquire lock
            lock.lock();

            // below is possible when current lock is incremented after removing that from slot by earlier {@link #unlock} operation.
            // if acquired lock for that slot is not same as the current lock, unlock the acquired lock and retry again to get a new lock.
            if (locks.get(k) != lock) {
                lock.unlock();
                continue;
            }

            return lock;
        }
    }

    /**
     * @return no of slots currently used.
     */
    public int occupiedSlots() {
        return locks.size();
    }

    public void runInSlot(K k, Runnable runnable) {
        Lock lock = lockSlot(k);
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

}
