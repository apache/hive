/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.test.concurrent;

import java.util.concurrent.Phaser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test utility for coordinating concurrent query execution in multi-threaded tests.
 *
 * <p>Uses {@link Phaser}s for synchronization:
 * <ul>
 *   <li><b>barrier</b>: Synchronizes threads at a barrier (used in Barrier mode).
 *   <li><b>turn</b>: Enforces sequential execution order (0 -> 1 -> 2 ...).
 * </ul>
 * <ul>
 *   <li><b>Ext-locking mode</b>: turn ensures sequential execution, barrier verification.
 *   <li><b>Barrier mode</b>: barrier syncs threads, turn enforces sequential commits.
 * </ul>
 *
 * @see HiveIcebergStorageHandlerStub
 */
public class TestUtilPhaser {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtilPhaser.class);

  private static TestUtilPhaser instance;

  private final Phaser barrier;
  private final Phaser turn;

  private TestUtilPhaser() {
    barrier = new Phaser() {
      @Override
      protected boolean onAdvance(int phase, int registeredParties) {
        // Prevent termination even if registered parties drop to zero
        return false;
      }
    };
    // Initialize with 1 party to drive the phase advances
    turn = new Phaser(1);
  }

  /**
   * Registers this thread with the barrier phaser.
   */
  public void register() {
    barrier.register();
  }

  public static synchronized TestUtilPhaser getInstance() {
    if (instance == null) {
      LOG.debug("Instantiating TestUtilPhaser for concurrent test synchronization");
      instance = new TestUtilPhaser();
    }
    return instance;
  }

  public Phaser getBarrier() {
    return barrier;
  }

  public static synchronized boolean isInstantiated() {
    return instance != null;
  }

  /**
   * Waits for this query's turn to execute/commit.
   * Uses turn to block until the phase reaches (queryIndex).
   * Query 0 proceeds immediately (phase 0).
   * Query 1 waits for phase 0 -> 1.
   */
  public void awaitTurn() {
    int queryIndex = ThreadContext.getQueryIndex();
    if (queryIndex > 0) {
      // Wait for phase to advance from (queryIndex - 1) to queryIndex
      // awaitAdvance returns immediately if current phase != arg
      // So we wait on (queryIndex - 1)
      turn.awaitAdvance(queryIndex - 1);
    }
  }

  /**
   * Signals this query completed, allowing next query to proceed.
   * In ext-locking mode, temporarily registers then deregisters to advance barrier phase.
   * Advances turn to allow next query to start.
   */
  public void completeTurn() {
    if (ThreadContext.useExtLocking()) {
      // Ext-locking mode: register and immediately arriveAndDeregister to advance barrier phase
      barrier.register();
      barrier.arriveAndDeregister();
    }

    // Advance execution phase: queryIndex -> queryIndex + 1
    turn.arrive();
  }

  public Phaser[] getResources() {
    return new Phaser[] {barrier, turn};
  }

  public static synchronized void destroyInstance() {
    if (instance != null) {
      for (Phaser phaser : instance.getResources()) {
        phaser.forceTermination();
      }
      LOG.debug("Destroying TestUtilPhaser and clearing thread context");
      instance = null;
    }
    ThreadContext.clear();
  }

  /**
   * Thread-local context for per-thread synchronization state.
   *
   * <p>Fields:
   * <ul>
   *   <li><b>queryIndex</b>: Position in SQL array (sql[0]=0, sql[1]=1, ...)</li>
   *   <li><b>useExtLocking</b>: Whether external locking mode is enabled</li>
   *   <li><b>synced</b>: Whether this thread has synced at barrier (for retry detection in barrier mode)</li>
   * </ul>
   */
  public static final class ThreadContext {
    private static final ThreadLocal<Integer> QUERY_INDEX = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> USE_EXT_LOCKING = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> SYNCED = new ThreadLocal<>();

    private ThreadContext() {
    }

    public static void setQueryIndex(int queryIndex) {
      QUERY_INDEX.set(queryIndex);
    }

    public static int getQueryIndex() {
      Integer idx = QUERY_INDEX.get();
      return idx != null ? idx : -1;
    }

    public static void setUseExtLocking(boolean useExtLocking) {
      USE_EXT_LOCKING.set(useExtLocking);
    }

    public static boolean useExtLocking() {
      Boolean extLocking = USE_EXT_LOCKING.get();
      return extLocking != null && extLocking;
    }

    public static void markSynced() {
      SYNCED.set(true);
    }

    public static boolean isSynced() {
      Boolean synced = SYNCED.get();
      return synced != null && synced;
    }

    private static void clear() {
      QUERY_INDEX.remove();
      USE_EXT_LOCKING.remove();
      SYNCED.remove();
    }
  }
}
