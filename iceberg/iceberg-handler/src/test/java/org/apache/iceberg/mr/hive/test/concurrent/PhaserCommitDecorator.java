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

import java.time.Duration;
import org.apache.commons.lang3.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestUtil that coordinates concurrent write testing.
 *
 * <p>Supports two synchronization modes:
 * <ul>
 *   <li><b>Ext-locking mode</b>: Tests external table locking. Counter ensures queries start sequentially,
 *       next query starts when current reaches validateCurrentSnapshot. Phaser verifies lock prevents
 *       concurrent execution.</li>
 *   <li><b>Barrier mode</b>: Tests optimistic concurrency. All queries start concurrently, sync at barrier
 *       in commitJobs, then commit sequentially by index. Supports retry on conflict.</li>
 * </ul>
 */
public class PhaserCommitDecorator {

  private static final Logger LOG =
      LoggerFactory.getLogger(PhaserCommitDecorator.class);

  private PhaserCommitDecorator() {
  }

  @FunctionalInterface
  public interface CommitAction<E extends Exception> {
    void run() throws E;
  }

  public static <E extends Exception> void execute(CommitAction<E> action) throws E {
    int queryIndex = TestUtilPhaser.ThreadContext.getQueryIndex();
    LOG.debug("Thread {} (queryIndex={}) entered commit", Thread.currentThread(), queryIndex);

    waitForAllWritesToComplete();

    LOG.debug("Thread {} (queryIndex={}) starting commit", Thread.currentThread(), queryIndex);
    action.run();  // delegate call

    // Barrier Mode: If the commit succeeded, release the turn so the next thread can proceed.
    // If commit failed (exception thrown), turn is NOT released, allowing retry logic to
    // reuse the current turn.
    if (TestUtilPhaser.isInstantiated() &&
        !TestUtilPhaser.ThreadContext.useExtLocking()) {

      TestUtilPhaser.getInstance().completeTurn();

      LOG.debug("Thread {} (queryIndex={}) committed and released turn",
          Thread.currentThread(), queryIndex);
    }
  }

  public static void onSnapshotValidated() {
    if (TestUtilPhaser.isInstantiated() &&
        TestUtilPhaser.ThreadContext.useExtLocking()) {

      // Ext-locking mode: Release the turn so the next sequential query can proceed
      // from 'awaitTurn' to start its own execution.
      TestUtilPhaser.getInstance().completeTurn();

      LOG.debug("Thread {} (queryIndex={}) validated snapshot and released turn",
          Thread.currentThread(),
          TestUtilPhaser.ThreadContext.getQueryIndex());
    }
  }

  private static void waitForAllWritesToComplete() {
    if (!TestUtilPhaser.isInstantiated()) {
      return;
    }

    int queryIndex = TestUtilPhaser.ThreadContext.getQueryIndex();
    boolean useExtLocking =
        TestUtilPhaser.ThreadContext.useExtLocking();

    if (useExtLocking) {
      // Ext-locking mode: Table lock should have prevented this query from starting concurrently.
      // Sleep briefly to ensure that if the lock is broken, the concurrent query has time
      // to incorrectly advance the phase.
      ThreadUtils.sleepQuietly(Duration.ofMillis(500));
      int phase = TestUtilPhaser.getInstance().getBarrier().getPhase();

      if (phase != queryIndex + 1) {
        throw new IllegalStateException(
            "External locking violation: query sql[%d] expected phase %d, got %d".formatted(
                queryIndex, queryIndex + 1, phase));
      }

    } else {
      // Barrier mode: Optimistic concurrency.
      // 1. Barrier: Wait for ALL queries to reach commit stage to maximize conflict potential.
      // 2. Sequential Commit: Execute commits one-by-one to control ordering.
      if (!TestUtilPhaser.ThreadContext.isSynced()) {
        LOG.debug("Thread {} (queryIndex={}) waiting at barrier", Thread.currentThread(), queryIndex);

        TestUtilPhaser.getInstance()
            .getBarrier()
            .arriveAndAwaitAdvance();

        TestUtilPhaser.ThreadContext.markSynced();
      }

      LOG.debug("Thread {} (queryIndex={}) waiting for commit turn",
          Thread.currentThread(), queryIndex);

      TestUtilPhaser.getInstance().awaitTurn();
    }
  }

}
