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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.test.concurrent;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobContext;
import org.apache.iceberg.mr.hive.HiveIcebergOutputCommitter;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;
import org.apache.iceberg.mr.hive.test.concurrent.TestUtilPhaser.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test stub for HiveIcebergStorageHandler that coordinates concurrent write testing.
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
public class HiveIcebergStorageHandlerStub extends HiveIcebergStorageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergStorageHandlerStub.class);

  @Override
  public HiveIcebergOutputCommitter getOutputCommitter() {

    return new HiveIcebergOutputCommitter() {
      @Override
      public void commitJobs(List<JobContext> originalContextList, Context.Operation operation) throws IOException {
        int queryIndex = ThreadContext.getQueryIndex();
        LOG.debug("Thread {} (queryIndex={}) entered commitJobs", Thread.currentThread(), queryIndex);

        waitForAllWritesToComplete();
        LOG.debug("Thread {} (queryIndex={}) starting commitJobs", Thread.currentThread(), queryIndex);

        super.commitJobs(originalContextList, operation);

        // Barrier Mode: If the commit succeeded, release the turn so the next thread can proceed.
        // If commit failed (exception thrown), turn is NOT released, allowing retry logic to
        // reuse the current turn.
        if (TestUtilPhaser.isInstantiated() && !ThreadContext.useExtLocking()) {
          TestUtilPhaser.getInstance().completeTurn();
          LOG.debug("Thread {} (queryIndex={}) committed and released turn",
              Thread.currentThread(), queryIndex);
        }
      }

      private static void waitForAllWritesToComplete() {
        if (!TestUtilPhaser.isInstantiated()) {
          return;
        }

        int queryIndex = ThreadContext.getQueryIndex();
        boolean useExtLocking = ThreadContext.useExtLocking();

        if (useExtLocking) {
          // Ext-locking mode: Table lock should have prevented this query from starting concurrently.
          // Sleep briefly to ensure that if the lock is broken, the concurrent query has time
          // to incorrectly advance the phase.
          ThreadUtils.sleepQuietly(Duration.ofMillis(500));
          int phase = TestUtilPhaser.getInstance().getBarrier().getPhase();

          if (phase != queryIndex + 1) {
            throw new IllegalStateException(
                String.format("External locking violation: query sql[%d] expected phase %d, got %d",
                    queryIndex, queryIndex + 1, phase));
          }
        } else {
          // Barrier mode: Optimistic concurrency.
          // 1. Barrier: Wait for ALL queries to reach commit stage to maximize conflict potential.
          // 2. Sequential Commit: Execute commits one-by-one to control ordering.
          if (!ThreadContext.isSynced()) {
            LOG.debug("Thread {} (queryIndex={}) waiting at barrier", Thread.currentThread(), queryIndex);
            TestUtilPhaser.getInstance().getBarrier().arriveAndAwaitAdvance();
            ThreadContext.markSynced();
          }

          LOG.debug("Thread {} (queryIndex={}) waiting for commit turn", Thread.currentThread(), queryIndex);
          TestUtilPhaser.getInstance().awaitTurn();
        }
      }
    };
  }

  @Override
  public void validateCurrentSnapshot(TableDesc tableDesc) {
    super.validateCurrentSnapshot(tableDesc);

    if (!TestUtilPhaser.isInstantiated()) {
      return;
    }

    if (ThreadContext.useExtLocking()) {
      // Ext-locking mode: Release the turn so the next sequential query can proceed
      // from 'awaitTurn' to start its own execution.
      TestUtilPhaser.getInstance().completeTurn();
      LOG.debug("Thread {} (queryIndex={}) validated snapshot and released turn",
          Thread.currentThread(), ThreadContext.getQueryIndex());
    }
  }
}
