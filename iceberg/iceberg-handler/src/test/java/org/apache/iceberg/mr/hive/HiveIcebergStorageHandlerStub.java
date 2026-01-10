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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobContext;
import org.apache.iceberg.mr.hive.TestUtilPhaser.ThreadContext;

/**
 * HiveIcebergStorageHandlerStub is used only for unit tests.
 * Currently, we use it to achieve a specific thread interleaving to simulate conflicts in concurrent writes
 * deterministically.
 */
public class HiveIcebergStorageHandlerStub extends HiveIcebergStorageHandler {

  @Override
  public HiveIcebergOutputCommitter getOutputCommitter() {
    return new HiveIcebergOutputCommitter() {
      @Override
      public void commitJobs(List<JobContext> originalContextList, Context.Operation operation) throws IOException {
        waitForAllWritesToComplete();

        super.commitJobs(originalContextList, operation);
      }

      private static void waitForAllWritesToComplete() {
        if (!TestUtilPhaser.isInstantiated()) {
          return;
        }

        int index = ThreadContext.getIndex();
        try {
          if (index < 0) {
            TestUtilPhaser.getInstance().getPhaser().arriveAndAwaitAdvance();
          } else {
            // For execution with ext-locking: verify next thread hasn't arrived due to present lock on a table
            int currentPhase = TestUtilPhaser.getInstance().getPhaser().getPhase();
            int nextThreadIndex = index + 1;

            // Wait 200ms for next thread to potentially arrive
            Thread.sleep(200);

            // Check if next thread has arrived (would advance phase to nextThreadIndex)
            int phaseAfterWait = TestUtilPhaser.getInstance().getPhaser().getPhase();
            if (phaseAfterWait >= nextThreadIndex) {
              throw new IllegalStateException(
                  String.format("Locking violation: thread %d arrived before thread %d committed. " +
                      "Phase before wait: %d, phase after wait: %d",
                      nextThreadIndex, index, currentPhase, phaseAfterWait));
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public void validateCurrentSnapshot(TableDesc tableDesc) {
    super.validateCurrentSnapshot(tableDesc);

    if (!TestUtilPhaser.isInstantiated() || ThreadContext.getIndex() < 0) {
      return;
    }
    // Signal next thread ONLY when ext-locking is enabled
    TestUtilPhaser.getInstance().completeTurn();
  }

}
