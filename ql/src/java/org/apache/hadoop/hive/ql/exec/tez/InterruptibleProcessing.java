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
package org.apache.hadoop.hive.ql.exec.tez;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class InterruptibleProcessing {
  private final static Logger LOG = LoggerFactory.getLogger(InterruptibleProcessing.class);
  private static final int CHECK_INTERRUPTION_AFTER_ROWS_DEFAULT = 1000,
      CHECK_INTERRUPTION_AFTER_ROWS_MAX = 100000, CHECK_INTERRUPTION_AFTER_ROWS_MIN = 1;
  private static final double TARGET_INTERRUPT_CHECK_TIME_NS = 3 * 1000000000.0;

  private int checkInterruptionAfterRows = CHECK_INTERRUPTION_AFTER_ROWS_DEFAULT;
  private long lastInterruptCheckNs = 0L;
  private int nRows;
  private volatile boolean isAborted;

  // Methods should really be protected, but some places have to use this as a field.

  public final void startAbortChecks() {
    lastInterruptCheckNs = System.nanoTime();
    nRows = 0;
  }

  public final void addRowAndMaybeCheckAbort() throws InterruptedException {
    if (nRows++ < checkInterruptionAfterRows) return;
    long time = System.nanoTime();
    checkAbortCondition();
    long elapsedNs = (time - lastInterruptCheckNs);
    if (elapsedNs >= 0) {
      // Make sure we don't get stuck at 0 time, however unlikely that is.
      double diff = elapsedNs == 0 ? 10 : TARGET_INTERRUPT_CHECK_TIME_NS / elapsedNs;
      int newRows = Math.min(CHECK_INTERRUPTION_AFTER_ROWS_MAX,
          Math.max(CHECK_INTERRUPTION_AFTER_ROWS_MIN, (int) (diff * checkInterruptionAfterRows)));
      if (checkInterruptionAfterRows != newRows && LOG.isDebugEnabled()) {
        LOG.debug("Adjusting abort check rows to " + newRows
            + " from " + checkInterruptionAfterRows);
      }
      checkInterruptionAfterRows = newRows;
    }
    nRows = 0;
    lastInterruptCheckNs = time;
  }

  public final void checkAbortCondition() throws InterruptedException {
    boolean isInterrupted = Thread.currentThread().isInterrupted();
    if (!isAborted && !isInterrupted) return;
    // Not cleaning the interrupt status.
    throw new InterruptedException("Processing thread aborted. Interrupt state: " + isInterrupted);
  }

  public final void setAborted(boolean b) {
    this.isAborted = b;
  }

  public void abort() {
    setAborted(true);
  }

  public final boolean isAborted() {
    return this.isAborted;
  }
}