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
package org.apache.hadoop.hive.llap.counters;

import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.tez.common.counters.TezCounters;

/**
 * Per query counters.
 */
public class WmFragmentCounters {
  private static enum State { NONE, QUEUED, RUNNING, DONE };
  private State currentState = State.NONE;
  private LlapWmCounters currentCounter = null;
  private long currentCounterStartTime = 0;
  private final AtomicLongArray fixedCounters;
  private final TezCounters tezCounters;

  public WmFragmentCounters(final TezCounters tezCounters) {
    this.fixedCounters = new AtomicLongArray(LlapWmCounters.values().length);
    this.tezCounters = tezCounters;
  }

  public void changeStateQueued(boolean isGuaranteed) {
    changeState(State.QUEUED, getQueuedCounter(isGuaranteed));
  }

  public void changeStateRunning(boolean isGuaranteed) {
    changeState(State.RUNNING, getRunningCounter(isGuaranteed));
  }

  private static LlapWmCounters getQueuedCounter(boolean isGuaranteed) {
    return isGuaranteed
        ? LlapWmCounters.GUARANTEED_QUEUED_NS : LlapWmCounters.SPECULATIVE_QUEUED_NS;
  }

  private static LlapWmCounters getRunningCounter(boolean isGuaranteed) {
    return isGuaranteed
        ? LlapWmCounters.GUARANTEED_RUNNING_NS : LlapWmCounters.SPECULATIVE_RUNNING_NS;
  }

  public void changeStateDone() {
    changeState(State.DONE, null);
  }

  public void changeGuaranteed(boolean isGuaranteed) {
    long newTime = System.nanoTime();
    long oldTime = -1;
    LlapWmCounters oldCounter = null;
    synchronized (this) {
      LlapWmCounters counter = null;
      switch (currentState) {
      case DONE:
      case NONE: return;
      case QUEUED: counter = getQueuedCounter(isGuaranteed); break;
      case RUNNING: counter = getRunningCounter(isGuaranteed); break;
      default: throw new AssertionError(currentState);
      }
      if (counter == currentCounter) return;
      if (currentCounter != null) {
        oldCounter = currentCounter;
        oldTime = currentCounterStartTime;
      }
      currentCounter = counter;
      currentCounterStartTime = newTime;
    }
    if (oldCounter != null) {
      incrCounter(oldCounter, newTime - oldTime);
    }
  }


  private void changeState(State newState, LlapWmCounters counter) {
    long newTime = System.nanoTime();
    long oldTime = -1;
    LlapWmCounters oldCounter = null;
    synchronized (this) {
      if (newState.ordinal() < currentState.ordinal()) return;
      if (counter == currentCounter) return;
      if (currentCounter != null) {
        oldCounter = currentCounter;
        oldTime = currentCounterStartTime;
      }
      currentCounter = counter;
      currentState = newState;
      currentCounterStartTime = newTime;
    }
    if (oldCounter != null) {
      incrCounter(oldCounter, newTime - oldTime);
    }
  }

  private void incrCounter(LlapWmCounters counter, long delta) {
    fixedCounters.addAndGet(counter.ordinal(), delta);
    if (tezCounters != null) {
      tezCounters.findCounter(LlapWmCounters.values()[counter.ordinal()]).increment(delta);
    }
  }

  @Override
  public String toString() {
    // We rely on NDC information in the logs to map counters to attempt.
    // If that is not available, appId should either be passed in, or extracted from NDC.
    StringBuilder sb = new StringBuilder("[ ");
    for (int i = 0; i < fixedCounters.length(); ++i) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(LlapWmCounters.values()[i].name()).append("=").append(fixedCounters.get(i));
    }
    sb.append(" ]");
    return sb.toString();
  }

  public TezCounters getTezCounters() {
    return tezCounters;
  }
}
