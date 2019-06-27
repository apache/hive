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

import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.tez.common.counters.CounterGroup;
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
  private final boolean addTaskTimeCounters;

  public WmFragmentCounters(boolean addTaskTimeCounters) {
    // Note: WmFragmentCounters are created before Tez counters are created.
    this.fixedCounters = new AtomicLongArray(LlapWmCounters.values().length);
    this.addTaskTimeCounters = addTaskTimeCounters;
  }

  public void changeStateQueued(boolean isGuaranteed) {
    changeState(State.QUEUED, getQueuedCounter(isGuaranteed));
  }

  public void changeStateRunning(boolean isGuaranteed) {
    changeState(State.RUNNING, getRunningCounter(isGuaranteed));
  }

  public long getQueueTime() {
    return fixedCounters.get(LlapWmCounters.GUARANTEED_QUEUED_NS.ordinal())
        + fixedCounters.get(LlapWmCounters.SPECULATIVE_QUEUED_NS.ordinal());
  }

  public long getRunningTime() {
    return fixedCounters.get(LlapWmCounters.GUARANTEED_RUNNING_NS.ordinal())
        + fixedCounters.get(LlapWmCounters.SPECULATIVE_RUNNING_NS.ordinal());
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
    // Note: there are so many different onSuccess/onFailure callbacks floating around that
    //       this will probably be called twice for the done state. This is ok given the sync.
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
  }

  public void dumpToTezCounters(TezCounters tezCounters, boolean isLast) {
    if (isLast) {
      changeStateDone(); // Record the final counters.
    }
    for (int i = 0; i < fixedCounters.length(); ++i) {
      tezCounters.findCounter(LlapWmCounters.values()[i]).setValue(fixedCounters.get(i));
    }

    // add queue and runtime (together with task count) on a "per daemon" level
    // to the Tez counters.
    if (addTaskTimeCounters) {
      String hostName = MetricsUtils.getHostName();
      long queued = fixedCounters.get(LlapWmCounters.GUARANTEED_QUEUED_NS.ordinal())
                    + fixedCounters.get(LlapWmCounters.SPECULATIVE_QUEUED_NS.ordinal());
      long running = fixedCounters.get(LlapWmCounters.GUARANTEED_RUNNING_NS.ordinal())
                     + fixedCounters.get(LlapWmCounters.SPECULATIVE_RUNNING_NS.ordinal());

      CounterGroup cg = tezCounters.getGroup("LlapTaskRuntimeAgg by daemon");
      cg.findCounter("QueueTime-" + hostName).setValue(queued);
      cg.findCounter("RunTime-" + hostName).setValue(running);
      cg.findCounter("Count-" + hostName).setValue(1);
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
}
