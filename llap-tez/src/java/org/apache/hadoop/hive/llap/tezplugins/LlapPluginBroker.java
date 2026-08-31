/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.llap.tezplugins;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

import com.google.common.annotations.VisibleForTesting;

/**
 * Broker that pairs up the two LLAP-side Tez plugins.
 *
 * <p>{@link LlapTaskCommunicator} and {@link LlapTaskSchedulerService} are constructed
 * independently by the Tez app-master and have to find each other so the communicator can call
 * {@code scheduler.notifyStarted(taskAttemptID)} from the {@code SubmitWork} response callback.
 * Tez offers no first-class way for the two to meet, so they do it here: whichever side
 * initializes first for a given DAG parks itself in the appropriate map, and the second side
 * finds it and pairs the two up.
 *
 * <p>The handshake is keyed on {@link ApplicationAttemptId} — both plugin contexts expose it,
 * and both plugins for the same DAG share it. Keying on the attempt id confines each handshake
 * to a single DAG so that, in JVMs hosting concurrent DAGs (MiniHS2, MiniLlapCluster, tests
 * fanning out concurrent inserts), the communicator for DAG-A cannot accidentally pair with the
 * scheduler for DAG-B.
 *
 * <p>A single JVM-wide {@link #INSTANCE} is shared by all plugin constructions. The pair-or-park
 * decision is atomic under {@link #lock}, so the pairing side always observes a consistent view
 * of the peer map. {@code shutdown()} on each plugin unconditionally calls the matching
 * {@code unregister…} method so a partial DAG init (only one of the two plugins was constructed
 * before something failed) does not leak an entry — entries are keyed on the never-reused
 * {@link ApplicationAttemptId} and would otherwise sit in the map until the JVM died.
 *
 * <p>Production impact of this class is nil. A real Tez {@code DAGAppMaster} runs at most one DAG
 * over its lifetime, so both maps hold at most one entry, put and removed at plugin construction
 * / shutdown. Nothing on the task submission or scheduling hot path touches this class.
 */
final class LlapPluginBroker {

  static final LlapPluginBroker INSTANCE = new LlapPluginBroker();

  /**
   * Single source of mutual exclusion for the two maps below: every read, write, and iteration
   * happens while this lock is held, so the maps themselves need no internal synchronization.
   * Held only for the tiny "look at the peer map, and either pair or park" window on both sides.
   */
  private final Object lock = new Object();

  private final Map<ApplicationAttemptId, LlapTaskCommunicator> pendingCommunicators =
      new HashMap<>();
  private final Map<ApplicationAttemptId, LlapTaskSchedulerService> pendingSchedulers =
      new HashMap<>();

  private LlapPluginBroker() { }

  /**
   * Called from the {@link LlapTaskCommunicator} constructor. If the peer scheduler for this DAG
   * has already parked itself, wire the two together and remove the parked entry. Otherwise park
   * this communicator so the scheduler picks it up when it arrives.
   */
  void registerCommunicator(ApplicationAttemptId appAttemptId, LlapTaskCommunicator communicator) {
    synchronized (lock) {
      LlapTaskSchedulerService peer = pendingSchedulers.remove(appAttemptId);
      if (peer != null) {
        // We are the last of the pair to initialize for this DAG.
        peer.setTaskCommunicator(communicator);
        communicator.setScheduler(peer);
      } else {
        pendingCommunicators.put(appAttemptId, communicator);
      }
    }
  }

  /**
   * Symmetric to {@link #registerCommunicator}: called from the {@link LlapTaskSchedulerService}
   * constructor.
   */
  void registerScheduler(ApplicationAttemptId appAttemptId, LlapTaskSchedulerService scheduler) {
    synchronized (lock) {
      LlapTaskCommunicator peer = pendingCommunicators.remove(appAttemptId);
      if (peer != null) {
        // We are the last of the pair to initialize for this DAG.
        scheduler.setTaskCommunicator(peer);
        peer.setScheduler(scheduler);
      } else {
        pendingSchedulers.put(appAttemptId, scheduler);
      }
    }
  }

  /**
   * Reap a parked communicator entry on shutdown. Safe to call unconditionally: when the plugin
   * did pair with a peer, the peer's constructor already removed the entry and the two-arg
   * {@code remove} here is a no-op.
   */
  void unregisterCommunicator(ApplicationAttemptId appAttemptId, LlapTaskCommunicator communicator) {
    synchronized (lock) {
      pendingCommunicators.remove(appAttemptId, communicator);
    }
  }

  /** Symmetric to {@link #unregisterCommunicator}. */
  void unregisterScheduler(ApplicationAttemptId appAttemptId, LlapTaskSchedulerService scheduler) {
    synchronized (lock) {
      pendingSchedulers.remove(appAttemptId, scheduler);
    }
  }

  @VisibleForTesting
  Map<ApplicationAttemptId, LlapTaskCommunicator> pendingCommunicatorsView() {
    return pendingCommunicators;
  }

  @VisibleForTesting
  Map<ApplicationAttemptId, LlapTaskSchedulerService> pendingSchedulersView() {
    return pendingSchedulers;
  }

  /** Drop all parked entries. For test cleanup between iterations. */
  @VisibleForTesting
  void clear() {
    synchronized (lock) {
      pendingCommunicators.clear();
      pendingSchedulers.clear();
    }
  }
}
