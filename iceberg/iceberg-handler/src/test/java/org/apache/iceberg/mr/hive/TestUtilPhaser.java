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

import java.util.concurrent.Phaser;

public class TestUtilPhaser {

  private static TestUtilPhaser instance;
  private final Phaser phaser;

  private TestUtilPhaser() {
    phaser = new Phaser();
  }

  public static synchronized TestUtilPhaser getInstance() {
    if (instance == null) {
      instance = new TestUtilPhaser();
    }
    return instance;
  }

  public Phaser getPhaser() {
    return phaser;
  }

  public static synchronized boolean isInstantiated() {
    return instance != null;
  }

  /**
   * Registers this thread for barrier-style synchronization.
   * Used only when commit synchronization is required.
   */
  public void register() {
    phaser.register();
  }

  /**
   * Wait until it's this thread's turn.
   * Uses Phaser phase as a monotonic sequence number.
   */
  public void awaitTurn(int index) {
    int phase;
    while ((phase = phaser.getPhase()) < index) {
      phaser.awaitAdvance(phase);
    }
  }

  /** Signal completion of this turn (advance to next phase) */
  public void completeTurn() {
    phaser.arriveAndAwaitAdvance();
  }

  public static synchronized void destroyInstance() {
    if (instance != null) {
      instance.getPhaser().forceTermination();
      instance = null;
    }
    ThreadContext.clear();
  }

  /** Thread-scoped test context */
  public static final class ThreadContext {
    private static final ThreadLocal<Integer> INDEX = new ThreadLocal<>();

    public static void setIndex(int index) {
      INDEX.set(index);
    }

    public static int getIndex() {
      Integer idx = INDEX.get();
      return idx != null ? idx : -1;
    }

    private static void clear() {
      INDEX.remove();
    }
  }
}
