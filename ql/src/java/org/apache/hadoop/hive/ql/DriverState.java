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

package org.apache.hadoop.hive.ql;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents the driver's state. Also has mechanism for locking for the time of state transitions.
 */
public class DriverState {
  private static ThreadLocal<DriverState> tlInstance = new ThreadLocal<DriverState>() {
    @Override
    protected DriverState initialValue() {
      return new DriverState();
    }
  };

  public static void setDriverState(DriverState state) {
    tlInstance.set(state);
  }

  public static DriverState getDriverState() {
    return tlInstance.get();
  }

  public static void removeDriverState() {
    tlInstance.remove();
  }

  /**
   * Enumeration of the potential driver states.
   */
  private enum State {
    INITIALIZED,
    COMPILING,
    COMPILED,
    EXECUTING,
    EXECUTED,
    // a state that the driver enters after close() has been called to clean the query results
    // and release the resources after the query has been executed
    CLOSED,
    // a state that the driver enters after destroy() is called and it is the end of driver life cycle
    DESTROYED,
    ERROR
  }

  // a lock is used for synchronizing the state transition and its associated resource releases
  private final ReentrantLock stateLock = new ReentrantLock();
  private final AtomicBoolean aborted = new AtomicBoolean();
  private State driverState = State.INITIALIZED;

  public void lock() {
    stateLock.lock();
  }

  public void unlock() {
    stateLock.unlock();
  }

  public boolean isAborted() {
    return aborted.get();
  }

  public void abort() {
    aborted.set(true);
  }

  public void compiling() {
    driverState = State.COMPILING;
  }

  public void compilingWithLocking() {
    lock();
    try {
      driverState = State.COMPILING;
    } finally {
      unlock();
    }
  }

  public boolean isCompiling() {
    return driverState == State.COMPILING;
  }

  public void compilationInterruptedWithLocking(boolean deferClose) {
    lock();
    try {
      driverState = deferClose ? State.EXECUTING : State.ERROR;
    } finally {
      unlock();
    }
  }

  public void compilationFinishedWithLocking(boolean wasError) {
    lock();
    try {
      driverState = wasError ? State.ERROR : State.COMPILED;
    } finally {
      unlock();
    }
  }

  public boolean isCompiled() {
    return driverState == State.COMPILED;
  }

  public void executing() {
    driverState = State.EXECUTING;
  }

  public boolean isExecuting() {
    return driverState == State.EXECUTING;
  }

  public void executionFinishedWithLocking(boolean wasError) {
    lock();
    try {
      if (!isDestroyed()) {
        driverState = wasError ? State.ERROR : State.EXECUTED;
      }
    } finally {
      unlock();
    }
  }

  public boolean isExecuted() {
    return driverState == State.EXECUTED;
  }

  public void closed() {
    driverState = State.CLOSED;
  }

  public boolean isClosed() {
    return driverState == State.CLOSED;
  }

  public void descroyed() {
    driverState = State.DESTROYED;
  }

  public boolean isDestroyed() {
    return driverState == State.DESTROYED;
  }

  public void error() {
    driverState = State.ERROR;
  }

  public boolean isError() {
    return driverState == State.ERROR;
  }

  @Override
  public String toString() {
    return String.format("%s(aborted:%s)", driverState, aborted.get());
  }
}
