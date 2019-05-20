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

package org.apache.hive.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractService.
 *
 */
public abstract class AbstractService implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractService.class);

  /**
   * Service state: initially {@link STATE#NOTINITED}.
   */
  private STATE state = STATE.NOTINITED;

  /**
   * Service name.
   */
  private final String name;
  /**
   * Service start time. Will be zero until the service is started.
   */
  private long startTime;

  /**
   * The configuration. Will be null until the service is initialized.
   */
  private HiveConf hiveConf;

  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  private final List<ServiceStateChangeListener> listeners =
      new ArrayList<ServiceStateChangeListener>();

  /**
   * Construct the service.
   *
   * @param name
   *          service name
   */
  public AbstractService(String name) {
    this.name = name;
  }

  // This probably doesn't need to be sync, but nobody calls this, so it doesn't matter.
  @Override
  public synchronized STATE getServiceState() {
    return state;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    ensureCurrentState(STATE.NOTINITED);
    setHiveConf(hiveConf);
    changeState(STATE.INITED);
    LOG.info("Service:" + getName() + " is inited.");
  }

  protected final void setHiveConf(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void start() {
    startTime = System.currentTimeMillis();
    ensureCurrentState(STATE.INITED);
    changeState(STATE.STARTED);
    LOG.info("Service:" + getName() + " is started.");
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void stop() {
    if (state == STATE.STOPPED ||
        state == STATE.INITED ||
        state == STATE.NOTINITED) {
      // already stopped, or else it was never
      // started (eg another service failing canceled startup)
      return;
    }
    ensureCurrentState(STATE.STARTED);
    changeState(STATE.STOPPED);
    LOG.info("Service:" + getName() + " is stopped.");
  }

  @Override
  public void register(ServiceStateChangeListener l) {
    synchronized (listeners) {
      listeners.add(l);
    }
  }

  @Override
  public void unregister(ServiceStateChangeListener l) {
    synchronized (listeners) {
      listeners.remove(l);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public HiveConf getHiveConf() {
    return hiveConf;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  /**
   * Verify that that a service is in a given state.
   *
   * @param currentState
   *          the desired state
   * @throws IllegalStateException
   *           if the service state is different from
   *           the desired state
   */
  private void ensureCurrentState(STATE currentState) {
    ServiceOperations.ensureCurrentState(state, currentState);
  }

  /**
   * Change to a new state and notify all listeners.
   * This is a private method that is only invoked from synchronized methods,
   * which avoid having to clone the listener list. It does imply that
   * the state change listener methods should be short lived, as they
   * will delay the state transition.
   *
   * @param newState
   *          new service state
   */
  private void changeState(STATE newState) {
    state = newState;
    // notify listeners
    for (ServiceStateChangeListener l : listeners) {
      l.stateChanged(this);
    }
  }

}
