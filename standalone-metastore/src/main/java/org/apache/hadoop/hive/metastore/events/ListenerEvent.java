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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all the events which are defined for metastore.
 *
 * This class is not thread-safe and not expected to be called in parallel.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@NotThreadSafe
public abstract class ListenerEvent {

  /**
   * status of the event, whether event was successful or not.
   */
  private final boolean status;
  private final IHMSHandler handler;

  /**
   * Key/value parameters used by listeners to store notifications results
   * i.e. DbNotificationListener sets a DB_NOTIFICATION_EVENT_ID.
   *
   * NotThreadSafe: The parameters map is not expected to be access in parallel by Hive, so keep it thread-unsafe
   * to avoid locking overhead.
   */
  private Map<String, String> parameters;

  /** For performance concerns, it is preferable to cache the unmodifiable parameters variable that will be returned on the
   * {@link #getParameters()} method. It is expected that {@link #putParameter(String, String)} is called less times
   * than {@link #getParameters()}, so performance may be better by using this cache.
   */
  private Map<String, String> unmodifiableParameters;

  // Listener parameters aren't expected to have many values. So far only
  // DbNotificationListener will add a parameter; let's set a low initial capacity for now.
  // If we find out many parameters are added, then we can adjust or remove this initial capacity.
  private static final int PARAMETERS_INITIAL_CAPACITY = 1;

  // Properties passed by the client, to be used in execution hooks.
  private EnvironmentContext environmentContext = null;

  public ListenerEvent(boolean status, IHMSHandler handler) {
    super();
    this.status = status;
    this.handler = handler;
    this.parameters = new HashMap<>(PARAMETERS_INITIAL_CAPACITY);
    updateUnmodifiableParameters();
  }

  /**
   * @return the status of event.
   */
  public boolean getStatus() {
    return status;
  }

  /**
   * Set the environment context of the event.
   *
   * @param environmentContext An EnvironmentContext object that contains environment parameters sent from
   *                           the HMS client.
   */
  public void setEnvironmentContext(EnvironmentContext environmentContext) {
    this.environmentContext = environmentContext;
  }

  /**
   * @return environment properties of the event
   */
  public EnvironmentContext getEnvironmentContext() {
    return environmentContext;
  }

  /**
   * You should use {@link #getIHMSHandler()} instead.
   * @return handler.
   */
  @Deprecated
  public HiveMetaStore.HMSHandler getHandler() {
    return (HiveMetaStore.HMSHandler)handler;
  }

  /**
   * @return the handler
   */
  public IHMSHandler getIHMSHandler() {
    return handler;
  }

  /**
   * Return all parameters of the listener event. Parameters are read-only (unmodifiable map). If a new parameter
   * must be added, please use the putParameter() method.
   *
   *
   * @return A map object with all parameters.
   */
  public final Map<String, String> getParameters() {
    return unmodifiableParameters;
  }

  /**
   * Put a new parameter to the listener event.
   *
   * Overridden parameters is not allowed, and an exception may be thrown to avoid a mis-configuration
   * between listeners setting the same parameters.
   *
   * @param name Name of the parameter.
   * @param value Value of the parameter.
   * @throws IllegalStateException if a parameter already exists.
   */
  public void putParameter(String name, String value) {
    putParameterIfAbsent(name, value);
    updateUnmodifiableParameters();
  }

  /**
   * Put a new set the parameters to the listener event.
   *
   * Overridden parameters is not allowed, and an exception may be thrown to avoid a mis-configuration
   * between listeners setting the same parameters.
   *
   * @param parameters A Map object with the a set of parameters.
   * @throws IllegalStateException if a parameter already exists.
   */
  public void putParameters(final Map<String, String> parameters) {
    if (parameters != null) {
      for (Map.Entry<String, String> entry : parameters.entrySet()) {
        putParameterIfAbsent(entry.getKey(), entry.getValue());
      }

      updateUnmodifiableParameters();
    }
  }

  /**
   * Put a parameter to the listener event only if the parameter is absent.
   *
   * Overridden parameters is not allowed, and an exception may be thrown to avoid a mis-configuration
   * between listeners setting the same parameters.
   *
   * @param name Name of the parameter.
   * @param value Value of the parameter.
   * @throws IllegalStateException if a parameter already exists.
   */
  private void putParameterIfAbsent(String name, String value) {
    if (parameters.containsKey(name)) {
      throw new IllegalStateException("Invalid attempt to overwrite a read-only parameter: " + name);
    }

    parameters.put(name, value);
  }

  /**
   * Keeps a cache of unmodifiable parameters returned by the getParameters() method.
   */
  private void updateUnmodifiableParameters() {
    unmodifiableParameters = Collections.unmodifiableMap(parameters);
  }
}
