/**
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

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;

import org.apache.hadoop.hive.metastore.api.EnvironmentContext;

/**
 * Base class for all the events which are defined for metastore.
 */

public abstract class ListenerEvent {

  /**
   * status of the event, whether event was successful or not.
   */
  private final boolean status;
  private final HMSHandler handler;

  // Properties passed by the client, to be used in execution hooks.
  private EnvironmentContext environmentContext = null;

  public ListenerEvent(boolean status, HMSHandler handler) {
    super();
    this.status = status;
    this.handler = handler;
  }

  /**
   * @return the handler
   */
  public HMSHandler getHandler() {
    return handler;
  }

  /**
   * @return the status of event.
   */
  public boolean getStatus() {
    return status;
  }

  public void setEnvironmentContext(EnvironmentContext environmentContext) {
    this.environmentContext = environmentContext;
  }

  /**
   * @return environment properties of the event
   */
  public EnvironmentContext getEnvironmentContext() {
    return environmentContext;
  }

}
