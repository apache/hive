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
package org.apache.hadoop.hive.llap.daemon.impl;

import java.util.Set;

import javax.management.MXBean;

/**
 * MXbean to expose llap daemon related information through JMX.
 */
@MXBean
public interface LlapDaemonMXBean {

  /**
   * Gets the rpc port.
   * @return the rpc port
   */
  public int getRpcPort();

  /**
   * Gets the number of executors.
   * @return number of executors
   */
  public int getNumExecutors();

  /**
   * Gets the number of active executors.
   * @return number of active executors
   */
  public int getNumActive();

  /**
   * Gets the shuffle port.
   * @return the shuffle port
   */
  public int getShufflePort();

  /**
   * CSV list of local directories
   * @return local dirs
   */
  public String getLocalDirs();

  /**
   * Executor states.
   * @return Executor states.
   */
  public Set<String> getExecutorsStatus();

  /**
   * Gets llap daemon configured executor memory per instance.
   * @return memory per instance
   */
  public long getExecutorMemoryPerInstance();

  /**
   * Gets llap daemon configured io memory per instance.
   * @return memory per instance
   */
  public long getIoMemoryPerInstance();

  /**
   * Checks if Llap IO is enabled
   * @return true if enabled, false if not
   */
  public boolean isIoEnabled();

  /**
   * Gets max available jvm memory.
   * @return max jvm memory
   */
  public long getMaxJvmMemory();
}