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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread that runs in the metastore, separate from the threads in the thrift service.
 */
public interface MetaStoreThread {

  /**
   * Set the Hive configuration for this thread.
   * @param conf
   */
  void setHiveConf(HiveConf conf);

  /**
   * Set the id for this thread.
   * @param threadId
   */
  void setThreadId(int threadId);

  /**
   * Initialize the thread.  This must not be called until after
   * {@link #setHiveConf(org.apache.hadoop.hive.conf.HiveConf)} and  {@link #setThreadId(int)}
   * have been called.
   * @param stop a flag to watch for when to stop.  If this value is set to true,
   *             the thread will terminate the next time through its main loop.
   * @param looped a flag that is set to true everytime a thread goes through it's main loop.
   *               This is purely for testing so that tests can assure themselves that the thread
   *               has run through it's loop once.  The test can set this value to false.  The
   *               thread should then assure that the loop has been gone completely through at
   *               least once.
   */
  void init(AtomicBoolean stop, AtomicBoolean looped) throws MetaException;

  /**
   * Run the thread in the background.  This must not be called until
   * {@link ##init(java.util.concurrent.atomic.AtomicBoolean, java.util.concurrent.atomic.AtomicBoolean)} has
   * been called.
   */
  void start();
}
