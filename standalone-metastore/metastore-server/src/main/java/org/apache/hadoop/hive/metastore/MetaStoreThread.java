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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread that runs in the metastore, separate from the threads in the thrift service.
 */
public interface MetaStoreThread extends Configurable {

  /**
   * Initialize the thread.  This must not be called until after
   * {@link #setConf(Configuration)} has been called.
   * @param stop a flag to watch for when to stop.  If this value is set to true,
   *             the thread will terminate the next time through its main loop.
   */
  // TODO: move these test parameters to more specific places... there's no need to have them here
  void init(AtomicBoolean stop) throws Exception;

  /**
   * Run the thread in the background.  This must not be called until
   * {@link MetaStoreThread#init(java.util.concurrent.atomic.AtomicBoolean)} has
   * been called.
   */
  void start();
}
