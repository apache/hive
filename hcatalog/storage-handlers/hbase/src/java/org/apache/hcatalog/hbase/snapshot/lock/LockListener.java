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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.hbase.snapshot.lock;

/**
 * This class has two methods which are call
 * back methods when a lock is acquired and
 * when the lock is released.
 *  This class has been used as-is from the zookeeper 3.3.4 recipes minor changes
 *  in the package name.
 */
public interface LockListener {
  /**
   * call back called when the lock
   * is acquired
   */
  public void lockAcquired();

  /**
   * call back called when the lock is
   * released.
   */
  public void lockReleased();
}
