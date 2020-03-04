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

package org.apache.hadoop.hive.ql.lockmgr;

import java.util.List;
import org.apache.hadoop.hive.ql.Driver.LockedDriverState;

/**
 * Manager for locks in Hive.  Users should not instantiate a lock manager
 * directly.  Instead they should get an instance from their instance of
 * {@link HiveTxnManager}.
 */
public interface HiveLockManager {

  public void setContext(HiveLockManagerCtx ctx) throws LockException;

  /**
   * @param key        object to be locked
   * @param mode       mode of the lock (SHARED/EXCLUSIVE)
   * @param keepAlive  if the lock needs to be persisted after the statement
   */
  public HiveLock lock(HiveLockObject key, HiveLockMode mode,
      boolean keepAlive) throws LockException;
  public List<HiveLock> lock(List<HiveLockObj> objs,
      boolean keepAlive, LockedDriverState lDrvState) throws LockException;
  public void unlock(HiveLock hiveLock) throws LockException;
  public void releaseLocks(List<HiveLock> hiveLocks);

  public List<HiveLock> getLocks(boolean verifyTablePartitions, boolean fetchData) throws LockException;
  public List<HiveLock> getLocks(HiveLockObject key, boolean verifyTablePartitions, boolean fetchData) throws LockException;
  public void close() throws LockException;
  public void prepareRetry() throws LockException;

  /**
   * refresh to enable new configurations.
   */
  public void refresh();

}
