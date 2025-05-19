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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * An empty implementation of TxnStore.MutexAPI
 */
public class NoMutex implements TxnStore.MutexAPI {

  @Override
  public LockHandle acquireLock(String key) throws MetaException {
    return new DummyHandle();
  }

  @Override
  public void acquireLock(String key, LockHandle handle) throws MetaException {
    // no-op
  }

  private static class DummyHandle implements LockHandle {

    private long lastUpdateTime = 0L;

    @Override
    public void releaseLocks() {
      // no-op
    }

    @Override
    public Long getLastUpdateTime() {
      return lastUpdateTime;
    }

    @Override
    public void releaseLocks(Long timestamp) {
      this.lastUpdateTime = timestamp;
    }

    @Override
    public void close() {
      // no-op
    }
  }

}
