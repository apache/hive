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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.LockComponent;

import java.util.List;

public abstract class HiveLock {

  public abstract HiveLockObject getHiveLockObject();

  public abstract HiveLockMode   getHiveLockMode();

  /**
   * Returns true if for this lock implementation, a single lock can in turn
   * lock multiple objects, e.g., multi-statement transaction.
   */
  public boolean mayContainComponents() {
    return false;
  }

  /**
   * Returns the lock components if a single lock can in turn
   * lock multiple objects, e.g., multi-statement transaction.
   *
   * Returns an empty list if the lock does not have multiple
   * components.
   */
  public List<LockComponent> getHiveLockComponents() {
    return ImmutableList.of();
  }
}
