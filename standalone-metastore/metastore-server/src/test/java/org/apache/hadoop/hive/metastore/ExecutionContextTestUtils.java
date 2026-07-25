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

import org.apache.hadoop.hive.metastore.metastore.PersistenceManagerProxy;
import org.datanucleus.ExecutionContext;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.cache.Level1Cache;
import org.datanucleus.state.DNStateManager;

import javax.jdo.PersistenceManager;

/**
 * Helpers for inspecting DataNucleus L1 (persistence context) cache in unit tests.
 */
public final class ExecutionContextTestUtils {

  private ExecutionContextTestUtils() {
  }

  public static ExecutionContext getExecutionContext(PersistenceManager pm) {
    if (pm instanceof JDOPersistenceManager) {
      return ((JDOPersistenceManager) pm).getExecutionContext();
    }
    if (pm instanceof PersistenceManagerProxy.ExecutionContextReference) {
      return ((PersistenceManagerProxy.ExecutionContextReference) pm).getExecutionContext();
    }
    throw new IllegalArgumentException("Unsupported PersistenceManager: " + pm.getClass());
  }

  public static int countCachedInstances(PersistenceManager pm, Class<?> clazz) {
    Level1Cache l1Cache = getExecutionContext(pm).getLevel1Cache();
    int count = 0;
    for (DNStateManager stateManager : l1Cache.values()) {
      if (clazz.isInstance(stateManager.getObject())) {
        count++;
      }
    }
    return count;
  }
}
