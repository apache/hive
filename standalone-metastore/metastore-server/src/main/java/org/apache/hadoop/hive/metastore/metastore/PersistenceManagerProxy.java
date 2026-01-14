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

package org.apache.hadoop.hive.metastore.metastore;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Objects;

import org.datanucleus.ExecutionContext;

public class PersistenceManagerProxy implements InvocationHandler  {
  private final PersistenceManager target;
  private final MethodHandle getExecutionContext;
  private final List<Query> openedQueries;

  private PersistenceManagerProxy(PersistenceManager pm, List<Query> trackOpenedQueries) {
    this.target = Objects.requireNonNull(pm);
    this.openedQueries = Objects.requireNonNull(trackOpenedQueries);
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      java.lang.invoke.MethodType type = java.lang.invoke.MethodType.methodType(ExecutionContext.class);
      this.getExecutionContext = lookup.findVirtual(target.getClass(), "getExecutionContext", type);
    } catch (Exception e) {
      throw new RuntimeException("Method getExecutionContext not found", e);
    }
  }

  public static PersistenceManager getProxy(PersistenceManager pm, List<Query> trackOpenedQueries) {
    return (PersistenceManager) Proxy.newProxyInstance(pm.getClass().getClassLoader(),
        new Class[] {PersistenceManager.class, ExecutionContextReference.class},
        new PersistenceManagerProxy(pm, trackOpenedQueries));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // Redirect if the interface method "getExecutionContext" is called
    if (method.getName().equals("getExecutionContext")) {
      return getExecutionContext.bindTo(target).invokeWithArguments(args);
    } else if (method.getName().equals("newQuery")) {
      Object result = method.invoke(target, args);
      openedQueries.add((Query) result);
      return result;
    }
    // Otherwise, proceed with the standard call
    return method.invoke(target, args);
  }

  //  PersistenceManager doesn't provide a way to get the ExecutionContext
  // if we create a proxy around the JDOPersistenceManager, which we use it
  // to save a savepoint, or generate the primary key.
  public interface ExecutionContextReference {
    /**
     * @return ExecutionContext the current JDOPersistenceManager holds
     */
    ExecutionContext getExecutionContext();
  }
}
