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

import javax.jdo.Query;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.RawStore;

public record TransactionHandler<T> (RawStore rs, T simpl, List<Query> closeQueriesAfterUse)
    implements InvocationHandler {

  @SuppressWarnings("unchecked")
  public static <T> T getProxy(Class<T> iface, TransactionHandler<T> handler) {
    List<Class> interfaces = new ArrayList<>();
    interfaces.add(iface);
    interfaces.addAll(Arrays.asList(iface.getInterfaces()));
    return (T) Proxy.newProxyInstance(iface.getClassLoader(),
        interfaces.toArray(new Class[0]), handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    boolean openTxn = method.getAnnotation(MetaDescriptor.NoTransaction.class) == null;
    boolean success = false;
    if (openTxn) {
      rs.openTransaction();
    }
    try {
      Object result = method.invoke(simpl, args);
      if (openTxn) {
        success = rs.commitTransaction();
      }
      return result;
    } catch (InvocationTargetException | UndeclaredThrowableException e) {
      throw e.getCause();
    } finally {
      if (openTxn && !success) {
        rs.rollbackTransaction();
      }
      for (Query q : closeQueriesAfterUse) {
        q.closeAll();
      }
      closeQueriesAfterUse.clear();
    }
  }
}
