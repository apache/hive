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

package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.hive.ql.metadata.AuthorizationException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class AuthorizationFactory {

  public static HiveAuthorizationProvider create(HiveAuthorizationProvider delegated) {
    return create(delegated, new DefaultAuthorizationExceptionHandler());
  }

  public static HiveAuthorizationProvider create(final HiveAuthorizationProvider delegated,
      final AuthorizationExceptionHandler handler) {

    InvocationHandler invocation = new InvocationHandler() {
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        invokeAuth(method, args);
        return null;
      }

      private void invokeAuth(Method method, Object[] args) throws Throwable {
        try {
          method.invoke(delegated, args);
        } catch (InvocationTargetException e) {
          if (e.getTargetException() instanceof AuthorizationException) {
            handler.exception((AuthorizationException) e.getTargetException());
          }
        }
      }
    };

    return (HiveAuthorizationProvider)Proxy.newProxyInstance(
        AuthorizationFactory.class.getClassLoader(),
        new Class[] {HiveAuthorizationProvider.class},
        invocation);
  }

  public static interface AuthorizationExceptionHandler {
    void exception(AuthorizationException exception) throws AuthorizationException;
  }

  public static class DefaultAuthorizationExceptionHandler
      implements AuthorizationExceptionHandler {
    public void exception(AuthorizationException exception) {
      throw exception;
    }
  }
}
