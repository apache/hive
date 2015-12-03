/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import com.google.common.reflect.AbstractInvocationHandler;

/**
 * Creates a proxied {@link IMetaStoreClient client} that wraps calls in a {@link PrivilegedExceptionAction} if the
 * {@link UserGroupInformation} is specified. Invokes directly otherwise.
 */
public class UgiMetaStoreClientFactory {

  private static Set<Method> I_META_STORE_CLIENT_METHODS = getIMetaStoreClientMethods();

  private final String metaStoreUri;
  private final HiveConf conf;
  private final boolean secureMode;
  private final UserGroupInformation authenticatedUser;
  private final String user;

  public UgiMetaStoreClientFactory(String metaStoreUri, HiveConf conf, UserGroupInformation authenticatedUser,
      String user, boolean secureMode) {
    this.metaStoreUri = metaStoreUri;
    this.conf = conf;
    this.authenticatedUser = authenticatedUser;
    this.user = user;
    this.secureMode = secureMode;
    if (metaStoreUri != null) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
    }
    if (secureMode) {
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
    }
  }

  public IMetaStoreClient newInstance() throws MetaException {
    return newInstance(new HiveMetaStoreClient(conf));
  }

  public IMetaStoreClient newInstance(IMetaStoreClient delegate) throws MetaException {
    return createProxy(delegate, user, authenticatedUser);
  }

  @Override
  public String toString() {
    return "UgiMetaStoreClientFactory [metaStoreUri=" + metaStoreUri + ", secureMode=" + secureMode
        + ", authenticatedUser=" + authenticatedUser + ", user=" + user + "]";
  }

  private IMetaStoreClient createProxy(final IMetaStoreClient delegate, final String user,
      final UserGroupInformation authenticatedUser) {
    InvocationHandler handler = new AbstractInvocationHandler() {

      @Override
      protected Object handleInvocation(Object proxy, final Method method, final Object[] args) throws Throwable {
        try {
          if (!I_META_STORE_CLIENT_METHODS.contains(method) || authenticatedUser == null) {
            return method.invoke(delegate, args);
          }
          try {
            return authenticatedUser.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                return method.invoke(delegate, args);
              }
            });
          } catch (IOException | InterruptedException e) {
            throw new TException("PrivilegedExceptionAction failed as user '" + user + "'.", e);
          }
        } catch (UndeclaredThrowableException | InvocationTargetException e) {
          throw e.getCause();
        }
      }
    };

    ClassLoader classLoader = IMetaStoreClient.class.getClassLoader();
    Class<?>[] interfaces = new Class<?>[] { IMetaStoreClient.class };
    Object proxy = Proxy.newProxyInstance(classLoader, interfaces, handler);
    return IMetaStoreClient.class.cast(proxy);
  }

  private static Set<Method> getIMetaStoreClientMethods() {
    return new HashSet<>(Arrays.asList(IMetaStoreClient.class.getDeclaredMethods()));
  }

}
