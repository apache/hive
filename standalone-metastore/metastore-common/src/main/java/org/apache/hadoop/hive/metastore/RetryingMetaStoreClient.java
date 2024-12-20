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

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.annotation.NoReconnect;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.annotations.VisibleForTesting;

/**
 * RetryingMetaStoreClient. Creates a proxy for a IMetaStoreClient
 * implementation and retries calls to it on failure.
 * If the login user is authenticated using keytab, it relogins user before
 * each call.
 *
 */
@InterfaceAudience.Public
public class RetryingMetaStoreClient implements InvocationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RetryingMetaStoreClient.class.getName());

  private final IMetaStoreClient base;
  private final UserGroupInformation ugi;
  private final int retryLimit;
  private final long retryDelaySeconds;
  private final ConcurrentHashMap<String, Long> metaCallTimeMap;
  private final long connectionLifeTimeInMillis;
  private long lastConnectionTime;
  private boolean localMetaStore;


  protected RetryingMetaStoreClient(Configuration conf, Class<?>[] constructorArgTypes,
                                    Object[] constructorArgs, ConcurrentHashMap<String, Long> metaCallTimeMap,
                                    Class<? extends IMetaStoreClient> msClientClass) throws MetaException {

    this.ugi = getUGI();

    if (this.ugi == null) {
      LOG.warn("RetryingMetaStoreClient unable to determine current user UGI.");
    }

    this.retryLimit = MetastoreConf.getIntVar(conf, ConfVars.THRIFT_FAILURE_RETRIES);
    this.retryDelaySeconds = MetastoreConf.getTimeVar(conf,
        ConfVars.CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);
    this.metaCallTimeMap = metaCallTimeMap;
    this.connectionLifeTimeInMillis = MetastoreConf.getTimeVar(conf,
        ConfVars.CLIENT_SOCKET_LIFETIME, TimeUnit.MILLISECONDS);
    this.lastConnectionTime = System.currentTimeMillis();
    String msUri = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    localMetaStore = (msUri == null) || msUri.trim().isEmpty();

    SecurityUtils.reloginExpiringKeytabUser();

    this.base = JavaUtils.newInstance(msClientClass, constructorArgTypes, constructorArgs);

    LOG.info("RetryingMetaStoreClient proxy=" + msClientClass + " ugi=" + this.ugi
        + " retries=" + this.retryLimit + " delay=" + this.retryDelaySeconds
        + " lifetime=" + this.connectionLifeTimeInMillis);
  }

  public static IMetaStoreClient getProxy(
      Configuration hiveConf, boolean allowEmbedded) throws MetaException {
    return getProxy(hiveConf, new Class[]{Configuration.class, HiveMetaHookLoader.class, Boolean.class},
        new Object[]{hiveConf, null, allowEmbedded}, null, HiveMetaStoreClient.class.getName()
    );
  }

  @VisibleForTesting
  public static IMetaStoreClient getProxy(Configuration hiveConf, HiveMetaHookLoader hookLoader,
      String mscClassName) throws MetaException {
    return getProxy(hiveConf, hookLoader, null, mscClassName, true);
  }

  public static IMetaStoreClient getProxy(Configuration hiveConf, HiveMetaHookLoader hookLoader,
      ConcurrentHashMap<String, Long> metaCallTimeMap, String mscClassName, boolean allowEmbedded)
          throws MetaException {

    return getProxy(hiveConf,
        new Class[] {Configuration.class, HiveMetaHookLoader.class, Boolean.class},
        new Object[] {hiveConf, hookLoader, allowEmbedded},
        metaCallTimeMap,
        mscClassName
    );
  }

  /**
   * This constructor is meant for Hive internal use only.
   * Please use getProxy(HiveConf conf, HiveMetaHookLoader hookLoader) for external purpose.
   */
  public static IMetaStoreClient getProxy(Configuration hiveConf, Class<?>[] constructorArgTypes,
      Object[] constructorArgs, String mscClassName) throws MetaException {
    return getProxy(hiveConf, constructorArgTypes, constructorArgs, null, mscClassName);
  }

  /**
   * This constructor is meant for Hive internal use only.
   * Please use getProxy(HiveConf conf, HiveMetaHookLoader hookLoader) for external purpose.
   */
  public static IMetaStoreClient getProxy(Configuration hiveConf, Class<?>[] constructorArgTypes,
      Object[] constructorArgs, ConcurrentHashMap<String, Long> metaCallTimeMap,
      String mscClassName) throws MetaException {

    @SuppressWarnings("unchecked")
    Class<? extends IMetaStoreClient> baseClass =
        JavaUtils.getClass(mscClassName, IMetaStoreClient.class);

    RetryingMetaStoreClient handler =
        new RetryingMetaStoreClient(hiveConf, constructorArgTypes, constructorArgs,
            metaCallTimeMap, baseClass);
    return (IMetaStoreClient) Proxy.newProxyInstance(
        RetryingMetaStoreClient.class.getClassLoader(), baseClass.getInterfaces(), handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object ret;
    int retriesMade = 0;
    TException caughtException;

    boolean allowReconnect = ! method.isAnnotationPresent(NoReconnect.class);
    boolean allowRetry = true;
    Annotation[] directives = method.getDeclaredAnnotations();
    if(directives != null) {
      for(Annotation a : directives) {
        if(a instanceof RetrySemantics.CannotRetry) {
          allowRetry = false;
        }
      }
    }

    while (true) {
      try {
        SecurityUtils.reloginExpiringKeytabUser();

        if (allowReconnect) {
          if (retriesMade > 0 || hasConnectionLifeTimeReached(method)) {
            if (this.ugi != null) {
              // Perform reconnect with the proper user context
              try {
                LOG.info("RetryingMetaStoreClient trying reconnect as " + this.ugi);

                this.ugi.doAs(
                  new PrivilegedExceptionAction<Object> () {
                    @Override
                    public Object run() throws MetaException {
                      base.reconnect();
                      return null;
                    }
                  });
              } catch (UndeclaredThrowableException e) {
                Throwable te = e.getCause();
                if (te instanceof PrivilegedActionException) {
                  throw te.getCause();
                } else {
                  throw te;
                }
              }
              lastConnectionTime = System.currentTimeMillis();
            } else {
              LOG.warn("RetryingMetaStoreClient unable to reconnect. No UGI information.");
              throw new MetaException("UGI information unavailable. Will not attempt a reconnect.");
            }
          }
        }

        if (metaCallTimeMap == null) {
          ret = method.invoke(base, args);
        } else {
          // need to capture the timing
          long startTime = System.currentTimeMillis();
          ret = method.invoke(base, args);
          long timeTaken = System.currentTimeMillis() - startTime;
          addMethodTime(method, timeTaken);
        }
        break;
      } catch (UndeclaredThrowableException e) {
        throw e.getCause();
      } catch (InvocationTargetException e) {
        Throwable t = e.getCause();
        // Metastore client needs retry for only TTransportException.
        if (TTransportException.class.isAssignableFrom(t.getClass())) {
          caughtException = (TTransportException) t;
        } else {
          throw t;
        }
      }

      if (retriesMade >= retryLimit || base.isLocalMetaStore() || !allowRetry) {
        throw caughtException;
      }
      retriesMade++;
      LOG.warn("MetaStoreClient lost connection. Attempting to reconnect (" + retriesMade + " of " +
          retryLimit + ") after " + retryDelaySeconds + "s. " + method.getName(), caughtException);
      Thread.sleep(retryDelaySeconds * 1000);
    }
    return ret;
  }

  /**
   * Returns the UGI for the current user.
   * @return the UGI for the current user.
   */
  private UserGroupInformation getUGI() {
    UserGroupInformation ugi = null;

    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      // Swallow the exception and let the call determine what to do.
    }

    return ugi;
  }

  private void addMethodTime(Method method, long timeTaken) {
    String methodStr = getMethodString(method);
    while (true) {
      Long curTime = metaCallTimeMap.get(methodStr), newTime = timeTaken;
      if (curTime != null && metaCallTimeMap.replace(methodStr, curTime, newTime + curTime)) break;
      if (curTime == null && (null == metaCallTimeMap.putIfAbsent(methodStr, newTime))) break;
    }
  }

  /**
   * @param method
   * @return String representation with arg types. eg getDatabase_(String, )
   */
  private String getMethodString(Method method) {
    StringBuilder methodSb = new StringBuilder(method.getName());
    methodSb.append("_(");
    if (method.getParameterTypes().length != 0) {
      Iterator<Class<?>> it =
          Arrays.stream(method.getParameterTypes()).iterator();
      methodSb.append(it.next().getSimpleName());
      while (it.hasNext()) {
        methodSb.append(", ");
        methodSb.append(it.next().getSimpleName());
      }
    }
    methodSb.append(")");
    return methodSb.toString();
  }

  private boolean hasConnectionLifeTimeReached(Method method) {
    if (connectionLifeTimeInMillis <= 0 || localMetaStore) {
      return false;
    }

    boolean shouldReconnect =
        (System.currentTimeMillis() - lastConnectionTime) >= connectionLifeTimeInMillis;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reconnection status for Method: " + method.getName() + " is " + shouldReconnect);
    }
    return shouldReconnect;
  }

}
