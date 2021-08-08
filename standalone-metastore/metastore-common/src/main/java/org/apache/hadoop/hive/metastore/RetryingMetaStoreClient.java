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
import java.util.regex.Pattern;

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
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
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
  private static final Pattern IO_JDO_TRANSPORT_PROTOCOL_EXCEPTION_PATTERN =
      Pattern.compile("(?s).*(IO|JDO[a-zA-Z]*|TProtocol|TTransport)Exception.*");

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

  /*
   * When we look at the (generated) IMetaStoreClient interface the method's throws clause has exceptions whose
   * superclass is Thrift's TException.
   *
   * Very commonly MetaException is thrown. But also ones more specific to a particular method such as:
   *
   *     AlreadyExistsException, InvalidObjectException, NoSuchObjectException, UnknownDBException, and more.
   *
   *  And generic ones like TProtocolException, TTransportException, etc.
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws TException {
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
      SecurityUtils.reloginExpiringKeytabUser();

      if (allowReconnect) {
        if (retriesMade > 0 || hasConnectionLifeTimeReached(method)) {
          if (this.ugi != null) {
            reconnect();
          } else {
            LOG.warn("RetryingMetaStoreClient unable to reconnect. No UGI information.");
            throw new MetaException("UGI information unavailable. Will not attempt a reconnect.");
          }
        }
      }

      try {
        if (metaCallTimeMap == null) {
          ret = method.invoke(base, args);
        } else {
          // need to capture the timing
          long startTime = System.currentTimeMillis();
          ret = method.invoke(base, args);
          long timeTaken = System.currentTimeMillis() - startTime;
          addMethodTime(method, timeTaken);
        }
        // Successful.
        break;
      } catch (Throwable t) {
        ExceptUtils.MethodInvokeResult methodInvokeResult =
                ExceptUtils.evaluateMethodInvokeThrowable(method, t, TException.class);
        // If the Throwable is not retryable, an TException or Error is thrown.
        caughtException = getRetryableExceptionFromInvoke(method.getName(), methodInvokeResult);
      }

      // Fall through to here with a retryable TException.
      if (retriesMade >= retryLimit || base.isLocalMetaStore() || !allowRetry) {
        throw ExceptUtils.wrapCaughtException(caughtException);
      }
      retriesMade++;
      LOG.warn("MetaStoreClient lost connection. Attempting to reconnect (" + retriesMade + " of " +
          retryLimit + ") after " + retryDelaySeconds + "s. " + method.getName(), caughtException);
      try {
        Thread.sleep(retryDelaySeconds * 1000);
      } catch (InterruptedException e) {
        MetaException me = ExceptUtils.wrapMetastoreClientException(method.getName(), e);
        LOG.error("Error happened calling method " + method.getName() + ": ", me);
        throw me;
      }
    }
    return ret;
  }

  // UNDONE: Retryable?
  private void reconnect() throws MetaException {
    // Perform reconnect with the proper user context
    try {
      LOG.info("RetryingMetaStoreClient trying reconnect as " + this.ugi);

      this.ugi.doAs(
              new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws MetaException {
                  base.reconnect();
                  return null;
                }
              });
    } catch (UndeclaredThrowableException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        ExceptUtils.handleFatalError("reconnect", (Error) cause);
      }
      if (cause instanceof PrivilegedActionException) {
        throw ExceptUtils.wrapMetastoreClientException("reconnect", cause.getCause());
      } else {
        throw ExceptUtils.wrapMetastoreClientException("reconnect", cause);
      }
    } catch (InterruptedException | IOException e) {
      throw ExceptUtils.wrapMetastoreClientException("reconnect", e);
    }
    lastConnectionTime = System.currentTimeMillis();
  }

  private TException getRetryableExceptionFromInvoke(String methodName,
                                                     ExceptUtils.MethodInvokeResult methodInvokeResult)
          throws TException {
    switch (methodInvokeResult.getResultKind()) {
      case BASE_DECLARED_METHOD_EXCEPTION:
        {
          // // UNDONE: If not Retryable, throw.
          return (TException) methodInvokeResult.getException();
        }
      case SUBCLASS_DECLARED_METHOD_EXCEPTION:
        // Client wants this kind of Exception from method.
        throw ExceptUtils.wrapCaughtException((TException) methodInvokeResult.getException());
      case OTHER_DECLARED_METHOD_EXCEPTION:
        {
          // No match to method's throws clause Exception list on client side.
          Exception e = methodInvokeResult.getException();
          if (e instanceof TException) {
            // Is is, however, a subclass of a base class.
            TException tException = (TException) e;
            if (!isRecoverableTException(tException)) {
              throw ExceptUtils.wrapCaughtException(tException);
            }
          }
          // Otherwise, it is unknown to this code -- assume it is not retryable..
          MetaException me = new MetaException(methodName + " error: ");
          me.initCause(e);
          throw me;
        }
      case UNDECLARED_METHOD_EXCEPTION:
        {
          Exception e = methodInvokeResult.getException();
          if (e instanceof TException) {
            TException tException = (TException) e;
            if (!isRecoverableTException(tException)) {
              throw ExceptUtils.wrapCaughtException(tException);
            }
            return tException;
          } else {
            // Assume not retryable.
            MetaException me = new MetaException(methodName + " error: ");
            me.initCause(methodInvokeResult.getException());
            throw me;
          }
        }
      case INVOCATION_EXCEPTION:
      case UNEXPECTED_EXCEPTION:
        {
          // Assume not retryable.
          MetaException me = new MetaException(methodName + " error: ");
          me.initCause(methodInvokeResult.getException());
          throw me;
        }
      case ERROR:
        {
          ExceptUtils.handleFatalError(methodName + " error: ", methodInvokeResult.getError());
          return null;   // Unreachable.
        }
      default:
        throw new RuntimeException("Unexpected method invoke result: " + methodInvokeResult.getResultKind());
    }
  }

  private MetaException throwUnexpectedBaseClass(Exception e) throws MetaException {
    MetaException me = new MetaException("Internal error");
    me.initCause(new RuntimeException("Only TException is currently a base class", e));
    return me;
  }

  private static boolean isRecoverableTException(TException e) {
    if (e instanceof MetaException) {
      return isRecoverableMetaException((MetaException) e);
    }
    return (e instanceof TProtocolException) || (e instanceof TTransportException);
  }

  private static boolean isRecoverableMetaException(MetaException e) {
    String m = e.getMessage();
    if (m == null) {
      return false;
    }
    if (m.contains("java.sql.SQLIntegrityConstraintViolationException")) {
      return false;
    }
    return IO_JDO_TRANSPORT_PROTOCOL_EXCEPTION_PATTERN.matcher(m).matches();
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
