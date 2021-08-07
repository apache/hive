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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.datanucleus.exceptions.NucleusException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RetryingHMSHandler implements InvocationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RetryingHMSHandler.class);
  private static final String CLASS_NAME = RetryingHMSHandler.class.getName();

  private static class Result {
    private final Object result;
    private final int numRetries;

    public Result(Object result, int numRetries) {
      this.result = result;
      this.numRetries = numRetries;
    }
  }

  private final IHMSHandler baseHandler;
  private final MetaStoreInit.MetaStoreInitData metaStoreInitData =
    new MetaStoreInit.MetaStoreInitData();

  private final Configuration origConf;            // base configuration
  private final Configuration activeConf;  // active configuration

  private RetryingHMSHandler(Configuration origConf, IHMSHandler baseHandler, boolean local) throws MetaException {
    this.origConf = origConf;
    this.baseHandler = baseHandler;
    if (local) {
      baseHandler.setConf(origConf); // tests expect configuration changes applied directly to metastore
    }
    activeConf = baseHandler.getConf();
    // This has to be called before initializing the instance of HMSHandler
    // Using the hook on startup ensures that the hook always has priority
    // over settings in *.xml.  The thread local conf needs to be used because at this point
    // it has already been initialized using hiveConf.
    MetaStoreInit.updateConnectionURL(origConf, getActiveConf(), null, metaStoreInitData);
    try {
      //invoking init method of baseHandler this way since it adds the retry logic
      //in case of transient failures in init method
      invoke(baseHandler, baseHandler.getClass().getDeclaredMethod("init", (Class<?>[]) null),
          null);
    } catch (MetaException e) {
      // Our RetryingHMSHandler.invoke method exception.
      MetaException me = ExceptUtils.wrapMetastoreClientException("init", e);
      LOG.error("HMSHandler fatal init error: ", me);
      throw me;
    } catch (NoSuchMethodException | SecurityException e) {
      // Class.getDeclaredMethod exceptions.
      MetaException me = ExceptUtils.wrapMetastoreClientException("init", e);
      LOG.error("HMSHandler fatal error: ", me);
      throw me;
    }
  }

  public static IHMSHandler getProxy(Configuration conf, IHMSHandler baseHandler, boolean local)
      throws MetaException {

    RetryingHMSHandler handler = new RetryingHMSHandler(conf, baseHandler, local);

    return (IHMSHandler) Proxy.newProxyInstance(
      RetryingHMSHandler.class.getClassLoader(),
      new Class[] { IHMSHandler.class }, handler);
  }

  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args) throws MetaException {
    int retryCount = -1;
    int threadId = baseHandler.getThreadId();
    boolean error = true;
    PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
    perfLogger.perfLogBegin(CLASS_NAME, method.getName());
    try {
      Result result = invokeInternal(proxy, method, args);
      retryCount = result.numRetries;
      error = false;
      return result.result;
    } finally {
      StringBuilder additionalInfo = new StringBuilder();
      additionalInfo.append("threadId=").append(threadId).append(" retryCount=").append(retryCount)
        .append(" error=").append(error);
      perfLogger.perfLogEnd(CLASS_NAME, method.getName(), additionalInfo.toString());
    }
  }

  public Result invokeInternal(final Object proxy, final Method method, final Object[] args) throws MetaException {

    boolean gotNewConnectUrl = false;
    boolean reloadConf = MetastoreConf.getBoolVar(origConf, ConfVars.HMS_HANDLER_FORCE_RELOAD_CONF);
    long retryInterval = MetastoreConf.getTimeVar(origConf,
        ConfVars.HMS_HANDLER_INTERVAL, TimeUnit.MILLISECONDS);
    int retryLimit = MetastoreConf.getIntVar(origConf, ConfVars.HMS_HANDLER_ATTEMPTS);
    long timeout = MetastoreConf.getTimeVar(origConf,
        ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    Deadline.registerIfNot(timeout);

    if (reloadConf) {
      MetaStoreInit.updateConnectionURL(origConf, getActiveConf(),
        null, metaStoreInitData);
    }

    int retryCount = 0;
    Throwable caughtException = null;
    while (true) {
      try {
        if (reloadConf || gotNewConnectUrl) {
          baseHandler.setConf(getActiveConf());
        }
        Object object = null;
        boolean isStarted = Deadline.startTimer(method.getName());
        try {
          object = method.invoke(baseHandler, args);
        } finally {
          if (isStarted) {
            Deadline.stopTimer();
          }
        }
        return new Result(object, retryCount);
      } catch (UndeclaredThrowableException e) {
        // Exception not declared in method's exception throws clause...
        Throwable cause = e.getCause();
        if (cause instanceof Error) {
          handleFatalError(method.getName(), cause);
        }
        if (cause instanceof javax.jdo.JDOException) {
          // Due to reflection, the jdo exception is wrapped in
          // invocationTargetException
          caughtException = e.getCause();
        } else if (cause instanceof MetaException && cause.getCause() != null
            && cause.getCause() instanceof javax.jdo.JDOException) {
          // The JDOException may be wrapped further in a MetaException
          caughtException = cause.getCause();
        } else {
          MetaException me = ExceptUtils.wrapMetastoreClientException(method.getName(), cause);
          LOG.error("Error happened calling method " + method.getName() + ": ", me);
          throw me;
        }
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof javax.jdo.JDOException) {
          // Due to reflection, the jdo exception is wrapped in
          // invocationTargetException
          caughtException = cause;
        } else if (cause instanceof NoSuchObjectException || e.getTargetException().getCause() instanceof NoSuchObjectException) {
          MetaException me = ExceptUtils.wrapMetastoreClientException(method.getName(), cause);
          String methodName = method.getName();
          if (!methodName.startsWith("get_database") && !methodName.startsWith("get_table")
              && !methodName.startsWith("get_partition") && !methodName.startsWith("get_function")) {
            LOG.error("Error happened calling method " + method.getName() + ": ", me);
          }
          throw me;
        } else if (cause instanceof MetaException && cause.getCause() != null) {
          Throwable actualCause = cause.getCause();
          if (actualCause instanceof javax.jdo.JDOException ||
                  actualCause instanceof NucleusException) {
            // The JDOException or the Nucleus Exception may be wrapped further in a MetaException
            caughtException = actualCause;
          } else if (actualCause instanceof DeadlineException) {
            // The Deadline Exception needs no retry and be thrown immediately.
            Deadline.clear();
            MetaException me = ExceptUtils.wrapMetastoreClientException(method.getName(), actualCause);
            LOG.error("Error happened calling method " + method.getName() + ": ", me);
            throw me;
          } else {
            MetaException me = ExceptUtils.wrapMetastoreClientException(method.getName(), cause);
            LOG.error("Error happened calling method " + method.getName() + ": ", me);
            throw me;
          }
        } else {
          MetaException me = ExceptUtils.wrapMetastoreClientException(method.getName(), cause);
          LOG.error("Error happened calling method " + method.getName() + ": ", me);
          throw me;
        }
      } catch (IllegalAccessException e) {
        MetaException me = ExceptUtils.wrapMetastoreClientException(method.getName(), e);
        LOG.error("Error happened calling method " + method.getName() + ": ", me);
        throw me;
      }
      MetaException me = ExceptUtils.wrapMetastoreClientException(method.getName(), caughtException);
      if (retryCount >= retryLimit) {
        LOG.error("HMSHandler fatal error calling method " + method.getName() + ": ", me);
        throw me;
      }

      assert (retryInterval >= 0);
      retryCount++;
      LOG.error(
        String.format(
          "Retrying HMSHandler after %d ms (attempt %d of %d)", retryInterval, retryCount, retryLimit) +
          " with error calling method " + method.getName() + ": ", me);

      try {
        Thread.sleep(retryInterval);
      } catch (InterruptedException e) {
        MetaException interruptMe = ExceptUtils.wrapMetastoreClientException(method.getName(), e);
        LOG.error("Error happened calling method " + method.getName() + ": ", interruptMe);
        throw interruptMe;
      }
      // If we have a connection error, the JDO connection URL hook might
      // provide us with a new URL to access the datastore.
      String lastUrl = MetaStoreInit.getConnectionURL(getActiveConf());
      gotNewConnectUrl = MetaStoreInit.updateConnectionURL(origConf, getActiveConf(),
        lastUrl, metaStoreInitData);
    }
  }

  /*
   * Technically, you are not suppose to catch java.lang.Error (OOM, etc.) but it is unavoidable with wrap
   * exceptions like UndeclaredThrowableException.
   *
   * Attempt to create wrapper Error to show we intercepted it here, log, and throw. Otherwise, rethrow.
   */
  private void handleFatalError(String operationName, Throwable cause) {
    boolean isRethrow = true;
    Error wrappedError = null;
    try {
      wrappedError = new Error(operationName + " fatal error: ", cause);
      LOG.error("Fatal error for " + operationName + ": ", wrappedError);
      isRethrow = false;
    } catch (Throwable t) {
      // Suppress..
    }
    if (!isRethrow) {
      throw wrappedError;
    } else {
      throw (Error) cause;
    }
  }

  public Configuration getActiveConf() {
    return activeConf;
  }
}
