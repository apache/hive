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

package org.apache.hadoop.hive.metastore;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
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

  private final HiveConf origConf;            // base configuration
  private final Configuration activeConf;  // active configuration

  private RetryingHMSHandler(HiveConf hiveConf, IHMSHandler baseHandler, boolean local) throws MetaException {
    this.origConf = hiveConf;
    this.baseHandler = baseHandler;
    if (local) {
      baseHandler.setConf(hiveConf); // tests expect configuration changes applied directly to metastore
    }
    activeConf = baseHandler.getConf();
    // This has to be called before initializing the instance of HMSHandler
    // Using the hook on startup ensures that the hook always has priority
    // over settings in *.xml.  The thread local conf needs to be used because at this point
    // it has already been initialized using hiveConf.
    MetaStoreInit.updateConnectionURL(hiveConf, getActiveConf(), null, metaStoreInitData);
    try {
      //invoking init method of baseHandler this way since it adds the retry logic
      //in case of transient failures in init method
      invoke(baseHandler, baseHandler.getClass().getDeclaredMethod("init", (Class<?>[]) null),
          null);
    } catch (Throwable e) {
      LOG.error("HMSHandler Fatal error: " + ExceptionUtils.getStackTrace(e));
      MetaException me = new MetaException(e.getMessage());
      me.initCause(e);
      throw me;
    }
  }

  public static IHMSHandler getProxy(HiveConf hiveConf, IHMSHandler baseHandler, boolean local)
      throws MetaException {

    RetryingHMSHandler handler = new RetryingHMSHandler(hiveConf, baseHandler, local);

    return (IHMSHandler) Proxy.newProxyInstance(
      RetryingHMSHandler.class.getClassLoader(),
      new Class[] { IHMSHandler.class }, handler);
  }

  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
    int retryCount = -1;
    int threadId = HiveMetaStore.HMSHandler.get();
    boolean error = true;
    PerfLogger perfLogger = PerfLogger.getPerfLogger(origConf, false);
    perfLogger.PerfLogBegin(CLASS_NAME, method.getName());
    try {
      Result result = invokeInternal(proxy, method, args);
      retryCount = result.numRetries;
      error = false;
      return result.result;
    } finally {
      StringBuffer additionalInfo = new StringBuffer();
      additionalInfo.append("threadId=").append(threadId).append(" retryCount=").append(retryCount)
        .append(" error=").append(error);
      perfLogger.PerfLogEnd(CLASS_NAME, method.getName(), additionalInfo.toString());
    }
  }

  public Result invokeInternal(final Object proxy, final Method method, final Object[] args) throws Throwable {

    boolean gotNewConnectUrl = false;
    boolean reloadConf = HiveConf.getBoolVar(origConf,
        HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF);
    long retryInterval = HiveConf.getTimeVar(origConf,
        HiveConf.ConfVars.HMSHANDLERINTERVAL, TimeUnit.MILLISECONDS);
    int retryLimit = HiveConf.getIntVar(origConf,
        HiveConf.ConfVars.HMSHANDLERATTEMPTS);
    long timeout = HiveConf.getTimeVar(origConf,
        HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

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

      } catch (javax.jdo.JDOException e) {
        caughtException = e;
      } catch (UndeclaredThrowableException e) {
        if (e.getCause() != null) {
          if (e.getCause() instanceof javax.jdo.JDOException) {
            // Due to reflection, the jdo exception is wrapped in
            // invocationTargetException
            caughtException = e.getCause();
          } else if (e.getCause() instanceof MetaException && e.getCause().getCause() != null
              && e.getCause().getCause() instanceof javax.jdo.JDOException) {
            // The JDOException may be wrapped further in a MetaException
            caughtException = e.getCause().getCause();
          } else {
            LOG.error(ExceptionUtils.getStackTrace(e.getCause()));
            throw e.getCause();
          }
        } else {
          LOG.error(ExceptionUtils.getStackTrace(e));
          throw e;
        }
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof javax.jdo.JDOException) {
          // Due to reflection, the jdo exception is wrapped in
          // invocationTargetException
          caughtException = e.getCause();
        } else if (e.getCause() instanceof NoSuchObjectException || e.getTargetException().getCause() instanceof NoSuchObjectException) {
          String methodName = method.getName();
          if (!methodName.startsWith("get_database") && !methodName.startsWith("get_table")
              && !methodName.startsWith("get_partition") && !methodName.startsWith("get_function")) {
            LOG.error(ExceptionUtils.getStackTrace(e.getCause()));
          }
          throw e.getCause();
        } else if (e.getCause() instanceof MetaException && e.getCause().getCause() != null) {
          if (e.getCause().getCause() instanceof javax.jdo.JDOException ||
              e.getCause().getCause() instanceof NucleusException) {
            // The JDOException or the Nucleus Exception may be wrapped further in a MetaException
            caughtException = e.getCause().getCause();
          } else if (e.getCause().getCause() instanceof DeadlineException) {
            // The Deadline Exception needs no retry and be thrown immediately.
            Deadline.clear();
            LOG.error("Error happens in method " + method.getName() + ": " +
                ExceptionUtils.getStackTrace(e.getCause()));
            throw e.getCause();
          } else {
            LOG.error(ExceptionUtils.getStackTrace(e.getCause()));
            throw e.getCause();
          }
        } else {
          LOG.error(ExceptionUtils.getStackTrace(e.getCause()));
          throw e.getCause();
        }
      }

      if (retryCount >= retryLimit) {
        LOG.error("HMSHandler Fatal error: " + ExceptionUtils.getStackTrace(caughtException));
        MetaException me = new MetaException(caughtException.getMessage());
        me.initCause(caughtException);
        throw me;
      }

      assert (retryInterval >= 0);
      retryCount++;
      LOG.error(
        String.format(
          "Retrying HMSHandler after %d ms (attempt %d of %d)", retryInterval, retryCount, retryLimit) +
          " with error: " + ExceptionUtils.getStackTrace(caughtException));

      Thread.sleep(retryInterval);
      // If we have a connection error, the JDO connection URL hook might
      // provide us with a new URL to access the datastore.
      String lastUrl = MetaStoreInit.getConnectionURL(getActiveConf());
      gotNewConnectUrl = MetaStoreInit.updateConnectionURL(origConf, getActiveConf(),
        lastUrl, metaStoreInitData);
    }
  }

  public Configuration getActiveConf() {
    return activeConf;
  }
}
