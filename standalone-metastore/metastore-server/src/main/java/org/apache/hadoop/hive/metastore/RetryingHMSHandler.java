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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.TimeUnit;

import javax.jdo.JDOException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.datanucleus.exceptions.NucleusException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RetryingHMSHandler extends AbstractHMSHandlerProxy {
  private static final Logger LOG = LoggerFactory.getLogger(RetryingHMSHandler.class);

  private final long retryInterval;
  private final int retryLimit;

  public RetryingHMSHandler(Configuration conf, IHMSHandler baseHandler, boolean local)
      throws MetaException {
    super(conf, baseHandler, local);
    retryInterval = MetastoreConf.getTimeVar(origConf,
        ConfVars.HMS_HANDLER_INTERVAL, TimeUnit.MILLISECONDS);
    retryLimit = MetastoreConf.getIntVar(origConf, ConfVars.HMS_HANDLER_ATTEMPTS);

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

  @Override
  protected void initBaseHandler() throws MetaException {
    // init operation has finished in constructor. noop here.
  }

  public Result invokeInternal(final Object proxy, final Method method, final Object[] args) throws Throwable {
    boolean gotNewConnectUrl = false;

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
        StringBuilder additionalInfo = new StringBuilder();
        additionalInfo.append("retryCount=").append(retryCount)
            .append(" error=").append(false);
        return new Result(object, additionalInfo.toString());
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
              && !methodName.startsWith("get_partition") && !methodName.startsWith("get_function")
              && !methodName.startsWith("get_stored_procedure") && !methodName.startsWith("find_package")) {
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

      Throwable rootCause = ExceptionUtils.getRootCause(caughtException);
      String errorMessage = ExceptionUtils.getMessage(caughtException) +
              (rootCause == null ? "" : ("\nRoot cause: " + rootCause));
      if (retryCount >= retryLimit || !isRecoverableException(caughtException)) {
        LOG.error("HMSHandler Fatal error: " + ExceptionUtils.getStackTrace(caughtException));
        throw new MetaException(errorMessage);
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

  private boolean isRecoverableException(Throwable t) {
    if (!(t instanceof JDOException || t instanceof NucleusException)) {
      return false;
    }
    return DatabaseProduct.isRecoverableException(t);
  }
}
