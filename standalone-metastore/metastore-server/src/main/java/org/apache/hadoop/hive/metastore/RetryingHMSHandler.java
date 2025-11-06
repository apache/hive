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

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import javax.jdo.JDOException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.RetryingExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.datanucleus.exceptions.NucleusException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RetryingHMSHandler extends AbstractHMSHandlerProxy {

  private final long retryInterval;
  private final int retryLimit;
  private final Predicate<Throwable> retryOnException;

  public RetryingHMSHandler(Configuration conf, IHMSHandler baseHandler, boolean local)
      throws MetaException {
    super(conf, baseHandler, local);
    retryInterval = MetastoreConf.getTimeVar(origConf,
        ConfVars.HMS_HANDLER_INTERVAL, TimeUnit.MILLISECONDS);
    retryLimit = MetastoreConf.getIntVar(origConf, ConfVars.HMS_HANDLER_ATTEMPTS);

    retryOnException = exception ->
      (ExceptionUtils.stream(exception).anyMatch(e -> e instanceof JDOException) ||
          ExceptionUtils.stream(exception).anyMatch(e -> e instanceof NucleusException)) &&
      ExceptionUtils.stream(exception).anyMatch(DatabaseProduct::isRecoverableException);

    new RetryingExecutor<>(retryLimit, () -> { baseHandler.init(); return null;})
        .commandName("init").onRetry(retryOnException)
        .sleepInterval(retryInterval).runWithMetaException();
  }

  @Override
  protected void initBaseHandler() throws MetaException {
    // init operation has finished in constructor. noop here.
  }

  public Result invokeInternal(final Object proxy, final Method method, final Object[] args) throws Throwable {
    final AtomicBoolean gotNewConnectUrl = new AtomicBoolean(false);

    if (reloadConf) {
      MetaStoreInit.updateConnectionURL(origConf, getActiveConf(),
        null, metaStoreInitData);
    }

    AtomicInteger retryCount = new AtomicInteger(0);
    return new RetryingExecutor<>(retryLimit, () -> {
      if (reloadConf || gotNewConnectUrl.get()) {
        baseHandler.setConf(getActiveConf());
      }
      Object object;
      boolean isStarted = Deadline.startTimer(method.getName());
      try {
        object = method.invoke(baseHandler, args);
      } catch (Exception e) {
        // If we have a connection error, the JDO connection URL hook might
        // provide us with a new URL to access the datastore.
        String lastUrl = MetaStoreInit.getConnectionURL(getActiveConf());
        gotNewConnectUrl.set(MetaStoreInit.updateConnectionURL(origConf, getActiveConf(),
            lastUrl, metaStoreInitData));
        throw e;
      } finally {
        if (isStarted) {
          Deadline.stopTimer();
        }
      }
      String additionalInfo = "retryCount=" + retryCount.getAndIncrement() + " error=" + false;
      return new Result(object, additionalInfo);
    }).commandName(method.getName())
        .onRetry(retryOnException)
        .sleepInterval(retryInterval)
        .run();
  }
  
}
