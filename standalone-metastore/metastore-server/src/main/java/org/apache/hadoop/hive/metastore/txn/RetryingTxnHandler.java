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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.retryhandling.RetryCallProperties;
import org.apache.hadoop.hive.metastore.txn.retryhandling.DataSourceWrapper;
import org.apache.hadoop.hive.metastore.txn.retryhandling.Retry;
import org.apache.hadoop.hive.metastore.txn.retryhandling.RetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;

/**
 * Responsible to use the {@link RetryHandler} to automatically retry the failed {@link TxnStore} methods if annotated with
 * the {@link Retry} annotation.
 */
public class RetryingTxnHandler implements InvocationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RetryingTxnHandler.class);

  /**
   * Gets the proxy interface for the given {@link TxnStore}.
   * @param realStore The real {@link TxnStore} to proxy.
   * @param dataSourceWrapper Used by {@link RetryHandler} to establish a retry-context.
   * @param retryHandler Responsible to re-execute the methods in case of failure.
   * @return Returns the proxy object capable of retrying the failed calls automatically and transparently. 
   */
  public static TxnStore getProxy(TxnStore realStore, DataSourceWrapper dataSourceWrapper, RetryHandler retryHandler) {
    RetryingTxnHandler handler = new RetryingTxnHandler(realStore, dataSourceWrapper, retryHandler);
    return (TxnStore) Proxy.newProxyInstance(
        RetryingTxnHandler.class.getClassLoader(),
        new Class[]{ TxnStore.class },
        handler);
  }

  private final DataSourceWrapper dataSourceWrapper;
  private final RetryHandler retryHandler;
  private final TxnStore realStore;

  private RetryingTxnHandler(TxnStore realStore, DataSourceWrapper dataSourceWrapper, RetryHandler retryHandler) {
    this.realStore = realStore;
    this.dataSourceWrapper = dataSourceWrapper;
    this.retryHandler = retryHandler;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    String strArgs = args == null ? "" : StringUtils.join(Arrays.asList(args), ",");
    String callerId = method.getName() + "(" + strArgs + ")";
    Retry retry = method.getAnnotation(Retry.class);
    if (retry != null) {
      RetryCallProperties properties = new RetryCallProperties()
          .withCallerId(callerId)
          .withDataSource(retry.datasource())
          .withLockInternally(retry.lockInternally())
          .withRollbackOnError(retry.rollbackOnError())
          .withRetryOnDuplicateKey(retry.retryOnDuplicateKey())
          .withTransactionIsolationLevel(retry.transactionIsolation());
      return retryHandler.executeWithRetry(dataSourceWrapper, properties,
          (DataSourceWrapper dataSourceWrapper) -> {
            try {
              LOG.info("Invoking method within retry context: {}", callerId);
              return method.invoke(realStore, args);
            } catch (IllegalAccessException | InvocationTargetException | UndeclaredThrowableException e) {
              if (e.getCause() instanceof MetaException) {
                throw (MetaException) e.getCause();
              } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
              } else {
                throw new RuntimeException(e);
              }
            }
          });
    } else {
      try {
        LOG.info("Invoking method without retry context: {}", callerId);
        return method.invoke(realStore, args);
      } catch (InvocationTargetException | UndeclaredThrowableException e) {
        throw e.getCause();
      }
    }
  }

}