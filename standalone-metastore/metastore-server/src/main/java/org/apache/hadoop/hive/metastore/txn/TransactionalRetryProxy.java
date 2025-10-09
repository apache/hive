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

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.RollbackException;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetry;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryCallProperties;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryFunction;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryHandler;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;

/**
 * Responsible for proxying an interface, and calling its methods by processing the following annotations: 
 * {@link SqlRetry} and {@link Transactional}. The annotated methods will be called accordingly: 
 * <ul>
 *   <li>SQL errors in methods annotated with {@link SqlRetry} will be caught and the method will be re-executed</li>
 *   <li>Methods annotated with {@link Transactional} will be executed after creating a transaction, and all operations done
 *   via {@link MultiDataSourceJdbcResource} will use the created transaction.</li>
 *   <li>In case a method is annotated with both annotations, the transaction will be inside the retry-call. This means 
 *   in case of SQL errors and retries, the transaction will be rolled back and a new one will be created for each retry
 *   attempt.</li>
 * </ul> 
 * Not annotated methods are called directly.
 */
public class TransactionalRetryProxy<T> implements InvocationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionalRetryProxy.class);

  /**
   * Gets the proxy interface for the given {@link TxnStore}.
   *
   * @param sqlRetryHandler Responsible to re-execute the methods in case of failure.
   * @param interfaceObject The real object to proxy.
   * @return Returns the proxy object capable of retrying the failed calls automatically and transparently.
   */
  public static <T> T getProxy(SqlRetryHandler sqlRetryHandler, MultiDataSourceJdbcResource jdbcResourceHandler, T interfaceObject) {
    TransactionalRetryProxy<T> handler = new TransactionalRetryProxy<>(interfaceObject, sqlRetryHandler, jdbcResourceHandler);
    //noinspection unchecked
    return (T) Proxy.newProxyInstance(
        TransactionalRetryProxy.class.getClassLoader(),
        ClassUtils.getAllInterfaces(interfaceObject.getClass()).toArray(new Class[0]),
        handler);
  }

  private final T interfaceObject;
  private final SqlRetryHandler sqlRetryHandler;
  private final MultiDataSourceJdbcResource jdbcResource;

  private TransactionalRetryProxy(T interfaceObject, SqlRetryHandler sqlRetryHandler, MultiDataSourceJdbcResource jdbcResource) {
    this.interfaceObject = interfaceObject;
    this.sqlRetryHandler = sqlRetryHandler;
    this.jdbcResource = jdbcResource;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    String strArgs;
    try {
      strArgs = args == null ? "" : StringUtils.join(Arrays.asList(args), ",");
    } catch (Exception e) {
      LOG.warn("Error while trying to stringify the method arguments.", e);
      strArgs = "unknown";
    }
    String callerId = method.getName() + "(" + strArgs + ")";
    SqlRetry retry = method.getAnnotation(SqlRetry.class);
    Transactional transactional = method.getAnnotation(Transactional.class);

    ThrowingSupplier functionToCall = () -> {
      try {
        return method.invoke(interfaceObject, args);
      } catch (InvocationTargetException | UndeclaredThrowableException e) {
        throw e.getCause();
      }
    };

    if (transactional != null) {
      ThrowingSupplier toCall = functionToCall;
      functionToCall = () -> {
        LOG.debug("Invoking method within transactional context: {}", callerId);
        TransactionContext context = null;
        try {
          jdbcResource.bindDataSource(transactional);
          context = jdbcResource.getTransactionManager().getNewTransaction(transactional.propagation().value());
          Object result = toCall.execute();
          LOG.debug("Successfull method invocation within transactional context: {}, going to commit.", callerId);
          if (context.isRollbackOnly()) {
            jdbcResource.getTransactionManager().rollback(context);
          } else if (!context.isCompleted()) {
            jdbcResource.getTransactionManager().commit(context);
          }
          return result;
        } catch (RollbackException e) {
          if (context != null && !context.isCompleted()) {
            jdbcResource.getTransactionManager().rollback(context);
          }          
          return e.getResult();
        } catch (Exception e) {
          if (context != null) {
            if (transactional.noRollbackFor().length > 0 || transactional.noRollbackForClassName().length > 0) {
              if (Arrays.stream(transactional.noRollbackFor()).anyMatch(ex -> ex.isInstance(e)) ||
                  Arrays.stream(transactional.noRollbackForClassName()).anyMatch(exName -> exName.equals(e.getClass().getName()))) {
                jdbcResource.getTransactionManager().commit(context);
              } else {
                jdbcResource.getTransactionManager().rollback(context);
              }
            } else if (transactional.rollbackFor().length > 0 || transactional.rollbackForClassName().length > 0) {
              if (Arrays.stream(transactional.rollbackFor()).anyMatch(ex -> ex.isInstance(e)) ||
                  Arrays.stream(transactional.rollbackForClassName()).anyMatch(exName -> exName.equals(e.getClass().getName()))) {
                jdbcResource.getTransactionManager().rollback(context);
              } else {
                jdbcResource.getTransactionManager().commit(context);                
              }
            } else {
              jdbcResource.getTransactionManager().rollback(context);
            }
          }
          throw e;
        } finally {
          jdbcResource.unbindDataSource();
        }
      };
    }

    if (retry != null) {
      SqlRetryCallProperties properties = new SqlRetryCallProperties()
          .withCallerId(callerId)
          .withLockInternally(retry.lockInternally())
          .withRetryOnDuplicateKey(retry.retryOnDuplicateKey());
      ThrowingSupplier toCall = functionToCall;
      SqlRetryFunction<Object> retryWrapper = () -> {
        try {
          LOG.debug("Invoking method within retry context: {}", callerId);
          Object result = toCall.execute();
          LOG.debug("Successfull method invocation within retry context: {}", callerId);
          return result;
        } catch (IllegalAccessException | InvocationTargetException | UndeclaredThrowableException e) {
          if (e.getCause() instanceof TException) {
            throw (TException) e.getCause();
          } else if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          } else {
            throw new RuntimeException(e);
          }
        } catch (TException | DataAccessException e) {
          throw e;
        } catch (Throwable e) {          
          if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          } else if (e.getCause() instanceof RuntimeException) {
              throw (RuntimeException)e.getCause();
          } else {
            throw new RuntimeException(e);
          }
        }
      };
      return sqlRetryHandler.executeWithRetry(properties, retryWrapper);
    } else {
      LOG.debug("Invoking method without retry context: {}", callerId);
      Object result = functionToCall.execute();
      LOG.debug("Successfull method invocation without retry context: {}", callerId);
      return result;
    }
  }

  private interface ThrowingSupplier {
    Object execute() throws Throwable;
  }

}