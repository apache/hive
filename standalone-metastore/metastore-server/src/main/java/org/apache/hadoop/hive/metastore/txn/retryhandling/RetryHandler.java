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
package org.apache.hadoop.hive.metastore.txn.retryhandling;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Supports building resilient APIs by automatically retrying failed function calls.
 */
public class RetryHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RetryHandler.class);

  private static final int ALLOWED_REPEATED_DEADLOCKS = 10;
  private static final String MANUAL_RETRY = "ManualRetry";

  /**
   * Derby specific concurrency control
   */
  private static final ReentrantLock derbyLock = new ReentrantLock(true);


  private DatabaseProduct dbProduct;

  private long deadlockRetryInterval;
  private long retryInterval;
  private int retryLimit;
  
  private Configuration conf;
  
  private static final ThreadLocal<RetryState> threadLocal = new ThreadLocal<>();


  public static String getMessage(Exception ex) {
    StringBuilder sb = new StringBuilder();
    sb.append(ex.getMessage());
    if (ex instanceof SQLException) {
      SQLException sqlEx = (SQLException)ex; 
      sb.append(" (SQLState=").append(sqlEx.getSQLState()).append(", ErrorCode=").append(sqlEx.getErrorCode()).append(")");
    }
    return sb.toString();
  }  
  
  public void init(Configuration conf, DatabaseProduct dbProduct){
    if (dbProduct == null) {
      throw new IllegalStateException("DB Type not determined yet.");
    }
    
    this.conf = conf;
    this.dbProduct = dbProduct;

    retryInterval = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.HMS_HANDLER_INTERVAL,
        TimeUnit.MILLISECONDS);
    retryLimit = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.HMS_HANDLER_ATTEMPTS);
    deadlockRetryInterval = retryInterval / 10;
  }

  /**
   * Executes the passed {@link TransactionalFunction}, and automatically retries the execution in case of failure.
   * @param properties {@link CallProperties} instance used to fine-tune/alter the execution-mechanism.
   * @param function The {@link TransactionalFunction} to execute.
   * @return Returns with the result of the execution of the passed {@link TransactionalFunction}.
   * @param <Result> Type of the result
   * @throws MetaException Thrown in case of execution error.
   */
  public <Result> Result executeWithRetry(DataSourceWrapper dataSourceWrapper, CallProperties properties, TransactionalFunction<Result> function) 
      throws MetaException {
    return executeWithRetryInternal(dataSourceWrapper, properties, function, retryLimit, ALLOWED_REPEATED_DEADLOCKS);
  }

  /**
   * Executes the passed {@link TransactionalVoidFunction}, and automatically retries the execution in case of failure.
   * @param properties {@link CallProperties} instance used to fine-tune/alter the execution-mechanism.
   * @param function The {@link TransactionalVoidFunction} to execute.
   * @throws MetaException Thrown in case of execution error.
   */
  public void executeWithRetry(DataSourceWrapper dataSourceWrapper, CallProperties properties, TransactionalVoidFunction function) throws MetaException {
    executeWithRetryInternal(dataSourceWrapper, properties, (TransactionStatus status, NamedParameterJdbcTemplate jdbcTemplate) -> { 
      function.call(status, jdbcTemplate); 
      return null;
    }, retryLimit, ALLOWED_REPEATED_DEADLOCKS);
  }

  /**
   * Executes the passed {@link TransactionalFunction}, without retry in case of failure.
   * @param properties {@link CallProperties} instance used to fine-tune/alter the execution-mechanism.
   * @param function The {@link TransactionalFunction} to execute.
   * @return Returns with the result of the execution of the passed {@link TransactionalFunction}.
   * @param <Result> Type of the result
   * @throws MetaException Thrown in case of execution error.
   */
  public  <Result> Result executeWithoutRetry(DataSourceWrapper dataSourceWrapper, CallProperties properties, TransactionalFunction<Result> function) 
      throws MetaException {
    return executeWithRetryInternal(dataSourceWrapper, properties, function, 0, 0);
  }

  /**
   * Executes the passed {@link TransactionalVoidFunction}, without retry in case of failure.
   * @param properties {@link CallProperties} instance used to fine-tune/alter the execution-mechanism.
   * @param function The {@link TransactionalVoidFunction} to execute.
   * @throws MetaException Thrown in case of execution error.
   */
  public void executeWithoutRetry(DataSourceWrapper dataSourceWrapper, CallProperties properties, TransactionalVoidFunction function) throws MetaException {
    executeWithRetryInternal(dataSourceWrapper, properties, (TransactionStatus status, NamedParameterJdbcTemplate jdbcTemplate) -> {
      function.call(status, jdbcTemplate);
      return null;
    }, 0, 0);
  }
  
  private <Result> Result executeWithRetryInternal(DataSourceWrapper dataSourceWrapper, CallProperties properties, TransactionalFunction<Result> function
      , int retryCount, int deadlockCount) throws MetaException {
    Objects.requireNonNull(function, "RetryFunction<Result> cannot be null!");

    RetryState oldState = threadLocal.get();
    if(oldState != null) {
      /*
        If there is a RetryState in the ThreadLocal, we are already inside a retry-call, so there is no need
        to establish a nested retry-call, we can join the existing one. Using this approach there is no need to separate
        retry-handling from the actual function body. If a retry method internally calls another method which
        uses RetryHandler, the inner call will simply join the already established retry-context (and transaction).
      */
      LOG.info("Already established retry-context ({}) detected, current retry-call will join it. The passed CallProperties " +
          "instance will be ignored, using the original one.", oldState);
      try {
        return function.call(oldState.status, oldState.jdbcTemplate);
      } catch (SQLException e) {
        throw new UncategorizedSQLException(null, null, e);
      }
    }

    Objects.requireNonNull(properties, "RetryCallProperties cannot be null!");
    LOG.debug("Running retry function:" + properties);
    
    if (org.apache.commons.lang3.StringUtils.isEmpty(properties.getCaller())) {
      properties.withCallerId(function.toString());
    }

    TransactionTemplate transactionTemplate = new TransactionTemplate(dataSourceWrapper.getTransactionManager());
    transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    transactionTemplate.setIsolationLevel(properties.getTransactionIsolationLevel());

    try {
      if (properties.isLockInternally()) {
        lockInternal();
      }
      // This exception handling is required to be able to pass managed exceptions through Spring JDBC's transactionmanagement layer
      return transactionTemplate.execute((TransactionStatus status) -> {
            try {
              NamedParameterJdbcTemplate jdbcTemplate = properties.getJdbcTemplate() == null
                  ? dataSourceWrapper.getJdbcTemplate()
                  : properties.getJdbcTemplate();
              
              //set TransactionStatus to allow detection of nested retry calls              
              threadLocal.set(new RetryState(status, jdbcTemplate));
              Result result = function.call(status, jdbcTemplate);
              LOG.debug("Going to commit transaction");
              return result;
            } catch (SQLException | MetaException e) {
              if (properties.isRollbackOnError()) {
                LOG.debug("Going to rollback due to the following error: ", e);
                status.setRollbackOnly();
              }
              if (e instanceof SQLException) {
                throw new UncategorizedSQLException(null, null, (SQLException) e);
              } else {
                throw new MetaWrapperException((MetaException) e);
              }
            } finally {
              //do not pollute the ThreadLocal
              threadLocal.remove();
            }
          }
      );
    } catch (DataAccessException e) {
      if (e.getCause() instanceof SQLException) {
        SQLException sqlEx = (SQLException) e.getCause();
        if (checkDeadlock(sqlEx, properties.getCaller(), deadlockCount)) {
          return executeWithRetryInternal(dataSourceWrapper, properties, function, retryCount, --deadlockCount);
        }
        if (checkRetryable(sqlEx, properties.getCaller(), properties.isRetryOnDuplicateKey(), retryCount)) {
          return executeWithRetryInternal(dataSourceWrapper, properties, function, --retryCount, deadlockCount);
        }
      }
      LOG.error("Execution failed for caller {}", properties, e);
      throw properties.getExceptionSupplier().apply(e);
    } catch (MetaWrapperException e) {
      //unwrap and re-throw
      LOG.error("Execution failed for caller {}", properties, e.getCause());
      throw (MetaException) e.getCause();
    } catch (Exception e) {
      LOG.error("Execution failed for caller {}", properties, e);
      throw properties.getExceptionSupplier().apply(e);
    } finally {
      if (properties.isLockInternally()) {
        unlockInternal();
      }
    }
  }
  
  private boolean checkDeadlock(SQLException e, String caller, int deadlockCnt) {
    if (dbProduct.isDeadlock(e)) {
      if (deadlockCnt > 0) {
        long waitInterval = deadlockRetryInterval * (ALLOWED_REPEATED_DEADLOCKS - deadlockCnt);
        LOG.warn("Deadlock detected in {}. Will wait {} ms try again up to {} times.", caller, waitInterval, deadlockCnt);
        // Pause for a just a bit for retrying to avoid immediately jumping back into the deadlock.
        try {
          Thread.sleep(waitInterval);
        } catch (InterruptedException ie) {
          // NOP
        }
        return true;
      } else {
        LOG.error("Too many repeated deadlocks in {}, giving up.", caller);
      }
    }
    return false;
  }
  
  /**
   * Determine if an exception was such that it makes sense to retry.  Unfortunately there is no standard way to do
   * this, so we have to inspect the error messages and catch the telltale signs for each
   * different database.
   * if the error is retry-able.
   * @param e exception that was thrown.
   * @param caller name of the method calling this (and other info useful to log)
   * @param retryOnDuplicateKey whether to retry on unique key constraint violation
   * @return True if retry is possible, false otherwise.
   */
  private boolean checkRetryable(SQLException e, String caller, boolean retryOnDuplicateKey, int retryCount) {

    LOG.error("Execution error, checking if retry is possible", e);
    
    // If you change this function, remove the @Ignore from TestTxnHandler.deadlockIsDetected()
    // to test these changes.
    // MySQL and MSSQL use 40001 as the state code for rollback.  Postgres uses 40001 and 40P01.
    // Oracle seems to return different SQLStates and messages each time,
    // so I've tried to capture the different error messages (there appear to be fewer different
    // error messages than SQL states).
    // Derby and newer MySQL driver use the new SQLTransactionRollbackException
    boolean retry = false;
    if (isRetryable(conf, e)) {
      //in MSSQL this means Communication Link Failure
      retry = waitForRetry(caller, e.getMessage(), retryCount);
    } else if (retryOnDuplicateKey && dbProduct.isDuplicateKeyError(e)) {
      retry = waitForRetry(caller, e.getMessage(), retryCount);
    } else {
      //make sure we know we saw an error that we don't recognize
      LOG.info("Non-retryable error in {} : {}", caller, getMessage(e));
    }
    return retry;
  }

  private boolean waitForRetry(String caller, String errMsg, int retryCount) {
    if (retryCount > 0) {
      LOG.warn("Retryable error detected in {}. Will wait {} ms and retry up to {} times. Error: {}", caller,
          retryInterval, retryCount, errMsg);
      try {
        Thread.sleep(retryInterval);
      } catch (InterruptedException ex) {
        //
      }
      return true;
    } else {
      LOG.error("Fatal error in {}. Retry limit ({}) reached. Last error: {}", caller, retryLimit, errMsg);
    }
    return false;
  }

  /**
   * Returns true if {@code ex} should be retried
   */
  static boolean isRetryable(Configuration conf, Exception ex) {
    if(ex instanceof SQLException) {
      SQLException sqlException = (SQLException)ex;
      if (MANUAL_RETRY.equalsIgnoreCase(sqlException.getSQLState())) {
        // Manual retry exception was thrown
        return true;
      }
      if ("08S01".equalsIgnoreCase(sqlException.getSQLState())) {
        //in MSSQL this means Communication Link Failure
        return true;
      }
      if ("ORA-08176".equalsIgnoreCase(sqlException.getSQLState()) ||
          sqlException.getMessage().contains("consistent read failure; rollback data not available")) {
        return true;
      }

      String regex = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.TXN_RETRYABLE_SQLEX_REGEX);
      if (regex != null && !regex.isEmpty()) {
        String[] patterns = regex.split(",(?=\\S)");
        String message = getMessage(ex);
        for (String p : patterns) {
          if (Pattern.matches(p, message)) {
            return true;
          }
        }
      }
      //see also https://issues.apache.org/jira/browse/HIVE-9938
    }
    return false;
  }

  /**
   * lockInternal() and {@link #unlockInternal()} are used to serialize those operations that require
   * Select ... For Update to sequence operations properly.  In practice that means when running
   * with Derby database.  See more notes at class level.
   */
  private void lockInternal() {
    if(dbProduct.isDERBY()) {
      derbyLock.lock();
    }
  }
  private void unlockInternal() {
    if(dbProduct.isDERBY()) {
      derbyLock.unlock();
    }
  }
  
  private static class RetryState {
    private final TransactionStatus status;
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public RetryState(TransactionStatus status, NamedParameterJdbcTemplate jdbcTemplate) {
      this.status = status;
      this.jdbcTemplate = jdbcTemplate;
    }
  }

}
