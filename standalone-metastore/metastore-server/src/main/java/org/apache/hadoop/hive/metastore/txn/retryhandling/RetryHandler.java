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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.support.DefaultTransactionDefinition;

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


  private final DataSourceWrapper jdbcTemplate;
  private final long deadlockRetryInterval;
  private final long retryInterval;
  private final int retryLimit;
  
  private Configuration conf;
  
  public static String getMessage(Exception ex) {
    StringBuilder sb = new StringBuilder();
    sb.append(ex.getMessage());
    if (ex instanceof SQLException) {
      SQLException sqlEx = (SQLException)ex; 
      sb.append(" (SQLState=").append(sqlEx.getSQLState()).append(", ErrorCode=").append(sqlEx.getErrorCode()).append(")");
    }
    return sb.toString();
  }
  
  public RetryHandler(Configuration conf, DataSourceWrapper jdbcTemplate){
    this.conf = conf;
    this.jdbcTemplate = jdbcTemplate;

    retryInterval = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.HMS_HANDLER_INTERVAL,
        TimeUnit.MILLISECONDS);
    retryLimit = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.HMS_HANDLER_ATTEMPTS);
    deadlockRetryInterval = retryInterval / 10;
  }

  /**
   * Establishes a {@link DataSourceWrapper.RetryContext} and executes the passed {@link TransactionalFunction}. 
   * Automatically retries the execution in case of failure.
   * @param properties {@link RetryCallProperties} instance used to fine-tune/alter the execution-mechanism.
   * @param function The {@link TransactionalFunction} to execute.
   * @return Returns with the result of the execution of the passed {@link TransactionalFunction}.
   * @param <Result> Type of the result
   * @throws MetaException Thrown in case of execution error.
   */
  public <Result> Result executeWithRetry(RetryCallProperties properties, TransactionalFunction<Result> function) 
      throws MetaException {
    return executeWithRetryInternal(properties, function, retryLimit, ALLOWED_REPEATED_DEADLOCKS);
  }

  /**
   * Executes the passed {@link TransactionalFunction}, without retry in case of failure.
   * @param properties {@link RetryCallProperties} instance used to fine-tune/alter the execution-mechanism.
   * @param function The {@link TransactionalFunction} to execute.
   * @return Returns with the result of the execution of the passed {@link TransactionalFunction}.
   * @param <Result> Type of the result
   * @throws MetaException Thrown in case of execution error.
   */
  public <Result> Result executeWithoutRetry(RetryCallProperties properties, TransactionalFunction<Result> function) 
      throws MetaException {
    return executeWithRetryInternal(properties, function, 0, 0);
  }

  private <Result> Result executeWithRetryInternal(RetryCallProperties properties, TransactionalFunction<Result> function
      , int retryCount, int deadlockCount) throws MetaException {
    Objects.requireNonNull(function, "RetryFunction<Result> cannot be null!");
    Objects.requireNonNull(properties, "RetryCallProperties cannot be null!");

    if (jdbcTemplate.hasRetryContext() && properties.getRetryPropagation().canJoinContext()) {
      /*
        If there is a RetryState in the ThreadLocal and we are allowed to join it, we can skip establishing a nested 
        retry-call (and transaction).
      */
      LOG.info("Already established retry-context detected, current retry-call will join it. The passed CallProperties " +
          "instance will be ignored, using the original one.");
      return function.execute(jdbcTemplate);
    }

    if (!properties.getRetryPropagation().canCreateContext()) {
      throw new MetaException("The current RetryPropagation mode (" + properties.getRetryPropagation().name() +
          ") allows only to join an existing retry-context, but there is no context to join!");
    }

    LOG.debug("Running retry function:" + properties);

    if (org.apache.commons.lang3.StringUtils.isEmpty(properties.getCaller())) {
      properties.withCallerId(function.toString());
    }

    try {
      if (properties.isLockInternally()) {
        lockInternal();
      }
      try {
        DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition(properties.getTransactionPropagationLevel());
        transactionDefinition.setIsolationLevel(properties.getTransactionIsolationLevel());
        jdbcTemplate.beginTransactionWithinRetryContext(transactionDefinition, properties.getDataSource());
        Result result = function.execute(jdbcTemplate);
        LOG.debug("Going to commit transaction");
        jdbcTemplate.commit();
        return result;
      } catch (Exception e) {
        if (properties.isRollbackOnError()) {
          jdbcTemplate.rollback();
        }
        throw e;
      }
    } catch (DataAccessException e) {
      SQLException sqlEx = null;
      if (e.getCause() instanceof SQLException) {
        sqlEx = (SQLException) e.getCause();
      }
      if (sqlEx != null) {
        if (checkDeadlock(sqlEx, properties.getCaller(), deadlockCount)) {
          return executeWithRetryInternal(properties, function, retryCount, --deadlockCount);
        }
        if (checkRetryable(sqlEx, properties.getCaller(), properties.isRetryOnDuplicateKey(), retryCount)) {
          return executeWithRetryInternal(properties, function, --retryCount, deadlockCount);
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
    if (jdbcTemplate.getDatabaseProduct().isDeadlock(e)) {
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
    } else if (retryOnDuplicateKey && jdbcTemplate.getDatabaseProduct().isDuplicateKeyError(e)) {
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
    if(jdbcTemplate.getDatabaseProduct().isDERBY()) {
      derbyLock.lock();
    }
  }
  private void unlockInternal() {
    if(jdbcTemplate.getDatabaseProduct().isDERBY()) {
      derbyLock.unlock();
    }
  }

}
