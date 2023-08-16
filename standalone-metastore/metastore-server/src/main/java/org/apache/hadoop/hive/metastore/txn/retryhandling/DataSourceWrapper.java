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

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Wraps multiple {@link DataSource}s into a single object and offers transaction management functionality. 
 * Allows access of the {@link NamedParameterJdbcTemplate}, {@link Connection} objects associated with the wrapped datasources.
 */
public class DataSourceWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(DataSourceWrapper.class);
  private static final ThreadLocal<RetryContext> threadLocal = new ThreadLocal<>();

  private final Map<String, DataSource> dataSources;
  private final Map<String, PlatformTransactionManager> transactionManagers = new HashMap<>();
  private final Map<String, NamedParameterJdbcTemplate> jdbcTemplates = new HashMap<>();
  private final DatabaseProduct databaseProduct;

  /**
   * Creates a new isntance of the {@link DataSourceWrapper} class
   * @param dataSources A {@link Map} of the datasource names and the corresponding datasources which needs to be wrapped
   *                    by this instance
   * @param databaseProduct A {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   */
  public DataSourceWrapper(Map<String, DataSource> dataSources, DatabaseProduct databaseProduct) {
    this.dataSources = dataSources;
    for(String dataSource : dataSources.keySet()) {
      NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSources.get(dataSource));
      jdbcTemplates.put(dataSource, jdbcTemplate);
      transactionManagers.put(dataSource, new DataSourceTransactionManager(Objects.requireNonNull(jdbcTemplate.getJdbcTemplate().getDataSource())));
    }
    this.databaseProduct = databaseProduct;
  }


  /**
   * Provides direct access to the {@link TransactionStatus} to allow manual savepoint creation, rollback, etc.
   * Can be called only within an established {@link RetryContext}.
   * @return Returns the {@link TransactionStatus} instance stored in the {@link RetryContext}.
   * @throws IllegalStateException Thrown when called outside a {@link RetryContext}.
   */
  public TransactionStatus getTransactionStatus() {
    return RetryContext.getRetryContext().transactionStatus;
  }

  /**
   * @return Returns true if the current {@link Thread} has an associated {@link RetryContext}, false otherwise.
   */
  boolean hasRetryContext() {
    return threadLocal.get() != null;
  }

  /**
   * @return Returns the {@link NamedParameterJdbcTemplate} associated with the current {@link RetryContext}.
   * Can be called only within an established {@link RetryContext}.
   * @throws IllegalStateException Thrown when called outside a {@link RetryContext}.
   */
  public NamedParameterJdbcTemplate getJdbcTemplate() {
    return jdbcTemplates.get(RetryContext.getRetryContext().datasource);
  }

  /**
   * Gets a connection to the {@link DataSource} associated with the current {@link RetryContext}. Ensures that the same 
   * instance is returned all the time within a particular transaction. Can be called only within an established 
   * {@link RetryContext}. 
   * @return Returns a connection to the datasource.
   * @throws IllegalArgumentException Thrown when called outside a {@link RetryContext}
   */
  public Connection getConnection() {
    return DataSourceUtils.getConnection(dataSources.get(RetryContext.getRetryContext().datasource));
  }

  /**
   * @return Returns a {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   */
  public DatabaseProduct getDatabaseProduct() {
    return databaseProduct;
  }

  /**
   * <p>Establishes a {@link RetryContext} on the current {@link Thread}: Creates a new transaction with the given 
   * {@link TransactionDefinition} using an internal {@link PlatformTransactionManager} associated with the reffered 
   * {@link DataSource}. Returns a {@link TransactionStatus} representing the created transaction. The returned 
   * {@link TransactionStatus} instance can be used for manual transaction handling.</p> 
   * <br>
   * <p>!!!! Please note that if there already is a {@link RetryContext} established, this call will create a nested</p>
   * {@link RetryContext} (and transaction).</b>  
   * @param definition The {@link TransactionDefinition} to use for creating a new transaction.
   * @param dataSource The identifier of the {@link DataSource} to use
   * @return Returns a {@link TransactionStatus} object representing the created transaction.
   * @throws TransactionException Forwarded from {@link PlatformTransactionManager#getTransaction(TransactionDefinition)}
   */
  public TransactionStatus getTransactionWithinRetryContext(TransactionDefinition definition, String dataSource) throws TransactionException {
    TransactionStatus status = transactionManagers.get(dataSource).getTransaction(definition);
    RetryContext.setRetryContext(status, dataSource);
    return status;
  }

  /**
   * Commits the transaction associated with the current {@link Thread} and clears the {@link RetryContext}.
   * Can be called only within an established {@link RetryContext}.
   * @throws TransactionException Forwarded from {@link PlatformTransactionManager#commit(TransactionStatus)}
   * @throws IllegalArgumentException Thrown when called outside a {@link RetryContext}
   */
  public void commit() throws TransactionException {
    try {
      RetryContext context = RetryContext.getRetryContext();
      transactionManagers.get(context.datasource).commit(context.transactionStatus);
    } finally {
      RetryContext.clearRetryContext();
    }
  }

  /**
   * Rollbacks the transaction associated with the current {@link Thread} and clears the {@link RetryContext}.
   * Can be called only within an established {@link RetryContext}.
   * @throws TransactionException Forwarded from {@link PlatformTransactionManager#rollback(TransactionStatus)}
   * @throws IllegalArgumentException Thrown when called outside a {@link RetryContext}
   */
  public void rollback() throws TransactionException {
    try {
      RetryContext context = RetryContext.getRetryContext();
      transactionManagers.get(context.datasource).rollback(context.transactionStatus);
    } finally {
      RetryContext.clearRetryContext();
    }
  }

  /**
   * Executes a {@link NamedParameterJdbcTemplate#update(String, org.springframework.jdbc.core.namedparam.SqlParameterSource)}
   * calls using the query string and parameters obtained from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} and
   * {@link ParameterizedCommand#getQueryParameters()} methods. Validates the resulted number of affected rows using the
   * {@link ParameterizedCommand#resultPolicy()} function.
   * @param command The {@link ParameterizedCommand} to execute.
   * @return Returns the number of affected rows.
   * @throws MetaException Forwarded from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} or
   *                       thrown if the update count was rejected by the {@link ParameterizedCommand#resultPolicy()} method
   */
  public Integer execute(ParameterizedCommand command) throws MetaException {
    return execute(command.getParameterizedQueryString(getDatabaseProduct()),
        command.getQueryParameters(), command.resultPolicy());
  }

  /**
   * Executes a {@link NamedParameterJdbcTemplate#update(String, org.springframework.jdbc.core.namedparam.SqlParameterSource)}
   * calls using the query string and {@link SqlParameterSource}. Validates the resulted number of affected rows using the
   * resultpolicy function.
   * @param query Parameterized query string.
   * @param params Qyery parameters
   * @param resultPolicy Result policy to use, or null, if no result policy.
   * @return Returns the number of affected rows.
   * @throws MetaException Forwarded from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} or
   *                       thrown if the update count was rejected by the {@link ParameterizedCommand#resultPolicy()} method
   */
  public Integer execute(String query, SqlParameterSource params,
                                Function<Integer, Boolean> resultPolicy) throws MetaException {
    LOG.debug("Going to execute command <{}>", query);
    int count = getJdbcTemplate().update(query, params);
    if (resultPolicy != null && !resultPolicy.apply(count)) {
      LOG.error("The update count was " + count + " which is not the expected. Rolling back.");
      throw new MetaException("The update count was " + count + " which is not the expected. Rolling back.");
    }
    LOG.debug("Command <{}> updated {} records.", query, count);
    return count;
  }

  /**
   * Executes a {@link NamedParameterJdbcTemplate#query(String, SqlParameterSource, ResultSetExtractor)} call using the query 
   * string and parameters obtained from {@link QueryHandler#getParameterizedQueryString(DatabaseProduct)} and 
   * {@link QueryHandler#getQueryParameters()} methods. Processes the result using the {@link QueryHandler#extractData(ResultSet)}
   * method ({@link QueryHandler} extends the {@link ResultSetExtractor} interface).
   * @param queryHandler The {@link QueryHandler} instance containing the query, {@link SqlParameterSource}, and {@link ResultSetExtractor}.
   * @return Returns with the object(s) constructed from the result of the executed query. 
   * @throws MetaException Forwarded from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)}.
   */
  public <Result> Result execute(QueryHandler<Result> queryHandler) throws MetaException {
    String queryStr = queryHandler.getParameterizedQueryString(getDatabaseProduct());
    LOG.debug("Going to execute query <{}>", queryStr);
    SqlParameterSource params = queryHandler.getQueryParameters();
    if (params != null) {
      return getJdbcTemplate().query(queryStr, params, queryHandler);
    } else {
      return getJdbcTemplate().query(queryStr, queryHandler);
    }
  }

  /**
   * Represents a transaciton associated with a {@link DataSource} and a {@link Thread}. Holds the {@link TransactionStatus}
   * representing the transaction, and the name of the {@link DataSource} used to create the transaction. Used for two things:
   * <ul>
   *   <li>
   *     Its presence tells to {@link RetryHandler} that there is no need for new (nested) context, it can use the
   *     existing one
   *   </li>
   *   <li>
   *     The {@link DataSourceWrapper#getTransactionStatus()}, {@link DataSourceWrapper#getConnection()}, 
   *     {@link DataSourceWrapper#getJdbcTemplate()}, {@link DataSourceWrapper#commit()}, {@link DataSourceWrapper#rollback()}
   *     methods are using it to identify the correct datasource which is associated with the current
   *     {@link RetryContext} (and {@link Thread}).
   *   </li>
   * </ul>
   */
  static class RetryContext {

    /**
     * Retrurns the previously established {@link RetryContext}. Can be called only within an established {@link RetryContext}.
     * @return Retruns the previously established {@link RetryContext}.
     * @throws IllegalStateException Thrown when called outside a {@link RetryContext}.
     */
    private static RetryContext getRetryContext() {
      RetryContext context = threadLocal.get();
      if (context == null) {
        throw new IllegalStateException("Trying to access a transactional resource without retry context!");
      }
      return context;
    }
    
    /**
     * Establishes a {@link RetryContext} on the current thread using the given {@link TransactionStatus}. 
     * @param status The {@link TransactionStatus} to bind to the current {@link Thread}.
     * @param datasource The name of the {@link DataSource} used to create the {@link TransactionStatus}.
     */
    private static void setRetryContext(TransactionStatus status, String datasource) {
      RetryContext old = threadLocal.get();
      threadLocal.set(new RetryContext(status, datasource, old));
    }

    /**
     * Clears a previously established {@link RetryContext} on the current {@link Thread}.
     */
    private static void clearRetryContext() {
      RetryContext top = threadLocal.get();
      if(top != null && top.parent != null) {
        threadLocal.set(top.parent);
      } else {
        threadLocal.remove();
      }
    }

    private final RetryContext parent;
    private final TransactionStatus transactionStatus;
    private final String datasource;

    public RetryContext(TransactionStatus transactionStatus, String datasource, RetryContext parent) {
      this.parent = parent;
      this.transactionStatus = transactionStatus;
      this.datasource = datasource;
    }
  }
}
