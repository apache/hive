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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Wraps multiple {@link DataSource}s into a single object and offers transaction management functionality. 
 * Allows access of the {@link NamedParameterJdbcTemplate}, {@link Connection} objects associated with the wrapped datasources.
 */
public class DataSourceWrapper {

  private static final ThreadLocal<RetryContext> threadLocal = new ThreadLocal<>();

  private final Map<String, DataSource> dataSources;
  private final Map<String, PlatformTransactionManager> transactionManagers = new HashMap<>();
  private final Map<String, NamedParameterJdbcTemplate> jdbcTemplates = new HashMap<>();
  private final DatabaseProduct databaseProduct;

  public TransactionStatus getTransactionStatus() throws MetaException {
    return getRetryContext().transactionStatus;
  }

  /**
   * Retrurns the previously established {@link RetryContext}. Can be called only within an established {@link RetryContext}.
   * @return Retruns the previously established {@link RetryContext}.
   * @throws IllegalStateException Thrown when called outside a {@link RetryContext}.
   */
  RetryContext getRetryContext() {
    RetryContext context = threadLocal.get();
    if (context == null) {
      throw new IllegalStateException("Trying to access TransactionStatus without retry context!");
    }
    return context;    
  }

  /**
   * Establishes a {@link RetryContext} on the current thread using the given {@link TransactionStatus}. 
   * @param status The {@link TransactionStatus} to bind to the current {@link Thread}.
   * @param datasource The name of the {@link DataSource} used to create the {@link TransactionStatus}.
   */
  void setRetryContext(TransactionStatus status, String datasource) {
    threadLocal.set(new RetryContext(status, datasource));
  }

  /**
   * Clears a previously established {@link RetryContext} on the current {@link Thread}.
   */
  void clearRetryContext() {
    threadLocal.remove();
  }

  /**
   * @return Returns true if the current {@link Thread} has an associated {@link RetryContext}, false otherwise.
   */
  boolean hasRetryContext() {
    return threadLocal.get() != null;
  }

  /**
   * Creates a new {@link TransactionTemplate} using an internal {@link PlatformTransactionManager} associated with the 
   * reffered {@link DataSource}. The returned template can be used to execute a 
   * {@link org.springframework.transaction.support.TransactionCallback} function within a transaction.
   * @param dataSource The identifier of the {@link DataSource} to use
   * @return Returns with the created {@link TransactionTemplate}.
   */
  TransactionTemplate getTransactionTemplate(String dataSource) {
    return new TransactionTemplate(transactionManagers.get(dataSource));
  }
  
  /**
   * @return Returns the {@link NamedParameterJdbcTemplate} associated with the current {@link RetryContext}.
   * Can be called only within an established {@link RetryContext}.
   * @throws IllegalStateException Thrown when called outside a {@link RetryContext}.
   */
  public NamedParameterJdbcTemplate getJdbcTemplate() {
    return jdbcTemplates.get(getRetryContext().datasource);
  }

  /**
   * Gets a connection to the {@link DataSource} associated with the current {@link RetryContext}. Ensures that the same 
   * instance is returned all the time within a particular transaction. Can be called only within an established 
   * {@link RetryContext}. 
   * @return Returns a connection to the datasource.
   * @throws IllegalArgumentException Thrown when called outside a {@link RetryContext}
   */
  public Connection getConnection() {
    return DataSourceUtils.getConnection(dataSources.get(getRetryContext().datasource));
  }

  /**
   * @return Returns a {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   */
  public DatabaseProduct getDatabaseProduct() {
    return databaseProduct;
  }

  /**
   * Establishes a {@link RetryContext} on the current {@link Thread}: Creates a new transaction with the given 
   * {@link TransactionDefinition} using an internal {@link PlatformTransactionManager} associated with the reffered 
   * {@link DataSource}. Returns a {@link TransactionStatus} representing the created transaction. The returned 
   * {@link TransactionStatus} instance can be used for manual transaction handling.
   * @param definition The {@link TransactionDefinition} to use for creating a new transaction.
   * @param dataSource The identifier of the {@link DataSource} to use
   * @return Returns a {@link TransactionStatus} object representing the created transaction.
   * @throws TransactionException Forwarded from {@link PlatformTransactionManager#getTransaction(TransactionDefinition)}
   */
  public TransactionStatus getTransactionWithinRetryContext(TransactionDefinition definition, String dataSource) throws TransactionException {
    TransactionStatus status = transactionManagers.get(dataSource).getTransaction(definition);
    setRetryContext(status, dataSource);
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
      RetryContext context = getRetryContext();
      transactionManagers.get(context.datasource).commit(context.transactionStatus);
    } finally {
      clearRetryContext();
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
      RetryContext context = getRetryContext();
      transactionManagers.get(context.datasource).rollback(context.transactionStatus);
    } finally {
      clearRetryContext();
    }
  }

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
   * Represents a transaciton associated with a {@link DataSource} and a {@link Thread}. Holds the {@link TransactionStatus} 
   * representing the transaction, and the name of the {@link DataSource} used to create the transaction. Used for two things:
   * <ul>
   *   <li>
   *     Its presence tells to {@link RetryHandler} that there is no need for new (nested) context, it can use the 
   *     existing one
   *   </li>
   *   <li>
   *     The {@link #getTransactionStatus()}, {@link #getConnection()}, {@link #getJdbcTemplate()}, {@link #commit()},
   *     {@link #rollback()} methods are using it to identify the correct instance which is associated with the current 
   *     {@link RetryContext} (and {@link Thread}).    
   *   </li>
   * </ul>
   */
  static class RetryContext {
    
    private final TransactionStatus transactionStatus;
    private final String datasource;

    public RetryContext(TransactionStatus transactionStatus, String datasource) {
      this.transactionStatus = transactionStatus;
      this.datasource = datasource;
    }
  }
}
