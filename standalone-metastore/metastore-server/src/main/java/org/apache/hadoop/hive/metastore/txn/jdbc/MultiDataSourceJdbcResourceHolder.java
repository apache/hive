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
package org.apache.hadoop.hive.metastore.txn.jdbc;

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
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Wraps multiple {@link DataSource}s into a single object and offers JDBC related resources. 
 * Allows access of the {@link NamedParameterJdbcTemplate}, {@link PlatformTransactionManager}, {@link Connection} 
 * objects associated with the wrapped datasources.
 */
public class MultiDataSourceJdbcResourceHolder {

  private static final Logger LOG = LoggerFactory.getLogger(MultiDataSourceJdbcResourceHolder.class);

  private final ThreadLocal<String> threadLocal = new ThreadLocal<>();

  private final Map<String, DataSource> dataSources = new HashMap<>();
  private final Map<String, PlatformTransactionManager> transactionManagers = new HashMap<>();
  private final Map<String, NamedParameterJdbcTemplate> jdbcTemplates = new HashMap<>();
  private final DatabaseProduct databaseProduct;

  /**
   * Creates a new instance of the {@link MultiDataSourceJdbcResourceHolder} class
   * @param databaseProduct A {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   */
  public MultiDataSourceJdbcResourceHolder(DatabaseProduct databaseProduct) {
    this.databaseProduct = databaseProduct;
  }

  /**
   * Adds an additional {@link DataSource} to this instance, specified by its name.
   *
   * @param dataSourceName The name of the {@link DataSource} to add.
   * @param dataSource     The {@link DataSource} to add.
   */
  public void registerDataSource(String dataSourceName, DataSource dataSource) {
    dataSources.put(dataSourceName, dataSource);
    jdbcTemplates.put(dataSourceName, new NamedParameterJdbcTemplate(dataSource));
    transactionManagers.put(dataSourceName, new DataSourceTransactionManager(dataSource));
  }

  private String getDataSourceName() {
    String dataSourceName = threadLocal.get();
    if (dataSourceName == null) {
      throw new IllegalStateException("In order to access the JDBC resources, first you need to obtain a transaction " +
          "using getTransaction(int propagation, String dataSourceName)!");
    }
    return dataSourceName;
  }

  /**
   * Begins a new transaction or returns an existing, depending on the passed Transaction Propagation.
   * The created transaction is wrapped into a {@link TransactionWrapper} which is {@link AutoCloseable} and allows using
   * the wrapper inside a try-with-resources block. Other methods like {@link #getConnection()}, {@link #getJdbcTemplate()},
   * {@link #getTransactionManager()}, {@link #execute(ParameterizedCommand)}, {@link #execute(String, SqlParameterSource, Function)},
   * {@link #execute(QueryHandler)} can be called only after calling this method, and before closing the wrapper.
   * @param propagation The transaction propagation to use.
   * @param dataSourceName The name of the {@link DataSource} to use for creating the transaction.
   * @return
   */
  public TransactionWrapper getTransaction(int propagation, String dataSourceName) {
    try {
      threadLocal.set(dataSourceName);
      return new TransactionWrapper(
          getTransactionManager().getTransaction(new DefaultTransactionDefinition(propagation)));
    } catch (Exception e) {
      threadLocal.remove();
      throw e;
    }
  }

  /**
   * @return Returns the {@link NamedParameterJdbcTemplate} associated with the current {@link TransactionWrapper}.
   * Can be called only within an established {@link TransactionWrapper}.
   * @throws IllegalStateException Thrown when called outside a {@link TransactionWrapper}.
   */
  public NamedParameterJdbcTemplate getJdbcTemplate() {
    return jdbcTemplates.get(getDataSourceName());
  }

  /**
   * Gets a connection to the {@link DataSource} associated with the current {@link TransactionWrapper}. Ensures that the same
   * instance is returned all the time within a particular transaction. Can be called only within an established
   * {@link TransactionWrapper}.
   *
   * @return Returns a connection to the datasource.
   * @throws IllegalArgumentException Thrown when called outside a {@link TransactionWrapper}
   */
  public Connection getConnection() {
    return DataSourceUtils.getConnection(dataSources.get(getDataSourceName()));
  }

  /**
   * Gets the {@link PlatformTransactionManager} associated with the current {@link TransactionWrapper}. Ensures that the same
   * instance is returned all the time within a particular transaction. Can be called only within an established
   * {@link TransactionWrapper}.
   * @return
   */
  public PlatformTransactionManager getTransactionManager() {
    return transactionManagers.get(getDataSourceName());
  }

  /**
   * @return Returns a {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   */
  public DatabaseProduct getDatabaseProduct() {
    return databaseProduct;
  }

  /**
   * Executes a {@link NamedParameterJdbcTemplate#update(String, org.springframework.jdbc.core.namedparam.SqlParameterSource)}
   * calls using the query string and parameters obtained from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} and
   * {@link ParameterizedCommand#getQueryParameters()} methods. Validates the resulted number of affected rows using the
   * {@link ParameterizedCommand#resultPolicy()} function.
   *
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
   *
   * @param query        Parameterized query string.
   * @param params       Qyery parameters
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
   *
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
   * Wraps the {@link TransactionStatus} object into an {@link AutoCloseable} object to allow using it in 
   * try-with-resources block. If the {@link TransactionStatus#isCompleted()} is false upon exiting the try block, 
   * the transaction will rolled back, otherwise nothing happens.
   * <br/><br/><b>In other words:</b> This wrapper automatically rolls back uncommitted transactions, but the commit
   * needs to be done manually using {@link PlatformTransactionManager#commit(TransactionStatus)} the following way:
   * {@code jdbcResourceHolder.getTransactionManager().commit(wrapper.getTxn());}
   */
  public class TransactionWrapper implements AutoCloseable {

    private final TransactionStatus txn;

    TransactionWrapper(TransactionStatus txn) {
      this.txn = txn;
    }

    /**
     * @return Returns the {@link TransactionStatus} instance wrapped by this object.
     */
    public TransactionStatus getTxn() {
      return txn;
    }

    /**
     * @see TransactionWrapper TransactionWrapper class level javadoc.
     */
    @Override
    public void close() {
      try {
        if (!txn.isCompleted()) {
          getTransactionManager().rollback(this.txn);
        }
      } finally {
        threadLocal.remove();
      }
    }
  }

}
