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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.utils.StackThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Holds multiple {@link DataSource}s as a single object and offers JDBC related resources. 
 * Allows access of the {@link NamedParameterJdbcTemplate}, {@link PlatformTransactionManager}, {@link Connection} 
 * objects associated with the registered datasources.
 */
public class MultiDataSourceJdbcResource {

  private static final Logger LOG = LoggerFactory.getLogger(MultiDataSourceJdbcResource.class);

  private final StackThreadLocal<String> threadLocal = new StackThreadLocal<>();

  private final Map<String, DataSource> dataSources = new HashMap<>();
  private final Map<String, TransactionContextManager> transactionManagers = new HashMap<>();
  private final Map<String, NamedParameterJdbcTemplate> jdbcTemplates = new HashMap<>();
  private final DatabaseProduct databaseProduct;
  private final Configuration conf;
  private final SQLGenerator sqlGenerator;

  /**
   * Creates a new instance of the {@link MultiDataSourceJdbcResource} class
   * @param databaseProduct A {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   */
  public MultiDataSourceJdbcResource(DatabaseProduct databaseProduct, Configuration conf, SQLGenerator sqlGenerator) {
    this.databaseProduct = databaseProduct;
    this.conf = conf;
    this.sqlGenerator = sqlGenerator;
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
    transactionManagers.put(dataSourceName, new TransactionContextManager(new DataSourceTransactionManager(dataSource)));
  }

  /**
   * Binds the current {@link Thread} to {@link DataSource} identified by the <b>dataSourceName</b> parameter.
   * Other methods like {@link #getConnection()}, {@link #getJdbcTemplate()}, {@link #execute(ParameterizedCommand)}, 
   * {@link #execute(String, SqlParameterSource, Function)}, {@link #execute(QueryHandler)} can be called only after 
   * calling this method, and before calling {@link #unbindDataSource()}.
   * @param dataSourceName The name of the {@link DataSource} bind to the current {@link Thread}.
   */
  public void bindDataSource(String dataSourceName) {
    threadLocal.set(dataSourceName);
  }
  
  public void bindDataSource(Transactional transactional) {
    threadLocal.set(transactional.value());
  }

  /**
   * Removes the binding between the current {@link Thread} and the {@link DataSource}. After calling this method, 
   * Other methods like {@link #getConnection()}, {@link #getJdbcTemplate()},
   * {@link #execute(ParameterizedCommand)}, {@link #execute(String, SqlParameterSource, Function)},
   * {@link #execute(QueryHandler)} will throw {@link IllegalStateException} because it cannot be determined for which
   * {@link DataSource} the JDBC resources should be returned.
   */
  public void unbindDataSource() {
    threadLocal.unset();
  }

  /**
   * @return Returns the {@link Configuration}  object used to create this {@link MultiDataSourceJdbcResource} instance.
   */
  public Configuration getConf() {
    return conf;
  }
  
  public SQLGenerator getSqlGenerator() {
    return sqlGenerator;
  }
  
  /**
   * Returns the {@link NamedParameterJdbcTemplate} associated with the current {@link Thread}.
   * Can be called only after {@link #bindDataSource(String)} and before {@link #unbindDataSource()}.
   * Ensures that the same instance is returned all the time.
   * @throws IllegalStateException Thrown when there is no bound {@link DataSource}.
   */
  public NamedParameterJdbcTemplate getJdbcTemplate() {
    return jdbcTemplates.get(getDataSourceName());
  }

  /**
   * Returns the {@link Connection} associated with the current {@link Thread}.
   * Can be called only after {@link #bindDataSource(String)} and before {@link #unbindDataSource()}.
   * Ensures that the same instance is returned all the time.
   * @throws IllegalStateException Thrown when there is no bound {@link DataSource}.
   */
  public Connection getConnection() {
    return DataSourceUtils.getConnection(dataSources.get(getDataSourceName()));
  }

  /**
   * Returns the {@link TransactionContextManager} associated with the current {@link Thread}.
   * Can be called only after {@link #bindDataSource(String)} and before {@link #unbindDataSource()}.
   * Ensures that the same instance is returned all the time.
   * @throws IllegalStateException Thrown when there is no bound {@link DataSource}.
   */
  public TransactionContextManager getTransactionManager() {
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
    if (!shouldExecute(command)) {
      return null;
    }
     try {
       return execute(command.getParameterizedQueryString(getDatabaseProduct()),
           command.getQueryParameters(), command.resultPolicy());
     } catch (Exception e) {
       handleError(command, e);
       throw e;
     }
  }

  /**
   * Executes a {@link org.springframework.jdbc.core.JdbcTemplate#batchUpdate(String, Collection, int, ParameterizedPreparedStatementSetter)} 
   * call using the query string obtained from {@link ParameterizedBatchCommand#getParameterizedQueryString(DatabaseProduct)},
   * the parameters obtained from {@link ParameterizedBatchCommand#getQueryParameters()}, and the
   * {@link org.springframework.jdbc.core.PreparedStatementSetter} obtained from 
   * {@link ParameterizedBatchCommand#getPreparedStatementSetter()} methods. The batchSize is coming fomr the 
   * {@link Configuration} object. After the execution, this method validates the resulted number of affected rows using the
   * {@link ParameterizedBatchCommand#resultPolicy()} function for each element in the batch.
   *
   * @param command The {@link ParameterizedBatchCommand} to execute.
   * @return Returns an integer array,containing the number of affected rows for each element in the batch.
   */
  public <T> int execute(ParameterizedBatchCommand<T> command) throws MetaException {
    if (!shouldExecute(command)) {
      return 0;
    }
    try {      
      int maxBatchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.JDBC_MAX_BATCH_SIZE);
      int[][] result = getJdbcTemplate().getJdbcTemplate().batchUpdate(
          command.getParameterizedQueryString(databaseProduct),
          command.getQueryParameters(),
          maxBatchSize,
          command.getPreparedStatementSetter()
      );
      
      Function<Integer, Boolean> resultPolicy = command.resultPolicy();
      if (resultPolicy != null && !Arrays.stream(result).allMatch(inner -> Arrays.stream(inner).allMatch(resultPolicy::apply))) {
        LOG.error("The update count was rejected in at least one of the result array. Rolling back.");
        throw new MetaException("The update count was rejected in at least one of the result array. Rolling back.");        
      }
      return Arrays.stream(result).reduce(0, (acc, i) -> acc + Arrays.stream(i).sum(), Integer::sum);      
    } catch (Exception e) {
      handleError(command, e);
      throw e;
    }
  }

  /**
   * Executes the passed {@link InClauseBatchCommand}. It estimates the length of the query and if it exceeds the limit
   * set in {@link org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars#DIRECT_SQL_MAX_QUERY_LENGTH}, or the 
   * number of elements in the IN() clause exceeds 
   * {@link org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars#DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE}, the query 
   * will be split to multiple queries.
   * @param command The {@link InClauseBatchCommand} to execute
   * @return Returns with the number of affected rows in total.
   * @param <T> The type of the elements in the IN() clause
   * @throws MetaException If {@link InClauseBatchCommand#getInClauseParameterName()} is blank, or the value of the 
   * IN() clause parameter in {@link InClauseBatchCommand#getQueryParameters()} is not exist or not an instance of List&lt;T&gt; 
   */
  public <T> int execute(InClauseBatchCommand<T> command) throws MetaException {
    if (!shouldExecute(command)) {
      return -1;
    }
    
    List<T> elements;    
    try {
      if (StringUtils.isBlank(command.getInClauseParameterName())) {
        throw new MetaException("The IN() clause parameter name (InClauseBatchCommand.getInClauseParameterName() " +
            "cannot be blank!");
      }
      try {
        //noinspection unchecked
        elements = (List<T>) command.getQueryParameters().getValue(command.getInClauseParameterName());
      } catch (ClassCastException e) {
        throw new MetaException("The parameter " + command.getInClauseParameterName() + "must be of type List<T>!");
      }
      MapSqlParameterSource params = (MapSqlParameterSource) command.getQueryParameters();
      String query = command.getParameterizedQueryString(databaseProduct);
      if (CollectionUtils.isEmpty(elements)) {
        throw new IllegalArgumentException("The elements list cannot be null or empty! An empty IN clause is invalid!");
      }
      if (!Pattern.compile("IN\\s*\\(\\s*:" + command.getInClauseParameterName() + "\\s*\\)", Pattern.CASE_INSENSITIVE).matcher(query).find()) {
        throw new IllegalArgumentException("The query must contain the IN(:" + command.getInClauseParameterName() + ") clause!");
      }

      int maxQueryLength = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.DIRECT_SQL_MAX_QUERY_LENGTH) * 1024;
      int batchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE);
      // The length of a single element is the string length of the longest element + 2 characters (comma, space) 
      int elementLength = elements.isEmpty() ? 1 : elements
          .stream()
          .max(command.getParameterLengthComparator())
          .orElseThrow(IllegalStateException::new).toString().length() + 2;
      // estimated base query size: query size + the length of all parameters.
      int baseQueryLength = query.length();
      int maxElementsByLength = (maxQueryLength - baseQueryLength) / elementLength;

      int inClauseMaxSize = Math.min(batchSize, maxElementsByLength);

      int fromIndex = 0, totalCount = 0;
      while (fromIndex < elements.size()) {
        int endIndex = Math.min(elements.size(), fromIndex + inClauseMaxSize);
        params.addValue(command.getInClauseParameterName(), elements.subList(fromIndex, endIndex));
        totalCount += getJdbcTemplate().update(query, params);
        fromIndex = endIndex;
      }
      return totalCount;
    } catch (Exception e) {
      handleError(command, e);
      throw e;
    }
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

  private String getDataSourceName() {
    return threadLocal.get();
  }

  private boolean shouldExecute(Object command) {
    return !(command instanceof ConditionalCommand) || ((ConditionalCommand)command).shouldBeUsed(databaseProduct);
  }
  
  private void handleError(Object command, Exception e) {
    if (command instanceof ConditionalCommand) {
      ((ConditionalCommand)command).onError(databaseProduct, e);
    }
  }
  
}
