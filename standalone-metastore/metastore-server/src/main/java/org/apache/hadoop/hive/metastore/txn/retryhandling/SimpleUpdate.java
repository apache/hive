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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.function.Function;

/**
 * A built-in implementation of the {@link TransactionalVoidFunction} interface, responsible for executing a statement and
 * validate the returned affected rows. Internally, it uses a {@link ParameterizedCommand} instance: 
 * <ul>
 *   <li>Executes the {@link ParameterizedQuery} in it, </li>
 *   <li>and then uses the {@link ParameterizedCommand#resultPolicy()} to validate the number of affected rows: If the 
 *   policy returns true, the number of affected rows is accepted, otherwise a {@link MetaException} is thrown.</li>
 * </ul> 
 */
public class SimpleUpdate implements TransactionalFunction<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleUpdate.class);
  
  private final ParameterizedCommand command;

  /**
   * Executes a {@link NamedParameterJdbcTemplate#update(String, org.springframework.jdbc.core.namedparam.SqlParameterSource)} 
   * calls using the query string and parameters obtained from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} and 
   * {@link ParameterizedCommand#getQueryParameters()} methods. Validates the resulted number of affected rows using the 
   * {@link ParameterizedCommand#resultPolicy()} function.
   * @param dataSourceWrapper A {@link DataSourceWrapper} instance responsible for providing all the necessary resources 
   *                          to be able to perform transactional database calls.
   * @return Returns the number of affected rows.
   * @throws MetaException Forwarded from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} or
   * thrown if the update count was rejected by the {@link ParameterizedCommand#resultPolicy()} method
   */
  @Override
  public Integer call(DataSourceWrapper dataSourceWrapper) throws MetaException {
    String queryStr = command.getParameterizedQueryString(dataSourceWrapper.getDatabaseProduct());
    LOG.debug("Going to execute command <{}>", queryStr);
    int count = dataSourceWrapper.getJdbcTemplate().update(queryStr, command.getQueryParameters());
    if (command.resultPolicy() != null && !command.resultPolicy().apply(count)) {
      LOG.error("The update count was " + count + " which is not the expected. Rolling back.");
      throw new MetaException("The update count was " + count + " which is not the expected. Rolling back.");
    }
    LOG.debug("Command <{}> updated {} records.", queryStr, count);
    return count;
  }

  /**
   * Creates a new instance of the {@link SimpleUpdate} class
   * @param command A {@link ParameterizedCommand} instance representing the SQL command (and its parameters) to execute.
   */
  public SimpleUpdate(ParameterizedCommand command) {
    this.command = command;
  }

  /**
   * Creates a new instance of the {@link SimpleUpdate} class. Internally creates a {@link ParameterizedCommand} from the
   * arguments.
   * @param parameterizedQuery See {@link ParameterizedQuery#getParameterizedQueryString(DatabaseProduct)}
   * @param params See {@link ParameterizedQuery#getQueryParameters()}
   * @param resultPolicy See {@link  ParameterizedCommand#resultPolicy()}
   */
  public SimpleUpdate(String parameterizedQuery, SqlParameterSource params, Function<Integer, Boolean> resultPolicy) {
    this(new SimpleParameterizedCommand(parameterizedQuery, params, resultPolicy));
  }
  
}