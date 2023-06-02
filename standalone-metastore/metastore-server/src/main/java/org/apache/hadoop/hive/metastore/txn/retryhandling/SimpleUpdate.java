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
import org.springframework.transaction.TransactionStatus;

import java.sql.SQLException;

/**
 * A built-in implementation of the {@link TransactionalVoidFunction} interface, responsible for executing a statement and
 * validate the returned affected rows. Internally, it uses a {@link ParameterizedCommand} instance: 
 * <ul>
 *   <li>Executes the {@link ParameterizedQuery} in it, </li>
 *   <li>and then uses the {@link ParameterizedCommand#resultPolicy()} to validate the number of affected rows: If the 
 *   policy returns true, the number of affected rows is accepted, otherwise an {@link SQLException} is thrown.</li>
 * </ul> 
 * This class is capable of executing multiple {@link ParameterizedCommand} instances in a loop. In this case the 
 * {@link ParameterizedCommand#resultPolicy()} called for every individual {@link ParameterizedCommand} result, not for 
 * the sum of their affected rows.
 */
public class SimpleUpdate implements TransactionalFunction<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleUpdate.class);
  
  private final ParameterizedCommand[] commands;
  private final DatabaseProduct databaseProduct;

  /**
   * Executes one or more {@link NamedParameterJdbcTemplate#update(String, org.springframework.jdbc.core.namedparam.SqlParameterSource)} 
   * calls using the query string and parameters obtained from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} and 
   * {@link ParameterizedCommand#getQueryParameters()} methods. Validates the resulted number of affected rows using the 
   * {@link ParameterizedCommand#resultPolicy()} function.
   * @param status A {@link TransactionStatus} instance which represents the database transaction. The implementing 
   *               funtion can use it to manage the transaction programatically: Rollback, create/delete savepoint, etc.
   * @param jdbcTemplate A {@link NamedParameterJdbcTemplate} instance which can be used to execute the SQL statements
   * @return Returns the total number of affected rows: If multiple {@link ParameterizedCommand} instances were passed in
   * the constructor, the returned number is the sum of the number of affected rows of all the update calls.
   * @throws SQLException Thrown if the update count was rejected by the {@link ParameterizedCommand#resultPolicy()} method.
   * @throws MetaException Forwarded from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)}.
   */
  @Override
  public Integer call(TransactionStatus status, NamedParameterJdbcTemplate jdbcTemplate) throws SQLException, MetaException {
    int total = 0;
    for(ParameterizedCommand command : commands) {
      String queryStr = command.getParameterizedQueryString(databaseProduct);
      LOG.debug("Going to execute command <{}>", queryStr);

      int count = jdbcTemplate.update(queryStr, command.getQueryParameters());
      if (command.resultPolicy() != null && !command.resultPolicy().apply(count)) {
        LOG.error("The update count was " + count + " which is not the expected. Rolling back.");
        throw new SQLException("The update count was " + count + " which is not the expected. Rolling back.");
      }
      LOG.debug("Command <{}> updated {} records.", queryStr, count);
      total += count;
    }
    return total;
  }

  /**
   * Creates a new instance of the {@link SimpleUpdate} class
   * @param databaseProduct A {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   * @param command One or more {@link ParameterizedCommand} instances representing the SQL commands to execute.
   */
  public SimpleUpdate(DatabaseProduct databaseProduct, ParameterizedCommand... command) {
    this.commands = command;
    this.databaseProduct = databaseProduct;
  }
  
}