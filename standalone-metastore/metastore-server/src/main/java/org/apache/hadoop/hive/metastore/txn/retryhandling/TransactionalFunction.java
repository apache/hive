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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.TransactionStatus;

import java.sql.SQLException;

/**
 * A functional interface representing a function call (typically a query) which has a result and done within a database transaction.
 * @param <Result> The type of the function call result
 */
@FunctionalInterface
public interface TransactionalFunction<Result> {

  /**
   * Implementations typically should execute database calls inside, and handle the transaction (through the passed
   * {@link TransactionStatus} interface) accordingly.
   * @param status A {@link TransactionStatus} instance which represents the database transaction. The implementing 
   *               funtion can use it to manage the transaction programatically: Rollback, create/delete savepoint, etc.
   * @param jdbcTemplate A {@link NamedParameterJdbcTemplate} instance which can be used to execute the SQL statements
   * @return Returns with the result of the function call. 
   * @throws SQLException Thrown if any of the JDBC calls fail
   * @throws MetaException Thrown in case of application error within the function
   */
  Result call(TransactionStatus status, NamedParameterJdbcTemplate jdbcTemplate) throws SQLException, MetaException;

}
