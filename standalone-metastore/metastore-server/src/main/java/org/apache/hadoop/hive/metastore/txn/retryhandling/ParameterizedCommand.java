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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.function.Function;

/**
 * Represents a parameterized command (for exmaple an UPDATE statement) as a Spring {@link NamedParameterJdbcTemplate}
 * style parameterized query string (for example: <b>SELECT * FROM TBL WHERE ID = :id</b>), its parameters, and a result
 * policy. The result policy is a <b>Function&lt;Integer, Boolean&gt;</b> function which must decide if the number of 
 * affected rows is acceptable or not.
 */
public abstract class ParameterizedCommand extends ParameterizedQuery {

  private static final Logger LOG = LoggerFactory.getLogger(ParameterizedCommand.class);

  /**
   * Built-in result policy which returns true only if the number of affected rows is exactly 1.
   */
  public static final Function<Integer, Boolean> EXACTLY_ONE_ROW = (Integer updateCount) -> updateCount == 1;
  /**
   * Built-in result policy which returns true only if the number of affected rows is 1 or greater.
   */
  public static final Function<Integer, Boolean> AT_LEAST_ONE_ROW = (Integer updateCount) -> updateCount >= 1;

  /**
   * @return Returns the result policy to be used to validate the number of affected rows.
   */
  protected abstract Function<Integer, Boolean> resultPolicy();

  /**
   * Executes a {@link NamedParameterJdbcTemplate#update(String, org.springframework.jdbc.core.namedparam.SqlParameterSource)}
   * calls using the query string and parameters obtained from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} and
   * {@link ParameterizedCommand#getQueryParameters()} methods. Validates the resulted number of affected rows using the
   * {@link ParameterizedCommand#resultPolicy()} function.
   *
   * @param dataSourceWrapper A {@link DataSourceWrapper} instance responsible for providing all the necessary resources
   *                          to be able to perform transactional database calls.
   * @return Returns the number of affected rows.
   * @throws MetaException Forwarded from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)} or
   *                       thrown if the update count was rejected by the {@link ParameterizedCommand#resultPolicy()} method
   */
  public Integer execute(DataSourceWrapper dataSourceWrapper) throws MetaException {
    return execute(dataSourceWrapper, getParameterizedQueryString(dataSourceWrapper.getDatabaseProduct()), 
        getQueryParameters(), resultPolicy());
  }

  public static Integer execute(DataSourceWrapper dataSourceWrapper, String query, SqlParameterSource params,
                         Function<Integer, Boolean> resultPolicy) throws MetaException {
    LOG.debug("Going to execute command <{}>", query);
    int count = dataSourceWrapper.getJdbcTemplate().update(query, params);
    if (resultPolicy != null && !resultPolicy.apply(count)) {
      LOG.error("The update count was " + count + " which is not the expected. Rolling back.");
      throw new MetaException("The update count was " + count + " which is not the expected. Rolling back.");
    }
    LOG.debug("Command <{}> updated {} records.", query, count);
    return count;
  }

}