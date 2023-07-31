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

import java.sql.ResultSet;

/**
 * A simple combination of the {@link ParameterizedQuery} and a {@link ResultSetExtractor}.
 * @param <Result> The type of the result 
 */
public abstract class QueryHandler<Result> extends ParameterizedQuery implements ResultSetExtractor<Result> {
  
  private static final Logger LOG = LoggerFactory.getLogger(QueryHandler.class);

  /**
   * Executes a {@link NamedParameterJdbcTemplate#query(String, SqlParameterSource, ResultSetExtractor)} call using the query 
   * string and parameters obtained from {@link QueryHandler#getParameterizedQueryString(DatabaseProduct)} and 
   * {@link QueryHandler#getQueryParameters()} methods. Processes the result using the {@link QueryHandler#extractData(ResultSet)}
   * method ({@link QueryHandler} extends the {@link ResultSetExtractor} interface).
   * @param dataSourceWrapper A {@link DataSourceWrapper} instance responsible for providing all the necessary resources 
   *                          to be able to perform transactional database calls.
   * @return Returns with the object(s) constructed from the result of the executed query. 
   * @throws MetaException Forwarded from {@link ParameterizedCommand#getParameterizedQueryString(DatabaseProduct)}.
   */  
  public Result execute(DataSourceWrapper dataSourceWrapper) throws MetaException {
    String queryStr = getParameterizedQueryString(dataSourceWrapper.getDatabaseProduct());
    LOG.debug("Going to execute query <{}>", queryStr);
    SqlParameterSource params = getQueryParameters();
    if (params != null) {
      return dataSourceWrapper.getJdbcTemplate().query(queryStr, params, this);
    } else {
      return dataSourceWrapper.getJdbcTemplate().query(queryStr, this);
    }
  }
  
}
