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
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.Comparator;

/**
 * Represents a query with an IN() clause. The values inside the IN() clause are passed separately
 * @param <T>
 */
public class InClauseBatchCommand<T> implements ParameterizedQuery {

  
  private final String query;
  private final SqlParameterSource queryParameters;
  private final String inClauseParameterName;
  private final Comparator<T> parmeterLengthComparator;
  

  public InClauseBatchCommand(String query, SqlParameterSource queryParameters, 
                              String inClauseParameterName, Comparator<T> parmeterLengthComparator) {
    this.query = query;
    this.queryParameters = queryParameters;
    this.inClauseParameterName = inClauseParameterName;
    this.parmeterLengthComparator = parmeterLengthComparator;
  }

  /**
   * The parameterized query string. The query must have exactly parameter inside the IN clause, and can have zero or 
   * more parameters everywhere else in the query string.
   * @see ParameterizedQuery#getParameterizedQueryString(DatabaseProduct) 
   */
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
    return query;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return queryParameters;
  }

  /**
   * @return Returns with the name of the parameter which is inside the IN clause.
   */
  public String getInClauseParameterName() {
    return inClauseParameterName;
  }

  /**
   * @return Returns a {@link Comparator} instance which can be used to determine the longest element in the 
   * list IN clause parameters. This is required to be able to estimate the final legth of the command.
   */
  public Comparator<T> getParameterLengthComparator() {
    return parmeterLengthComparator;
  }
  
}
