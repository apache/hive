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
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.util.List;
import java.util.function.Function;

/**
 * Represents a parameterized batch command (for exmaple an UPDATE statement) as a Spring 
 * {@link org.springframework.jdbc.core.JdbcTemplate} style parameterized query string 
 * (for example: <b>UPDATE TBL SET COL1 = ? WHERE ID = ?</b>), its parameters, and a result policy. The result policy 
 * is a <b>Function&lt;Integer, Boolean&gt;</b> function which must decide if the number of 
 * affected rows is acceptable or not. It is called for each result in the batch.
 * <pre></pre>   
 * Please note that for batch command, named parameters cannot be used! 
 * (like <b>UPDATE TBL SET COL1 = :value WHERE ID = :id</b>)
 */
public interface ParameterizedBatchCommand<T> {

  /**
   * The parameterized query string. It is allowed if the query has no parameters at all.
   * @param databaseProduct A {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   * @return Returns the parameterized query string.
   */
  String getParameterizedQueryString(DatabaseProduct databaseProduct);

  /**
   * A {@link List} instance containing the required parameters for the query string.
   */
  List<T> getQueryParameters();

  /**
   * Implementations must return a {@link ParameterizedPreparedStatementSetter} instance which will be 
   * responsible for setting the parameter values for all the items in the batch 
   */
  ParameterizedPreparedStatementSetter<T> getPreparedStatementSetter();

  /**
   * @return Returns the result policy to be used to validate the number of affected rows.
   */
  Function<Integer, Boolean> resultPolicy();  

}
