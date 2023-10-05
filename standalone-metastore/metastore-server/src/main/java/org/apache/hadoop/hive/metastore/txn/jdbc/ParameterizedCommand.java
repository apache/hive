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

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.function.Function;

/**
 * Represents a parameterized command (for exmaple an UPDATE statement) as a Spring {@link NamedParameterJdbcTemplate}
 * style parameterized query string (for example: <b>UPDATE TBL SET COL1 = :value WHERE ID = :id</b>), its parameters, and a result
 * policy. The result policy is a <b>Function&lt;Integer, Boolean&gt;</b> function which must decide if the number of 
 * affected rows is acceptable or not.
 */
public interface ParameterizedCommand extends ParameterizedQuery {

  /**
   * Built-in result policy which returns true only if the number of affected rows is exactly 1.
   */
  Function<Integer, Boolean> EXACTLY_ONE_ROW = (Integer updateCount) -> updateCount == 1;
  /**
   * Built-in result policy which returns true only if the number of affected rows is 1 or greater.
   */
  Function<Integer, Boolean> AT_LEAST_ONE_ROW = (Integer updateCount) -> updateCount >= 1;

  /**
   * @return Returns the result policy to be used to validate the number of affected rows.
   */
  Function<Integer, Boolean> resultPolicy();

}