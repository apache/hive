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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * A pair of a Spring {@link NamedParameterJdbcTemplate} style parameterized query string <br>
 * (for example: <b>SELECT * FROM TBL WHERE ID = :id</b>) and its parameters.
 */
public interface ParameterizedQuery {

  /**
   * The parameterized query string. It is allowed if the query has no parameters at all.
   * @param databaseProduct A {@link DatabaseProduct} instance representing the type of the underlying HMS dabatabe.
   * @return Returns the parameterized query string.
   * @throws MetaException Thrown if the query string cannot be assembled.
   */
  String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException;

  /**
   * An {@link SqlParameterSource} instance containing the required parameters for the query string. If the query is not
   * parameterized, this method can either return null or an empty {@link SqlParameterSource}.
   * @return Returns the {@link SqlParameterSource} containing the parameters for the query.
   */
  SqlParameterSource getQueryParameters();
  
}
