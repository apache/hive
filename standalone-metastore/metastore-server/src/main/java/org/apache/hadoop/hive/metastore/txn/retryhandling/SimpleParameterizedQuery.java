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
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Basic implementation of the {@link ParameterizedQuery} interface.
 */
public class SimpleParameterizedQuery implements ParameterizedQuery {
  
  private final String query;
  private final SqlParameterSource params;
  
  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
    return query;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return params;
  }

  public SimpleParameterizedQuery(String query, SqlParameterSource params) {
    this.query = query;
    this.params = params;
  }

}
