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

import java.util.Comparator;
import java.util.List;

public interface InClauseBatchCommand<T> extends ParameterizedQuery {

  /**
   * The parameterized query string. The query must have exactly parameter inside the IN clause, and can have zero or 
   * more parameters everywhere else in the query string.
   * @see ParameterizedQuery#getParameterizedQueryString(DatabaseProduct) 
   */
  String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException;

  /**
   * The list of the parameters for the IN clause. If the list is too long, it will be split and the query will
   * be executed multiple times.
   */
  List<T> getInClauseParameters();

  /**
   * @return Returns with the name of the parameter which is inside the IN clause.
   */
  String getInClauseParameterName();

  /**
   * @return Returns a {@link Comparator<T>} instance which can be used to determine the longest element in the 
   * list IN clause parameters. This is required to be able to estimate the final legth of the command.
   */
  Comparator<T> getParameterLengthComparator(); 
  
}
