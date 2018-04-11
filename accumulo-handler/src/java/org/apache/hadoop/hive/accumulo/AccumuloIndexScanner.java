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

package org.apache.hadoop.hive.accumulo;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Specification for implementing a AccumuloIndexScanner.
 */
public interface AccumuloIndexScanner {

  /**
   * Initialize the index scanner implementation with the runtime configuration.
   *
   * @param conf  - the hadoop configuration
   */
  void init(Configuration conf);

  /**
   * Check if column is defined as being indexed.
   *
   * @param columnName - the hive column name
   * @return true if the column is indexed
   */
  boolean isIndexed(String columnName);

  /**
   * Get a list of rowid ranges by scanning a column index.
   *
   * @param column     - the hive column name
   * @param indexRange - Key range to scan on the index table
   * @return List of matching rowid ranges or null if too many matches found
   *
   */
  List<Range> getIndexRowRanges(String column, Range indexRange);

}
