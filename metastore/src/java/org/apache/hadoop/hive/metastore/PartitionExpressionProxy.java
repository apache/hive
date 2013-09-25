/**
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

package org.apache.hadoop.hive.metastore;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * The proxy interface that metastore uses to manipulate and apply
 * serialized filter expressions coming from client.
 */
public interface PartitionExpressionProxy {

  /**
   * Converts serialized Hive expression into filter in the format suitable for Filter.g.
   * @param expr Serialized expression.
   * @return The filter string.
   */
  public String convertExprToFilter(byte[] expr) throws MetaException;

  /**
   * Filters the partition names via serialized Hive expression.
   * @param columnNames Partition column names in the underlying table.
   * @param expr Serialized expression.
   * @param defaultPartitionName Default partition name from job or server configuration.
   * @param partitionNames Partition names; the list is modified in place.
   * @return Whether there were any unknown partitions preserved in the name list.
   */
  public boolean filterPartitionsByExpr(List<String> columnNames, byte[] expr,
      String defaultPartitionName, List<String> partitionNames) throws MetaException;
}
