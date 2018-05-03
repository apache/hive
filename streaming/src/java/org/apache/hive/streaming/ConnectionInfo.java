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

package org.apache.hive.streaming;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Helper interface to get connection related information.
 */
public interface ConnectionInfo {

  /**
   * Get metastore URI that metastore client uses.
   *
   * @return - metastore URI used by client
   */
  String getMetastoreUri();

  /**
   * Get the table used by streaming connection.
   *
   * @return - table
   */
  Table getTable();

  /**
   * Get any static partitions specified during streaming connection creation.
   *
   * @return - static partition values
   */
  List<String> getStaticPartitionValues();

  /**
   * Get if the specified table is partitioned table or not.
   *
   * @return - true if partitioned table else false
   */
  boolean isPartitionedTable();

  /**
   * Get if dynamic partitioning is used.
   *
   * @return - true if dynamic partitioning case else false
   */
  boolean isDynamicPartitioning();

  /**
   * Get agent info that is set during streaming connection.
   *
   * @return - agent info
   */
  String getAgentInfo();
}
