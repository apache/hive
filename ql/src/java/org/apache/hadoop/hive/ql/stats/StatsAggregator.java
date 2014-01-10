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

package org.apache.hadoop.hive.ql.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Task;

/**
 * An interface for any possible implementation for gathering statistics.
 */

public interface StatsAggregator {

  /**
   * This method connects to the temporary storage.
   *
   * @param hconf
   *          HiveConf that contains the connection parameters.
   * @param sourceTask
   * @return true if connection is successful, false otherwise.
   */
  public boolean connect(Configuration hconf, Task sourceTask);

  /**
   * This method aggregates a given statistic from all tasks (partial stats).
   * After aggregation, this method also automatically removes all records
   * that have been aggregated.
   *
   * @param keyPrefix
   *          a prefix of the keys used in StatsPublisher to publish stats.
   *          Any rows that starts with the same prefix will be aggregated. For example, if
   *          the StatsPublisher uses the following compound key to publish stats:
   *
   *          the output directory name (unique per FileSinkOperator) +
   *          the partition specs (only for dynamic partitions) +
   *          taskID (last component of task file)
   *
   *          The keyPrefix for aggregation could be first 2 components. This will aggregates stats
   *          across all tasks for each partition.
   *
   * @param statType
   *          a string noting the key to be published. Ex: "numRows".
   * @return a string representation of a long value, null if there are any error/exception.
   */
  public String aggregateStats(String keyPrefix, String statType);

  /**
   * This method closes the connection to the temporary storage.
   *
   * @return true if close connection is successful, false otherwise.
   */
  public boolean closeConnection();

  /**
   * This method is called after all statistics have been aggregated. Since we support multiple
   * statistics, we do not perform automatic cleanup after aggregation.
   * After this method is called, closeConnection must be called as well.
   * This method is also used to clear the temporary statistics that have been published without
   * being aggregated.
   * Typically this happens when a job fails, or is forcibly stopped after publishing some
   * statistics.
   *
   * @param keyPrefix
   *          a prefix of the keys used in StatsPublisher to publish stats. It is the same
   *          as the first parameter in aggregateStats().
   * @return true if cleanup is successful, false otherwise.
   */
  public boolean cleanUp(String keyPrefix);
}
