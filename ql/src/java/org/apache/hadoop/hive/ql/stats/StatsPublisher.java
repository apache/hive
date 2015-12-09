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

import java.util.Map;

/**
 * An interface for any possible implementation for publishing statics.
 */

public interface StatsPublisher {

  /**
   * This method does the necessary one-time initializations, possibly creating the tables and
   * database (if not exist).
   * This method is usually called in the Hive client side rather than by the mappers/reducers
   * so that it is initialized only once.
   * @param hconf HiveConf that contains the configurations parameters used to connect to
   * intermediate stats database.
   * @return true if initialization is successful, false otherwise.
   */
  public boolean init(StatsCollectionContext context);

  /**
   * This method connects to the intermediate statistics database.
   * @param hconf HiveConf that contains the connection parameters.
   * @return true if connection is successful, false otherwise.
   */
  public boolean connect(StatsCollectionContext context);

  /**
   * This method publishes a given statistic into a disk storage, possibly HBase or MySQL.
   *
   * @param fileID
   *          : a string identification the statistics to be published by all mappers/reducers
   *          and then gathered. The statID is unique per output partition per task, e.g.,:
   *          the output directory name (uniq per FileSinkOperator) +
   *          the partition specs (only for dynamic partitions) +
   *          taskID (last component of task file)
   * @param stats
   *          : a map containing key-value pairs, where key is a string representing the statistic
   *          to be published,
   *          and value is a string representing the value for the given statistic
   * @return true if successful, false otherwise
   */
  public boolean publishStat(String fileID, Map<String, String> stats);

  /**
   * This method closes the connection to the temporary storage.
   */
  public boolean closeConnection(StatsCollectionContext context);

}
