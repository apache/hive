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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;

import java.util.Map;

/**
 * RulePartitionPruner interface.
 *
 * Used to retrieve an engine specific pruned PartitionList.
 */
public interface RulePartitionPruner {

  /**
   * Return the PrunedPartitionList after pruning the partitions.
   */
  public PrunedPartitionList prune(HiveConf conf, Map<String, PrunedPartitionList> partitionCache)
      throws HiveException;

  /**
   * Return the PrunedPartitionList. This method gets called when there is no pruning
   * to be done, but there still may be a PrunedPartitionList that has to be returned
   * in cases like if a dummy partition is created when there are no partitions or if
   * the caller wants all the partitions to be returned.
   */
  public PrunedPartitionList getNonPruneList(HiveConf conf,
      Map<String, PrunedPartitionList> partitionCache) throws HiveException;
}
