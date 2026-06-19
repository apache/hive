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
package org.apache.hadoop.hive.ql.optimizer.listbucketingpruner;

import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;

/**
 * Context used by list bucketing pruner to get all partitions
 *
 */
public class LBOpPartitionWalkerCtx implements NodeProcessorCtx {

  private final ParseContext parseContext;

  private PrunedPartitionList partitions;

  /**
   * Constructor.
   */
  public LBOpPartitionWalkerCtx(ParseContext parseContext) {
    this.parseContext = parseContext;
  }

  /**
   * Return parse context.
   *
   * @return
   */
  public ParseContext getParseContext() {
    return parseContext;
  }

  /**
   * Return partitions.
   *
   * @return the partitions
   */
  public PrunedPartitionList getPartitions() {
    return partitions;
  }

  /**
   * Set partitions.
   *
   * @param partitions the partitions to set
   */
  public void setPartitions(PrunedPartitionList partitions) {
    this.partitions = partitions;
  }

}
