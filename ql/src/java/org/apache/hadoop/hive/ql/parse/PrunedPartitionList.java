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

package org.apache.hadoop.hive.ql.parse;

import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.Partition;

/**
 * The list of pruned partitions.
 */
public class PrunedPartitionList {
  // confirmed partitions - satisfy the partition criteria
  private Set<Partition> confirmedPartns;

  // unknown partitions - may/may not satisfy the partition criteria
  private Set<Partition> unknownPartns;

  // denied partitions - do not satisfy the partition criteria
  private final Set<Partition> deniedPartns;

  /**
   * @param confirmedPartns
   *          confirmed paritions
   * @param unknownPartns
   *          unknown partitions
   */
  public PrunedPartitionList(Set<Partition> confirmedPartns,
      Set<Partition> unknownPartns, Set<Partition> deniedPartns) {
    this.confirmedPartns = confirmedPartns;
    this.unknownPartns = unknownPartns;
    this.deniedPartns = deniedPartns;
  }

  /**
   * get confirmed partitions
   * 
   * @return confirmedPartns confirmed paritions
   */
  public Set<Partition> getConfirmedPartns() {
    return confirmedPartns;
  }

  /**
   * get unknown partitions
   * 
   * @return unknownPartns unknown paritions
   */
  public Set<Partition> getUnknownPartns() {
    return unknownPartns;
  }

  /**
   * get denied partitions
   * 
   * @return deniedPartns denied paritions
   */
  public Set<Partition> getDeniedPartns() {
    return deniedPartns;
  }

  /**
   * set confirmed partitions
   * 
   * @param confirmedPartns
   *          confirmed paritions
   */
  public void setConfirmedPartns(Set<Partition> confirmedPartns) {
    this.confirmedPartns = confirmedPartns;
  }

  /**
   * set unknown partitions
   * 
   * @param unknownPartns
   *          unknown partitions
   */
  public void setUnknownPartns(Set<Partition> unknownPartns) {
    this.unknownPartns = unknownPartns;
  }
}
