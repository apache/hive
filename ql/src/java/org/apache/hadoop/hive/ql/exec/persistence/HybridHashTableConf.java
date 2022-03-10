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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This conf class is a wrapper of a list of HybridHashTableContainers and some common info shared
 * among them, which is used in n-way join (multiple small tables are involved).
 */
public class HybridHashTableConf {
  private List<HybridHashTableContainer> loadedContainerList; // A list of already loaded containers
  private int numberOfPartitions = 0; // Number of partitions each table should have
  private int nextSpillPartition = -1;       // The partition to be spilled next

  public HybridHashTableConf() {
    loadedContainerList = new ArrayList<HybridHashTableContainer>();
  }

  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }

  public void setNumberOfPartitions(int numberOfPartitions) {
    this.numberOfPartitions = numberOfPartitions;
    this.nextSpillPartition = numberOfPartitions - 1;
  }

  public int getNextSpillPartition() {
    return this.nextSpillPartition;
  }

  public void setNextSpillPartition(int nextSpillPartition) {
    this.nextSpillPartition = nextSpillPartition;
  }


  public List<HybridHashTableContainer> getLoadedContainerList() {
    return loadedContainerList;
  }

  /**
   * Spill one in-memory partition from tail for all previously loaded HybridHashTableContainers.
   * Also mark that partition number as spill-on-creation for future created containers.
   * @return amount of memory freed; 0 if only one last partition is in memory for each container
   */
  public long spill() throws IOException {
    if (nextSpillPartition == 0) {
      return 0;
    }
    long memFreed = 0;
    for (HybridHashTableContainer container : loadedContainerList) {
      memFreed += container.spillPartition(nextSpillPartition);
      container.setSpill(true);
    }
    nextSpillPartition--;
    return memFreed;
  }

  /**
   * Check if a partition should be spilled directly on creation
   * @param partitionId the partition to create
   * @return true if it should be spilled directly, false otherwise
   */
  public boolean doSpillOnCreation(int partitionId) {
    return nextSpillPartition != -1 && partitionId > nextSpillPartition;
  }
}
