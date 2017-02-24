/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

public class SortByShuffler implements SparkShuffler<BytesWritable> {

  private final boolean totalOrder;
  private final SparkPlan sparkPlan;

  /**
   * @param totalOrder whether this shuffler provides total order shuffle.
   */
  public SortByShuffler(boolean totalOrder, SparkPlan sparkPlan) {
    this.totalOrder = totalOrder;
    this.sparkPlan = sparkPlan;
  }

  @Override
  public JavaPairRDD<HiveKey, BytesWritable> shuffle(
      JavaPairRDD<HiveKey, BytesWritable> input, int numPartitions) {
    JavaPairRDD<HiveKey, BytesWritable> rdd;
    if (totalOrder) {
      if (numPartitions > 0) {
        if (numPartitions > 1 && input.getStorageLevel() == StorageLevel.NONE()) {
          input.persist(StorageLevel.DISK_ONLY());
          sparkPlan.addCachedRDDId(input.id());
        }
        rdd = input.sortByKey(true, numPartitions);
      } else {
        rdd = input.sortByKey(true);
      }
    } else {
      Partitioner partitioner = new HashPartitioner(numPartitions);
      rdd = input.repartitionAndSortWithinPartitions(partitioner);
    }
    return rdd;
  }

  @Override
  public String getName() {
    return "SortBy";
  }

}
