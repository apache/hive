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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

public class ShuffleTran implements SparkTran<HiveKey, BytesWritable, HiveKey, Iterable<BytesWritable>> {
  private final SparkShuffler shuffler;
  private final int numOfPartitions;
  private final boolean toCache;
  private final SparkPlan sparkPlan;

  public ShuffleTran(SparkPlan sparkPlan, SparkShuffler sf, int n) {
    this(sparkPlan, sf, n, false);
  }

  public ShuffleTran(SparkPlan sparkPlan, SparkShuffler sf, int n, boolean toCache) {
    shuffler = sf;
    numOfPartitions = n;
    this.toCache = toCache;
    this.sparkPlan = sparkPlan;
  }

  @Override
  public JavaPairRDD<HiveKey, Iterable<BytesWritable>> transform(JavaPairRDD<HiveKey, BytesWritable> input) {
    JavaPairRDD<HiveKey, Iterable<BytesWritable>> result = shuffler.shuffle(input, numOfPartitions);
    if (toCache) {
      sparkPlan.addCachedRDDId(result.id());
      result = result.persist(StorageLevel.MEMORY_AND_DISK());
    }
    return result;
  }
}
