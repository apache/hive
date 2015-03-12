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
package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import com.google.common.collect.ImmutableList;

// Use this once we have Join Algorithm selection
public class HiveCostUtil {

  private static final double CPU_COST         = 1.0;
  private static final double NET_COST         = 150.0 * CPU_COST;
  private static final double LOCAL_WRITE_COST = 4.0 * NET_COST;
  private static final double LOCAL_READ_COST  = 4.0 * NET_COST;
  private static final double HDFS_WRITE_COST  = 10.0 * LOCAL_WRITE_COST;
  private static final double HDFS_READ_COST   = 1.5 * LOCAL_READ_COST;

  public static RelOptCost computCardinalityBasedCost(HiveRelNode hr) {
    return new HiveCost(hr.getRows(), 0, 0);
  }

  public static HiveCost computeCost(HiveTableScan t) {
    double cardinality = t.getRows();
    return new HiveCost(cardinality, 0, HDFS_WRITE_COST * cardinality * 0);
  }

  public static double computeCommonJoinCPUCost(
          ImmutableList<Double> cardinalities,
          ImmutableBitSet sorted) {
    // Sort-merge join
    assert cardinalities.size() == sorted.length();
    double cpuCost = 0.0;
    for (int i=0; i<cardinalities.size(); i++) {
      double cardinality = cardinalities.get(i);
      if (!sorted.get(i)) {
        // Sort cost
        cpuCost += cardinality * Math.log(cardinality) * CPU_COST;
      }
      // Merge cost
      cpuCost += cardinality * CPU_COST;
    }
    return cpuCost;
  }

  public static double computeCommonJoinIOCost(
          ImmutableList<Pair<Double, Double>> relationInfos) {
    // Sort-merge join
    double ioCost = 0.0;
    for (Pair<Double,Double> relationInfo : relationInfos) {
      double cardinality = relationInfo.left;
      double averageTupleSize = relationInfo.right;
      // Write cost
      ioCost += cardinality * averageTupleSize * LOCAL_WRITE_COST;
      // Read cost
      ioCost += cardinality * averageTupleSize * LOCAL_READ_COST;
      // Net transfer cost
      ioCost += cardinality * averageTupleSize * NET_COST;
    }
    return ioCost;
  }

  public static double computeMapJoinCPUCost(
          ImmutableList<Double> cardinalities,
          ImmutableBitSet streaming) {
    // Hash-join
    double cpuCost = 0.0;
    for (int i=0; i<cardinalities.size(); i++) {
      double cardinality = cardinalities.get(i);
      if (!streaming.get(i)) {
        cpuCost += cardinality;
      }
      cpuCost += cardinality * CPU_COST;
    }
    return cpuCost;
  }

  public static double computeMapJoinIOCost(
          ImmutableList<Pair<Double, Double>> relationInfos,
          ImmutableBitSet streaming, int parallelism) {
    // Hash-join
    double ioCost = 0.0;
    for (int i=0; i<relationInfos.size(); i++) {
      double cardinality = relationInfos.get(i).left;
      double averageTupleSize = relationInfos.get(i).right;
      if (!streaming.get(i)) {
        ioCost += cardinality * averageTupleSize * NET_COST * parallelism;
      }
    }
    return ioCost;
  }

  public static double computeBucketMapJoinCPUCost(
          ImmutableList<Double> cardinalities,
          ImmutableBitSet streaming) {
    // Hash-join
    double cpuCost = 0.0;
    for (int i=0; i<cardinalities.size(); i++) {
      double cardinality = cardinalities.get(i);
      if (!streaming.get(i)) {
        cpuCost += cardinality * CPU_COST;
      }
      cpuCost += cardinality * CPU_COST;
    }
    return cpuCost;
  }

  public static double computeBucketMapJoinIOCost(
          ImmutableList<Pair<Double, Double>> relationInfos,
          ImmutableBitSet streaming, int parallelism) {
    // Hash-join
    double ioCost = 0.0;
    for (int i=0; i<relationInfos.size(); i++) {
      double cardinality = relationInfos.get(i).left;
      double averageTupleSize = relationInfos.get(i).right;
      if (!streaming.get(i)) {
        ioCost += cardinality * averageTupleSize * NET_COST * parallelism;
      }
    }
    return ioCost;
  }

  public static double computeSMBMapJoinCPUCost(
          ImmutableList<Double> cardinalities) {
    // Hash-join
    double cpuCost = 0.0;
    for (int i=0; i<cardinalities.size(); i++) {
      cpuCost += cardinalities.get(i) * CPU_COST;
    }
    return cpuCost;
  }

  public static double computeSMBMapJoinIOCost(
          ImmutableList<Pair<Double, Double>> relationInfos,
          ImmutableBitSet streaming, int parallelism) {
    // Hash-join
    double ioCost = 0.0;
    for (int i=0; i<relationInfos.size(); i++) {
      double cardinality = relationInfos.get(i).left;
      double averageTupleSize = relationInfos.get(i).right;
      if (!streaming.get(i)) {
        ioCost += cardinality * averageTupleSize * NET_COST * parallelism;
      }
    }
    return ioCost;
  }

}
