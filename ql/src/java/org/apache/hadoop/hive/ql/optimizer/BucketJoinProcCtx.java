/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;

public class BucketJoinProcCtx implements NodeProcessorCtx {
  private static final Logger LOG =
    LoggerFactory.getLogger(BucketJoinProcCtx.class.getName());

  private final HiveConf conf;

  private Set<JoinOperator> rejectedJoinOps = new HashSet<JoinOperator>();

  // The set of join operators which can be converted to a bucketed map join
  private Set<JoinOperator> convertedJoinOps = new HashSet<JoinOperator>();

  // In checking if a mapjoin can be converted to bucket mapjoin,
  // some join alias could be changed: alias -> newAlias
  private transient Map<String, String> aliasToNewAliasMap;

  private Map<String, List<Integer>> tblAliasToNumberOfBucketsInEachPartition;
  private Map<String, List<List<String>>> tblAliasToBucketedFilePathsInEachPartition;
  private Map<Partition, List<String>> bigTblPartsToBucketFileNames;
  private Map<Partition, Integer> bigTblPartsToBucketNumber;
  private List<String> joinAliases;
  private String baseBigAlias;
  private boolean bigTablePartitioned;

  public BucketJoinProcCtx(HiveConf conf) {
    this.conf = conf;
  }

  public HiveConf getConf() {
    return conf;
  }

  public Set<JoinOperator> getRejectedJoinOps() {
    return rejectedJoinOps;
  }

  public Set<JoinOperator> getConvertedJoinOps() {
    return convertedJoinOps;
  }

  public void setRejectedJoinOps(Set<JoinOperator> rejectedJoinOps) {
    this.rejectedJoinOps = rejectedJoinOps;
  }

  public void setConvertedJoinOps(Set<JoinOperator> setOfConvertedJoins) {
    this.convertedJoinOps = setOfConvertedJoins;
  }

  public Map<String, List<Integer>> getTblAliasToNumberOfBucketsInEachPartition() {
    return tblAliasToNumberOfBucketsInEachPartition;
  }

  public Map<String, List<List<String>>> getTblAliasToBucketedFilePathsInEachPartition() {
    return tblAliasToBucketedFilePathsInEachPartition;
  }

  public Map<Partition, List<String>> getBigTblPartsToBucketFileNames() {
    return bigTblPartsToBucketFileNames;
  }

  public Map<Partition, Integer> getBigTblPartsToBucketNumber() {
    return bigTblPartsToBucketNumber;
  }

  public void setTblAliasToNumberOfBucketsInEachPartition(
    Map<String, List<Integer>> tblAliasToNumberOfBucketsInEachPartition) {
    this.tblAliasToNumberOfBucketsInEachPartition = tblAliasToNumberOfBucketsInEachPartition;
  }

  public void setTblAliasToBucketedFilePathsInEachPartition(
    Map<String, List<List<String>>> tblAliasToBucketedFilePathsInEachPartition) {
    this.tblAliasToBucketedFilePathsInEachPartition = tblAliasToBucketedFilePathsInEachPartition;
  }

  public void setBigTblPartsToBucketFileNames(
    Map<Partition, List<String>> bigTblPartsToBucketFileNames) {
    this.bigTblPartsToBucketFileNames = bigTblPartsToBucketFileNames;
  }

  public void setBigTblPartsToBucketNumber(Map<Partition, Integer> bigTblPartsToBucketNumber) {
    this.bigTblPartsToBucketNumber = bigTblPartsToBucketNumber;
  }

  public void setJoinAliases(List<String> joinAliases) {
    this.joinAliases = joinAliases;
  }

  public void setBaseBigAlias(String baseBigAlias) {
    this.baseBigAlias = baseBigAlias;
  }

  public List<String> getJoinAliases() {
    return joinAliases;
  }

  public String getBaseBigAlias() {
    return baseBigAlias;
  }

  public boolean isBigTablePartitioned() {
    return bigTablePartitioned;
  }

  public void setBigTablePartitioned(boolean bigTablePartitioned) {
    this.bigTablePartitioned = bigTablePartitioned;
  }

  public void setAliasToNewAliasMap(Map<String, String> aliasToNewAliasMap) {
    this.aliasToNewAliasMap = aliasToNewAliasMap;
  }

  public Map<String, String> getAliasToNewAliasMap() {
    return aliasToNewAliasMap;
  }
}
