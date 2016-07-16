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

package org.apache.hadoop.hive.ql.io.rcfile.stats;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESTATSDBCLASS;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst.StatDB;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileBlockMergeInputFormat;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.mapred.Mapper;

/**
 * Partial Scan Work.
 *
 */
@Explain(displayName = "Partial Scan Statistics")
public class PartialScanWork extends MapWork implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient List<Path> inputPaths;
  private String aggKey;
  private String statsTmpDir;

  public PartialScanWork() {
  }

  public PartialScanWork(List<Path> inputPaths) {
    super();
    this.inputPaths = inputPaths;
    PartitionDesc partDesc = new PartitionDesc();
    partDesc.setInputFileFormatClass(RCFileBlockMergeInputFormat.class);
    for(Path path: this.inputPaths) {
      this.addPathToPartitionInfo(path, partDesc);
    }
  }

  public List<Path> getInputPaths() {
    return inputPaths;
  }

  public void setInputPaths(List<Path> inputPaths) {
    this.inputPaths = inputPaths;
  }

  public Class<? extends Mapper> getMapperClass() {
    return PartialScanMapper.class;
  }

  @Override
  public Long getMinSplitSize() {
    return null;
  }

  @Override
  public String getInputformat() {
    return CombineHiveInputFormat.class.getName();
  }

  @Override
  public boolean isGatheringStats() {
    return true;
  }

  /**
   * @return the aggKey
   */
  @Explain(displayName = "Stats Aggregation Key Prefix", explainLevels = { Level.EXTENDED })
  public String getAggKey() {
    return aggKey;
  }

  /**
   * @param aggKey the aggKey to set
   */
  public void setAggKey(String aggKey) {
    this.aggKey = aggKey;
  }

  public String getStatsTmpDir() {
    return statsTmpDir;
  }

  public void setStatsTmpDir(String statsTmpDir, HiveConf conf) {
    this.statsTmpDir = HiveConf.getVar(conf, HIVESTATSDBCLASS).equalsIgnoreCase(StatDB.fs.name())
      ?  statsTmpDir : "";
  }

}
