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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileBlockMergeInputFormat;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.Mapper;

/**
 * Partial Scan Work.
 *
 */
@Explain(displayName = "Partial Scan Statistics")
public class PartialScanWork extends MapWork implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<String> inputPaths;
  private String aggKey;

  public PartialScanWork() {
  }

  public PartialScanWork(List<String> inputPaths) {
    super();
    this.inputPaths = inputPaths;
    PartitionDesc partDesc = new PartitionDesc();
    partDesc.setInputFileFormatClass(RCFileBlockMergeInputFormat.class);
    if(this.getPathToPartitionInfo() == null) {
      this.setPathToPartitionInfo(new LinkedHashMap<String, PartitionDesc>());
    }
    for(String path: this.inputPaths) {
      this.getPathToPartitionInfo().put(path, partDesc);
    }
  }

  public List<String> getInputPaths() {
    return inputPaths;
  }

  public void setInputPaths(List<String> inputPaths) {
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
  @Explain(displayName = "Stats Aggregation Key Prefix", normalExplain = false)
  public String getAggKey() {
    return aggKey;
  }

  /**
   * @param aggKey the aggKey to set
   */
  public void setAggKey(String aggKey) {
    this.aggKey = aggKey;
  }

}
