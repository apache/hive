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

package org.apache.hadoop.hive.ql.io.rcfile.merge;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Mapper;

@Explain(displayName = "Block level merge")
public class MergeWork extends MapredWork implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<String> inputPaths;
  private String outputDir;
  private boolean hasDynamicPartitions;
  private DynamicPartitionCtx dynPartCtx;

  public MergeWork() {
  }

  public MergeWork(List<String> inputPaths, String outputDir) {
    this(inputPaths, outputDir, false, null);
  }

  public MergeWork(List<String> inputPaths, String outputDir,
      boolean hasDynamicPartitions, DynamicPartitionCtx dynPartCtx) {
    super();
    this.inputPaths = inputPaths;
    this.outputDir = outputDir;
    this.hasDynamicPartitions = hasDynamicPartitions;
    this.dynPartCtx = dynPartCtx;
    PartitionDesc partDesc = new PartitionDesc();
    partDesc.setInputFileFormatClass(RCFileBlockMergeInputFormat.class);
    if(this.getPathToPartitionInfo() == null) {
      this.setPathToPartitionInfo(new LinkedHashMap<String, PartitionDesc>());
    }
    if(this.getNumReduceTasks() == null) {
      this.setNumReduceTasks(0);
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

  public String getOutputDir() {
    return outputDir;
  }

  public void setOutputDir(String outputDir) {
    this.outputDir = outputDir;
  }

  public Class<? extends Mapper> getMapperClass() {
    return RCFileMergeMapper.class;
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
    return false;
  }

  public boolean hasDynamicPartitions() {
    return this.hasDynamicPartitions;
  }

  public void setHasDynamicPartitions(boolean hasDynamicPartitions) {
    this.hasDynamicPartitions = hasDynamicPartitions;
  }

  @Override
  public void resolveDynamicPartitionMerge(HiveConf conf, Path path,
      TableDesc tblDesc, ArrayList<String> aliases, PartitionDesc partDesc) {

    String inputFormatClass = conf.getVar(HiveConf.ConfVars.HIVEMERGEINPUTFORMATBLOCKLEVEL);
    try {
      partDesc.setInputFileFormatClass((Class <? extends InputFormat>)
          Class.forName(inputFormatClass));
    } catch (ClassNotFoundException e) {
      String msg = "Merge input format class not found";
      throw new RuntimeException(msg);
    }
    super.resolveDynamicPartitionMerge(conf, path, tblDesc, aliases, partDesc);

    // Add the DP path to the list of input paths
    inputPaths.add(path.toString());
  }

  public DynamicPartitionCtx getDynPartCtx() {
    return dynPartCtx;
  }

  public void setDynPartCtx(DynamicPartitionCtx dynPartCtx) {
    this.dynPartCtx = dynPartCtx;
  }

}
