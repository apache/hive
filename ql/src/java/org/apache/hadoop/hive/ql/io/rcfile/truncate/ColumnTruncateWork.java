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

package org.apache.hadoop.hive.ql.io.rcfile.truncate;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileBlockMergeInputFormat;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.Mapper;

@Explain(displayName = "Column Truncate")
public class ColumnTruncateWork extends MapWork implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient Path inputDir;
  private Path outputDir;
  private boolean hasDynamicPartitions;
  private DynamicPartitionCtx dynPartCtx;
  private boolean isListBucketingAlterTableConcatenate;
  private ListBucketingCtx listBucketingCtx;
  private List<Integer> droppedColumns;

  public ColumnTruncateWork() {
  }

  public ColumnTruncateWork(List<Integer> droppedColumns, Path inputDir, Path outputDir) {
    this(droppedColumns, inputDir, outputDir, false, null);
  }

  public ColumnTruncateWork(List<Integer> droppedColumns, Path inputDir, Path outputDir,
      boolean hasDynamicPartitions, DynamicPartitionCtx dynPartCtx) {
    super();
    this.droppedColumns = droppedColumns;
    this.inputDir = inputDir;
    this.outputDir = outputDir;
    this.hasDynamicPartitions = hasDynamicPartitions;
    this.dynPartCtx = dynPartCtx;
    PartitionDesc partDesc = new PartitionDesc();
    partDesc.setInputFileFormatClass(RCFileBlockMergeInputFormat.class);
    if(this.getPathToPartitionInfo() == null) {
      this.setPathToPartitionInfo(new LinkedHashMap<String, PartitionDesc>());
    }
    this.getPathToPartitionInfo().put(inputDir.toString(), partDesc);
  }

  public Path getInputDir() {
    return inputDir;
  }

  public void setInputPaths(Path inputDir) {
    this.inputDir = inputDir;
  }

  public Path getOutputDir() {
    return outputDir;
  }

  public void setOutputDir(Path outputDir) {
    this.outputDir = outputDir;
  }

  public Class<? extends Mapper> getMapperClass() {
    return ColumnTruncateMapper.class;
  }

  @Override
  public Long getMinSplitSize() {
    return null;
  }

  @Override
  public String getInputformat() {
    return BucketizedHiveInputFormat.class.getName();
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

  public DynamicPartitionCtx getDynPartCtx() {
    return dynPartCtx;
  }

  public void setDynPartCtx(DynamicPartitionCtx dynPartCtx) {
    this.dynPartCtx = dynPartCtx;
  }

  /**
   * @return the listBucketingCtx
   */
  public ListBucketingCtx getListBucketingCtx() {
    return listBucketingCtx;
  }

  /**
   * @param listBucketingCtx the listBucketingCtx to set
   */
  public void setListBucketingCtx(ListBucketingCtx listBucketingCtx) {
    this.listBucketingCtx = listBucketingCtx;
  }

  /**
   * @return the isListBucketingAlterTableConcatenate
   */
  public boolean isListBucketingAlterTableConcatenate() {
    return isListBucketingAlterTableConcatenate;
  }

  public List<Integer> getDroppedColumns() {
    return droppedColumns;
  }

  public void setDroppedColumns(List<Integer> droppedColumns) {
    this.droppedColumns = droppedColumns;
  }

}
