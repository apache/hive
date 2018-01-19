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
package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.fs.Path;

/**
 *
 */
public class FileMergeDesc extends AbstractOperatorDesc {
  private DynamicPartitionCtx dpCtx;
  private Path outputPath;
  private int listBucketingDepth;
  private boolean hasDynamicPartitions;
  private boolean isListBucketingAlterTableConcatenate;
  private Long txnId;
  private int stmtId;
  private boolean isMmTable;

  public FileMergeDesc(DynamicPartitionCtx dynPartCtx, Path outputDir) {
    this.dpCtx = dynPartCtx;
    this.outputPath = outputDir;
  }

  public DynamicPartitionCtx getDpCtx() {
    return dpCtx;
  }

  public void setDpCtx(DynamicPartitionCtx dpCtx) {
    this.dpCtx = dpCtx;
  }

  public Path getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(Path outputPath) {
    this.outputPath = outputPath;
  }

  public int getListBucketingDepth() {
    return listBucketingDepth;
  }

  public void setListBucketingDepth(int listBucketingDepth) {
    this.listBucketingDepth = listBucketingDepth;
  }

  public boolean hasDynamicPartitions() {
    return hasDynamicPartitions;
  }

  public void setHasDynamicPartitions(boolean hasDynamicPartitions) {
    this.hasDynamicPartitions = hasDynamicPartitions;
  }

  public boolean isListBucketingAlterTableConcatenate() {
    return isListBucketingAlterTableConcatenate;
  }

  public void setListBucketingAlterTableConcatenate(boolean isListBucketingAlterTableConcatenate) {
    this.isListBucketingAlterTableConcatenate = isListBucketingAlterTableConcatenate;
  }

  public Long getTxnId() {
    return txnId;
  }

  public void setTxnId(Long txnId) {
    this.txnId = txnId;
  }

  public int getStmtId() {
    return stmtId;
  }

  public void setStmtId(int stmtId) {
    this.stmtId = stmtId;
  }

  public boolean getIsMmTable() {
    return isMmTable;
  }

  public void setIsMmTable(boolean isMmTable) {
    this.isMmTable = isMmTable;
  }
}
