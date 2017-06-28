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

package org.apache.hadoop.hive.ql.plan;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * Truncates managed table or partition
 */
@Explain(displayName = "Truncate Table or Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class TruncateTableDesc extends DDLDesc {

  private static final long serialVersionUID = 1L;

  private String tableName;
  private Map<String, String> partSpec;
  private List<Integer> columnIndexes;
  private Path inputDir;
  private Path outputDir;
  private ListBucketingCtx lbCtx;
  private ReplicationSpec replicationSpec;

  public TruncateTableDesc() {
  }

  public TruncateTableDesc(String tableName, Map<String, String> partSpec, ReplicationSpec replicationSpec) {
    this.tableName = tableName;
    this.partSpec = partSpec;
    this.replicationSpec = replicationSpec;
  }

  @Explain(displayName = "TableName", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Explain(displayName = "Partition Spec", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(Map<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  @Explain(displayName = "Column Indexes")
  public List<Integer> getColumnIndexes() {
    return columnIndexes;
  }

  public void setColumnIndexes(List<Integer> columnIndexes) {
    this.columnIndexes = columnIndexes;
  }

  public Path getInputDir() {
    return inputDir;
  }

  public void setInputDir(Path inputDir) {
    this.inputDir = inputDir;
  }

  public Path getOutputDir() {
    return outputDir;
  }

  public void setOutputDir(Path outputDir) {
    this.outputDir = outputDir;
  }

  public ListBucketingCtx getLbCtx() {
    return lbCtx;
  }

  public void setLbCtx(ListBucketingCtx lbCtx) {
    this.lbCtx = lbCtx;
  }

  /**
   * @return what kind of replication scope this truncate is running under.
   * This can result in a "TRUNCATE IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec() { return this.replicationSpec; }
}
