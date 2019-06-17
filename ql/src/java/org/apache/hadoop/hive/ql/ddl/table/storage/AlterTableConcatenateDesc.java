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

package org.apache.hadoop.hive.ql.ddl.table.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... [PARTITION ... ] CONCATENATE commands.
 */
@Explain(displayName = "Concatenate", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableConcatenateDesc implements DDLDesc {
  static {
    DDLTask2.registerOperation(AlterTableConcatenateDesc.class, AlterTableConcatenateOperation.class);
  }

  private String tableName;
  private Map<String, String> partSpec;
  private ListBucketingCtx lbCtx; // context for list bucketing.

  private List<Path> inputDir = new ArrayList<Path>();
  private Path outputDir = null;
  private Class<? extends InputFormat> inputFormatClass;
  private TableDesc tableDesc;

  public AlterTableConcatenateDesc(String tableName,
      Map<String, String> partSpec) {
    this.tableName = tableName;
    this.partSpec = partSpec;
  }

  @Explain(displayName = "table name")
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "partition desc")
  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  public Path getOutputDir() {
    return outputDir;
  }

  public void setOutputDir(Path outputDir) {
    this.outputDir = outputDir;
  }

  public List<Path> getInputDir() {
    return inputDir;
  }

  public void setInputDir(List<Path> inputDir) {
    this.inputDir = inputDir;
  }

  public ListBucketingCtx getLbCtx() {
    return lbCtx;
  }

  public void setLbCtx(ListBucketingCtx lbCtx) {
    this.lbCtx = lbCtx;
  }

  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
  }

  public void setTableDesc(TableDesc tableDesc) {
    this.tableDesc = tableDesc;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }
}
