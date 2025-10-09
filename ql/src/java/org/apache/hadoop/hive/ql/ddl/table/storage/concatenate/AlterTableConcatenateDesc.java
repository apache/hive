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

package org.apache.hadoop.hive.ql.ddl.table.storage.concatenate;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... [PARTITION ... ] CONCATENATE commands.
 */
@Explain(displayName = "Concatenate", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
@SuppressWarnings("rawtypes")
public class AlterTableConcatenateDesc extends AbstractAlterTableDesc implements DDLDesc {
  private final String tableName;
  private final Map<String, String> partitionSpec;
  private final ListBucketingCtx lbCtx;
  private final Path inputDir;
  private final Path outputDir;
  private final Class<? extends InputFormat> inputFormatClass;
  private final TableDesc tableDesc;

  public AlterTableConcatenateDesc(TableName tableName, Map<String, String> partitionSpec, ListBucketingCtx lbCtx,
      Path inputDir, Path outputDir, Class<? extends InputFormat> inputFormatClass, TableDesc tableDesc) throws SemanticException {
    super(AlterTableType.MERGEFILES, tableName, partitionSpec, null, false, false, null);
    this.tableName = tableName.getNotEmptyDbTable();
    this.partitionSpec = partitionSpec;
    this.lbCtx = lbCtx;
    this.inputDir = inputDir;
    this.outputDir = outputDir;
    this.inputFormatClass = inputFormatClass;
    this.tableDesc = tableDesc;
  }

  /** For Explain only. */
  @Explain(displayName = "partition spec")
  public Map<String, String> getPartitionSpec() {
    return partitionSpec;
  }

  public ListBucketingCtx getLbCtx() {
    return lbCtx;
  }

  public Path getInputDir() {
    return inputDir;
  }

  public Path getOutputDir() {
    return outputDir;
  }

  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }
}
