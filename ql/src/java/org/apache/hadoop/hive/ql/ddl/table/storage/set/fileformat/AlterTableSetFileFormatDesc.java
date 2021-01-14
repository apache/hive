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

package org.apache.hadoop.hive.ql.ddl.table.storage.set.fileformat;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... SET FILEFORMAT ... commands.
 */
@Explain(displayName = "Set File Format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableSetFileFormatDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final String inputFormat;
  private final String outputFormat;
  private final String serdeName;

  public AlterTableSetFileFormatDesc(TableName tableName, Map<String, String> partitionSpec, String inputFormat,
      String outputFormat, String serdeName) throws SemanticException {
    super(AlterTableType.SET_FILE_FORMAT, tableName, partitionSpec, null, false, false, null);
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serdeName = serdeName;
  }

  @Explain(displayName = "input format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getInputFormat() {
    return inputFormat;
  }

  @Explain(displayName = "output format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOutputFormat() {
    return outputFormat;
  }

  @Explain(displayName = "serde name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getSerdeName() {
    return serdeName;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
