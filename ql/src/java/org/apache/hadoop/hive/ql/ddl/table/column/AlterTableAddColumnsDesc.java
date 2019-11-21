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

package org.apache.hadoop.hive.ql.ddl.table.column;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... ADD COLUMNS ... commands.
 */
@Explain(displayName = "Add Columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableAddColumnsDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final List<FieldSchema> newColumns;

  public AlterTableAddColumnsDesc(TableName tableName, Map<String, String> partitionSpec, boolean isCascade,
      List<FieldSchema> newColumns) throws SemanticException {
    super(AlterTableType.ADDCOLS, tableName, partitionSpec, null, isCascade, false, null);
    this.newColumns = newColumns;
  }

  public List<FieldSchema> getNewColumns() {
    return newColumns;
  }

  // Only for explain
  @Explain(displayName = "new columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getNewColsString() {
    return Utilities.getFieldSchemaString(newColumns);
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
