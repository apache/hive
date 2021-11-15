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
package org.apache.hadoop.hive.ql.ddl.table.column.change;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableWithConstraintsDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.constraint.Constraints;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... CHANGE COLUMN ... commands.
 */
@Explain(displayName = "Change Column", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableChangeColumnDesc extends AbstractAlterTableWithConstraintsDesc {
  private static final long serialVersionUID = 1L;

  private final String oldColumnName;
  private final String newColumnName;
  private final String newColumnType;
  private final String newColumnComment;
  private final boolean first;
  private final String afterColumn;

  public AlterTableChangeColumnDesc(TableName tableName, Map<String, String> partitionSpec, boolean isCascade,
      Constraints constraints, String oldColumnName, String newColumnName, String newColumnType,
      String newColumnComment, boolean first, String afterColumn) throws SemanticException {
    super(AlterTableType.RENAME_COLUMN, tableName, partitionSpec, null, isCascade, false, null, constraints);

    this.oldColumnName = oldColumnName;
    this.newColumnName = newColumnName;
    this.newColumnType = newColumnType;
    this.newColumnComment = newColumnComment;
    this.first = first;
    this.afterColumn = afterColumn;
  }

  @Explain(displayName = "old column name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOldColumnName() {
    return oldColumnName;
  }

  @Explain(displayName = "new column name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getNewColumnName() {
    return newColumnName;
  }

  @Explain(displayName = "new column type", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getNewColumnType() {
    return newColumnType;
  }

  @Explain(displayName = "new column comment", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getNewColumnComment() {
    return newColumnComment;
  }

  @Explain(displayName = "first", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isFirst() {
    return first;
  }

  @Explain(displayName = "after column", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getAfterColumn() {
    return afterColumn;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
