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

package org.apache.hadoop.hive.ql.ddl.table.constraint.add;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableWithConstraintsDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.constraint.Constraints;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... ADD CONSTRAINT ... commands.
 */
@Explain(displayName = "Add Constraint", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableAddConstraintDesc extends AbstractAlterTableWithConstraintsDesc {
  private static final long serialVersionUID = 1L;

  public AlterTableAddConstraintDesc(TableName tableName, ReplicationSpec replicationSpec, Constraints constraints)
      throws SemanticException {
    super(AlterTableType.ADD_CONSTRAINT, tableName, null, replicationSpec, false, false, null, constraints);
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
