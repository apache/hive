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

package org.apache.hadoop.hive.ql.ddl.table.branch.create;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.AlterTableCreateBranchSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "CreateBranch operation", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableCreateBranchDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final AlterTableCreateBranchSpec createBranchSpec;

  public AlterTableCreateBranchDesc(TableName tableName, AlterTableCreateBranchSpec createBranchSpec)
      throws SemanticException {
    super(AlterTableType.CREATEBRANCH, tableName, null, null, false, false, null);
    this.createBranchSpec = createBranchSpec;
  }

  public AlterTableCreateBranchSpec getCreateBranchSpec() {
    return createBranchSpec;
  }

  @Explain(displayName = "spec", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getExplainOutput() {
    return createBranchSpec.toString();
  }
}
