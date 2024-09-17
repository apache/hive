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

package org.apache.hadoop.hive.ql.ddl.table.snapshotref;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.AlterTableSnapshotRefSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;

@Explain(displayName = "SnapshotRef Operation", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT,
    Explain.Level.EXTENDED })
public class AlterTableSnapshotRefDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  protected AlterTableSnapshotRefSpec alterTableSnapshotRefSpec;

  public AlterTableSnapshotRefDesc(AlterTableType alterTableType, TableName tableName, AlterTableSnapshotRefSpec
      alterTableSnapshotRefSpec) throws SemanticException {
    super(alterTableType, tableName, null, null, false, false, null);
    this.alterTableSnapshotRefSpec = alterTableSnapshotRefSpec;
  }

  public AlterTableSnapshotRefSpec getAlterTableSnapshotRefSpec() {
    return alterTableSnapshotRefSpec;
  }

  @Explain(displayName = "spec", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
  public String getExplainOutput() {
    return alterTableSnapshotRefSpec.toString();
  }
}
