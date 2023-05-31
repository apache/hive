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

package org.apache.hadoop.hive.ql.ddl.table.tag.create;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.AlterTableTagSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;

@Explain(displayName = "CreateTag operation", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
public class AlterTableCreateTagDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final AlterTableTagSpec alterTableTagSpec;

  public AlterTableCreateTagDesc(TableName tableName, AlterTableTagSpec alterTableTagSpec)
      throws SemanticException {
    super(AlterTableType.CREATE_TAG, tableName, null, null, false, false, null);
    this.alterTableTagSpec = alterTableTagSpec;
  }

  public AlterTableTagSpec getAlterTableTagSpec() {
    return alterTableTagSpec;
  }

  @Explain(displayName = "spec", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
  public String getExplainOutput() {
    return alterTableTagSpec.toString();
  }
}
