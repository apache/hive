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

package org.apache.hadoop.hive.ql.ddl.table.misc.owner;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.authorization.AuthorizationParseUtils;

/**
 * Analyzer for set owner commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_OWNER)
public class AlterTableSetOwnerAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableSetOwnerAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpecFromFramework, ASTNode command)
      throws SemanticException {
    PrincipalDesc ownerPrincipal = AuthorizationParseUtils.getPrincipalDesc((ASTNode) command.getChild(0));

    if (ownerPrincipal.getType() == null) {
      throw new SemanticException("Owner type can't be null in alter table set owner command");
    }

    if (ownerPrincipal.getName() == null) {
      throw new SemanticException("Owner name can't be null in alter table set owner command");
    }

    AlterTableSetOwnerDesc desc  = new AlterTableSetOwnerDesc(tableName, ownerPrincipal);
    addInputsOutputsAlterTable(tableName, null, desc, desc.getType(), false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc), conf));

    setAcidDdlDesc(getTable(tableName), desc);

  }
}
