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

package org.apache.hadoop.hive.ql.ddl.dataconnector.drop;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for data connector dropping commands.
 */
@DDLType(types = HiveParser.TOK_DROPDATACONNECTOR)
public class DropDataConnectorAnalyzer extends BaseSemanticAnalyzer {
  public DropDataConnectorAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String connectorName = unescapeIdentifier(root.getChild(0).getText());
    boolean ifExists = root.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null;

    DataConnector connector = getDataConnector(connectorName, !ifExists);
    if (connector == null) {
      return;
    }

    inputs.add(new ReadEntity(connector));
    // Neither DummyTxnManager nor DbTxnManageer acquire any locks with `DATACONNECTOR` type
    outputs.add(new WriteEntity(connector, WriteEntity.WriteType.DDL_NO_LOCK));

    DropDataConnectorDesc desc = new DropDataConnectorDesc(connectorName, ifExists);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
