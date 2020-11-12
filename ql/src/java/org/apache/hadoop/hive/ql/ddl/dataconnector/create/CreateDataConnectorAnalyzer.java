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

package org.apache.hadoop.hive.ql.ddl.dataconnector.create;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for dataconnector creation commands.
 */
@DDLType(types = HiveParser.TOK_CREATEDATACONNECTOR)
public class CreateDataConnectorAnalyzer extends BaseSemanticAnalyzer {
  public CreateDataConnectorAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    boolean ifNotExists = false;
    String comment = null;
    String url = null;
    String type = null;
    Map<String, String> props = null;

    String connectorName = unescapeIdentifier(root.getChild(0).getText());
    for (int i = 1; i < root.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) root.getChild(i);
      switch (childNode.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_DATACONNECTORCOMMENT:
        comment = unescapeSQLString(childNode.getChild(0).getText());
        break;
      case HiveParser.TOK_DATACONNECTORPROPERTIES:
        props = getProps((ASTNode) childNode.getChild(0));
        break;
      case HiveParser.TOK_DATACONNECTORURL:
        url = unescapeSQLString(childNode.getChild(0).getText());
        // outputs.add(toWriteEntity(url));
        break;
      case HiveParser.TOK_DATACONNECTORTYPE:
        type = unescapeSQLString(childNode.getChild(0).getText());
        break;
      default:
        throw new SemanticException("Unrecognized token in CREATE CONNECTOR statement");
      }
    }

    CreateDataConnectorDesc desc = null;
    DataConnector connector = new DataConnector(connectorName, type, url);
    if (comment != null)
      connector.setDescription(comment);
    if (props != null)
      connector.setParameters(props);

    desc = new CreateDataConnectorDesc(connectorName, type, url, ifNotExists, comment, props);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    outputs.add(new WriteEntity(connector, WriteEntity.WriteType.DDL_NO_LOCK));
  }
}
