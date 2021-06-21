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

package org.apache.hadoop.hive.ql.ddl.dataconnector.alter.properties;

import java.util.Map;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.dataconnector.alter.AbstractAlterDataConnectorAnalyzer;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for connector set properties commands.
 */
@DDLType(types = HiveParser.TOK_ALTERDATACONNECTOR_PROPERTIES)
public class AlterDataConnectorSetPropertiesAnalyzer extends AbstractAlterDataConnectorAnalyzer {
  public AlterDataConnectorSetPropertiesAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String connectorName = unescapeIdentifier(root.getChild(0).getText());

    Map<String, String> dbProps = null;
    for (int i = 1; i < root.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) root.getChild(i);
      if (childNode.getToken().getType() == HiveParser.TOK_DATACONNECTORPROPERTIES) {
        dbProps = getProps((ASTNode) childNode.getChild(0));
        break;
      } else {
        throw new SemanticException("Unrecognized token in ALTER CONNECTOR statement");
      }
    }

    AlterDataConnectorSetPropertiesDesc desc = new AlterDataConnectorSetPropertiesDesc(connectorName, dbProps);
    addAlterDataConnectorDesc(desc);
  }
}
