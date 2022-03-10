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

package org.apache.hadoop.hive.ql.ddl.table.storage.skewed;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import com.google.common.collect.Sets;

/**
 * Analyzer for set skewed location commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_SKEWED_LOCATION)
public class AlterTableSetSkewedLocationAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableSetSkewedLocationAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    List<Node> locationNodes = command.getChildren();
    if (locationNodes == null) {
      throw new SemanticException(ErrorMsg.ALTER_TBL_SKEWED_LOC_NO_LOC.getMsg());
    }

    Map<List<String>, String> locations = new HashMap<>();
    for (Node locationNode : locationNodes) {
      List<Node> locationListNodes = ((ASTNode) locationNode).getChildren();
      if (locationListNodes == null) {
        throw new SemanticException(ErrorMsg.ALTER_TBL_SKEWED_LOC_NO_LOC.getMsg());
      }

      for (Node locationListNode : locationListNodes) {
        List<Node> locationMapNodes = ((ASTNode) locationListNode).getChildren();
        if (locationMapNodes == null) {
          throw new SemanticException(ErrorMsg.ALTER_TBL_SKEWED_LOC_NO_LOC.getMsg());
        }

        for (Node locationMapNode : locationMapNodes) {
          List<Node> locationMapNodeMaps = ((ASTNode) locationMapNode).getChildren();
          if ((locationMapNodeMaps == null) || (locationMapNodeMaps.size() != 2)) {
            throw new SemanticException(ErrorMsg.ALTER_TBL_SKEWED_LOC_NO_MAP.getMsg());
          }

          List<String> keyList = new LinkedList<String>();
          ASTNode node = (ASTNode) locationMapNodeMaps.get(0);
          if (node.getToken().getType() == HiveParser.TOK_TABCOLVALUES) {
            keyList = SkewedTableUtils.getSkewedValuesFromASTNode(node);
          } else if (isConstant(node)) {
            keyList.add(PlanUtils.stripQuotes(node.getText()));
          } else {
            throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
          }

          String newLocation =
              PlanUtils.stripQuotes(unescapeSQLString(((ASTNode) locationMapNodeMaps.get(1)).getText()));
          validateSkewedLocationString(newLocation);
          locations.put(keyList, newLocation);
          outputs.add(toWriteEntity(newLocation));
        }
      }
    }

    AbstractAlterTableDesc desc = new AlterTableSetSkewedLocationDesc(tableName, partitionSpec, locations);
    setAcidDdlDesc(getTable(tableName), desc);
    addInputsOutputsAlterTable(tableName, partitionSpec, desc, AlterTableType.SET_SKEWED_LOCATION, false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  private static final Set<Integer> CONSTANT_TYPES = Sets.newHashSet(HiveParser.Number, HiveParser.StringLiteral,
      HiveParser.IntegralLiteral, HiveParser.NumberLiteral, HiveParser.CharSetName, HiveParser.KW_TRUE,
      HiveParser.KW_FALSE);

  private boolean isConstant(ASTNode node) {
    return CONSTANT_TYPES.contains(node.getToken().getType());
  }

  private void validateSkewedLocationString(String location) throws SemanticException {
    try {
      URI locationUri = new URI(location);
      if (!locationUri.isAbsolute() || locationUri.getScheme() == null || locationUri.getScheme().trim().equals("")) {
        throw new SemanticException(location + " is not absolute or has no scheme information. " +
            "Please specify a complete absolute uri with scheme information.");
      }
    } catch (URISyntaxException e) {
      throw new SemanticException(e);
    }
  }
}
