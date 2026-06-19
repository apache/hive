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

package org.apache.hadoop.hive.ql.ddl.function.create;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.function.AbstractFunctionAnalyzer;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Analyzer for function creation commands.
 */
@DDLType(types = HiveParser.TOK_CREATEFUNCTION)
public class CreateFunctionAnalyzer extends AbstractFunctionAnalyzer {
  private static final Logger SESSION_STATE_LOG = LoggerFactory.getLogger("SessionState");

  public CreateFunctionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String functionName = root.getChild(0).getText().toLowerCase();
    boolean isTemporary = (root.getFirstChildWithType(HiveParser.TOK_TEMPORARY) != null);
    if (isTemporary && FunctionUtils.isQualifiedFunctionName(functionName)) {
      throw new SemanticException("Temporary function cannot be created with a qualified name.");
    }

    String className = unescapeSQLString(root.getChild(1).getText());

    List<ResourceUri> resources = getResourceList(root);
    if (!isTemporary && resources == null) {
      SESSION_STATE_LOG.warn("permanent functions created without USING  clause will not be replicated.");
    }

    CreateFunctionDesc desc = new CreateFunctionDesc(functionName, className, isTemporary, resources, null);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    addEntities(functionName, className, isTemporary, resources);
  }

  private static final Map<Integer, ResourceType> TOKEN_TYPE_TO_RESOURCE_TYPE = ImmutableMap.of(
      HiveParser.TOK_JAR, ResourceType.JAR,
      HiveParser.TOK_FILE, ResourceType.FILE,
      HiveParser.TOK_ARCHIVE, ResourceType.ARCHIVE);

  private List<ResourceUri> getResourceList(ASTNode ast) throws SemanticException {
    List<ResourceUri> resources = null;

    ASTNode resourcesNode = (ASTNode) ast.getFirstChildWithType(HiveParser.TOK_RESOURCE_LIST);
    if (resourcesNode != null) {
      resources = new ArrayList<ResourceUri>();
      for (int idx = 0; idx < resourcesNode.getChildCount(); ++idx) {
        // ^(TOK_RESOURCE_URI $resType $resPath)
        ASTNode node = (ASTNode) resourcesNode.getChild(idx);
        if (node.getToken().getType() != HiveParser.TOK_RESOURCE_URI) {
          throw new SemanticException("Expected token type TOK_RESOURCE_URI but found " + node.getToken().toString());
        }
        if (node.getChildCount() != 2) {
          throw new SemanticException("Expected 2 child nodes of TOK_RESOURCE_URI but found " + node.getChildCount());
        }

        ASTNode resourceTypeNode = (ASTNode) node.getChild(0);
        ASTNode resourceUriNode = (ASTNode) node.getChild(1);
        ResourceType resourceType = TOKEN_TYPE_TO_RESOURCE_TYPE.get(resourceTypeNode.getType());
        if (resourceType == null) {
          throw new SemanticException("Unexpected token " + resourceTypeNode);
        }

        resources.add(new ResourceUri(resourceType, PlanUtils.stripQuotes(resourceUriNode.getText())));
      }
    }

    return resources;
  }
}
