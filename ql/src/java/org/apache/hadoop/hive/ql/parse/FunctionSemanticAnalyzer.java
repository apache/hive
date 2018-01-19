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

package org.apache.hadoop.hive.ql.parse;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.ReloadFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DropFunctionDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

/**
 * FunctionSemanticAnalyzer.
 *
 */
public class FunctionSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionSemanticAnalyzer.class);
  private static final Logger SESISON_STATE_LOG= LoggerFactory.getLogger("SessionState");

  public FunctionSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    if (ast.getType() == HiveParser.TOK_CREATEFUNCTION) {
      analyzeCreateFunction(ast);
    } else if (ast.getType() == HiveParser.TOK_DROPFUNCTION) {
      analyzeDropFunction(ast);
    } else if (ast.getType() == HiveParser.TOK_RELOADFUNCTION) {
      rootTasks.add(TaskFactory.get(new FunctionWork(new ReloadFunctionDesc()), conf));
    }

    LOG.info("analyze done");
  }

  private void analyzeCreateFunction(ASTNode ast) throws SemanticException {
    // ^(TOK_CREATEFUNCTION identifier StringLiteral ({isTempFunction}? => TOK_TEMPORARY))
    String functionName = ast.getChild(0).getText().toLowerCase();
    boolean isTemporaryFunction = (ast.getFirstChildWithType(HiveParser.TOK_TEMPORARY) != null);
    String className = unescapeSQLString(ast.getChild(1).getText());

    // Temp functions are not allowed to have qualified names.
    if (isTemporaryFunction && FunctionUtils.isQualifiedFunctionName(functionName)) {
      throw new SemanticException("Temporary function cannot be created with a qualified name.");
    }

    // find any referenced resources
    List<ResourceUri> resources = getResourceList(ast);
    if (!isTemporaryFunction && resources == null) {
      SESISON_STATE_LOG.warn("permanent functions created without USING  clause will not be replicated.");
    }

    CreateFunctionDesc desc =
        new CreateFunctionDesc(functionName, isTemporaryFunction, className, resources, null);
    rootTasks.add(TaskFactory.get(new FunctionWork(desc), conf));

    addEntities(functionName, className, isTemporaryFunction, resources);
  }

  private void analyzeDropFunction(ASTNode ast) throws SemanticException {
    // ^(TOK_DROPFUNCTION identifier ifExists? $temp?)
    String functionName = ast.getChild(0).getText();
    boolean ifExists = (ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);
    // we want to signal an error if the function doesn't exist and we're
    // configured not to ignore this
    boolean throwException =
      !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);

    FunctionInfo info = FunctionRegistry.getFunctionInfo(functionName);
    if (info == null) {
      if (throwException) {
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
      } else {
        // Fail silently
        return;
      }
    } else if (info.isBuiltIn()) {
      throw new SemanticException(ErrorMsg.DROP_NATIVE_FUNCTION.getMsg(functionName));
    }

    boolean isTemporaryFunction = (ast.getFirstChildWithType(HiveParser.TOK_TEMPORARY) != null);
    DropFunctionDesc desc = new DropFunctionDesc(functionName, isTemporaryFunction, null);
    rootTasks.add(TaskFactory.get(new FunctionWork(desc), conf));

    addEntities(functionName, info.getClassName(), isTemporaryFunction, null);
  }

  private ResourceType getResourceType(ASTNode token) throws SemanticException {
    switch (token.getType()) {
      case HiveParser.TOK_JAR:
        return ResourceType.JAR;
      case HiveParser.TOK_FILE:
        return ResourceType.FILE;
      case HiveParser.TOK_ARCHIVE:
        return ResourceType.ARCHIVE;
      default:
        throw new SemanticException("Unexpected token " + token.toString());
    }
  }

  private List<ResourceUri> getResourceList(ASTNode ast) throws SemanticException {
    List<ResourceUri> resources = null;
    ASTNode resourcesNode = (ASTNode) ast.getFirstChildWithType(HiveParser.TOK_RESOURCE_LIST);

    if (resourcesNode != null) {
      resources = new ArrayList<ResourceUri>();
      for (int idx = 0; idx < resourcesNode.getChildCount(); ++idx) {
        // ^(TOK_RESOURCE_URI $resType $resPath)
        ASTNode resNode = (ASTNode) resourcesNode.getChild(idx);
        if (resNode.getToken().getType() != HiveParser.TOK_RESOURCE_URI) {
          throw new SemanticException("Expected token type TOK_RESOURCE_URI but found "
              + resNode.getToken().toString());
        }
        if (resNode.getChildCount() != 2) {
          throw new SemanticException("Expected 2 child nodes of TOK_RESOURCE_URI but found "
              + resNode.getChildCount());
        }
        ASTNode resTypeNode = (ASTNode) resNode.getChild(0);
        ASTNode resUriNode = (ASTNode) resNode.getChild(1);
        ResourceType resourceType = getResourceType(resTypeNode);
        resources.add(new ResourceUri(resourceType, PlanUtils.stripQuotes(resUriNode.getText())));
      }
    }

    return resources;
  }

  /**
   * Add write entities to the semantic analyzer to restrict function creation to privileged users.
   */
  private void addEntities(String functionName, String className, boolean isTemporaryFunction,
      List<ResourceUri> resources) throws SemanticException {
    // If the function is being added under a database 'namespace', then add an entity representing
    // the database (only applicable to permanent/metastore functions).
    // We also add a second entity representing the function name.
    // The authorization api implementation can decide which entities it wants to use to
    // authorize the create/drop function call.

    // Add the relevant database 'namespace' as a WriteEntity
    Database database = null;

    // temporary functions don't have any database 'namespace' associated with it,
    // it matters only for permanent functions
    if (!isTemporaryFunction) {
      try {
        String[] qualifiedNameParts = FunctionUtils.getQualifiedFunctionNameParts(functionName);
        String dbName = qualifiedNameParts[0];
        functionName = qualifiedNameParts[1];
        database = getDatabase(dbName);
      } catch (HiveException e) {
        LOG.error("Failed to get database ", e);
        throw new SemanticException(e);
      }
    }
    if (database != null) {
      outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_NO_LOCK));
    }

    // Add the function name as a WriteEntity
    outputs.add(new WriteEntity(database, functionName, className, Type.FUNCTION,
        WriteEntity.WriteType.DDL_NO_LOCK));

    if (resources != null) {
      for (ResourceUri resource : resources) {
        String uriPath = resource.getUri();
        outputs.add(toWriteEntity(uriPath));
      }
    }
  }
}
