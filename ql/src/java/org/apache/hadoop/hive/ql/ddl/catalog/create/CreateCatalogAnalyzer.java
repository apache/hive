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

package org.apache.hadoop.hive.ql.ddl.catalog.create;

import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for catalog creation commands.
 */
@DDLSemanticAnalyzerFactory.DDLType(types = HiveParser.TOK_CREATECATALOG)
public class CreateCatalogAnalyzer extends BaseSemanticAnalyzer {
  public CreateCatalogAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String catalogName = unescapeIdentifier(root.getChild(0).getText());
    String locationUrl = unescapeSQLString(root.getChild(1).getChild(0).getText());
    outputs.add(toWriteEntity(locationUrl));

    boolean ifNotExists = false;
    String comment = null;

    for (int i = 2; i < root.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) root.getChild(i);
      switch (childNode.getToken().getType()) {
        case HiveParser.TOK_IFNOTEXISTS:
          ifNotExists = true;
          break;
        case HiveParser.TOK_CATALOGCOMMENT:
          comment = unescapeSQLString(childNode.getChild(0).getText());
          break;
        default:
          throw new SemanticException("Unrecognized token in CREATE CATALOG statement");
      }
    }

    CreateCatalogDesc desc = new CreateCatalogDesc(catalogName, comment, locationUrl, ifNotExists);
    Catalog catalog = new Catalog(catalogName, locationUrl);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
    outputs.add(new WriteEntity(catalog, WriteEntity.WriteType.DDL_NO_LOCK));
  }
}
