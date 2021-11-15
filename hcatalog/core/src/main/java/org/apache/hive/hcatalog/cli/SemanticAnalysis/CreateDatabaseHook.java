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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.database.create.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hive.hcatalog.common.HCatConstants;

final class CreateDatabaseHook extends HCatSemanticAnalyzerBase {

  String databaseName;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
    throws SemanticException {

    Hive db;
    try {
      db = context.getHive();
    } catch (HiveException e) {
      throw new SemanticException("Couldn't get Hive DB instance in semantic analysis phase.", e);
    }

    // Analyze and create tbl properties object
    int numCh = ast.getChildCount();

    databaseName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));

    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);

      switch (child.getToken().getType()) {

      case HiveParser.TOK_IFNOTEXISTS:
        try {
          List<String> dbs = db.getDatabasesByPattern(databaseName);
          if (dbs != null && dbs.size() > 0) { // db exists
            return ast;
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        break;
      }
    }

    return ast;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
              List<Task<?>> rootTasks) throws SemanticException {
    context.getConf().set(HCatConstants.HCAT_CREATE_DB_NAME, databaseName);
    super.postAnalyze(context, rootTasks);
  }

  @Override
  protected void authorizeDDLWork(HiveSemanticAnalyzerHookContext cntxt, Hive hive, DDLWork work)
      throws HiveException {
    DDLDesc ddlDesc = work.getDDLDesc();
    if (ddlDesc instanceof CreateDatabaseDesc) {
      CreateDatabaseDesc createDb = (CreateDatabaseDesc)ddlDesc;
      Database db = new Database(createDb.getName(), createDb.getComment(),
          createDb.getLocationUri(), createDb.getDatabaseProperties());
      authorize(db, Privilege.CREATE);
    }
  }
}
