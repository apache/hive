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
package org.apache.hcatalog.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hcatalog.common.AuthUtils;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatException;

public class HCatSemanticAnalyzer extends AbstractSemanticAnalyzerHook {

  private AbstractSemanticAnalyzerHook hook;
  private ASTNode ast;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {

    this.ast = ast;
    switch (ast.getToken().getType()) {

    // Howl wants to intercept following tokens and special-handle them.
    case HiveParser.TOK_CREATETABLE:
      hook = new CreateTableHook();
      return hook.preAnalyze(context, ast);

    case HiveParser.TOK_CREATEDATABASE:
      hook = new CreateDatabaseHook();
      return hook.preAnalyze(context, ast);

    // DML commands used in Howl where we use the same implementation as default Hive.
    case HiveParser.TOK_SHOWDATABASES:
    case HiveParser.TOK_DROPDATABASE:
    case HiveParser.TOK_SWITCHDATABASE:
      return ast;

    // Howl will allow these operations to be performed since they are DDL statements.
    case HiveParser.TOK_DROPTABLE:
    case HiveParser.TOK_DESCTABLE:
    case HiveParser.TOK_ALTERTABLE_ADDCOLS:
    case HiveParser.TOK_ALTERTABLE_RENAME:
    case HiveParser.TOK_ALTERTABLE_DROPPARTS:
    case HiveParser.TOK_ALTERTABLE_PROPERTIES:
    case HiveParser.TOK_ALTERTABLE_SERIALIZER:
    case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
    case HiveParser.TOK_SHOW_TABLESTATUS:
    case HiveParser.TOK_SHOWTABLES:
    case HiveParser.TOK_SHOWPARTITIONS:
      return ast;

    case HiveParser.TOK_ALTERTABLE_ADDPARTS:
      hook = new AddPartitionHook();
      return hook.preAnalyze(context, ast);

    case HiveParser.TOK_ALTERTABLE_PARTITION:
      if (((ASTNode)ast.getChild(1)).getToken().getType() == HiveParser.TOK_ALTERTABLE_FILEFORMAT) {
        hook = new AlterTableFileFormatHook();
        return hook.preAnalyze(context, ast);
      } else {
        return ast;
      }

    // In all other cases, throw an exception. Its a white-list of allowed operations.
    default:
      throw new SemanticException("Operation not supported.");

    }
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    try{

      switch (ast.getToken().getType()) {

      case HiveParser.TOK_DESCTABLE:
        authorize(getFullyQualifiedName((ASTNode) ast.getChild(0).getChild(0)), context, FsAction.READ, false);
        break;

      case HiveParser.TOK_SHOWPARTITIONS:
        authorize(BaseSemanticAnalyzer.getUnescapedName((ASTNode)ast.getChild(0)), context, FsAction.READ, false);
        break;

      case HiveParser.TOK_ALTERTABLE_ADDPARTS:
      case HiveParser.TOK_DROPTABLE:
      case HiveParser.TOK_ALTERTABLE_ADDCOLS:
      case HiveParser.TOK_ALTERTABLE_RENAME:
      case HiveParser.TOK_ALTERTABLE_DROPPARTS:
      case HiveParser.TOK_ALTERTABLE_PROPERTIES:
      case HiveParser.TOK_ALTERTABLE_SERIALIZER:
      case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
        authorize(BaseSemanticAnalyzer.getUnescapedName((ASTNode)ast.getChild(0)), context, FsAction.WRITE, false);
        break;

      case HiveParser.TOK_ALTERTABLE_PARTITION:
        authorize(BaseSemanticAnalyzer.unescapeIdentifier(((ASTNode)ast.getChild(0)).getChild(0).getText()), context, FsAction.WRITE, false);
        break;

      case HiveParser.TOK_SWITCHDATABASE:
        authorize(BaseSemanticAnalyzer.getUnescapedName((ASTNode)ast.getChild(0)), context, FsAction.READ, true);
        break;

      case HiveParser.TOK_DROPDATABASE:
        authorize(BaseSemanticAnalyzer.getUnescapedName((ASTNode)ast.getChild(0)), context, FsAction.WRITE, true);
        break;

      case HiveParser.TOK_CREATEDATABASE:
      case HiveParser.TOK_SHOWDATABASES:
      case HiveParser.TOK_SHOW_TABLESTATUS:
      case HiveParser.TOK_SHOWTABLES:
        // We do no checks for show tables/db , create db. Its always allowed.

      case HiveParser.TOK_CREATETABLE:
        // No checks for Create Table, since its not possible to compute location
        // here easily. So, it is especially handled in CreateTable post hook.
        break;

      default:
        throw new HCatException(ErrorType.ERROR_INTERNAL_EXCEPTION, "Unexpected token: "+ast.getToken());
      }
    } catch(HCatException e){
      throw new SemanticException(e);
    } catch (MetaException e) {
      throw new SemanticException(e);
    } catch (HiveException e) {
      throw new SemanticException(e);
  }

    if(hook != null){
      hook.postAnalyze(context, rootTasks);
    }
  }

  private void authorize(String name, HiveSemanticAnalyzerHookContext cntxt, FsAction action, boolean isDBOp)
                                                      throws MetaException, HiveException, HCatException{


    Warehouse wh = new Warehouse(cntxt.getConf());
    if(!isDBOp){
      // Do validations for table path.
      Table tbl;
      try{
        tbl = cntxt.getHive().getTable(name);
      }
      catch(InvalidTableException ite){
        // Table itself doesn't exist in metastore, nothing to validate.
        return;
      }
      Path path = tbl.getPath();
      if(path != null){
        AuthUtils.authorize(wh.getDnsPath(path), action, cntxt.getConf());
      } else{
        // This will happen, if table exists in metastore for a given
        // tablename, but has no path associated with it, so there is nothing to check.
        // In such cases, do no checks and allow whatever hive behavior is for it.
        return;
      }
    } else{
      // Else, its a DB operation.
      AuthUtils.authorize(wh.getDefaultDatabasePath(name), action, cntxt.getConf());
    }
  }


  private String getFullyQualifiedName(ASTNode ast) {
    // Copied verbatim from DDLSemanticAnalyzer, since its private there.
    if (ast.getChildCount() == 0) {
      return ast.getText();
    }

    return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
        + getFullyQualifiedName((ASTNode) ast.getChild(1));
  }
}
