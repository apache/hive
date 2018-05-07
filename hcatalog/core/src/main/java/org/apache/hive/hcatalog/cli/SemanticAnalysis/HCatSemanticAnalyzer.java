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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.DropDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.ShowDatabasesDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowTableStatusDesc;
import org.apache.hadoop.hive.ql.plan.ShowTablesDesc;
import org.apache.hadoop.hive.ql.plan.SwitchDatabaseDesc;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatException;

import java.io.Serializable;
import java.util.List;

public class HCatSemanticAnalyzer extends HCatSemanticAnalyzerBase {

  private AbstractSemanticAnalyzerHook hook;
  private ASTNode ast;


  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
    throws SemanticException {

    this.ast = ast;
    switch (ast.getToken().getType()) {

    // HCat wants to intercept following tokens and special-handle them.
    case HiveParser.TOK_CREATETABLE:
      hook = new CreateTableHook();
      return hook.preAnalyze(context, ast);

    case HiveParser.TOK_CREATEDATABASE:
      hook = new CreateDatabaseHook();
      return hook.preAnalyze(context, ast);

    case HiveParser.TOK_ALTERTABLE:
      if (((ASTNode) ast.getChild(1)).getToken().getType() == HiveParser.TOK_ALTERTABLE_FILEFORMAT) {
        return ast;
      } else if (((ASTNode) ast.getChild(1)).getToken().getType() == HiveParser.TOK_ALTERTABLE_MERGEFILES) {
        // unsupported
        throw new SemanticException("Operation not supported.");
      } else {
        return ast;
      }

      // HCat will allow these operations to be performed.
      // Database DDL
    case HiveParser.TOK_SHOWDATABASES:
    case HiveParser.TOK_DROPDATABASE:
    case HiveParser.TOK_SWITCHDATABASE:
    case HiveParser.TOK_DESCDATABASE:
    case HiveParser.TOK_ALTERDATABASE_PROPERTIES:

      // View DDL
      // "alter view add partition" does not work because of the nature of implementation
      // of the DDL in hive. Hive will internally invoke another Driver on the select statement,
      // and HCat does not let "select" statement through. I cannot find a way to get around it
      // without modifying hive code. So just leave it unsupported.
      //case HiveParser.TOK_ALTERVIEW_ADDPARTS:
    case HiveParser.TOK_ALTERVIEW_DROPPARTS:
    case HiveParser.TOK_ALTERVIEW_PROPERTIES:
    case HiveParser.TOK_ALTERVIEW_RENAME:
    case HiveParser.TOK_ALTERVIEW:
    case HiveParser.TOK_CREATEVIEW:
    case HiveParser.TOK_DROPVIEW:

      // Authorization DDL
    case HiveParser.TOK_CREATEROLE:
    case HiveParser.TOK_DROPROLE:
    case HiveParser.TOK_GRANT_ROLE:
    case HiveParser.TOK_GRANT_WITH_OPTION:
    case HiveParser.TOK_GRANT:
    case HiveParser.TOK_REVOKE_ROLE:
    case HiveParser.TOK_REVOKE:
    case HiveParser.TOK_SHOW_GRANT:
    case HiveParser.TOK_SHOW_ROLE_GRANT:

      // Misc DDL
    case HiveParser.TOK_LOCKTABLE:
    case HiveParser.TOK_UNLOCKTABLE:
    case HiveParser.TOK_SHOWLOCKS:
    case HiveParser.TOK_DESCFUNCTION:
    case HiveParser.TOK_SHOWFUNCTIONS:
    case HiveParser.TOK_EXPLAIN:

      // Table DDL
    case HiveParser.TOK_ALTERTABLE_ADDPARTS:
    case HiveParser.TOK_ALTERTABLE_ADDCOLS:
    case HiveParser.TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION:
    case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
    case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
    case HiveParser.TOK_ALTERTABLE_DROPPARTS:
    case HiveParser.TOK_ALTERTABLE_PROPERTIES:
    case HiveParser.TOK_ALTERTABLE_RENAME:
    case HiveParser.TOK_ALTERTABLE_RENAMECOL:
    case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
    case HiveParser.TOK_ALTERTABLE_SERIALIZER:
    case HiveParser.TOK_ALTERTABLE_TOUCH:
    case HiveParser.TOK_DESCTABLE:
    case HiveParser.TOK_DROPTABLE:
    case HiveParser.TOK_SHOW_TABLESTATUS:
    case HiveParser.TOK_SHOWPARTITIONS:
    case HiveParser.TOK_SHOWTABLES:
      return ast;

    // In all other cases, throw an exception. Its a white-list of allowed operations.
    default:
      throw new SemanticException("Operation not supported.");

    }
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
              List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    try {

      switch (ast.getToken().getType()) {

      case HiveParser.TOK_CREATETABLE:
      case HiveParser.TOK_CREATEDATABASE:

        // HCat will allow these operations to be performed.
        // Database DDL
      case HiveParser.TOK_SHOWDATABASES:
      case HiveParser.TOK_DROPDATABASE:
      case HiveParser.TOK_SWITCHDATABASE:
      case HiveParser.TOK_DESCDATABASE:
      case HiveParser.TOK_ALTERDATABASE_PROPERTIES:

        break;

        // View DDL
        //case HiveParser.TOK_ALTERVIEW_ADDPARTS:
      case HiveParser.TOK_ALTERVIEW:
        switch (ast.getChild(1).getType()) {
          case HiveParser.TOK_ALTERVIEW_ADDPARTS:
          case HiveParser.TOK_ALTERVIEW_DROPPARTS:
          case HiveParser.TOK_ALTERVIEW_RENAME:
          case HiveParser.TOK_ALTERVIEW_PROPERTIES:
          case HiveParser.TOK_ALTERVIEW_DROPPROPERTIES:
        }
        break;

      case HiveParser.TOK_CREATEVIEW:
      case HiveParser.TOK_DROPVIEW:

        // Authorization DDL
      case HiveParser.TOK_CREATEROLE:
      case HiveParser.TOK_DROPROLE:
      case HiveParser.TOK_GRANT_ROLE:
      case HiveParser.TOK_GRANT_WITH_OPTION:
      case HiveParser.TOK_GRANT:
      case HiveParser.TOK_REVOKE_ROLE:
      case HiveParser.TOK_REVOKE:
      case HiveParser.TOK_SHOW_GRANT:
      case HiveParser.TOK_SHOW_ROLE_GRANT:

        // Misc DDL
      case HiveParser.TOK_LOCKTABLE:
      case HiveParser.TOK_UNLOCKTABLE:
      case HiveParser.TOK_SHOWLOCKS:
      case HiveParser.TOK_DESCFUNCTION:
      case HiveParser.TOK_SHOWFUNCTIONS:
      case HiveParser.TOK_EXPLAIN:
        break;

        // Table DDL
      case HiveParser.TOK_ALTERTABLE:
        switch (ast.getChild(1).getType()) {
          case HiveParser.TOK_ALTERTABLE_ADDPARTS:
          case HiveParser.TOK_ALTERTABLE_ADDCOLS:
          case HiveParser.TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION:
          case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
          case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
          case HiveParser.TOK_ALTERTABLE_DROPPARTS:
          case HiveParser.TOK_ALTERTABLE_PROPERTIES:
          case HiveParser.TOK_ALTERTABLE_DROPPROPERTIES:
          case HiveParser.TOK_ALTERTABLE_RENAME:
          case HiveParser.TOK_ALTERTABLE_RENAMECOL:
          case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
          case HiveParser.TOK_ALTERTABLE_SERIALIZER:
          case HiveParser.TOK_ALTERTABLE_TOUCH:
          case HiveParser.TOK_ALTERTABLE_ARCHIVE:
          case HiveParser.TOK_ALTERTABLE_UNARCHIVE:
          case HiveParser.TOK_ALTERTABLE_EXCHANGEPARTITION:
          case HiveParser.TOK_ALTERTABLE_SKEWED:
          case HiveParser.TOK_ALTERTABLE_FILEFORMAT:
          case HiveParser.TOK_ALTERTABLE_LOCATION:
          case HiveParser.TOK_ALTERTABLE_MERGEFILES:
          case HiveParser.TOK_ALTERTABLE_RENAMEPART:
          case HiveParser.TOK_ALTERTABLE_SKEWED_LOCATION:
          case HiveParser.TOK_ALTERTABLE_BUCKETS:
          case HiveParser.TOK_ALTERTABLE_COMPACT:
        }
        break;

      case HiveParser.TOK_DESCTABLE:
      case HiveParser.TOK_DROPTABLE:
      case HiveParser.TOK_SHOW_TABLESTATUS:
      case HiveParser.TOK_SHOWPARTITIONS:
      case HiveParser.TOK_SHOWTABLES:
        break;

      default:
        throw new HCatException(ErrorType.ERROR_INTERNAL_EXCEPTION, "Unexpected token: " + ast.getToken());
      }

      authorizeDDL(context, rootTasks);

    } catch (HCatException e) {
      throw new SemanticException(e);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    if (hook != null) {
      hook.postAnalyze(context, rootTasks);
    }
  }

  private String extractTableName(String compoundName) {
    /*
    * the table name can potentially be a dot-format one with column names
    * specified as part of the table name. e.g. a.b.c where b is a column in
    * a and c is a field of the object/column b etc. For authorization
    * purposes, we should use only the first part of the dotted name format.
    *
    */

    String[] words = compoundName.split("\\.");
    return words[0];
  }

  @Override
  protected void authorizeDDLWork(HiveSemanticAnalyzerHookContext cntxt, Hive hive, DDLWork work)
    throws HiveException {
    // DB opereations, none of them are enforced by Hive right now.

    ShowDatabasesDesc showDatabases = work.getShowDatabasesDesc();
    if (showDatabases != null) {
      authorize(HiveOperation.SHOWDATABASES.getInputRequiredPrivileges(),
        HiveOperation.SHOWDATABASES.getOutputRequiredPrivileges());
    }

    DropDatabaseDesc dropDb = work.getDropDatabaseDesc();
    if (dropDb != null) {
      Database db = cntxt.getHive().getDatabase(dropDb.getDatabaseName());
      if (db != null){
        // if above returned a null, then the db does not exist - probably a
        // "drop database if exists" clause - don't try to authorize then.
        authorize(db, Privilege.DROP);
      }
    }

    DescDatabaseDesc descDb = work.getDescDatabaseDesc();
    if (descDb != null) {
      Database db = cntxt.getHive().getDatabase(descDb.getDatabaseName());
      authorize(db, Privilege.SELECT);
    }

    SwitchDatabaseDesc switchDb = work.getSwitchDatabaseDesc();
    if (switchDb != null) {
      Database db = cntxt.getHive().getDatabase(switchDb.getDatabaseName());
      authorize(db, Privilege.SELECT);
    }

    ShowTablesDesc showTables = work.getShowTblsDesc();
    if (showTables != null) {
      String dbName = showTables.getDbName() == null ? SessionState.get().getCurrentDatabase()
        : showTables.getDbName();
      authorize(cntxt.getHive().getDatabase(dbName), Privilege.SELECT);
    }

    ShowTableStatusDesc showTableStatus = work.getShowTblStatusDesc();
    if (showTableStatus != null) {
      String dbName = showTableStatus.getDbName() == null ? SessionState.get().getCurrentDatabase()
        : showTableStatus.getDbName();
      authorize(cntxt.getHive().getDatabase(dbName), Privilege.SELECT);
    }

    // TODO: add alter database support in HCat

    // Table operations.

    DropTableDesc dropTable = work.getDropTblDesc();
    if (dropTable != null) {
      if (dropTable.getPartSpecs() == null) {
        // drop table is already enforced by Hive. We only check for table level location even if the
        // table is partitioned.
      } else {
        //this is actually a ALTER TABLE DROP PARITITION statement
        for (DropTableDesc.PartSpec partSpec : dropTable.getPartSpecs()) {
          // partitions are not added as write entries in drop partitions in Hive
          Table table = hive.getTable(SessionState.get().getCurrentDatabase(), dropTable.getTableName());
          List<Partition> partitions = null;
          try {
            partitions = hive.getPartitionsByFilter(table, partSpec.getPartSpec().getExprString());
          } catch (Exception e) {
            throw new HiveException(e);
          }
          for (Partition part : partitions) {
            authorize(part, Privilege.DROP);
          }
        }
      }
    }

    AlterTableDesc alterTable = work.getAlterTblDesc();
    if (alterTable != null) {
      Table table = hive.getTable(SessionState.get().getCurrentDatabase(),
          Utilities.getDbTableName(alterTable.getOldName())[1], false);

      Partition part = null;
      if (alterTable.getPartSpec() != null) {
        part = hive.getPartition(table, alterTable.getPartSpec(), false);
      }

      String newLocation = alterTable.getNewLocation();

      /* Hcat requires ALTER_DATA privileges for ALTER TABLE LOCATION statements
      * for the old table/partition location and the new location.
      */
      if (alterTable.getOp() == AlterTableDesc.AlterTableTypes.ALTERLOCATION) {
        if (part != null) {
          authorize(part, Privilege.ALTER_DATA); // authorize for the old
          // location, and new location
          part.setLocation(newLocation);
          authorize(part, Privilege.ALTER_DATA);
        } else {
          authorize(table, Privilege.ALTER_DATA); // authorize for the old
          // location, and new location
          table.getTTable().getSd().setLocation(newLocation);
          authorize(table, Privilege.ALTER_DATA);
        }
      }
      //other alter operations are already supported by Hive
    }

    // we should be careful when authorizing table based on just the
    // table name. If columns have separate authorization domain, it
    // must be honored
    DescTableDesc descTable = work.getDescTblDesc();
    if (descTable != null) {
      String tableName = extractTableName(descTable.getTableName());
      authorizeTable(cntxt.getHive(), tableName, Privilege.SELECT);
    }

    ShowPartitionsDesc showParts = work.getShowPartsDesc();
    if (showParts != null) {
      String tableName = extractTableName(showParts.getTabName());
      authorizeTable(cntxt.getHive(), tableName, Privilege.SELECT);
    }
  }
}
