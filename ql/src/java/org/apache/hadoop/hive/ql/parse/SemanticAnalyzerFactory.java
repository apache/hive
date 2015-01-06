/**
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

import java.util.HashMap;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * SemanticAnalyzerFactory.
 *
 */
public final class SemanticAnalyzerFactory {

  static HashMap<Integer, HiveOperation> commandType = new HashMap<Integer, HiveOperation>();
  static HashMap<Integer, HiveOperation[]> tablePartitionCommandType = new HashMap<Integer, HiveOperation[]>();

  static {
    commandType.put(HiveParser.TOK_EXPLAIN, HiveOperation.EXPLAIN);
    commandType.put(HiveParser.TOK_LOAD, HiveOperation.LOAD);
    commandType.put(HiveParser.TOK_EXPORT, HiveOperation.EXPORT);
    commandType.put(HiveParser.TOK_IMPORT, HiveOperation.IMPORT);
    commandType.put(HiveParser.TOK_CREATEDATABASE, HiveOperation.CREATEDATABASE);
    commandType.put(HiveParser.TOK_DROPDATABASE, HiveOperation.DROPDATABASE);
    commandType.put(HiveParser.TOK_SWITCHDATABASE, HiveOperation.SWITCHDATABASE);
    commandType.put(HiveParser.TOK_CREATETABLE, HiveOperation.CREATETABLE);
    commandType.put(HiveParser.TOK_TRUNCATETABLE, HiveOperation.TRUNCATETABLE);
    commandType.put(HiveParser.TOK_DROPTABLE, HiveOperation.DROPTABLE);
    commandType.put(HiveParser.TOK_DESCTABLE, HiveOperation.DESCTABLE);
    commandType.put(HiveParser.TOK_DESCFUNCTION, HiveOperation.DESCFUNCTION);
    commandType.put(HiveParser.TOK_MSCK, HiveOperation.MSCK);
    commandType.put(HiveParser.TOK_ALTERTABLE_ADDCOLS, HiveOperation.ALTERTABLE_ADDCOLS);
    commandType.put(HiveParser.TOK_ALTERTABLE_REPLACECOLS, HiveOperation.ALTERTABLE_REPLACECOLS);
    commandType.put(HiveParser.TOK_ALTERTABLE_RENAMECOL, HiveOperation.ALTERTABLE_RENAMECOL);
    commandType.put(HiveParser.TOK_ALTERTABLE_RENAME, HiveOperation.ALTERTABLE_RENAME);
    commandType.put(HiveParser.TOK_ALTERTABLE_DROPPARTS, HiveOperation.ALTERTABLE_DROPPARTS);
    commandType.put(HiveParser.TOK_ALTERTABLE_ADDPARTS, HiveOperation.ALTERTABLE_ADDPARTS);
    commandType.put(HiveParser.TOK_ALTERTABLE_TOUCH, HiveOperation.ALTERTABLE_TOUCH);
    commandType.put(HiveParser.TOK_ALTERTABLE_ARCHIVE, HiveOperation.ALTERTABLE_ARCHIVE);
    commandType.put(HiveParser.TOK_ALTERTABLE_UNARCHIVE, HiveOperation.ALTERTABLE_UNARCHIVE);
    commandType.put(HiveParser.TOK_ALTERTABLE_PROPERTIES, HiveOperation.ALTERTABLE_PROPERTIES);
    commandType.put(HiveParser.TOK_ALTERTABLE_DROPPROPERTIES, HiveOperation.ALTERTABLE_PROPERTIES);
    commandType.put(HiveParser.TOK_SHOWDATABASES, HiveOperation.SHOWDATABASES);
    commandType.put(HiveParser.TOK_SHOWTABLES, HiveOperation.SHOWTABLES);
    commandType.put(HiveParser.TOK_SHOWCOLUMNS, HiveOperation.SHOWCOLUMNS);
    commandType.put(HiveParser.TOK_SHOW_TABLESTATUS, HiveOperation.SHOW_TABLESTATUS);
    commandType.put(HiveParser.TOK_SHOW_TBLPROPERTIES, HiveOperation.SHOW_TBLPROPERTIES);
    commandType.put(HiveParser.TOK_SHOW_CREATETABLE, HiveOperation.SHOW_CREATETABLE);
    commandType.put(HiveParser.TOK_SHOWFUNCTIONS, HiveOperation.SHOWFUNCTIONS);
    commandType.put(HiveParser.TOK_SHOWINDEXES, HiveOperation.SHOWINDEXES);
    commandType.put(HiveParser.TOK_SHOWPARTITIONS, HiveOperation.SHOWPARTITIONS);
    commandType.put(HiveParser.TOK_SHOWLOCKS, HiveOperation.SHOWLOCKS);
    commandType.put(HiveParser.TOK_SHOWDBLOCKS, HiveOperation.SHOWLOCKS);
    commandType.put(HiveParser.TOK_SHOWCONF, HiveOperation.SHOWCONF);
    commandType.put(HiveParser.TOK_CREATEFUNCTION, HiveOperation.CREATEFUNCTION);
    commandType.put(HiveParser.TOK_DROPFUNCTION, HiveOperation.DROPFUNCTION);
    commandType.put(HiveParser.TOK_CREATEMACRO, HiveOperation.CREATEMACRO);
    commandType.put(HiveParser.TOK_DROPMACRO, HiveOperation.DROPMACRO);
    commandType.put(HiveParser.TOK_CREATEVIEW, HiveOperation.CREATEVIEW);
    commandType.put(HiveParser.TOK_DROPVIEW, HiveOperation.DROPVIEW);
    commandType.put(HiveParser.TOK_CREATEINDEX, HiveOperation.CREATEINDEX);
    commandType.put(HiveParser.TOK_DROPINDEX, HiveOperation.DROPINDEX);
    commandType.put(HiveParser.TOK_ALTERINDEX_REBUILD, HiveOperation.ALTERINDEX_REBUILD);
    commandType.put(HiveParser.TOK_ALTERINDEX_PROPERTIES, HiveOperation.ALTERINDEX_PROPS);
    commandType.put(HiveParser.TOK_ALTERVIEW_PROPERTIES, HiveOperation.ALTERVIEW_PROPERTIES);
    commandType.put(HiveParser.TOK_ALTERVIEW_DROPPROPERTIES, HiveOperation.ALTERVIEW_PROPERTIES);
    commandType.put(HiveParser.TOK_ALTERVIEW_ADDPARTS, HiveOperation.ALTERTABLE_ADDPARTS);
    commandType.put(HiveParser.TOK_ALTERVIEW_DROPPARTS, HiveOperation.ALTERTABLE_DROPPARTS);
    commandType.put(HiveParser.TOK_ALTERVIEW_RENAME, HiveOperation.ALTERVIEW_RENAME);
    commandType.put(HiveParser.TOK_ALTERVIEW, HiveOperation.ALTERVIEW_AS);
    commandType.put(HiveParser.TOK_QUERY, HiveOperation.QUERY);
    commandType.put(HiveParser.TOK_LOCKTABLE, HiveOperation.LOCKTABLE);
    commandType.put(HiveParser.TOK_UNLOCKTABLE, HiveOperation.UNLOCKTABLE);
    commandType.put(HiveParser.TOK_LOCKDB, HiveOperation.LOCKDB);
    commandType.put(HiveParser.TOK_UNLOCKDB, HiveOperation.UNLOCKDB);
    commandType.put(HiveParser.TOK_CREATEROLE, HiveOperation.CREATEROLE);
    commandType.put(HiveParser.TOK_DROPROLE, HiveOperation.DROPROLE);
    commandType.put(HiveParser.TOK_GRANT, HiveOperation.GRANT_PRIVILEGE);
    commandType.put(HiveParser.TOK_REVOKE, HiveOperation.REVOKE_PRIVILEGE);
    commandType.put(HiveParser.TOK_SHOW_GRANT, HiveOperation.SHOW_GRANT);
    commandType.put(HiveParser.TOK_GRANT_ROLE, HiveOperation.GRANT_ROLE);
    commandType.put(HiveParser.TOK_REVOKE_ROLE, HiveOperation.REVOKE_ROLE);
    commandType.put(HiveParser.TOK_SHOW_ROLES, HiveOperation.SHOW_ROLES);
    commandType.put(HiveParser.TOK_SHOW_SET_ROLE, HiveOperation.SHOW_ROLES);
    commandType.put(HiveParser.TOK_SHOW_ROLE_PRINCIPALS, HiveOperation.SHOW_ROLE_PRINCIPALS);
    commandType.put(HiveParser.TOK_SHOW_ROLE_GRANT, HiveOperation.SHOW_ROLE_GRANT);
    commandType.put(HiveParser.TOK_ALTERDATABASE_PROPERTIES, HiveOperation.ALTERDATABASE);
    commandType.put(HiveParser.TOK_ALTERDATABASE_OWNER, HiveOperation.ALTERDATABASE_OWNER);
    commandType.put(HiveParser.TOK_DESCDATABASE, HiveOperation.DESCDATABASE);
    commandType.put(HiveParser.TOK_ALTERTABLE_SKEWED, HiveOperation.ALTERTABLE_SKEWED);
    commandType.put(HiveParser.TOK_ANALYZE, HiveOperation.ANALYZE_TABLE);
    commandType.put(HiveParser.TOK_ALTERTABLE_PARTCOLTYPE, HiveOperation.ALTERTABLE_PARTCOLTYPE);
    commandType.put(HiveParser.TOK_SHOW_COMPACTIONS, HiveOperation.SHOW_COMPACTIONS);
    commandType.put(HiveParser.TOK_SHOW_TRANSACTIONS, HiveOperation.SHOW_TRANSACTIONS);
  }

  static {
    tablePartitionCommandType.put(
        HiveParser.TOK_ALTERTABLE_PROTECTMODE,
        new HiveOperation[] { HiveOperation.ALTERTABLE_PROTECTMODE,
            HiveOperation.ALTERPARTITION_PROTECTMODE });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_FILEFORMAT,
        new HiveOperation[] { HiveOperation.ALTERTABLE_FILEFORMAT,
            HiveOperation.ALTERPARTITION_FILEFORMAT });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_LOCATION,
        new HiveOperation[] { HiveOperation.ALTERTABLE_LOCATION,
            HiveOperation.ALTERPARTITION_LOCATION });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_MERGEFILES,
        new HiveOperation[] {HiveOperation.ALTERTABLE_MERGEFILES,
            HiveOperation.ALTERPARTITION_MERGEFILES });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_SERIALIZER,
        new HiveOperation[] {HiveOperation.ALTERTABLE_SERIALIZER,
            HiveOperation.ALTERPARTITION_SERIALIZER });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES,
        new HiveOperation[] {HiveOperation.ALTERTABLE_SERDEPROPERTIES,
            HiveOperation.ALTERPARTITION_SERDEPROPERTIES });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_RENAMEPART,
        new HiveOperation[] {null, HiveOperation.ALTERTABLE_RENAMEPART});
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_COMPACT,
        new HiveOperation[] {HiveOperation.ALTERTABLE_COMPACT, HiveOperation.ALTERTABLE_COMPACT});
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_SKEWED_LOCATION,
        new HiveOperation[] {HiveOperation.ALTERTBLPART_SKEWED_LOCATION,
            HiveOperation.ALTERTBLPART_SKEWED_LOCATION });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_BUCKETS,
        new HiveOperation[] {HiveOperation.ALTERTABLE_BUCKETNUM,
            HiveOperation.ALTERPARTITION_BUCKETNUM});
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_CLUSTER_SORT,
        new HiveOperation[] {HiveOperation.ALTERTABLE_CLUSTER_SORT,
            HiveOperation.ALTERTABLE_CLUSTER_SORT});
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_UPDATECOLSTATS,
        new HiveOperation[] {HiveOperation.ALTERTABLE_UPDATETABLESTATS,
            HiveOperation.ALTERTABLE_UPDATEPARTSTATS});
  }

  public static BaseSemanticAnalyzer get(HiveConf conf, ASTNode tree)
      throws SemanticException {
    if (tree.getToken() == null) {
      throw new RuntimeException("Empty Syntax Tree");
    } else {
      setSessionCommandType(commandType.get(tree.getType()));

      switch (tree.getType()) {
      case HiveParser.TOK_EXPLAIN:
        return new ExplainSemanticAnalyzer(conf);
      case HiveParser.TOK_EXPLAIN_SQ_REWRITE:
        return new ExplainSQRewriteSemanticAnalyzer(conf);
      case HiveParser.TOK_LOAD:
        return new LoadSemanticAnalyzer(conf);
      case HiveParser.TOK_EXPORT:
        return new ExportSemanticAnalyzer(conf);
      case HiveParser.TOK_IMPORT:
        return new ImportSemanticAnalyzer(conf);
      case HiveParser.TOK_ALTERTABLE: {
        Tree child = tree.getChild(1);
        switch (child.getType()) {
          case HiveParser.TOK_ALTERTABLE_RENAME:
          case HiveParser.TOK_ALTERTABLE_TOUCH:
          case HiveParser.TOK_ALTERTABLE_ARCHIVE:
          case HiveParser.TOK_ALTERTABLE_UNARCHIVE:
          case HiveParser.TOK_ALTERTABLE_ADDCOLS:
          case HiveParser.TOK_ALTERTABLE_RENAMECOL:
          case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
          case HiveParser.TOK_ALTERTABLE_DROPPARTS:
          case HiveParser.TOK_ALTERTABLE_ADDPARTS:
          case HiveParser.TOK_ALTERTABLE_PARTCOLTYPE:
          case HiveParser.TOK_ALTERTABLE_PROPERTIES:
          case HiveParser.TOK_ALTERTABLE_DROPPROPERTIES:
          case HiveParser.TOK_ALTERTABLE_EXCHANGEPARTITION:
          case HiveParser.TOK_ALTERTABLE_SKEWED:
          setSessionCommandType(commandType.get(child.getType()));
          return new DDLSemanticAnalyzer(conf);
        }
        HiveOperation commandType =
            tablePartitionCommandType.get(child.getType())[tree.getChildCount() > 2 ? 1 : 0];
        setSessionCommandType(commandType);
        return new DDLSemanticAnalyzer(conf);
      }
      case HiveParser.TOK_ALTERVIEW: {
        Tree child = tree.getChild(1);
        switch (child.getType()) {
          case HiveParser.TOK_ALTERVIEW_PROPERTIES:
          case HiveParser.TOK_ALTERVIEW_DROPPROPERTIES:
          case HiveParser.TOK_ALTERVIEW_ADDPARTS:
          case HiveParser.TOK_ALTERVIEW_DROPPARTS:
          case HiveParser.TOK_ALTERVIEW_RENAME:
            setSessionCommandType(commandType.get(child.getType()));
            return new DDLSemanticAnalyzer(conf);
        }
        // TOK_ALTERVIEW_AS
        assert child.getType() == HiveParser.TOK_QUERY;
        setSessionCommandType(HiveOperation.ALTERVIEW_AS);
        return new SemanticAnalyzer(conf);
      }
      case HiveParser.TOK_CREATEDATABASE:
      case HiveParser.TOK_DROPDATABASE:
      case HiveParser.TOK_SWITCHDATABASE:
      case HiveParser.TOK_DROPTABLE:
      case HiveParser.TOK_DROPVIEW:
      case HiveParser.TOK_DESCDATABASE:
      case HiveParser.TOK_DESCTABLE:
      case HiveParser.TOK_DESCFUNCTION:
      case HiveParser.TOK_MSCK:
      case HiveParser.TOK_ALTERINDEX_REBUILD:
      case HiveParser.TOK_ALTERINDEX_PROPERTIES:
      case HiveParser.TOK_SHOWDATABASES:
      case HiveParser.TOK_SHOWTABLES:
      case HiveParser.TOK_SHOWCOLUMNS:
      case HiveParser.TOK_SHOW_TABLESTATUS:
      case HiveParser.TOK_SHOW_TBLPROPERTIES:
      case HiveParser.TOK_SHOW_CREATETABLE:
      case HiveParser.TOK_SHOWFUNCTIONS:
      case HiveParser.TOK_SHOWPARTITIONS:
      case HiveParser.TOK_SHOWINDEXES:
      case HiveParser.TOK_SHOWLOCKS:
      case HiveParser.TOK_SHOWDBLOCKS:
      case HiveParser.TOK_SHOW_COMPACTIONS:
      case HiveParser.TOK_SHOW_TRANSACTIONS:
      case HiveParser.TOK_SHOWCONF:
      case HiveParser.TOK_CREATEINDEX:
      case HiveParser.TOK_DROPINDEX:
      case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
      case HiveParser.TOK_LOCKTABLE:
      case HiveParser.TOK_UNLOCKTABLE:
      case HiveParser.TOK_LOCKDB:
      case HiveParser.TOK_UNLOCKDB:
      case HiveParser.TOK_CREATEROLE:
      case HiveParser.TOK_DROPROLE:
      case HiveParser.TOK_GRANT:
      case HiveParser.TOK_REVOKE:
      case HiveParser.TOK_SHOW_GRANT:
      case HiveParser.TOK_GRANT_ROLE:
      case HiveParser.TOK_REVOKE_ROLE:
      case HiveParser.TOK_SHOW_ROLE_GRANT:
      case HiveParser.TOK_SHOW_ROLE_PRINCIPALS:
      case HiveParser.TOK_SHOW_ROLES:
      case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
      case HiveParser.TOK_ALTERDATABASE_OWNER:
      case HiveParser.TOK_TRUNCATETABLE:
      case HiveParser.TOK_SHOW_SET_ROLE:
        return new DDLSemanticAnalyzer(conf);

      case HiveParser.TOK_CREATEFUNCTION:
      case HiveParser.TOK_DROPFUNCTION:
        return new FunctionSemanticAnalyzer(conf);

      case HiveParser.TOK_ANALYZE:
        return new ColumnStatsSemanticAnalyzer(conf);

      case HiveParser.TOK_CREATEMACRO:
      case HiveParser.TOK_DROPMACRO:
        return new MacroSemanticAnalyzer(conf);

      case HiveParser.TOK_UPDATE_TABLE:
      case HiveParser.TOK_DELETE_FROM:
        return new UpdateDeleteSemanticAnalyzer(conf);

      default: {
        SemanticAnalyzer semAnalyzer = HiveConf
            .getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED) ? new CalcitePlanner(conf)
            : new SemanticAnalyzer(conf);
        return semAnalyzer;
      }
      }
    }
  }

  private static void setSessionCommandType(HiveOperation commandType) {
    if (SessionState.get() != null) {
      SessionState.get().setCommandType(commandType);
    }
  }

  private SemanticAnalyzerFactory() {
    // prevent instantiation
  }
}
