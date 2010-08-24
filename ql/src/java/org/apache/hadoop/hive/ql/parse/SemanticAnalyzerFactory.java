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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * SemanticAnalyzerFactory.
 *
 */
public final class SemanticAnalyzerFactory {

  static HashMap<Integer, String> commandType = new HashMap<Integer, String>();
  static HashMap<Integer, String[]> tablePartitionCommandType = new HashMap<Integer, String[]>(); 

  static {
    commandType.put(HiveParser.TOK_EXPLAIN, "EXPLAIN");
    commandType.put(HiveParser.TOK_LOAD, "LOAD");
    commandType.put(HiveParser.TOK_CREATETABLE, "CREATETABLE");
    commandType.put(HiveParser.TOK_DROPTABLE, "DROPTABLE");
    commandType.put(HiveParser.TOK_DESCTABLE, "DESCTABLE");
    commandType.put(HiveParser.TOK_DESCFUNCTION, "DESCFUNCTION");
    commandType.put(HiveParser.TOK_MSCK, "MSCK");
    commandType.put(HiveParser.TOK_ALTERTABLE_ADDCOLS, "ALTERTABLE_ADDCOLS");
    commandType.put(HiveParser.TOK_ALTERTABLE_REPLACECOLS, "ALTERTABLE_REPLACECOLS");
    commandType.put(HiveParser.TOK_ALTERTABLE_RENAMECOL, "ALTERTABLE_RENAMECOL");
    commandType.put(HiveParser.TOK_ALTERTABLE_RENAME, "ALTERTABLE_RENAME");
    commandType.put(HiveParser.TOK_ALTERTABLE_DROPPARTS, "ALTERTABLE_DROPPARTS");
    commandType.put(HiveParser.TOK_ALTERTABLE_ADDPARTS, "ALTERTABLE_ADDPARTS");
    commandType.put(HiveParser.TOK_ALTERTABLE_TOUCH, "ALTERTABLE_TOUCH");
    commandType.put(HiveParser.TOK_ALTERTABLE_ARCHIVE, "ALTERTABLE_ARCHIVE");
    commandType.put(HiveParser.TOK_ALTERTABLE_UNARCHIVE, "ALTERTABLE_UNARCHIVE");
    commandType.put(HiveParser.TOK_ALTERTABLE_PROPERTIES, "ALTERTABLE_PROPERTIES");
    commandType.put(HiveParser.TOK_ALTERTABLE_SERIALIZER, "ALTERTABLE_SERIALIZER");
    commandType.put(HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES, "ALTERTABLE_SERDEPROPERTIES");
    commandType.put(HiveParser.TOK_SHOWTABLES, "SHOWTABLES");
    commandType.put(HiveParser.TOK_SHOW_TABLESTATUS, "SHOW_TABLESTATUS");
    commandType.put(HiveParser.TOK_SHOWFUNCTIONS, "SHOWFUNCTIONS");
    commandType.put(HiveParser.TOK_SHOWPARTITIONS, "SHOWPARTITIONS");
    commandType.put(HiveParser.TOK_SHOWLOCKS, "SHOWLOCKS");
    commandType.put(HiveParser.TOK_CREATEFUNCTION, "CREATEFUNCTION");
    commandType.put(HiveParser.TOK_DROPFUNCTION, "DROPFUNCTION");
    commandType.put(HiveParser.TOK_CREATEVIEW, "CREATEVIEW");
    commandType.put(HiveParser.TOK_DROPVIEW, "DROPVIEW");
    commandType.put(HiveParser.TOK_CREATEINDEX, "CREATEINDEX");
    commandType.put(HiveParser.TOK_DROPINDEX, "DROPINDEX");
    commandType.put(HiveParser.TOK_ALTERINDEX_REBUILD, "ALTERINDEX_REBUILD");
    commandType.put(HiveParser.TOK_ALTERVIEW_PROPERTIES, "ALTERVIEW_PROPERTIES");
    commandType.put(HiveParser.TOK_QUERY, "QUERY");
    commandType.put(HiveParser.TOK_LOCKTABLE, "LOCKTABLE");
    commandType.put(HiveParser.TOK_UNLOCKTABLE, "UNLOCKTABLE");
  }
  
  static {
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_ALTERPARTS_PROTECTMODE, 
        new String[] { "ALTERTABLE_PROTECTMODE", "ALTERPARTITION_PROTECTMODE" });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_FILEFORMAT,
        new String[] { "ALTERTABLE_FILEFORMAT", "ALTERPARTITION_FILEFORMAT" });
    tablePartitionCommandType.put(HiveParser.TOK_ALTERTABLE_LOCATION,
        new String[] { "ALTERTABLE_LOCATION", "ALTERPARTITION_LOCATION" });
  }
  

  public static BaseSemanticAnalyzer get(HiveConf conf, ASTNode tree)
      throws SemanticException {
    if (tree.getToken() == null) {
      throw new RuntimeException("Empty Syntax Tree");
    } else {
      setSessionCommandType(commandType.get(tree.getToken().getType()));

      switch (tree.getToken().getType()) {
      case HiveParser.TOK_EXPLAIN:
        return new ExplainSemanticAnalyzer(conf);
      case HiveParser.TOK_LOAD:
        return new LoadSemanticAnalyzer(conf);
      case HiveParser.TOK_DROPTABLE:
      case HiveParser.TOK_DROPVIEW:
      case HiveParser.TOK_DESCTABLE:
      case HiveParser.TOK_DESCFUNCTION:
      case HiveParser.TOK_MSCK:
      case HiveParser.TOK_ALTERTABLE_ADDCOLS:
      case HiveParser.TOK_ALTERTABLE_RENAMECOL:
      case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
      case HiveParser.TOK_ALTERTABLE_RENAME:
      case HiveParser.TOK_ALTERTABLE_DROPPARTS:
      case HiveParser.TOK_ALTERTABLE_ADDPARTS:
      case HiveParser.TOK_ALTERTABLE_PROPERTIES:
      case HiveParser.TOK_ALTERTABLE_SERIALIZER:
      case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
      case HiveParser.TOK_ALTERINDEX_REBUILD:
      case HiveParser.TOK_ALTERVIEW_PROPERTIES:
      case HiveParser.TOK_SHOWTABLES:
      case HiveParser.TOK_SHOW_TABLESTATUS:
      case HiveParser.TOK_SHOWFUNCTIONS:
      case HiveParser.TOK_SHOWPARTITIONS:
      case HiveParser.TOK_SHOWLOCKS:
      case HiveParser.TOK_CREATEINDEX:
      case HiveParser.TOK_DROPINDEX:
      case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
      case HiveParser.TOK_ALTERTABLE_TOUCH:
      case HiveParser.TOK_ALTERTABLE_ARCHIVE:
      case HiveParser.TOK_ALTERTABLE_UNARCHIVE:
      case HiveParser.TOK_LOCKTABLE:
      case HiveParser.TOK_UNLOCKTABLE:
        return new DDLSemanticAnalyzer(conf);
      case HiveParser.TOK_ALTERTABLE_PARTITION:
        String commandType = null;
        Integer type = ((ASTNode) tree.getChild(1)).getToken().getType();
        if (tree.getChild(0).getChildCount() > 1) {
          commandType = tablePartitionCommandType.get(type)[1];
        } else {
          commandType = tablePartitionCommandType.get(type)[0];
        }
        setSessionCommandType(commandType);
        return new DDLSemanticAnalyzer(conf);
      case HiveParser.TOK_CREATEFUNCTION:
      case HiveParser.TOK_DROPFUNCTION:
        return new FunctionSemanticAnalyzer(conf);
      default:
        return new SemanticAnalyzer(conf);
      }
    }
  }

  private static void setSessionCommandType(String commandType) {
    if (SessionState.get() != null) {
      SessionState.get().setCommandType(commandType);
    }
  }

  private SemanticAnalyzerFactory() {
    // prevent instantiation
  }
}
