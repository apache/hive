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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Utilities for semantic analyzers.
 */
public final class AnalyzeCommandUtils {
  private AnalyzeCommandUtils() {
    throw new UnsupportedOperationException("AnalyzeCommandUtils should not be instantiated");
  }

  public static boolean isPartitionLevelStats(ASTNode tree) {
    boolean isPartitioned = false;
    ASTNode child = (ASTNode) tree.getChild(0);
    if (child.getChildCount() > 1) {
      child = (ASTNode) child.getChild(1);
      if (child.getToken().getType() == HiveParser.TOK_PARTSPEC) {
        isPartitioned = true;
      }
    }
    return isPartitioned;
  }

  public static Table getTable(ASTNode tree, BaseSemanticAnalyzer sa) throws SemanticException {
    String tableName = ColumnStatsSemanticAnalyzer.getUnescapedName((ASTNode) tree.getChild(0).getChild(0));
    String currentDb = SessionState.get().getCurrentDatabase();
    String [] names = Utilities.getDbTableName(currentDb, tableName);
    return sa.getTable(names[0], names[1], true);
  }

  public static Map<String,String> getPartKeyValuePairsFromAST(Table tbl, ASTNode tree,
      HiveConf hiveConf) throws SemanticException {
    ASTNode child = ((ASTNode) tree.getChild(0).getChild(1));
    Map<String,String> partSpec = new HashMap<String, String>();
    if (child != null) {
      partSpec = BaseSemanticAnalyzer.getValidatedPartSpec(tbl, child, hiveConf, false);
    } //otherwise, it is the case of analyze table T compute statistics for columns;
    return partSpec;
  }
}