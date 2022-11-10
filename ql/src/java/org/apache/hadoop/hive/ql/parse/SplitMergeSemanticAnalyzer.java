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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A subclass of the {@link MergeSemanticAnalyzer} that just handles
 * merge statements. This version of rewrite adds two insert branches for the update clause one for
 * inserting new values of updated records and one for inserting the deleted delta records of updated records.
 */
public class SplitMergeSemanticAnalyzer extends MergeSemanticAnalyzer {

  SplitMergeSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected int addDestNamePrefixOfUpdate(int insClauseIdx, Context rewrittenCtx) {
    rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
    rewrittenCtx.addDeleteOfUpdateDestNamePrefix(insClauseIdx + 1, Context.DestClausePrefix.DELETE);
    return 2;
  }

  @Override
  public void analyze(ASTNode tree, Table targetTable, ASTNode tableNameNode) throws SemanticException {
    analyzeMerge(tree, targetTable, tableNameNode);
  }

  @Override
  protected String handleUpdate(ASTNode whenMatchedUpdateClause, StringBuilder rewrittenQueryStr,
                                String onClauseAsString, String deleteExtraPredicate, String hintStr,
                                ColumnAppender columnAppender, String targetName, List<String> values) {
    rewrittenQueryStr.append("    -- update clause (insert part)\n");
    appendInsertBranch(rewrittenQueryStr, hintStr, values);

    String extraPredicate = addWhereClauseOfUpdate(
            rewrittenQueryStr, onClauseAsString, whenMatchedUpdateClause, deleteExtraPredicate);

    rewrittenQueryStr.append("\n");

    rewrittenQueryStr.append("    -- update clause (delete part)\n");
    handleDelete(whenMatchedUpdateClause, rewrittenQueryStr, onClauseAsString,
            deleteExtraPredicate, hintStr, columnAppender);

    return extraPredicate;
  }

  /**
   * @param onClauseAsString - because there is no clone() and we need to use in multiple places
   * @param updateExtraPredicate - see notes at caller
   */
  @Override
  protected String handleDelete(ASTNode whenMatchedDeleteClause, StringBuilder rewrittenQueryStr,
                                String onClauseAsString, String updateExtraPredicate,
                                String hintStr, ColumnAppender columnAppender) {
    assert (
            getWhenClauseOperation(whenMatchedDeleteClause).getType() == HiveParser.TOK_UPDATE) ||
            getWhenClauseOperation(whenMatchedDeleteClause).getType() == HiveParser.TOK_DELETE;

    return super.handleDelete(
            whenMatchedDeleteClause, rewrittenQueryStr, onClauseAsString, updateExtraPredicate, hintStr, columnAppender);
  }

  @Override
  protected boolean allowOutputMultipleTimes() {
    return true;
  }
}
