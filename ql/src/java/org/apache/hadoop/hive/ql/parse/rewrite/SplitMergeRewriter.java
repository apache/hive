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
package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SplitMergeRewriter extends NativeMergeRewriter {
  public SplitMergeRewriter(MultiInsertSqlBuilder sqlBuilder, HiveConf conf) {
    super(sqlBuilder, conf);
  }

  @Override
  public void handleWhenMatchedUpdate(Table targetTable, String targetAlias, Map<String, String> newValues,
                                      String hintStr, String onClauseAsString,
                                      String extraPredicate, String deleteExtraPredicate) {
    sqlBuilder.append("    -- update clause (insert part)\n");
    List<String> values = new ArrayList<>(targetTable.getCols().size() + targetTable.getPartCols().size());
    addValues(targetTable, targetAlias, newValues, values);
    sqlBuilder.appendInsertBranch(hintStr, values);

    addWhereClauseOfUpdate(onClauseAsString, extraPredicate, deleteExtraPredicate);

    sqlBuilder.append("\n");

    sqlBuilder.append("    -- update clause (delete part)\n");
    handleWhenMatchedDelete(hintStr, onClauseAsString, extraPredicate, deleteExtraPredicate);
  }

  @Override
  public int addDestNamePrefixOfUpdate(int insClauseIdx, Context rewrittenCtx) {
    rewrittenCtx.addDestNamePrefix(insClauseIdx, Context.DestClausePrefix.INSERT);
    rewrittenCtx.addDeleteOfUpdateDestNamePrefix(insClauseIdx + 1, Context.DestClausePrefix.DELETE);
    return 2;
  }

  @Override
  public void setOperation(Context context) {
    context.setOperation(Context.Operation.MERGE, true);
  }
}
