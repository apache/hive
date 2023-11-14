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
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlBuilderFactory;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class SplitMergeRewriter extends MergeRewriter {

  public SplitMergeRewriter(Hive db, HiveConf conf, SqlBuilderFactory sqlBuilderFactory) {
    super(db, conf, sqlBuilderFactory);
  }

  @Override
  protected MergeWhenClauseSqlBuilder createMergeSqlBuilder(
      MergeStatement mergeStatement, MultiInsertSqlBuilder sqlBuilder) {
    return new SplitMergeWhenClauseSqlBuilder(conf, sqlBuilder, mergeStatement);
  }

  static class SplitMergeWhenClauseSqlBuilder extends MergeWhenClauseSqlBuilder {

    SplitMergeWhenClauseSqlBuilder(HiveConf conf, MultiInsertSqlBuilder sqlBuilder, MergeStatement mergeStatement) {
      super(conf, sqlBuilder, mergeStatement);
    }

    @Override
    public void appendWhenMatchedUpdateClause(MergeStatement.UpdateClause updateClause) {
      Table targetTable = mergeStatement.getTargetTable();
      String targetAlias = mergeStatement.getTargetAlias();
      String onClauseAsString = mergeStatement.getOnClauseAsText();

      sqlBuilder.append("    -- update clause (insert part)\n");
      List<String> values = new ArrayList<>(targetTable.getCols().size() + targetTable.getPartCols().size());
      addValues(targetTable, targetAlias, updateClause.getNewValuesMap(), values);
      sqlBuilder.appendInsertBranch(hintStr, values);
      hintStr = null;

      addWhereClauseOfUpdate(
          onClauseAsString, updateClause.getExtraPredicate(), updateClause.getDeleteExtraPredicate(), sqlBuilder);

      sqlBuilder.append("\n");

      sqlBuilder.append("    -- update clause (delete part)\n");
      handleWhenMatchedDelete(onClauseAsString,
          updateClause.getExtraPredicate(), updateClause.getDeleteExtraPredicate(), hintStr, sqlBuilder);
    }
  }

  @Override
  public void setOperation(Context context) {
    context.setOperation(Context.Operation.MERGE, true);
  }

  @Override
  public List<Context.DestClausePrefix> getUpdateDestClausePrefixes() {
    return asList(Context.DestClausePrefix.INSERT, Context.DestClausePrefix.DELETE);
  }
}
