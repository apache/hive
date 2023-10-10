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

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlBuilderFactory;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.WhereClausePatcher;

public class DeleteRewriter implements Rewriter<DeleteStatement> {

  protected final SqlBuilderFactory sqlBuilderFactory;
  private final WhereClausePatcher whereClausePatcher;

  public DeleteRewriter(SqlBuilderFactory sqlBuilderFactory, WhereClausePatcher whereClausePatcher) {
    this.sqlBuilderFactory = sqlBuilderFactory;
    this.whereClausePatcher = whereClausePatcher;
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, DeleteStatement deleteBlock)
      throws SemanticException {
    MultiInsertSqlBuilder sqlBuilder = sqlBuilderFactory.createSqlBuilder();

    sqlBuilder.append("insert into table ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());
    sqlBuilder.appendPartitionColsOfTarget();

    sqlBuilder.append(" select ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.DELETE);
    sqlBuilder.removeLastChar();
    sqlBuilder.append(" from ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());

    sqlBuilder.appendSortBy(sqlBuilder.getSortKeys());

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlBuilder.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode)rewrittenTree.getChildren().get(1);
    rewrittenCtx.setOperation(Context.Operation.DELETE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.DELETE);

    if (deleteBlock.getWhereTree() != null) {
      whereClausePatcher.patch(rewrittenInsert, deleteBlock.getWhereTree());
    }

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
