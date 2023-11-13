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

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.COWWithClauseBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlBuilderFactory;

public class CopyOnWriteDeleteRewriter implements Rewriter<DeleteStatement> {

  private final HiveConf conf;
  protected final SqlBuilderFactory sqlBuilderFactory;
  private final COWWithClauseBuilder cowWithClauseBuilder;

  public CopyOnWriteDeleteRewriter(
      HiveConf conf, SqlBuilderFactory sqlBuilderFactory, COWWithClauseBuilder cowWithClauseBuilder) {
    this.sqlBuilderFactory = sqlBuilderFactory;
    this.conf = conf;
    this.cowWithClauseBuilder = cowWithClauseBuilder;
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, DeleteStatement deleteBlock)
      throws SemanticException {

    Tree wherePredicateNode = deleteBlock.getWhereTree().getChild(0);
    String whereClause = context.getTokenRewriteStream().toString(
        wherePredicateNode.getTokenStartIndex(), wherePredicateNode.getTokenStopIndex());
    String filePathCol = HiveUtils.unparseIdentifier(VirtualColumn.FILE_PATH.name(), conf);

    MultiInsertSqlBuilder sqlBuilder = sqlBuilderFactory.createSqlBuilder();

    cowWithClauseBuilder.appendWith(sqlBuilder, filePathCol, whereClause);

    sqlBuilder.append("insert into table ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());
    sqlBuilder.appendPartitionColsOfTarget();

    sqlBuilder.append(" select ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.DELETE);
    sqlBuilder.removeLastChar();

    sqlBuilder.append(" from ");
    sqlBuilder.append(sqlBuilder.getTargetTableFullName());

    // Add the inverted where clause, since we want to hold the records which doesn't satisfy the condition.
    sqlBuilder.append("\nwhere NOT (").append(whereClause).append(")");
    sqlBuilder.append("\n");
    // Add the file path filter that matches the delete condition.
    sqlBuilder.append("AND ").append(filePathCol);
    sqlBuilder.append(" IN ( select ").append(filePathCol).append(" from t )");
    sqlBuilder.append("\nunion all");
    sqlBuilder.append("\nselect * from t");

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlBuilder.toString());
    Context rewrittenCtx = rr.rewrittenCtx;

    rewrittenCtx.setOperation(Context.Operation.DELETE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.DELETE);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
