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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.COWWithClauseBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SetClausePatcher;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlBuilderFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CopyOnWriteUpdateRewriter implements Rewriter<UpdateStatement> {

  private final HiveConf conf;
  private final SqlBuilderFactory sqlBuilderFactory;
  private final COWWithClauseBuilder cowWithClauseBuilder;
  private final SetClausePatcher setClausePatcher;


  public CopyOnWriteUpdateRewriter(HiveConf conf, SqlBuilderFactory sqlBuilderFactory,
                                   COWWithClauseBuilder cowWithClauseBuilder, SetClausePatcher setClausePatcher) {
    this.conf = conf;
    this.sqlBuilderFactory = sqlBuilderFactory;
    this.cowWithClauseBuilder = cowWithClauseBuilder;
    this.setClausePatcher = setClausePatcher;
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateStatement updateBlock)
      throws SemanticException {

    Tree wherePredicateNode = updateBlock.getWhereTree().getChild(0);
    String whereClause = context.getTokenRewriteStream().toString(
        wherePredicateNode.getTokenStartIndex(), wherePredicateNode.getTokenStopIndex());
    String filePathCol = HiveUtils.unparseIdentifier("FILE__PATH", conf);

    MultiInsertSqlBuilder sqlBuilder = sqlBuilderFactory.createSqlBuilder();

    cowWithClauseBuilder.appendWith(sqlBuilder, filePathCol, whereClause);

    sqlBuilder.append("insert into table ");
    sqlBuilder.appendTargetTableName();
    sqlBuilder.appendPartitionColsOfTarget();

    int columnOffset = sqlBuilder.getDeleteValues(Context.Operation.UPDATE).size();
    sqlBuilder.append(" select ");
    sqlBuilder.appendAcidSelectColumns(Context.Operation.UPDATE);
    sqlBuilder.removeLastChar();

    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetCols().size());
    List<FieldSchema> nonPartCols = updateBlock.getTargetTable().getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      sqlBuilder.append(',');
      String name = nonPartCols.get(i).getName();
      ASTNode setCol = updateBlock.getSetCols().get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);
      sqlBuilder.append(identifier);
      sqlBuilder.append(" AS ").append(identifier);
      if (setCol != null) {
        // This is one of the columns we're setting, record it's position so we can come back
        // later and patch it up.
        // Add one to the index because the select has the ROW__ID as the first column.
        setColExprs.put(columnOffset + i, setCol);
      }
    }

    sqlBuilder.append(" from ");
    sqlBuilder.appendTargetTableName();

    if (updateBlock.getWhereTree() != null) {
      sqlBuilder.append("\nwhere ");
      sqlBuilder.append(whereClause);
      sqlBuilder.append("\nunion all");
      sqlBuilder.append("\nselect ");
      sqlBuilder.appendAcidSelectColumns(Context.Operation.DELETE);
      sqlBuilder.removeLastChar();
      sqlBuilder.append(" from ");
      sqlBuilder.appendTargetTableName();
      // Add the inverted where clause, since we want to hold the records which doesn't satisfy the condition.
      sqlBuilder.append("\nwhere NOT (").append(whereClause).append(")");
      sqlBuilder.append("\n").indent();
      // Add the file path filter that matches the delete condition.
      sqlBuilder.append("AND ").append(filePathCol);
      sqlBuilder.append(" IN ( select ").append(filePathCol).append(" from t )");
      sqlBuilder.append("\nunion all");
      sqlBuilder.append("\nselect * from t");
    }

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlBuilder.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode) new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
            rewrittenTree, HiveParser.TOK_FROM, HiveParser.TOK_SUBQUERY, HiveParser.TOK_UNIONALL).getChild(0).getChild(0)
        .getChild(1);

    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);

    setClausePatcher.patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
