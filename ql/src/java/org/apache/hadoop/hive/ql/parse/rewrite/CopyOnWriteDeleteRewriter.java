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
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.COWWithClauseBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

public class CopyOnWriteDeleteRewriter implements Rewriter<DeleteStatement> {

  private final HiveConf conf;
  protected final SqlGeneratorFactory sqlGeneratorFactory;
  private final COWWithClauseBuilder cowWithClauseBuilder;

  public CopyOnWriteDeleteRewriter(HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    this.sqlGeneratorFactory = sqlGeneratorFactory;
    this.conf = conf;
    this.cowWithClauseBuilder = new COWWithClauseBuilder();
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, DeleteStatement deleteBlock)
      throws SemanticException {

    ASTNode whereTree = deleteBlock.getWhereTree();
    String whereClause = "true";
    if (whereTree != null) {
      Tree wherePredicateNode = whereTree.getChild(0);
      whereClause = context.getTokenRewriteStream()
          .toString(wherePredicateNode.getTokenStartIndex(), wherePredicateNode.getTokenStopIndex());
    }
    String filePathCol = HiveUtils.unparseIdentifier(VirtualColumn.FILE_PATH.getName(), conf);

    MultiInsertSqlGenerator sqlGenerator = sqlGeneratorFactory.createSqlGenerator();

    cowWithClauseBuilder.appendWith(sqlGenerator, filePathCol, whereClause);

    sqlGenerator.append("insert into table ");
    sqlGenerator.append(sqlGenerator.getTargetTableFullName());
    sqlGenerator.appendPartitionColsOfTarget();

    sqlGenerator.append(" select ");
    sqlGenerator.appendAcidSelectColumns(Context.Operation.DELETE);
    sqlGenerator.removeLastChar();

    sqlGenerator.append(" from ");
    sqlGenerator.append(sqlGenerator.getTargetTableFullName());

    // Add the inverted where clause, since we want to hold the records which doesn't satisfy the condition.
    sqlGenerator.append("\nwhere ");
    sqlGenerator.append("( NOT(%s) OR (%s) IS NULL )".replace("%s", whereClause));
    sqlGenerator.append("\n");
    // Add the file path filter that matches the delete condition.
    sqlGenerator.append("AND ").append(filePathCol);
    sqlGenerator.append(" IN ( select ").append(filePathCol).append(" from t )");
    sqlGenerator.append("\nunion all");
    sqlGenerator.append("\nselect * from t");

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlGenerator.toString());
    Context rewrittenCtx = rr.rewrittenCtx;

    rewrittenCtx.setOperation(Context.Operation.DELETE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.DELETE);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
