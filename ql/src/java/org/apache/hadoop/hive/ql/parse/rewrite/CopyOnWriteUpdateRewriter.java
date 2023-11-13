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
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.COWWithClauseBuilder;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SetClausePatcher;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CopyOnWriteUpdateRewriter implements Rewriter<UpdateStatement> {

  private final HiveConf conf;
  private final SqlGeneratorFactory sqlGeneratorFactory;
  private final COWWithClauseBuilder cowWithClauseBuilder;
  private final SetClausePatcher setClausePatcher;


  public CopyOnWriteUpdateRewriter(HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    this.conf = conf;
    this.sqlGeneratorFactory = sqlGeneratorFactory;
    this.cowWithClauseBuilder = new COWWithClauseBuilder();
    this.setClausePatcher = new SetClausePatcher();
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateStatement updateBlock)
      throws SemanticException {

    String filePathCol = HiveUtils.unparseIdentifier(VirtualColumn.FILE_PATH.getName(), conf);
    MultiInsertSqlGenerator sqlGenerator = sqlGeneratorFactory.createSqlGenerator();

    String whereClause = null;
    int columnOffset = 0;
    
    boolean shouldOverwrite = updateBlock.getWhereTree() == null;
    if (shouldOverwrite) {
      sqlGenerator.append("insert overwrite table ");
    } else {
      Tree wherePredicateNode = updateBlock.getWhereTree().getChild(0);
      whereClause = context.getTokenRewriteStream().toString(
          wherePredicateNode.getTokenStartIndex(), wherePredicateNode.getTokenStopIndex());
      
      cowWithClauseBuilder.appendWith(sqlGenerator, filePathCol, whereClause);
      sqlGenerator.append("insert into table ");

      columnOffset = sqlGenerator.getDeleteValues(Context.Operation.UPDATE).size();
    }
    sqlGenerator.appendTargetTableName();
    sqlGenerator.appendPartitionColsOfTarget();
    
    sqlGenerator.append(" select ");
    if (!shouldOverwrite) {
      sqlGenerator.appendAcidSelectColumns(Context.Operation.UPDATE);
      sqlGenerator.removeLastChar();
    }

    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetCols().size());
    List<FieldSchema> nonPartCols = updateBlock.getTargetTable().getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      if (columnOffset > 0 || i > 0) {
        sqlGenerator.append(',');
      }
      String name = nonPartCols.get(i).getName();
      ASTNode setCol = updateBlock.getSetCols().get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);
      sqlGenerator.append(identifier);
      sqlGenerator.append(" AS ").append(identifier);
      if (setCol != null) {
        // This is one of the columns we're setting, record it's position so we can come back
        // later and patch it up.
        // Add one to the index because the select has the ROW__ID as the first column.
        setColExprs.put(columnOffset + i, setCol);
      }
    }

    sqlGenerator.append(" from ");
    sqlGenerator.appendTargetTableName();

    if (whereClause != null) {
      sqlGenerator.append("\nwhere ");
      sqlGenerator.append(whereClause);
      sqlGenerator.append("\nunion all");
      sqlGenerator.append("\nselect ");
      sqlGenerator.appendAcidSelectColumns(Context.Operation.DELETE);
      sqlGenerator.removeLastChar();
      sqlGenerator.append(" from ");
      sqlGenerator.appendTargetTableName();
      // Add the inverted where clause, since we want to hold the records which doesn't satisfy the condition.
      sqlGenerator.append("\nwhere NOT (").append(whereClause).append(")");
      sqlGenerator.append("\n").indent();
      // Add the file path filter that matches the delete condition.
      sqlGenerator.append("AND ").append(filePathCol);
      sqlGenerator.append(" IN ( select ").append(filePathCol).append(" from t )");
      sqlGenerator.append("\nunion all");
      sqlGenerator.append("\nselect * from t");
    }

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlGenerator.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode) (!shouldOverwrite ? 
      new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
            rewrittenTree, HiveParser.TOK_FROM, HiveParser.TOK_SUBQUERY, HiveParser.TOK_UNIONALL)
            .getChild(0).getChild(0) : rewrittenTree)
        .getChild(1);

    if (shouldOverwrite) {
      rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.INSERT);
    } else {
      rewrittenCtx.setOperation(Context.Operation.UPDATE);
      rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);
    }
    
    setClausePatcher.patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    return rr;
  }
}
