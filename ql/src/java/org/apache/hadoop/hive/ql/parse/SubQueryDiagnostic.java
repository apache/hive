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

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.ql.Context;


/*
 * Contains functionality that helps with understanding how a SubQuery was rewritten.
 */
public class SubQueryDiagnostic {

  static QBSubQueryRewrite getRewrite(QBSubQuery subQuery, 
      TokenRewriteStream stream,
      Context ctx) {
    if (ctx.isExplainSkipExecution()) {
      return new QBSubQueryRewrite(subQuery, stream);
    } else {
      return new QBSubQueryRewriteNoop(subQuery, stream);
    }
  }
  
  /*
   * Responsible for capturing SubQuery rewrites and providing the rewritten query
   * as SQL.
   */
  public static class QBSubQueryRewrite {
    QBSubQuery subQuery;
    TokenRewriteStream stream;
    
    /*
     * the rewritten where Clause
     */
    String whereClause;
    
    /*
     * any additions to the SubQueries Select Clause.
     */
    String selectClauseAdditions;
    
    /*
     * additions to the Group By Clause.
     */
    String gByClauseAdditions;
    boolean addGroupByClause;
    
    String joiningCondition;
    
    String outerQueryPostJoinCond;
      
    
    QBSubQueryRewrite(QBSubQuery subQuery,
        TokenRewriteStream stream) {
      this.subQuery = subQuery;
      this.stream = stream;
    }
    
    public String getRewrittenQuery() {

      ASTNode sqAST = subQuery.getSubQueryAST();

      if (whereClause != null) {
        ASTNode whereAST = (ASTNode) sqAST.getChild(1).getChild(2);
        stream.replace(subQuery.getAlias(), 
            whereAST.getTokenStartIndex(), 
            whereAST.getTokenStopIndex(),
            whereClause);
      }

      if (selectClauseAdditions != null) {
        ASTNode selectClause = (ASTNode) sqAST.getChild(1).getChild(1);
        stream.insertAfter(subQuery.getAlias(),
            selectClause.getTokenStopIndex(), selectClauseAdditions);
      }

      if (gByClauseAdditions != null) {
        if (!addGroupByClause) {
          ASTNode groupBy = (ASTNode) sqAST.getChild(1).getChild(3);
          stream.insertAfter(subQuery.getAlias(),
              groupBy.getTokenStopIndex(), gByClauseAdditions);
        }
        else {
          gByClauseAdditions = " group by " + gByClauseAdditions;
          stream.insertAfter(subQuery.getAlias(),
              sqAST.getTokenStopIndex() - 1, gByClauseAdditions);
        }
      }
      
      try {
        return  
        stream.toString(subQuery.getAlias(), 
            sqAST.getTokenStartIndex(),
            sqAST.getTokenStopIndex())
             + " " + subQuery.getAlias();
      } finally {
        stream.deleteProgram(subQuery.getAlias());
      }
    }
    
    public String getJoiningCondition() {
      return joiningCondition;
    }
    
    void addWhereClauseRewrite(ASTNode predicate) {
      String cond = stream.toString(predicate.getTokenStartIndex(), predicate.getTokenStopIndex());
      addWhereClauseRewrite(cond);
    }
    
    void addWhereClauseRewrite(String cond) {
      whereClause = whereClause == null ? "where " : whereClause + " and ";
      whereClause += cond;
    }
    
    void addSelectClauseRewrite(ASTNode selectExpr, String alias) {
      if ( selectClauseAdditions == null ) {
        selectClauseAdditions = "";
      }
      
      selectClauseAdditions += ", " + 
          stream.toString(selectExpr.getTokenStartIndex(), selectExpr.getTokenStopIndex()) +
          " as " + alias;
    }
    
    void setAddGroupByClause() {
      this.addGroupByClause = true;
    }
    
    
    void addGByClauseRewrite(ASTNode selectExpr) {
      if ( gByClauseAdditions == null ) {
        gByClauseAdditions = "";
      }
      
      if ( !addGroupByClause || !gByClauseAdditions.equals("") ) {
        gByClauseAdditions += ", ";
      }
      
      gByClauseAdditions += stream.toString(
          selectExpr.getTokenStartIndex(), 
          selectExpr.getTokenStopIndex());
    }
  
    /*
     * joinCond represents a correlated predicate.
     * leftIsRewritten, rightIsRewritten indicates if either side has been replaced by a column alias.
     * 
     * If a side is not rewritten, we get its text from the tokenstream. 
     * For rewritten conditions we form the text based on the table and column reference.
     */
    void addJoinCondition(ASTNode joinCond, boolean leftIsRewritten, boolean rightIsRewritten) {
      StringBuilder b = new StringBuilder();
      
      if ( joiningCondition == null ) {
        joiningCondition = " on ";
      } else {
        b.append(" and ");
      }
      addCondition(b, (ASTNode) joinCond.getChild(0), leftIsRewritten);
      b.append(" = ");
      addCondition(b, (ASTNode) joinCond.getChild(1), rightIsRewritten);
      
      joiningCondition += b.toString();
    }
    
    private void addCondition(StringBuilder b, ASTNode cond, boolean rewritten) {
      if ( !rewritten ) {
        b.append(stream.toString(cond.getTokenStartIndex(), cond.getTokenStopIndex()));
      } else {
        addReference(b, cond);
      }
    }
    
    private void addReference(StringBuilder b, ASTNode ref) {
      if ( ref.getType() == HiveParser.DOT ) {
        b.append(ref.getChild(0).getChild(0).getText()).
        append(".").
        append(ref.getChild(1).getText());
      } else {
        b.append(ref.getText());
      }
    }
    
    void addPostJoinCondition(ASTNode cond) {
      StringBuilder b = new StringBuilder();
      addReference(b, (ASTNode) cond.getChild(1));
      outerQueryPostJoinCond = b.toString() + " is null";
    }
    
    public String getOuterQueryPostJoinCond() {
      return outerQueryPostJoinCond;
    }
  }

  /*
   * In the non explain code path, we don't need to track Query rewrites.
   * All add fns during Plan generation are Noops.
   * If the get Rewrite methods are called, an UnsupportedOperationException is thrown.
   */
  public static class QBSubQueryRewriteNoop extends QBSubQueryRewrite {

    QBSubQueryRewriteNoop(QBSubQuery subQuery, TokenRewriteStream stream) {
      super(subQuery, stream);
    }

    @Override
    public final String getRewrittenQuery() {
      throw new UnsupportedOperationException();
    }

    @Override
    public final String getJoiningCondition() {
      throw new UnsupportedOperationException();
    }

    @Override
    final void addWhereClauseRewrite(ASTNode predicate) {
    }

    @Override
    final void addWhereClauseRewrite(String cond) {
    }

    @Override
    final void addSelectClauseRewrite(ASTNode selectExpr, String alias) {
    }

    @Override
    final void setAddGroupByClause() {
    }

    @Override
    final void addGByClauseRewrite(ASTNode selectExpr) {
    }

    @Override
    final void addJoinCondition(ASTNode joinCond, boolean leftIsRewritten,
        boolean rightIsRewritten) {
    }

    @Override
    final void addPostJoinCondition(ASTNode cond) {
    }

    @Override
    public final String getOuterQueryPostJoinCond() {
      throw new UnsupportedOperationException();
    }
    
  }
  
}