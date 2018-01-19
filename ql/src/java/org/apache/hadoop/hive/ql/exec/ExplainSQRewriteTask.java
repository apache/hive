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
package org.apache.hadoop.hive.ql.exec;

import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBSubQuery;
import org.apache.hadoop.hive.ql.parse.SubQueryDiagnostic;
import org.apache.hadoop.hive.ql.plan.ExplainSQRewriteWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;


public class ExplainSQRewriteTask extends Task<ExplainSQRewriteWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  @Override
  public StageType getType() {
    return StageType.EXPLAIN;
  }
  
  @Override
  public int execute(DriverContext driverContext) {

    PrintStream out = null;
    try {
      Path resFile = new Path(work.getResFile());
      OutputStream outS = resFile.getFileSystem(conf).create(resFile);
      out = new PrintStream(outS);

      QB qb = work.getQb();
      TokenRewriteStream stream = work.getCtx().getTokenRewriteStream();
      String program = "sq rewrite";
      ASTNode ast = work.getAst();
      
      try {
        addRewrites(stream, qb, program, out);
        out.println("\nRewritten Query:\n" + stream.toString(program, 
            ast.getTokenStartIndex(), ast.getTokenStopIndex()));
      } finally {
        stream.deleteProgram(program);
      }
      
      out.close();
      out = null;
      return (0);
    }
    catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(),
          "\n" + StringUtils.stringifyException(e));
      return (1);
    }
    finally {
      IOUtils.closeStream(out);
    }
  }
  
  void addRewrites(TokenRewriteStream stream, QB qb, String program,
      PrintStream out) {
    QBSubQuery sqW = qb.getWhereClauseSubQueryPredicate();
    QBSubQuery sqH = qb.getHavingClauseSubQueryPredicate();

    if (sqW != null || sqH != null) {

      ASTNode sqNode = sqW != null ? sqW.getOriginalSubQueryASTForRewrite()
          : sqH.getOriginalSubQueryASTForRewrite();
      ASTNode tokQry = getQueryASTNode(sqNode);
      ASTNode tokFrom = (ASTNode) tokQry.getChild(0);

      StringBuilder addedJoins = new StringBuilder();

      if (sqW != null) {
        addRewrites(stream, sqW, program, out, qb.getId(), true, addedJoins);
      }

      if (sqH != null) {
        addRewrites(stream, sqH, program, out, qb.getId(), false, addedJoins);
      }
      stream.insertAfter(program, tokFrom.getTokenStopIndex(), addedJoins);
    }
    
    Set<String> sqAliases = qb.getSubqAliases();
    for(String sqAlias : sqAliases) {
      addRewrites(stream, qb.getSubqForAlias(sqAlias).getQB(), program, out);
    }
  }
  
  void addRewrites(TokenRewriteStream stream, QBSubQuery sq, String program, 
      PrintStream out, String qbAlias, boolean isWhere, StringBuilder addedJoins) {
    ASTNode sqNode = sq.getOriginalSubQueryASTForRewrite();
    ASTNode tokQry = getQueryASTNode(sqNode);
    ASTNode tokInsert = (ASTNode) tokQry.getChild(1);
    ASTNode tokWhere = null;
    
    for(int i=0; i < tokInsert.getChildCount(); i++) {
      if ( tokInsert.getChild(i).getType() == HiveParser.TOK_WHERE) {
        tokWhere = (ASTNode) tokInsert.getChild(i);
        break;
      }
    }
    
    SubQueryDiagnostic.QBSubQueryRewrite diag = sq.getDiagnostic();
      String sqStr = diag.getRewrittenQuery();
      String joinCond = diag.getJoiningCondition();
      
      /*
       * the SubQuery predicate has been hoisted as a Join. The SubQuery predicate is replaced
       * by a 'true' predicate in the Outer QB's where/having clause.
       */
      stream.replace(program, sqNode.getTokenStartIndex(),
          sqNode.getTokenStopIndex(),
          "1 = 1");
      
      String sqJoin = " " + 
          getJoinKeyWord(sq) + 
          " " +
          sqStr + 
          " " +
          joinCond;
      addedJoins.append(" ").append(sqJoin);
      
      String postJoinCond = diag.getOuterQueryPostJoinCond();
      if ( postJoinCond != null ) {
        stream.insertAfter(program, tokWhere.getTokenStopIndex(), " and " + postJoinCond);
      }
      
      String qualifier = isWhere ? "Where Clause " : "Having Clause ";
      if ( qbAlias != null ) {
        qualifier = qualifier + "for Query Block '" + qbAlias + "' ";
      }
      out.println(String.format("\n%s Rewritten SubQuery:\n%s", 
          qualifier, diag.getRewrittenQuery()));
      out.println(String.format("\n%s SubQuery Joining Condition:\n%s", 
          qualifier, diag.getJoiningCondition()));
  }
  
  private String getJoinKeyWord(QBSubQuery sq) {
    switch (sq.getJoinType()) {
    case LEFTOUTER:
      return "left outer join";
    case LEFTSEMI:
      return "left semi join";
    case RIGHTOUTER:
      return "right outer join";
    case FULLOUTER:
      return "full outer join";
    case INNER:
    default:
      return "inner join";
    }
  }
  
  private ASTNode getQueryASTNode(ASTNode node) {
    while( node != null && node.getType() != HiveParser.TOK_QUERY ) {
      node = (ASTNode) node.getParent();
    }
    return node;
  }

  @Override
  public String getName() {
    return "EXPLAIN REWRITE";
  }

  public List<FieldSchema> getResultSchema() {
    FieldSchema tmpFieldSchema = new FieldSchema();
    List<FieldSchema> colList = new ArrayList<FieldSchema>();

    tmpFieldSchema.setName(ExplainTask.EXPL_COLUMN_NAME);
    tmpFieldSchema.setType(STRING_TYPE_NAME);

    colList.add(tmpFieldSchema);
    return colList;
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }
}
