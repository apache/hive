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

package org.apache.hadoop.hive.ql.optimizer.index;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;


/**
 * RewriteParseContextGenerator is a class that offers methods to generate operator tree
 * for input queries. It is implemented on lines of the analyzeInternal(..) method
 * of {@link SemanticAnalyzer} but it creates only the ParseContext for the input query command.
 * It does not optimize or generate map-reduce tasks for the input query.
 * This can be used when you need to create operator tree for an internal query.
 *
 */
public final class RewriteParseContextGenerator {

  private static final Log LOG = LogFactory.getLog(RewriteParseContextGenerator.class.getName());

  /**
   * Parse the input {@link String} command and generate an operator tree.
   * @param conf
   * @param command
   * @throws SemanticException
   */
  public static Operator<? extends OperatorDesc> generateOperatorTree(HiveConf conf,
      String command) throws SemanticException {
    Operator<? extends OperatorDesc> operatorTree;
    try {
      Context ctx = new Context(conf);
      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
      assert(sem instanceof SemanticAnalyzer);
      operatorTree = doSemanticAnalysis((SemanticAnalyzer) sem, tree, ctx);
      LOG.info("Sub-query Semantic Analysis Completed");
    } catch (IOException e) {
      LOG.error("IOException in generating the operator " +
        "tree for input command - " + command + " " , e);
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    } catch (ParseException e) {
      LOG.error("ParseException in generating the operator " +
        "tree for input command - " + command + " " , e);
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    } catch (SemanticException e) {
      LOG.error("SemanticException in generating the operator " +
        "tree for input command - " + command + " " , e);
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }
    return operatorTree;
  }

  /**
   * For the input ASTNode tree, perform a semantic analysis and check metadata
   * Generate a operator tree and return it.
   *
   * @param ctx
   * @param sem
   * @param ast
   * @return
   * @throws SemanticException
   */
  private static Operator<?> doSemanticAnalysis(SemanticAnalyzer sem,
      ASTNode ast, Context ctx) throws SemanticException {
    QB qb = new QB(null, null, false);
    ASTNode child = ast;
    ParseContext subPCtx = ((SemanticAnalyzer) sem).getParseContext();
    subPCtx.setContext(ctx);
    ((SemanticAnalyzer) sem).initParseCtx(subPCtx);

    LOG.info("Starting Sub-query Semantic Analysis");
    sem.doPhase1(child, qb, sem.initPhase1Ctx(), null);
    LOG.info("Completed phase 1 of Sub-query Semantic Analysis");

    sem.getMetaData(qb);
    LOG.info("Completed getting MetaData in Sub-query Semantic Analysis");

    LOG.info("Sub-query Abstract syntax tree: " + ast.toStringTree());
    Operator<?> operator = sem.genPlan(qb);

    LOG.info("Sub-query Completed plan generation");
    return operator;
  }

}
