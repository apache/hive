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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;

/**
 * JoinCondTypeCheckProcFactory is used by Calcite planner(CBO) to generate Join Conditions from Join Condition AST.
 * Reasons for sub class:
 * 1. Additional restrictions on what is supported in Join Conditions
 * 2. Column handling is different
 * 3. Join Condn expr has two input RR as opposed to one.
 */

/**
 * TODO:<br>
 * 1. Could we use combined RR instead of list of RR ?<br>
 * 2. Use Column Processing from TypeCheckProcFactory<br>
 * 3. Why not use GB expr ?
 */
public class JoinCondTypeCheckProcFactory extends TypeCheckProcFactory {

  public static Map<ASTNode, ExprNodeDesc> genExprNode(ASTNode expr, TypeCheckCtx tcCtx)
      throws SemanticException {
    return TypeCheckProcFactory.genExprNode(expr, tcCtx, new JoinCondTypeCheckProcFactory());
  }

  /**
   * Processor for table columns.
   */
  public static class JoinCondColumnExprProcessor extends ColumnExprProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      JoinTypeCheckCtx ctx = (JoinTypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;
      ASTNode parent = stack.size() > 1 ? (ASTNode) stack.get(stack.size() - 2) : null;

      if (expr.getType() != HiveParser.TOK_TABLE_OR_COL) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr), expr);
        return null;
      }

      assert (expr.getChildCount() == 1);
      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText());

      boolean qualifiedAccess = (parent != null && parent.getType() == HiveParser.DOT);

      ColumnInfo colInfo = null;
      if (!qualifiedAccess) {
        colInfo = getColInfo(ctx, null, tableOrCol, expr);
        // It's a column.
        return new ExprNodeColumnDesc(colInfo);
      } else if (hasTableAlias(ctx, tableOrCol, expr)) {
        return null;
      } else {
        // Qualified column access for which table was not found
        throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(expr));
      }
    }

    private static boolean hasTableAlias(JoinTypeCheckCtx ctx, String tabName, ASTNode expr)
        throws SemanticException {
      int tblAliasCnt = 0;
      for (RowResolver rr : ctx.getInputRRList()) {
        if (rr.hasTableAlias(tabName))
          tblAliasCnt++;
      }

      if (tblAliasCnt > 1) {
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr));
      }

      return (tblAliasCnt == 1) ? true : false;
    }

    private static ColumnInfo getColInfo(JoinTypeCheckCtx ctx, String tabName, String colAlias,
        ASTNode expr) throws SemanticException {
      ColumnInfo tmp;
      ColumnInfo cInfoToRet = null;

      for (RowResolver rr : ctx.getInputRRList()) {
        tmp = rr.get(tabName, colAlias);
        if (tmp != null) {
          if (cInfoToRet != null) {
            throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr));
          }
          cInfoToRet = tmp;
        }
      }

      return cInfoToRet;
    }
  }

  /**
   * Factory method to get ColumnExprProcessor.
   *
   * @return ColumnExprProcessor.
   */
  @Override
  public ColumnExprProcessor getColumnExprProcessor() {
    return new JoinCondColumnExprProcessor();
  }

  /**
   * The default processor for typechecking.
   */
  public static class JoinCondDefaultExprProcessor extends DefaultExprProcessor {
    @Override
    protected List<String> getReferenceableColumnAliases(TypeCheckCtx ctx) {
      JoinTypeCheckCtx jCtx = (JoinTypeCheckCtx) ctx;
      List<String> possibleColumnNames = new ArrayList<String>();
      for (RowResolver rr : jCtx.getInputRRList()) {
        possibleColumnNames.addAll(rr.getReferenceableColumnAliases(null, -1));
      }

      return possibleColumnNames;
    }

    @Override
    protected ExprNodeColumnDesc processQualifiedColRef(TypeCheckCtx ctx, ASTNode expr,
        Object... nodeOutputs) throws SemanticException {
      String tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getChild(0)
          .getText());
      // NOTE: tableAlias must be a valid non-ambiguous table alias,
      // because we've checked that in TOK_TABLE_OR_COL's process method.
      ColumnInfo colInfo = getColInfo((JoinTypeCheckCtx) ctx, tableAlias,
          ((ExprNodeConstantDesc) nodeOutputs[1]).getValue().toString(), expr);

      if (colInfo == null) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)), expr);
        return null;
      }
      return new ExprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName(), tableAlias,
          colInfo.getIsVirtualCol());
    }

    private static ColumnInfo getColInfo(JoinTypeCheckCtx ctx, String tabName, String colAlias,
        ASTNode expr) throws SemanticException {
      ColumnInfo tmp;
      ColumnInfo cInfoToRet = null;

      for (RowResolver rr : ctx.getInputRRList()) {
        tmp = rr.get(tabName, colAlias);
        if (tmp != null) {
          if (cInfoToRet != null) {
            throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr));
          }
          cInfoToRet = tmp;
        }
      }

      return cInfoToRet;
    }

    @Override
    protected void validateUDF(ASTNode expr, boolean isFunction, TypeCheckCtx ctx, FunctionInfo fi,
        List<ExprNodeDesc> children, GenericUDF genericUDF) throws SemanticException {
      super.validateUDF(expr, isFunction, ctx, fi, children, genericUDF);

      JoinTypeCheckCtx jCtx = (JoinTypeCheckCtx) ctx;

      // Join Condition can not contain disjunctions
      if (genericUDF instanceof GenericUDFOPOr) {
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_3.getMsg(expr));
      }

      // Non Conjunctive elements have further limitations in Join conditions
      if (!(genericUDF instanceof GenericUDFOPAnd)) {
        // Non Comparison UDF other than 'and' can not use inputs from both side
        if (!(genericUDF instanceof GenericUDFBaseCompare)) {
          if (genericUDFargsRefersToBothInput(genericUDF, children, jCtx.getInputRRList())) {
            throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr));
          }
        } else if (genericUDF instanceof GenericUDFBaseCompare) {
          // Comparisons of non literals LHS/RHS can not refer to inputs from
          // both sides
          if (children.size() == 2 && !(children.get(0) instanceof ExprNodeConstantDesc)
              && !(children.get(1) instanceof ExprNodeConstantDesc)) {
            if (comparisonUDFargsRefersToBothInput((GenericUDFBaseCompare) genericUDF, children,
                jCtx.getInputRRList())) {
              throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(expr));
            }
          }
        }
      }
    }

    private static boolean genericUDFargsRefersToBothInput(GenericUDF udf,
        List<ExprNodeDesc> children, List<RowResolver> inputRRList) {
      boolean argsRefersToBothInput = false;

      Map<Integer, ExprNodeDesc> hasCodeToColDescMap = new HashMap<Integer, ExprNodeDesc>();
      for (ExprNodeDesc child : children) {
        ExprNodeDescUtils.getExprNodeColumnDesc(child, hasCodeToColDescMap);
      }
      Set<Integer> inputRef = getInputRef(hasCodeToColDescMap.values(), inputRRList);

      if (inputRef.size() > 1)
        argsRefersToBothInput = true;

      return argsRefersToBothInput;
    }

    private static boolean comparisonUDFargsRefersToBothInput(GenericUDFBaseCompare comparisonUDF,
        List<ExprNodeDesc> children, List<RowResolver> inputRRList) {
      boolean argsRefersToBothInput = false;

      Map<Integer, ExprNodeDesc> lhsHashCodeToColDescMap = new HashMap<Integer, ExprNodeDesc>();
      Map<Integer, ExprNodeDesc> rhsHashCodeToColDescMap = new HashMap<Integer, ExprNodeDesc>();
      ExprNodeDescUtils.getExprNodeColumnDesc(children.get(0), lhsHashCodeToColDescMap);
      ExprNodeDescUtils.getExprNodeColumnDesc(children.get(1), rhsHashCodeToColDescMap);
      Set<Integer> lhsInputRef = getInputRef(lhsHashCodeToColDescMap.values(), inputRRList);
      Set<Integer> rhsInputRef = getInputRef(rhsHashCodeToColDescMap.values(), inputRRList);

      if (lhsInputRef.size() > 1 || rhsInputRef.size() > 1)
        argsRefersToBothInput = true;

      return argsRefersToBothInput;
    }

    private static Set<Integer> getInputRef(Collection<ExprNodeDesc> colDescSet,
        List<RowResolver> inputRRList) {
      String tableAlias;
      RowResolver inputRR;
      Set<Integer> inputLineage = new HashSet<Integer>();

      for (ExprNodeDesc col : colDescSet) {
        ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) col;
        tableAlias = colDesc.getTabAlias();

        for (int i = 0; i < inputRRList.size(); i++) {
          inputRR = inputRRList.get(i);

          // If table Alias is present check if InputRR has that table and then
          // check for internal name
          // else if table alias is null then check with internal name in all
          // inputRR.
          if (tableAlias != null) {
            if (inputRR.hasTableAlias(tableAlias)) {
              if (inputRR.doesInvRslvMapContain(colDesc.getColumn())) {
                inputLineage.add(i);
              }
            }
          } else {
            if (inputRR.doesInvRslvMapContain(colDesc.getColumn())) {
              inputLineage.add(i);
            }
          }
        }
      }

      return inputLineage;
    }
  }

  /**
   * Factory method to get DefaultExprProcessor.
   *
   * @return DefaultExprProcessor.
   */
  @Override
  public DefaultExprProcessor getDefaultExprProcessor() {
    return new JoinCondDefaultExprProcessor();
  }
}
