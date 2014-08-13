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

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;

import java.util.List;
import java.util.Stack;

/**
 * Factory of methods used by {@link RewriteGBUsingIndex}
 * to determine if the rewrite optimization can be applied to the input query.
 *
 */
public final class RewriteCanApplyProcFactory {

  /**
   * Check for conditions in FilterOperator that do not meet rewrite criteria.
   */
  private static class CheckFilterProc implements NodeProcessor {

    private TableScanOperator topOp;

    public CheckFilterProc(TableScanOperator topOp) {
      this.topOp = topOp;
    }

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator operator = (FilterOperator)nd;
      RewriteCanApplyCtx canApplyCtx = (RewriteCanApplyCtx)ctx;
      FilterDesc conf = operator.getConf();
      //The filter operator should have a predicate of ExprNodeGenericFuncDesc type.
      //This represents the comparison operator
      ExprNodeDesc oldengfd = conf.getPredicate();
      if(oldengfd == null){
        canApplyCtx.setWhrClauseColsFetchException(true);
        return null;
      }
      ExprNodeDesc backtrack = ExprNodeDescUtils.backtrack(oldengfd, operator, topOp);
      if (backtrack == null) {
        canApplyCtx.setWhrClauseColsFetchException(true);
        return null;
      }
      //Add the predicate columns to RewriteCanApplyCtx's predColRefs list to check later
      //if index keys contain all filter predicate columns and vice-a-versa
      for (String col : backtrack.getCols()) {
        canApplyCtx.getPredicateColumnsList().add(col);
      }
      return null;
    }
  }

 public static CheckFilterProc canApplyOnFilterOperator(TableScanOperator topOp) {
    return new CheckFilterProc(topOp);
  }

   /**
   * Check for conditions in GroupByOperator that do not meet rewrite criteria.
   *
   */
  private static class CheckGroupByProc implements NodeProcessor {

     private TableScanOperator topOp;

     public CheckGroupByProc(TableScanOperator topOp) {
       this.topOp = topOp;
     }

     public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
         Object... nodeOutputs) throws SemanticException {
       GroupByOperator operator = (GroupByOperator)nd;
       RewriteCanApplyCtx canApplyCtx = (RewriteCanApplyCtx)ctx;
       //for each group-by clause in query, only one GroupByOperator of the
       //GBY-RS-GBY sequence is stored in  getGroupOpToInputTables
       //we need to process only this operator
       //Also, we do not rewrite for cases when same query branch has multiple group-by constructs
       if(canApplyCtx.getParseContext().getGroupOpToInputTables().containsKey(operator) &&
           !canApplyCtx.isQueryHasGroupBy()){

         canApplyCtx.setQueryHasGroupBy(true);
         GroupByDesc conf = operator.getConf();
         List<AggregationDesc> aggrList = conf.getAggregators();
         if(aggrList != null && aggrList.size() > 0){
             for (AggregationDesc aggregationDesc : aggrList) {
               canApplyCtx.setAggFuncCnt(canApplyCtx.getAggFuncCnt() + 1);
               //In the current implementation, we do not support more than 1 agg funcs in group-by
               if(canApplyCtx.getAggFuncCnt() > 1) {
                 return false;
               }
               String aggFunc = aggregationDesc.getGenericUDAFName();
               if(!("count".equals(aggFunc))){
                 canApplyCtx.setAggFuncIsNotCount(true);
                 return false;
               }
               List<ExprNodeDesc> para = aggregationDesc.getParameters();
               //for a valid aggregation, it needs to have non-null parameter list
               if (para == null) {
                 canApplyCtx.setAggFuncColsFetchException(true);
               } else if (para.size() == 0) {
                 //count(*) case
                 canApplyCtx.setCountOnAllCols(true);
                 canApplyCtx.setAggFunction("_count_of_all");
               } else if (para.size() == 1) {
                 ExprNodeDesc expr = ExprNodeDescUtils.backtrack(para.get(0), operator, topOp);
                 if (expr instanceof ExprNodeColumnDesc){
                   //Add the columns to RewriteCanApplyCtx's selectColumnsList list
                   //to check later if index keys contain all select clause columns
                   //and vice-a-versa. We get the select column 'actual' names only here
                   //if we have a agg func along with group-by
                   //SelectOperator has internal names in its colList data structure
                   canApplyCtx.getSelectColumnsList().add(
                       ((ExprNodeColumnDesc) expr).getColumn());
                   //Add the columns to RewriteCanApplyCtx's aggFuncColList list to check later
                   //if columns contained in agg func are index key columns
                   canApplyCtx.getAggFuncColList().add(
                       ((ExprNodeColumnDesc) expr).getColumn());
                   canApplyCtx.setAggFunction("_count_of_" +
                       ((ExprNodeColumnDesc) expr).getColumn() + "");
                 } else if(expr instanceof ExprNodeConstantDesc) {
                   //count(1) case
                   canApplyCtx.setCountOfOne(true);
                   canApplyCtx.setAggFunction("_count_of_1");
                 }
               } else {
                 throw new SemanticException("Invalid number of arguments for count");
               }
             }
         }

         //we need to have non-null group-by keys for a valid group-by operator
         List<ExprNodeDesc> keyList = conf.getKeys();
         if(keyList == null || keyList.size() == 0){
           canApplyCtx.setGbyKeysFetchException(true);
         }
         for (ExprNodeDesc expr : keyList) {
           checkExpression(canApplyCtx, expr);
         }
       }
       return null;
     }

     private void checkExpression(RewriteCanApplyCtx canApplyCtx, ExprNodeDesc expr){
       if(expr instanceof ExprNodeColumnDesc){
         //Add the group-by keys to RewriteCanApplyCtx's gbKeyNameList list to check later
         //if all keys are from index columns
         canApplyCtx.getGbKeyNameList().addAll(expr.getCols());
       }else if(expr instanceof ExprNodeGenericFuncDesc){
         ExprNodeGenericFuncDesc funcExpr = (ExprNodeGenericFuncDesc)expr;
         List<ExprNodeDesc> childExprs = funcExpr.getChildren();
         for (ExprNodeDesc childExpr : childExprs) {
           if(childExpr instanceof ExprNodeColumnDesc){
             canApplyCtx.getGbKeyNameList().addAll(expr.getCols());
             canApplyCtx.getSelectColumnsList().add(((ExprNodeColumnDesc) childExpr).getColumn());
           }else if(childExpr instanceof ExprNodeGenericFuncDesc){
             checkExpression(canApplyCtx, childExpr);
           }
         }
       }
     }
   }

   public static CheckGroupByProc canApplyOnGroupByOperator(TableScanOperator topOp) {
     return new CheckGroupByProc(topOp);
   }
}
