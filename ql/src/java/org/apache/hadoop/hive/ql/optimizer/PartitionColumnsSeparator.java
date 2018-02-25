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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.ForwardWalker;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TypeRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * This optimization will take a Filter expression, and if its predicate contains
 * an IN operator whose children are constant structs or structs containing constant fields,
 * it will try to generate predicate with IN clauses containing only partition columns.
 * This predicate is in turn used by the partition pruner to prune the columns that are not
 * part of the original IN(STRUCT(..)..) predicate.
 */
public class PartitionColumnsSeparator extends Transform {

  private static final Log LOG = LogFactory.getLog(PartitionColumnsSeparator.class);
  private static final String IN_UDF =
    GenericUDFIn.class.getAnnotation(Description.class).name();
  private static final String STRUCT_UDF =
    GenericUDFStruct.class.getAnnotation(Description.class).name();
  private static final String AND_UDF =
    GenericUDFOPAnd.class.getAnnotation(Description.class).name();

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // 1. Trigger transformation
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"), new StructInTransformer());

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new ForwardWalker(disp);

    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private class StructInTransformer implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator filterOp = (FilterOperator) nd;
      ExprNodeDesc predicate = filterOp.getConf().getPredicate();

      // Generate the list bucketing pruning predicate as 2 separate IN clauses
      // containing the partitioning and non-partitioning columns.
      ExprNodeDesc newPredicate = generateInClauses(predicate);
      if (newPredicate != null) {
        // Replace filter in current FIL with new FIL
        if (LOG.isDebugEnabled()) {
          LOG.debug("Generated new predicate with IN clause: " + newPredicate);
        }
        final List<ExprNodeDesc> subExpr =
                new ArrayList<ExprNodeDesc>(2);
        subExpr.add(predicate);
        subExpr.add(newPredicate);
        ExprNodeGenericFuncDesc newFilterPredicate = new ExprNodeGenericFuncDesc(
                TypeInfoFactory.booleanTypeInfo,
                FunctionRegistry.getFunctionInfo(AND_UDF).getGenericUDF(), subExpr);
        filterOp.getConf().setPredicate(newFilterPredicate);
      }

      return null;
    }

    private ExprNodeDesc generateInClauses(ExprNodeDesc predicate) throws SemanticException {
      Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
      exprRules.put(new TypeRule(ExprNodeGenericFuncDesc.class), new StructInExprProcessor());

      // The dispatcher fires the processor corresponding to the closest matching
      // rule and passes the context along
      Dispatcher disp = new DefaultRuleDispatcher(null, exprRules, null);
      GraphWalker egw = new PreOrderOnceWalker(disp);

      List<Node> startNodes = new ArrayList<Node>();
      startNodes.add(predicate);

      HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
      egw.startWalking(startNodes, outputMap);
      return (ExprNodeDesc) outputMap.get(predicate);
    }
  }

  /**
   * The StructInExprProcessor processes the IN clauses of the following format :
   * STRUCT(T1.a, T1.b, T2.b, T2.c) IN (STRUCT(1, 2, 3, 4) , STRUCT(2, 3, 4, 5))
   * where T1.a, T1.b, T2.c are all partition columns and T2.b is a non-partition
   * column. The resulting additional predicate generated after
   * StructInExprProcessor.process() looks like :
   *    STRUCT(T1.a, T1.b) IN (STRUCT(1, 2), STRUCT(2, 3))
   *    AND
   *    STRUCT(T2.b) IN (STRUCT(4), STRUCT(5))
   * The additional predicate generated is used to prune the partitions that are
   * part of the given query. Once the partitions are pruned, the partition condition
   * remover is expected to remove the redundant predicates from the plan.
   */
  private class StructInExprProcessor implements NodeProcessor {

    /** TableInfo is populated in PASS 1 of process(). It contains the information required
     * to generate an IN clause of the following format:
     * STRUCT(T1.a, T1.b) IN (const STRUCT(1, 2), const STRUCT(2, 3))
     * In the above e.g. please note that all elements of the struct come from the same table.
     * The populated TableStructInfo is used to generate the IN clause in PASS 2 of process().
     * The table struct information class has the following fields:
     * 1. Expression Node Descriptor for the Left Hand Side of the IN clause for the table
     * 2. 2-D List of expression node descriptors which corresponds to the elements of IN clause
     */
    class TableInfo {
      List<ExprNodeDesc> exprNodeLHSDescriptor;
      List<List<ExprNodeDesc>> exprNodeRHSStructs;

      public TableInfo() {
       exprNodeLHSDescriptor = new ArrayList<ExprNodeDesc>();
       exprNodeRHSStructs = new ArrayList<List<ExprNodeDesc>>();
      }
    }

    // Mapping from expression node to is an expression containing only
    // partition or virtual column or constants
    private Map<ExprNodeDesc, Boolean> exprNodeToPartOrVirtualColOrConstExpr =
      new IdentityHashMap<ExprNodeDesc, Boolean>();

    /**
     * This function iterates through the entire subtree under a given expression node
     * and makes sure that the expression contain only constant nodes or
     * partition/virtual columns as leaf nodes.
     * @param en Expression Node Descriptor for the root node.
     * @return true if the subtree rooted under en has only partition/virtual columns or
     * constant values as the leaf nodes. Else, return false.
     */
    private boolean exprContainsOnlyPartitionColOrVirtualColOrConstants(ExprNodeDesc en) {
      if (en == null) {
        return true;
      }
      if (exprNodeToPartOrVirtualColOrConstExpr.containsKey(en)) {
        return exprNodeToPartOrVirtualColOrConstExpr.get(en);
      }
      if (en instanceof ExprNodeColumnDesc) {
        boolean ret = ((ExprNodeColumnDesc)en).getIsPartitionColOrVirtualCol();
        exprNodeToPartOrVirtualColOrConstExpr.put(en, ret);
        return ret;
      }
      if (en.getChildren() != null) {
        for (ExprNodeDesc cn : en.getChildren()) {
          if (!exprContainsOnlyPartitionColOrVirtualColOrConstants(cn)) {
            exprNodeToPartOrVirtualColOrConstExpr.put(en, false);
            return false;
          }
        }
      }
      exprNodeToPartOrVirtualColOrConstExpr.put(en, true);
      return true;
    }


    /**
     * Check if the expression node satisfies the following :
     * Has atleast one subexpression containing a partition/virtualcolumn and has
     * exactly refer to a single table alias.
     * @param en Expression Node Descriptor
     * @return true if there is atleast one subexpression with partition/virtual column
     * and has exactly refer to a single table alias. If not, return false.
     */
    private boolean hasAtleastOneSubExprWithPartColOrVirtualColWithOneTableAlias(ExprNodeDesc en) {
      if (en == null || en.getChildren() == null) {
        return false;
      }
      for (ExprNodeDesc cn : en.getChildren()) {
        if (exprContainsOnlyPartitionColOrVirtualColOrConstants(cn) && getTableAlias(cn) != null) {
          return true;
        }
      }
      return false;
    }


    /**
     * Check if the expression node satisfies the following :
     * Has all subexpressions containing constants or a partition/virtual column/coming from the
     * same table
     * @param en Expression Node Descriptor
     * @return true/false based on the condition specified in the above description.
     */
    private boolean hasAllSubExprWithConstOrPartColOrVirtualColWithOneTableAlias(ExprNodeDesc en) {
      if (!exprContainsOnlyPartitionColOrVirtualColOrConstants(en)) {
        return false;
      }

      Set<String> s = new HashSet<String>();
      Set<ExprNodeDesc> visited = new HashSet<ExprNodeDesc>();

      return getTableAliasHelper(en, s, visited);
    }


    /**
     * Return the expression node descriptor if the input expression node is a GenericUDFIn.
     * Else, return null.
     * @param en Expression Node Descriptor
     * @return The expression node descriptor if the input expression node represents an IN clause.
     * Else, return null.
     */
    private ExprNodeGenericFuncDesc getInExprNode(ExprNodeDesc en) {
      if (en == null) {
        return null;
      }

      if (en instanceof ExprNodeGenericFuncDesc && ((ExprNodeGenericFuncDesc)(en)).getGenericUDF()
          instanceof GenericUDFIn) {
        return (ExprNodeGenericFuncDesc) en;
      }
      return null;
    }


    /**
     * Helper used by getTableAlias
     * @param en Expression Node Descriptor
     * @param s Set of the table Aliases associated with the current Expression node.
     * @param visited Visited ExpressionNode set.
     * @return true if en has at most one table associated with it, else return false.
     */
    private boolean getTableAliasHelper(ExprNodeDesc en, Set<String> s, Set<ExprNodeDesc> visited) {
      visited.add(en);

      // The current expression node is a column, see if the column alias is already a part of
      // the return set, s. If not and we already have an entry in set s, this is an invalid expression
      // and return false.
      if (en instanceof ExprNodeColumnDesc) {
        if (s.size() > 0 &&
           !s.contains(((ExprNodeColumnDesc)en).getTabAlias())) {
          return false;
        }
        if (s.size() == 0) {
          s.add(((ExprNodeColumnDesc)en).getTabAlias());
        }
        return true;
      }
      if (en.getChildren() == null) {
        return true;
      }

      // Iterative through the children in a DFS manner to see if there is more than 1 table alias
      // referenced by the current expression node.
      for (ExprNodeDesc cn : en.getChildren()) {
        if (visited.contains(cn)) {
          continue;
        }
        if (cn instanceof ExprNodeColumnDesc) {
          s.add(((ExprNodeColumnDesc) cn).getTabAlias());
        } else if (!(cn instanceof ExprNodeConstantDesc)) {
          if (!getTableAliasHelper(cn, s, visited)) {
            return false;
          }
        }
      }
      return true;
    }


    /**
     * If the given expression has just a single table associated with it,
     * return the table alias associated with it. Else, return null.
     * @param en
     * @return The table alias associated with the expression if there is a single table
     * reference. Else, return null.
     */
    private String getTableAlias(ExprNodeDesc en) {
      Set<String> s = new HashSet<String>();
      Set<ExprNodeDesc> visited = new HashSet<ExprNodeDesc>();
      boolean singleTableAlias = getTableAliasHelper(en, s, visited);

      if (!singleTableAlias || s.size() == 0) {
        return null;
      }
      StringBuilder ans = new StringBuilder();
      for (String st : s) {
        ans.append(st);
      }
      return ans.toString();
    }


    /**
     * The main process method for StructInExprProcessor to generate additional predicates
     * containing only partition columns.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprNodeGenericFuncDesc fd = getInExprNode((ExprNodeDesc)nd);

      /***************************************************************************************\
       BEGIN : Early terminations for Partition Column Separator
      /***************************************************************************************/
      // 1. If the input node is not an IN operator, we bail out.
      if (fd == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition columns not separated for " + fd + ", is not IN operator : ");
        }
        return null;
      }

      // 2. Check if the input is an IN operator with struct children
      List<ExprNodeDesc> children = fd.getChildren();
      if (!(children.get(0) instanceof ExprNodeGenericFuncDesc) ||
          (!(((ExprNodeGenericFuncDesc) children.get(0)).getGenericUDF()
           instanceof GenericUDFStruct))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition columns not separated for " + fd + ", children size " +
           children.size() + ", child expression : " + children.get(0).getExprString());
        }
        return null;
      }

      // 3. See if the IN (STRUCT(EXP1, EXP2,..) has atleast one expression with partition
      // column with single table alias. If not bail out.
      // We might have expressions containing only partitioning columns, say, T1.A + T2.B
      // where T1.A and T2.B are both partitioning columns.
      // However, these expressions should not be considered as valid expressions for separation.
      if (!hasAtleastOneSubExprWithPartColOrVirtualColWithOneTableAlias(children.get(0))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition columns not separated for " + fd +
            ", there are no expression containing partition columns in struct fields");
        }
        return null;
      }

      // 4. See if all the field expressions of the left hand side of IN are expressions 
      // containing constants or only partition columns coming from same table.
      // If so, we need not perform this optimization and we should bail out.
      if (hasAllSubExprWithConstOrPartColOrVirtualColWithOneTableAlias(children.get(0))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition columns not separated for " + fd +
          ", all fields are expressions containing constants or only partition columns"
          + "coming from same table");
        }
        return null;
      }

      /***************************************************************************************\
       END : Early terminations for Partition Column Separator
      /***************************************************************************************/


      /***************************************************************************************\
       BEGIN : Actual processing of the IN (STRUCT(..)) expression.
      /***************************************************************************************/
      Map<String, TableInfo> tableAliasToInfo =
        new HashMap<>();
      ExprNodeGenericFuncDesc originalStructDesc = ((ExprNodeGenericFuncDesc) children.get(0));
      List<ExprNodeDesc> originalDescChildren = originalStructDesc.getChildren();
      /**
       * PASS 1 : Iterate through the original IN(STRUCT(..)) and populate the tableAlias to
       * predicate information inside tableAliasToInfo.
       */
      for (int i = 0; i < originalDescChildren.size(); i++) {
        ExprNodeDesc en =  originalDescChildren.get(i);
        String tabAlias = null;

        // If the current expression node does not have a virtual/partition column or
        // single table alias reference, ignore it and move to the next expression node.
        if (!exprContainsOnlyPartitionColOrVirtualColOrConstants(en) ||
            (tabAlias = getTableAlias(en)) == null) {
          continue;
        }

        TableInfo currTableInfo = null;

        // If the table alias to information map already contains the current table,
        // use the existing TableInfo object. Else, create a new one.
        if (tableAliasToInfo.containsKey(tabAlias)) {
          currTableInfo = tableAliasToInfo.get(tabAlias);
        } else {
          currTableInfo = new TableInfo();
        }
        currTableInfo.exprNodeLHSDescriptor.add(en);

        // Iterate through the children nodes of the IN clauses starting from index 1,
        // which corresponds to the right hand side of the IN list.
        // Insert the value corresponding to the current expression in currExprNodeInfo.exprNodeValues.
        for (int j = 1; j < children.size(); j++) {
          ExprNodeDesc currChildStructExpr = children.get(j);
          ExprNodeDesc newConstStructElement = null;

          // 1. Get the constant value associated with the current element in the struct.
          // If the current child struct expression is a constant struct.
          if (currChildStructExpr instanceof ExprNodeConstantDesc) {
            List<Object> cnCols = (List<Object>)(((ExprNodeConstantDesc) (children.get(j))).getValue());
            newConstStructElement = new ExprNodeConstantDesc(cnCols.get(i));
          } else {
            // This better be a generic struct with constant values as the children.
            List<ExprNodeDesc> cnChildren = ((ExprNodeGenericFuncDesc) children.get(j)).getChildren();
            newConstStructElement = new ExprNodeConstantDesc(
              (((ExprNodeConstantDesc) (cnChildren.get(i))).getValue()));
          }

          // 2. Insert the current constant value into exprNodeStructs list.
          // If there is no struct corresponding to the current element, create a new one, insert
          // the constant value into it and add the struct as part of exprNodeStructs.
          if (currTableInfo.exprNodeRHSStructs.size() < j) {
            List<ExprNodeDesc> newConstStructList = new ArrayList<ExprNodeDesc>();
            newConstStructList.add(newConstStructElement);
            currTableInfo.exprNodeRHSStructs.add(newConstStructList);
          } else {
            // We already have a struct node for the current index. Insert the constant value
            // into the corresponding struct node.
            currTableInfo.exprNodeRHSStructs.get(j-1).add(newConstStructElement);
          }
        }

        // Insert the current table alias entry into the map if not already present in tableAliasToInfo.
        if (!tableAliasToInfo.containsKey(tabAlias)) {
          tableAliasToInfo.put(tabAlias, currTableInfo);
        }
      }

      /**
       * PASS 2 : Iterate through the tableAliasToInfo populated via PASS 1
       * to generate the new expression.
       */
      // subExpr is the list containing generated IN clauses as a result of this optimization.
      final List<ExprNodeDesc> subExpr =
        new ArrayList<ExprNodeDesc>(originalDescChildren.size()+1);

      for (Entry<String, TableInfo> entry :
        tableAliasToInfo.entrySet()) {
        TableInfo currTableInfo = entry.getValue();
        List<List<ExprNodeDesc>> currConstStructList = currTableInfo.exprNodeRHSStructs;

        // IN(STRUCT(..)..) ExprNodeDesc list for the current table alias.
        List<ExprNodeDesc> currInStructExprList = new ArrayList<ExprNodeDesc>();

        // Add the left hand side of the IN clause which contains the struct definition.
        currInStructExprList.add(ExprNodeGenericFuncDesc.newInstance
          (FunctionRegistry.getFunctionInfo(STRUCT_UDF).getGenericUDF(),
          STRUCT_UDF,
          currTableInfo.exprNodeLHSDescriptor));

        // Generate the right hand side of the IN clause
        for (int i = 0; i < currConstStructList.size(); i++) {
          List<ExprNodeDesc> currConstStruct = currConstStructList.get(i);

          // Add the current constant struct to the right hand side of the IN clause.
          currInStructExprList.add(ExprNodeGenericFuncDesc.newInstance
            (FunctionRegistry.getFunctionInfo(STRUCT_UDF).getGenericUDF(),
            STRUCT_UDF,
            currConstStruct));
        }

        // Add the newly generated IN clause to subExpr.
        subExpr.add(new ExprNodeGenericFuncDesc(
          TypeInfoFactory.booleanTypeInfo, FunctionRegistry.
          getFunctionInfo(IN_UDF).getGenericUDF(), currInStructExprList));
      }
      /***************************************************************************************\
       END : Actual processing of the IN (STRUCT(..)) expression.
      /***************************************************************************************/

      // If there is only 1 table ALIAS, return it
      if (subExpr.size() == 1) {
        // Return the new expression containing only partition columns
        return subExpr.get(0);
      }
      // Return the new expression containing only partition columns
      // after concatenating them with AND operator
      return new ExprNodeGenericFuncDesc(
        TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getFunctionInfo(AND_UDF).getGenericUDF(), subExpr);
    }
  }
}
