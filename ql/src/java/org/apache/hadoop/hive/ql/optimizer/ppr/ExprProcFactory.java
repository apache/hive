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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.ql.udf.UDFOPAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPOr;
import org.apache.hadoop.hive.ql.udf.UDFOPNot;
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * Expression processor factory for partition pruning. Each processor tries
 * to convert the expression subtree into a partition pruning expression.
 * This expression is then used to figure out whether a particular partition
 * should be scanned or not.
 */
public class ExprProcFactory {

  /**
   * Processor for column expressions.
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      
      exprNodeDesc newcd = null;
      exprNodeColumnDesc cd = (exprNodeColumnDesc) nd;
      ExprProcCtx epc = (ExprProcCtx) procCtx;
      if (cd.getTabAlias().equalsIgnoreCase(epc.getTabAlias()) && cd.getIsParititonCol())
        newcd = cd.clone();
      else {
        newcd = new exprNodeConstantDesc(cd.getTypeInfo(), null);
        epc.setHasNonPartCols(true);
      }
      
      return newcd;
    }

  }
  
  /**
   * Process function descriptors. 
   */
  public static class FuncExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      exprNodeDesc newfd = null;
      exprNodeFuncDesc fd = (exprNodeFuncDesc) nd;

      boolean unknown = false;
      // Check if any of the children is unknown for non logical operators
      if (!fd.getUDFMethod().getDeclaringClass().equals(UDFOPAnd.class)
          && !fd.getUDFMethod().getDeclaringClass().equals(UDFOPOr.class)
          && !fd.getUDFMethod().getDeclaringClass().equals(UDFOPNot.class))
        for(Object child: nodeOutputs) {
          exprNodeDesc child_nd = (exprNodeDesc)child;
          if (child_nd instanceof exprNodeConstantDesc &&
              ((exprNodeConstantDesc)child_nd).getValue() == null) {
            unknown = true;
          }
        }
    
      if (fd.getUDFClass().getAnnotation(UDFType.class) != null &&
          (fd.getUDFClass().getAnnotation(UDFType.class).deterministic() == false ||
           unknown))
        newfd = new exprNodeConstantDesc(fd.getTypeInfo(), null);
      else {
        // Create the list of children
        ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>();
        for(Object child: nodeOutputs) {
          children.add((exprNodeDesc) child);
        }
        // Create a copy of the function descriptor
        newfd = new exprNodeFuncDesc(fd.getMethodName(),
                                     fd.getTypeInfo(), fd.getUDFClass(),
                                     fd.getUDFMethod(), children);        
      }
      
      return newfd;
    }

  }

  /**
   * If all children are candidates and refer only to one table alias then this expr is a candidate
   * else it is not a candidate but its children could be final candidates
   */
  public static class GenericFuncExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      exprNodeDesc newfd = null;
      exprNodeGenericFuncDesc fd = (exprNodeGenericFuncDesc) nd;

      boolean unknown = false;
      // Check if any of the children is unknown
      for(Object child: nodeOutputs) {
        exprNodeDesc child_nd = (exprNodeDesc)child;
        if (child_nd instanceof exprNodeConstantDesc &&
            ((exprNodeConstantDesc)child_nd).getValue() == null) {
          unknown = true;
        }
      }
      
      if (unknown)
        newfd = new exprNodeConstantDesc(fd.getTypeInfo(), null);
      else {
        // Create the list of children
        ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>();
        for(Object child: nodeOutputs) {
          children.add((exprNodeDesc) child);
        }
        // Create a copy of the function descriptor
        newfd = new exprNodeGenericFuncDesc(fd.getTypeInfo(), fd.getGenericUDFClass(), children);
      }
      
      return newfd;
    }

  }
  
  public static class FieldExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      
      exprNodeFieldDesc fnd = (exprNodeFieldDesc)nd;
      boolean unknown = false;
      int idx = 0;
      exprNodeDesc left_nd = null;
      for(Object child: nodeOutputs) {
        exprNodeDesc child_nd = (exprNodeDesc) child;
        if (child_nd instanceof exprNodeConstantDesc &&
            ((exprNodeConstantDesc)child_nd).getValue() == null)
          unknown = true;
        left_nd = child_nd;
      }

      assert(idx == 0);

      exprNodeDesc newnd = null;
      if (unknown) {
        newnd = new exprNodeConstantDesc(fnd.getTypeInfo(), null);
      }
      else {
        newnd = new exprNodeFieldDesc(fnd.getTypeInfo(), left_nd, fnd.getFieldName(), fnd.getIsList());
      }
      return newnd;
    }

  }

  /**
   * Processor for constants and null expressions. For such expressions
   * the processor simply clones the exprNodeDesc and returns it.
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      if (nd instanceof exprNodeConstantDesc)
        return ((exprNodeConstantDesc)nd).clone();
      else if (nd instanceof exprNodeNullDesc)
        return ((exprNodeNullDesc)nd).clone();
        
      assert(false);
      return null;
    }
  }

  public static NodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  public static NodeProcessor getFuncProcessor() {
    return new FuncExprProcessor();
  }

  public static NodeProcessor getGenericFuncProcessor() {
    return new GenericFuncExprProcessor();
  }

  public static NodeProcessor getFieldProcessor() {
    return new FieldExprProcessor();
  }

  public static NodeProcessor getColumnProcessor() {
    return new ColumnExprProcessor();
  }
  
  /**
   * Generates the partition pruner for the expression tree
   * @param tabAlias The table alias of the partition table that is being considered for pruning
   * @param pred The predicate from which the partition pruner needs to be generated 
   * @return hasNonPartCols returns true/false depending upon whether this pred has a non partition column
   * @throws SemanticException
   */
  public static exprNodeDesc genPruner(String tabAlias,  exprNodeDesc pred, 
      boolean hasNonPartCols) throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    ExprProcCtx pprCtx= new ExprProcCtx(tabAlias);
    
    // create a walker which walks the tree in a DFS manner while maintaining the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(new RuleRegExp("R1", exprNodeColumnDesc.class.getName() + "%"), getColumnProcessor());
    exprRules.put(new RuleRegExp("R2", exprNodeFieldDesc.class.getName() + "%"), getFieldProcessor());
    exprRules.put(new RuleRegExp("R3", exprNodeFuncDesc.class.getName() + "%"), getFuncProcessor());
    exprRules.put(new RuleRegExp("R5", exprNodeGenericFuncDesc.class.getName() + "%"), getGenericFuncProcessor());
  
    // The dispatcher fires the processor corresponding to the closest matching rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(), exprRules, pprCtx);
    GraphWalker egw = new DefaultGraphWalker(disp);
  
    List<Node> startNodes = new ArrayList<Node>();
    startNodes.add(pred);
    
    HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
    egw.startWalking(startNodes, outputMap);
    hasNonPartCols = pprCtx.getHasNonPartCols();

    // Get the exprNodeDesc corresponding to the first start node;
    return (exprNodeDesc)outputMap.get(pred);
  }

}
