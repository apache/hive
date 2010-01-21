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
package org.apache.hadoop.hive.ql.ppd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;

/**
 * Context for Expression Walker for determining predicate pushdown candidates
 * It contains a ExprInfo object for each expression that is processed.
 */
public class ExprWalkerInfo implements NodeProcessorCtx {

  /** Information maintained for an expr while walking an expr tree */
  private static class ExprInfo {
    /**
     * true if expr rooted at this node doesn't contain more than one table
     * alias
     */
    public boolean isCandidate = false;
    /** alias that this expression refers to */
    public String alias = null;
    /** new expr for this expression. */
    public exprNodeDesc convertedExpr = null;

    public ExprInfo() {
    }

    public ExprInfo(boolean isCandidate, String alias, exprNodeDesc replacedNode) {
      this.isCandidate = isCandidate;
      this.alias = alias;
      convertedExpr = replacedNode;
    }
  }

  protected static final Log LOG = LogFactory.getLog(OpProcFactory.class
      .getName());;
  private Operator<? extends Serializable> op = null;
  private RowResolver toRR = null;

  /**
   * this map contains a expr infos. Each key is a node in the expression tree
   * and the information for each node is the value which is used while walking
   * the tree by its parent
   */
  private final Map<String, List<exprNodeDesc>> pushdownPreds;
  /**
   * Values the expression sub-trees (predicates) that can be pushed down for
   * root expression tree. Since there can be more than one alias in an
   * expression tree, this is a map from the alias to predicates.
   */
  private final Map<exprNodeDesc, ExprInfo> exprInfoMap;
  private boolean isDeterministic = true;

  public ExprWalkerInfo() {
    pushdownPreds = new HashMap<String, List<exprNodeDesc>>();
    exprInfoMap = new HashMap<exprNodeDesc, ExprInfo>();
  }

  public ExprWalkerInfo(Operator<? extends Serializable> op,
      final RowResolver toRR) {
    this.op = op;
    this.toRR = toRR;

    pushdownPreds = new HashMap<String, List<exprNodeDesc>>();
    exprInfoMap = new HashMap<exprNodeDesc, ExprInfo>();
  }

  /**
   * @return the op of this expression
   */
  public Operator<? extends Serializable> getOp() {
    return op;
  }

  /**
   * @return the row resolver of the operator of this expression
   */
  public RowResolver getToRR() {
    return toRR;
  }

  /**
   * @return converted expression for give node. If there is none then returns
   *         null.
   */
  public exprNodeDesc getConvertedNode(Node nd) {
    ExprInfo ei = exprInfoMap.get(nd);
    if (ei == null) {
      return null;
    }
    return ei.convertedExpr;
  }

  /**
   * adds a replacement node for this expression
   * 
   * @param oldNode
   *          original node
   * @param newNode
   *          new node
   */
  public void addConvertedNode(exprNodeDesc oldNode, exprNodeDesc newNode) {
    ExprInfo ei = exprInfoMap.get(oldNode);
    if (ei == null) {
      ei = new ExprInfo();
      exprInfoMap.put(oldNode, ei);
    }
    ei.convertedExpr = newNode;
    exprInfoMap.put(newNode, new ExprInfo(ei.isCandidate, ei.alias, null));
  }

  /**
   * Returns true if the specified expression is pushdown candidate else false
   * 
   * @param expr
   * @return true or false
   */
  public boolean isCandidate(exprNodeDesc expr) {
    ExprInfo ei = exprInfoMap.get(expr);
    if (ei == null) {
      return false;
    }
    return ei.isCandidate;
  }

  /**
   * Marks the specified expr to the specified value
   * 
   * @param expr
   * @param b
   *          can
   */
  public void setIsCandidate(exprNodeDesc expr, boolean b) {
    ExprInfo ei = exprInfoMap.get(expr);
    if (ei == null) {
      ei = new ExprInfo();
      exprInfoMap.put(expr, ei);
    }
    ei.isCandidate = b;
  }

  /**
   * Returns the alias of the specified expr
   * 
   * @param expr
   * @return The alias of the expression
   */
  public String getAlias(exprNodeDesc expr) {
    ExprInfo ei = exprInfoMap.get(expr);
    if (ei == null) {
      return null;
    }
    return ei.alias;
  }

  /**
   * Adds the specified alias to the specified expr
   * 
   * @param expr
   * @param alias
   */
  public void addAlias(exprNodeDesc expr, String alias) {
    if (alias == null) {
      return;
    }
    ExprInfo ei = exprInfoMap.get(expr);
    if (ei == null) {
      ei = new ExprInfo();
      exprInfoMap.put(expr, ei);
    }
    ei.alias = alias;
  }

  /**
   * Adds the specified expr as the top-most pushdown expr (ie all its children
   * can be pushed)
   * 
   * @param expr
   */
  public void addFinalCandidate(exprNodeDesc expr) {
    String alias = getAlias(expr);
    if (pushdownPreds.get(alias) == null) {
      pushdownPreds.put(alias, new ArrayList<exprNodeDesc>());
    }
    pushdownPreds.get(alias).add(expr.clone());
  }

  /**
   * Returns the list of pushdown expressions for each alias that appear in the
   * current operator's RowResolver. The exprs in each list can be combined
   * using conjunction (AND)
   * 
   * @return the map of alias to a list of pushdown predicates
   */
  public Map<String, List<exprNodeDesc>> getFinalCandidates() {
    return pushdownPreds;
  }

  /**
   * Merges the specified pushdown predicates with the current class
   * 
   * @param ewi
   *          ExpressionWalkerInfo
   */
  public void merge(ExprWalkerInfo ewi) {
    if (ewi == null) {
      return;
    }
    for (Entry<String, List<exprNodeDesc>> e : ewi.getFinalCandidates()
        .entrySet()) {
      List<exprNodeDesc> predList = pushdownPreds.get(e.getKey());
      if (predList != null) {
        predList.addAll(e.getValue());
      } else {
        pushdownPreds.put(e.getKey(), e.getValue());
      }
    }
  }

  /**
   * sets the deterministic flag for this expression
   * 
   * @param b
   *          deterministic or not
   */
  public void setDeterministic(boolean b) {
    isDeterministic = b;
  }

  /**
   * @return whether this expression is deterministic or not
   */
  public boolean isDeterministic() {
    return isDeterministic;
  }
}
