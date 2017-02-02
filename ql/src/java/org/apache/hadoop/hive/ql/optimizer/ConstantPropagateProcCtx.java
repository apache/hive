/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * This class implements the processor context for Constant Propagate.
 *
 * ConstantPropagateProcCtx keeps track of propagated constants in a column->const map for each
 * operator, enabling constants to be revolved across operators.
 */
public class ConstantPropagateProcCtx implements NodeProcessorCtx {

  public enum ConstantPropagateOption {
    FULL,      // Do full constant propagation
    SHORTCUT,  // Only perform expression short-cutting - remove unnecessary AND/OR operators
               // if one of the child conditions is true/false.
  };

  private static final Logger LOG = LoggerFactory
      .getLogger(ConstantPropagateProcCtx.class);

  private final Map<Operator<? extends Serializable>, Map<ColumnInfo, ExprNodeDesc>> opToConstantExprs;
  private final Set<Operator<? extends Serializable>> opToDelete;
  private ConstantPropagateOption constantPropagateOption = ConstantPropagateOption.FULL;

  public ConstantPropagateProcCtx() {
    this(ConstantPropagateOption.FULL);
  }

  public ConstantPropagateProcCtx(ConstantPropagateOption option) {
    opToConstantExprs =
        new HashMap<Operator<? extends Serializable>, Map<ColumnInfo, ExprNodeDesc>>();
    opToDelete = new HashSet<Operator<? extends Serializable>>();
    this.constantPropagateOption = option;
  }

  public Map<Operator<? extends Serializable>, Map<ColumnInfo, ExprNodeDesc>> getOpToConstantExprs() {
    return opToConstantExprs;
  }

  /**
   * Get propagated constant map from parents.
   *
   * Traverse all parents of current operator, if there is propagated constant (determined by
   * assignment expression like column=constant value), resolve the column using RowResolver and add
   * it to current constant map.
   *
   * @param op
   *        operator getting the propagated constants.
   * @return map of ColumnInfo to ExprNodeDesc. The values of that map must be either
   *         ExprNodeConstantDesc or ExprNodeNullDesc.
   */
  public Map<ColumnInfo, ExprNodeDesc> getPropagatedConstants(Operator<? extends Serializable> op) {
    // this map should map columnInfo to ExprConstantNodeDesc
    Map<ColumnInfo, ExprNodeDesc> constants = new HashMap<ColumnInfo, ExprNodeDesc>();
    if (op.getSchema() == null) {
      return constants;
    }
    RowSchema rs = op.getSchema();
    LOG.debug("Getting constants of op:" + op + " with rs:" + rs);

    if (op.getParentOperators() == null) {
      return constants;
    }

    // A previous solution is based on tableAlias and colAlias, which is
    // unsafe, esp. when CBO generates derived table names. see HIVE-13602.
    // For correctness purpose, we only trust colExpMap.
    // We assume that CBO can do the constantPropagation before this function is
    // called to help improve the performance.
    // UnionOperator, LimitOperator and FilterOperator are special, they should already be
    // column-position aligned.

    List<Map<Integer, ExprNodeDesc>> parentsToConstant = new ArrayList<>();
    boolean areAllParentsContainConstant = true;
    boolean noParentsContainConstant = true;
    for (Operator<?> parent : op.getParentOperators()) {
      Map<ColumnInfo, ExprNodeDesc> constMap = opToConstantExprs.get(parent);
      if (constMap == null) {
        LOG.debug("Constant of Op " + parent.getOperatorId() + " is not found");
        areAllParentsContainConstant = false;
      } else {
        noParentsContainConstant = false;
        Map<Integer, ExprNodeDesc> map = new HashMap<>();
        for (Entry<ColumnInfo, ExprNodeDesc> entry : constMap.entrySet()) {
          map.put(parent.getSchema().getPosition(entry.getKey().getInternalName()),
              entry.getValue());
        }
        parentsToConstant.add(map);
        LOG.debug("Constant of Op " + parent.getOperatorId() + " " + constMap);
      }
    }
    if (noParentsContainConstant) {
      return constants;
    }

    ArrayList<ColumnInfo> signature = op.getSchema().getSignature();
    if (op instanceof LimitOperator || op instanceof FilterOperator) {
      // there should be only one parent.
      if (op.getParentOperators().size() == 1) {
        Map<Integer, ExprNodeDesc> parentToConstant = parentsToConstant.get(0);
        for (int index = 0; index < signature.size(); index++) {
          if (parentToConstant.containsKey(index)) {
            constants.put(signature.get(index), parentToConstant.get(index));
          }
        }
      }
    } else if (op instanceof UnionOperator && areAllParentsContainConstant) {
      for (int index = 0; index < signature.size(); index++) {
        ExprNodeDesc constant = null;
        for (Map<Integer, ExprNodeDesc> parentToConstant : parentsToConstant) {
          if (!parentToConstant.containsKey(index)) {
            // if this parent does not contain a constant at this position, we
            // continue to look at other positions.
            constant = null;
            break;
          } else {
            if (constant == null) {
              constant = parentToConstant.get(index);
            } else {
              // compare if they are the same constant.
              ExprNodeDesc nextConstant = parentToConstant.get(index);
              if (!nextConstant.isSame(constant)) {
                // they are not the same constant. for example, union all of 1
                // and 2.
                constant = null;
                break;
              }
            }
          }
        }
        // we have checked all the parents for the "index" position.
        if (constant != null) {
          constants.put(signature.get(index), constant);
        }
      }
    } else if (op instanceof JoinOperator) {
      JoinOperator joinOp = (JoinOperator) op;
      Iterator<Entry<Byte, List<ExprNodeDesc>>> itr = joinOp.getConf().getExprs().entrySet()
          .iterator();
      while (itr.hasNext()) {
        Entry<Byte, List<ExprNodeDesc>> e = itr.next();
        int tag = e.getKey();
        Operator<?> parent = op.getParentOperators().get(tag);
        List<ExprNodeDesc> exprs = e.getValue();
        if (exprs == null) {
          continue;
        }
        for (ExprNodeDesc expr : exprs) {
          // we are only interested in ExprNodeColumnDesc
          if (expr instanceof ExprNodeColumnDesc) {
            String parentColName = ((ExprNodeColumnDesc) expr).getColumn();
            // find this parentColName in its parent's rs
            int parentPos = parent.getSchema().getPosition(parentColName);
            if (parentsToConstant.get(tag).containsKey(parentPos)) {
              // this position in parent is a constant
              // reverse look up colExprMap to find the childColName
              if (op.getColumnExprMap() != null && op.getColumnExprMap().entrySet() != null) {
                for (Entry<String, ExprNodeDesc> entry : op.getColumnExprMap().entrySet()) {
                  if (entry.getValue().isSame(expr)) {
                    // now propagate the constant from the parent to the child
                    constants.put(signature.get(op.getSchema().getPosition(entry.getKey())),
                        parentsToConstant.get(tag).get(parentPos));
                  }
                }
              }
            }
          }
        }
      }
    } else {
      // there should be only one parent.
      if (op.getParentOperators().size() == 1) {
        Operator<?> parent = op.getParentOperators().get(0);
        if (op.getColumnExprMap() != null && op.getColumnExprMap().entrySet() != null) {
          for (Entry<String, ExprNodeDesc> entry : op.getColumnExprMap().entrySet()) {
            if (op.getSchema().getPosition(entry.getKey()) == -1) {
              // Not present
              continue;
            }
            ExprNodeDesc expr = entry.getValue();
            if (expr instanceof ExprNodeColumnDesc) {
              String parentColName = ((ExprNodeColumnDesc) expr).getColumn();
              // find this parentColName in its parent's rs
              int parentPos = parent.getSchema().getPosition(parentColName);
              if (parentsToConstant.get(0).containsKey(parentPos)) {
                // this position in parent is a constant
                // now propagate the constant from the parent to the child
                constants.put(signature.get(op.getSchema().getPosition(entry.getKey())),
                    parentsToConstant.get(0).get(parentPos));
              }
            }
          }
        }
      }
    }
    LOG.debug("Offering constants " + constants.keySet() + " to operator " + op.toString());
    return constants;
  }

  public void addOpToDelete(Operator<? extends Serializable> op) {
    opToDelete.add(op);
  }

  public Set<Operator<? extends Serializable>> getOpToDelete() {
    return opToDelete;
  }

  public ConstantPropagateOption getConstantPropagateOption() {
    return constantPropagateOption;
  }

  public void setConstantPropagateOption(
      ConstantPropagateOption constantPropagateOption) {
    this.constantPropagateOption = constantPropagateOption;
  }
}
