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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * This class implements the processor context for Constant Propagate.
 * 
 * ConstantPropagateProcCtx keeps track of propagated constants in a column->const map for each
 * operator, enabling constants to be revolved across operators.
 */
public class ConstantPropagateProcCtx implements NodeProcessorCtx {

  private static final org.apache.commons.logging.Log LOG = LogFactory
      .getLog(ConstantPropagateProcCtx.class);

  private final Map<Operator<? extends Serializable>, Map<ColumnInfo, ExprNodeDesc>> opToConstantExprs;
  private final Map<Operator<? extends OperatorDesc>, OpParseContext> opToParseCtx;
  private final List<Operator<? extends Serializable>> opToDelete;

  public ConstantPropagateProcCtx(Map<Operator<? extends OperatorDesc>, OpParseContext> opToParseCtx) {
    opToConstantExprs =
        new HashMap<Operator<? extends Serializable>, Map<ColumnInfo, ExprNodeDesc>>();
    opToDelete = new ArrayList<Operator<? extends Serializable>>();
    this.opToParseCtx = opToParseCtx;
  }

  public Map<Operator<? extends Serializable>, Map<ColumnInfo, ExprNodeDesc>> getOpToConstantExprs() {
    return opToConstantExprs;
  }


  public Map<Operator<? extends OperatorDesc>, OpParseContext> getOpToParseCtxMap() {
    return opToParseCtx;
  }

  /**
   * Resolve a ColumnInfo based on given RowResolver.
   * 
   * @param ci
   * @param rr
   * @param parentRR 
   * @return
   * @throws SemanticException
   */
  private ColumnInfo resolve(ColumnInfo ci, RowResolver rr, RowResolver parentRR)
      throws SemanticException {
    // Resolve new ColumnInfo from <tableAlias, alias>
    String alias = ci.getAlias();
    if (alias == null) {
      alias = ci.getInternalName();
    }
    String tblAlias = ci.getTabAlias();
    ColumnInfo rci = rr.get(tblAlias, alias);
    if (rci == null && rr.getRslvMap().size() == 1 && parentRR.getRslvMap().size() == 1) {
      rci = rr.get(null, alias);
    }
    if (rci == null) {
      return null;
    }
    String[] tmp = rr.reverseLookup(rci.getInternalName());
    rci.setTabAlias(tmp[0]);
    rci.setAlias(tmp[1]);
    LOG.debug("Resolved "
        + ci.getTabAlias() + "." + ci.getAlias() + " as "
        + rci.getTabAlias() + "." + rci.getAlias() + " with rr: " + rr);
    return rci;
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
  public Map<ColumnInfo, ExprNodeDesc> getPropagatedConstants(
      Operator<? extends Serializable> op) {
    Map<ColumnInfo, ExprNodeDesc> constants = new HashMap<ColumnInfo, ExprNodeDesc>();
    OpParseContext parseCtx = opToParseCtx.get(op);
    if (parseCtx == null) {
      return constants;
    }
    RowResolver rr = parseCtx.getRowResolver();
    LOG.debug("Getting constants of op:" + op + " with rr:" + rr);
    
    try {
      if (op.getParentOperators() == null) {
        return constants;
      }

      if (op instanceof UnionOperator) {
        String alias = (String) rr.getRslvMap().keySet().toArray()[0];
        // find intersection
        Map<ColumnInfo, ExprNodeDesc> intersection = null;
        for (Operator<?> parent : op.getParentOperators()) {
          Map<ColumnInfo, ExprNodeDesc> unionConst = opToConstantExprs.get(parent);
          LOG.debug("Constant of op " + parent.getOperatorId() + " " + unionConst);
          if (intersection == null) {
            intersection = new HashMap<ColumnInfo, ExprNodeDesc>();
            for (Entry<ColumnInfo, ExprNodeDesc> e : unionConst.entrySet()) {
              ColumnInfo ci = new ColumnInfo(e.getKey());
              ci.setTabAlias(alias);
              intersection.put(ci, e.getValue());
            }
          } else {
            Iterator<Entry<ColumnInfo, ExprNodeDesc>> itr = intersection.entrySet().iterator();
            while (itr.hasNext()) {
              Entry<ColumnInfo, ExprNodeDesc> e = itr.next();
              boolean found = false;
              for (Entry<ColumnInfo, ExprNodeDesc> f : opToConstantExprs.get(parent).entrySet()) {
                if (e.getKey().getInternalName().equals(f.getKey().getInternalName())) {
                  if (e.getValue().isSame(f.getValue())) {
                    found = true;
                  }
                  break;
                }
              }
              if (!found) {
                itr.remove();
              }
            }
          }
          if (intersection.isEmpty()) {
            return intersection;
          }
        }
        LOG.debug("Propagated union constants:" + intersection);
        return intersection;
      }

      for (Operator<? extends Serializable> parent : op.getParentOperators()) {
        Map<ColumnInfo, ExprNodeDesc> c = opToConstantExprs.get(parent);
        for (Entry<ColumnInfo, ExprNodeDesc> e : c.entrySet()) {
          ColumnInfo ci = e.getKey();
          ColumnInfo rci = null;
          ExprNodeDesc constant = e.getValue();
          rci = resolve(ci, rr, opToParseCtx.get(parent).getRowResolver());
          if (rci != null) {
            constants.put(rci, constant);
          } else {
            LOG.debug("Can't resolve " + ci.getTabAlias() + "." + ci.getAlias() + " from rr:"
                + rr);
          }

        }

      }
      LOG.debug("Offerring constants " + constants.keySet()
          + " to operator " + op.toString());
      return constants;
    } catch (SemanticException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public RowResolver getRowResolver(Operator<? extends Serializable> op) {
    OpParseContext parseCtx = opToParseCtx.get(op);
    if (parseCtx == null) {
      return null;
    }
    return parseCtx.getRowResolver();
  }

  public void addOpToDelete(Operator<? extends Serializable> op) {
    opToDelete.add(op);
  }

  public List<Operator<? extends Serializable>> getOpToDelete() {
    return opToDelete;
  }
}
