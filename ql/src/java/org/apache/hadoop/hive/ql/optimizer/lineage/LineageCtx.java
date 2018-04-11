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

package org.apache.hadoop.hive.ql.optimizer.lineage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Predicate;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * This class contains the lineage context that is passed
 * while walking the operator tree in Lineage. The context
 * contains the LineageInfo structure that is passed to the
 * pre-execution hooks.
 */
public class LineageCtx implements NodeProcessorCtx {

  public static class Index implements Serializable {

    /**
     * The map contains an index from the (operator, columnInfo) to the
     * dependency vector for that tuple. This is used to generate the
     * dependency vectors during the walk of the operator tree.
     */
    private final Map<Operator<? extends OperatorDesc>,
                      LinkedHashMap<ColumnInfo, Dependency>> depMap;

    /**
     * A map from operator to the conditions strings.
     */
    private final Map<Operator<? extends OperatorDesc>, Set<Predicate>> condMap;

    /**
     * A map from a final select operator id to the select operator
     * and the corresponding target table in case an insert into query.
     */
    private LinkedHashMap<String, ObjectPair<SelectOperator, Table>> finalSelectOps;

    /**
     * Constructor.
     */
    public Index() {
      depMap =
        new LinkedHashMap<Operator<? extends OperatorDesc>,
                          LinkedHashMap<ColumnInfo, Dependency>>();
      condMap = new HashMap<Operator<? extends OperatorDesc>, Set<Predicate>>();
      finalSelectOps =
        new LinkedHashMap<String, ObjectPair<SelectOperator, Table>>();
    }

    /**
     * Gets the dependency for an operator, columninfo tuple.
     * @param op The operator whose dependency is being inspected.
     * @param col The column info whose dependency is being inspected.
     * @return Dependency for that particular operator, columninfo tuple.
     *         null if no dependency is found.
     */
    public Dependency getDependency(Operator<? extends OperatorDesc> op,
      ColumnInfo col) {
      Map<ColumnInfo, Dependency> colMap = depMap.get(op);
      if (colMap == null) {
        return null;
      }

      return colMap.get(col);
    }

    /**
     * Gets the dependency for a tuple of an operator,
     * and a ColumnInfo with specified internal name.
     *
     * @param op The operator whose dependency is being inspected.
     * @param internalName The internal name of the column info
     * @return Dependency for that particular operator, ColumnInfo tuple.
     *         null if no dependency is found.
     */
    public Dependency getDependency(
        Operator<? extends OperatorDesc> op, String internalName) {
      Map<ColumnInfo, Dependency> colMap = depMap.get(op);
      if (colMap != null) {
        for (Map.Entry<ColumnInfo, Dependency> e: colMap.entrySet()) {
          if (e.getKey().getInternalName().equals(internalName)) {
            return e.getValue();
          }
        }
      }
      return null;
    }

    /**
     * Puts the dependency for an operator, columninfo tuple.
     * @param op The operator whose dependency is being inserted.
     * @param col The column info whose dependency is being inserted.
     * @param dep The dependency.
     */
    public void putDependency(Operator<? extends OperatorDesc> op,
        ColumnInfo col, Dependency dep) {
      LinkedHashMap<ColumnInfo, Dependency> colMap = depMap.get(op);
      if (colMap == null) {
        colMap = new LinkedHashMap<ColumnInfo, Dependency>();
        depMap.put(op, colMap);
      }
      colMap.put(col, dep);
    }

    /**
     * Merges the new dependencies in dep to the existing dependencies
     * of (op, ci).
     *
     * @param op The operator of the column whose dependency is being modified.
     * @param ci The column info of the associated column.
     * @param dep The new dependency.
     */
    public void mergeDependency(Operator<? extends OperatorDesc> op,
        ColumnInfo ci, Dependency dep) {
      Dependency old_dep = getDependency(op, ci);
      if (old_dep == null) {
        putDependency(op, ci, dep);
      } else {
        LineageInfo.DependencyType new_type =
          LineageCtx.getNewDependencyType(old_dep.getType(),
              LineageInfo.DependencyType.EXPRESSION);
        old_dep.setType(new_type);
        Set<BaseColumnInfo> bci_set = new LinkedHashSet<BaseColumnInfo>(old_dep.getBaseCols());
        bci_set.addAll(dep.getBaseCols());
        old_dep.setBaseCols(bci_set);
        // TODO: Fix the expressions later.
        old_dep.setExpr(null);
      }
    }

    public Map<ColumnInfo, Dependency> getDependencies(Operator<? extends OperatorDesc> op) {
      return depMap.get(op);
    }

    public void addPredicate(Operator<? extends OperatorDesc> op, Predicate cond) {
      Set<Predicate> conds = condMap.get(op);
      if (conds == null) {
        conds = new LinkedHashSet<Predicate>();
        condMap.put(op, conds);
      }
      for (Predicate p: conds) {
        if (StringUtils.equals(cond.getExpr(), p.getExpr())) {
          p.getBaseCols().addAll(cond.getBaseCols());
          return;
        }
      }
      conds.add(cond);
    }

    public void copyPredicates(Operator<? extends OperatorDesc> srcOp,
        Operator<? extends OperatorDesc> tgtOp) {
      Set<Predicate> conds = getPredicates(srcOp);
      if (conds != null) {
        for (Predicate cond: conds) {
          addPredicate(tgtOp, cond);
        }
      }
    }

    public Set<Predicate> getPredicates(Operator<? extends OperatorDesc> op) {
      return condMap.get(op);
    }

    public void addFinalSelectOp(
        SelectOperator sop, Operator<? extends OperatorDesc> sinkOp) {
      String operatorId = sop.getOperatorId();
      if (!finalSelectOps.containsKey(operatorId)) {
        Table table = null;
        if (sinkOp instanceof FileSinkOperator) {
          FileSinkOperator fso = (FileSinkOperator) sinkOp;
          table = fso.getConf().getTable();
        }
        finalSelectOps.put(operatorId,
          new ObjectPair<SelectOperator, Table>(sop, table));
      }
    }

    public LinkedHashMap<String,
        ObjectPair<SelectOperator, Table>> getFinalSelectOps() {
      return finalSelectOps;
    }

    public void clear() {
      finalSelectOps.clear();
      depMap.clear();
      condMap.clear();
    }
  }

  /**
   * The map contains an index from the (operator, columnInfo) to the
   * dependency vector for that tuple. This is used to generate the
   * dependency vectors during the walk of the operator tree.
   */
  private final Index index;

  /**
   * Parse context to get to the table metadata information.
   */
  private final ParseContext pctx;

  /**
   * Constructor.
   *
   * @param pctx The parse context that is used to get table metadata information.
   * @param index The dependency map.
   */
  public LineageCtx(ParseContext pctx, Index index) {
    this.index = index;
    this.pctx = pctx;
  }

  /**
   * Gets the parse context.
   *
   * @return ParseContext
   */
  public ParseContext getParseCtx() {
    return pctx;
  }

  /**
   * Gets the dependency index.
   *
   * @return Index
   */
  public Index getIndex() {
    return index;
  }

  /**
   * Gets the new dependency type by comparing the old dependency type and the
   * current dependency type. The current dependency type is the dependency imposed
   * by the current expression. Typically the dependency type is computed using
   * the following rules:
   *    SCRIPT - In case anywhere in the lineage tree there was a script operator, otherwise
   *    EXPRESSION - In case anywhere in the lineage tree a union,
   *                 udf, udaf or udtf was done, otherwise
   *    SIMPLE - This captures direct column copies.
   *
   * @param old_type The old dependency type.
   * @param curr_type The current operators dependency type.
   * @return the dependency type
   */
  public static LineageInfo.DependencyType getNewDependencyType(
      LineageInfo.DependencyType old_type, LineageInfo.DependencyType curr_type) {
    if (old_type == LineageInfo.DependencyType.SCRIPT ||
        curr_type == LineageInfo.DependencyType.SCRIPT) {
      return LineageInfo.DependencyType.SCRIPT;
    }

    if (old_type == LineageInfo.DependencyType.EXPRESSION ||
        curr_type == LineageInfo.DependencyType.EXPRESSION) {
      return LineageInfo.DependencyType.EXPRESSION;
    }

    return LineageInfo.DependencyType.SIMPLE;
  }
}
