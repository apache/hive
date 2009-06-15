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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcessor;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;

/**
 * Factory for generating the different node processors used by ColumnPruner.
 */
public class ColumnPrunerProcFactory {

  /**
   * Node Processor for Column Pruning on Filter Operators.
   */
  public static class ColumnPrunerFilterProc implements NodeProcessor {  
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs) throws SemanticException {
      FilterOperator op = (FilterOperator)nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx)ctx;
      exprNodeDesc condn = op.getConf().getPredicate();
      // get list of columns used in the filter
      List<String> cl = condn.getCols();
      // merge it with the downstream col list
      cppCtx.getPrunedColLists().put(op, Utilities.mergeUniqElems(cppCtx.genColLists(op), cl));
      return null;
    }
  }
  
  /**
   * Factory method to get the ColumnPrunerFilterProc class.
   * @return ColumnPrunerFilterProc
   */
  public static ColumnPrunerFilterProc getFilterProc() {
    return new ColumnPrunerFilterProc();
  }
  
  /**
   * Node Processor for Column Pruning on Group By Operators.
   */
  public static class ColumnPrunerGroupByProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs) throws SemanticException {
      GroupByOperator op = (GroupByOperator)nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx)ctx;
      List<String> colLists = new ArrayList<String>();
      groupByDesc conf = op.getConf();
      ArrayList<exprNodeDesc> keys = conf.getKeys();
      for (exprNodeDesc key : keys)
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());

      ArrayList<aggregationDesc> aggrs = conf.getAggregators();
      for (aggregationDesc aggr : aggrs) { 
        ArrayList<exprNodeDesc> params = aggr.getParameters();
        for (exprNodeDesc param : params) 
          colLists = Utilities.mergeUniqElems(colLists, param.getCols());
      }

      cppCtx.getPrunedColLists().put(op, colLists);
      return null;
    }
  }

  /**
   * Factory method to get the ColumnPrunerGroupByProc class.
   * @return ColumnPrunerGroupByProc
   */
  public static ColumnPrunerGroupByProc getGroupByProc() {
    return new ColumnPrunerGroupByProc();
  }

  /**
   * The Default Node Processor for Column Pruning.
   */
  public static class ColumnPrunerDefaultProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs) throws SemanticException {
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx)ctx;
      cppCtx.getPrunedColLists().put((Operator<? extends Serializable>)nd, 
          cppCtx.genColLists((Operator<? extends Serializable>)nd));
      
      return null;
    }
  }

  /**
   * Factory method to get the ColumnPrunerDefaultProc class.
   * @return ColumnPrunerDefaultProc
   */
  public static ColumnPrunerDefaultProc getDefaultProc() {
    return new ColumnPrunerDefaultProc();
  }
  
  /**
   * The Node Processor for Column Pruning on Reduce Sink Operators.
   */
  public static class ColumnPrunerReduceSinkProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator op = (ReduceSinkOperator)nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx)ctx;
      HashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap = 
          cppCtx.getOpToParseCtxMap();
      RowResolver redSinkRR = opToParseCtxMap.get(op).getRR();
      reduceSinkDesc conf = op.getConf();
      List<Operator<? extends Serializable>> childOperators = op.getChildOperators();
      List<Operator<? extends Serializable>> parentOperators = op.getParentOperators();
      List<String> childColLists = new ArrayList<String>();

      for(Operator<? extends Serializable> child: childOperators)
        childColLists = Utilities.mergeUniqElems(childColLists, cppCtx.getPrunedColLists().get(child));

      List<String> colLists = new ArrayList<String>();
      ArrayList<exprNodeDesc> keys = conf.getKeyCols();
      for (exprNodeDesc key : keys)
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());

      if ((childOperators.size() == 1) && (childOperators.get(0) instanceof JoinOperator)) {
        assert parentOperators.size() == 1;
        Operator<? extends Serializable> par = parentOperators.get(0);
        RowResolver parRR = opToParseCtxMap.get(par).getRR();
        RowResolver childRR = opToParseCtxMap.get(childOperators.get(0)).getRR();

        for (String childCol : childColLists) {
          String [] nm = childRR.reverseLookup(childCol);
          ColumnInfo cInfo = redSinkRR.get(nm[0],nm[1]);
          if (cInfo != null) {
            cInfo = parRR.get(nm[0], nm[1]);
            if (!colLists.contains(cInfo.getInternalName()))
              colLists.add(cInfo.getInternalName());
          }
        }
      }
      else {
        // Reduce Sink contains the columns needed - no need to aggregate from children
        ArrayList<exprNodeDesc> vals = conf.getValueCols();
        for (exprNodeDesc val : vals)
          colLists = Utilities.mergeUniqElems(colLists, val.getCols());
      }

      cppCtx.getPrunedColLists().put(op, colLists);
      return null;
    }
  }

  /**
   * The Factory method to get ColumnPrunerReduceSinkProc class.
   * @return ColumnPrunerReduceSinkProc
   */
  public static ColumnPrunerReduceSinkProc getReduceSinkProc() {
    return new ColumnPrunerReduceSinkProc();
  }

  /**
   * The Node Processor for Column Pruning on Select Operators.
   */
  public static class ColumnPrunerSelectProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs) throws SemanticException {
      SelectOperator op = (SelectOperator)nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx)ctx;
      List<String> cols = new ArrayList<String>();

      if(op.getChildOperators() != null) {
        for(Operator<? extends Serializable> child: op.getChildOperators()) {
          // If one of my children is a FileSink or Script, return all columns.
          // Without this break, a bug in ReduceSink to Extract edge column pruning will manifest
          // which should be fixed before remove this
          if ((child instanceof FileSinkOperator)
              || (child instanceof ScriptOperator)
              || (child instanceof LimitOperator) || (child instanceof UnionOperator)) {
            cppCtx.getPrunedColLists().put(op, cppCtx.getColsFromSelectExpr(op));
            return null;
          }
          cols = Utilities.mergeUniqElems(cols, cppCtx.getPrunedColLists().get(child));
        }
      }

      selectDesc conf = op.getConf();
      // The input to the select does not matter. Go over the expressions 
      // and return the ones which have a marked column
      cppCtx.getPrunedColLists().put(op, cppCtx.getSelectColsFromChildren(op, cols));
      
      // do we need to prune the select operator?
      List<exprNodeDesc> originalColList = op.getConf().getColList();
      List<String> columns = new ArrayList<String>();
      for (exprNodeDesc expr : originalColList)
        Utilities.mergeUniqElems(columns, expr.getCols());
      // by now, 'prunedCols' are columns used by child operators, and 'columns'
      // are columns used by this select operator.
      ArrayList<String> originalOutputColumnNames = conf.getOutputColumnNames();
      if (cols.size() < originalOutputColumnNames.size()) {
        ArrayList<exprNodeDesc> newColList = new ArrayList<exprNodeDesc>();
        ArrayList<String> newOutputColumnNames = new ArrayList<String>();
        Vector<ColumnInfo> rs_oldsignature = op.getSchema().getSignature();
        Vector<ColumnInfo> rs_newsignature = new Vector<ColumnInfo>();
        RowResolver old_rr = cppCtx.getOpToParseCtxMap().get(op).getRR();
        RowResolver new_rr = new RowResolver();
        for(String col : cols){
          int index = originalOutputColumnNames.indexOf(col);
          newOutputColumnNames.add(col);
          newColList.add(originalColList.get(index));
          rs_newsignature.add(rs_oldsignature.get(index));
          String[] tabcol = old_rr.reverseLookup(col);
          ColumnInfo columnInfo = old_rr.get(tabcol[0], tabcol[1]);
          new_rr.put(tabcol[0], tabcol[1], columnInfo);
        }
        cppCtx.getOpToParseCtxMap().get(op).setRR(new_rr);
        op.getSchema().setSignature(rs_newsignature);
        conf.setColList(newColList);
        conf.setOutputColumnNames(newOutputColumnNames);
        handleChildren(op, cols);
      }
      return null;
    }

    /**
     * since we pruned the select operator, we should let its children operator
     * know that. ReduceSinkOperator may send out every output columns of its
     * parent select. When the select operator is pruned, its child reduce
     * sink(direct child) operator should also be pruned.
     * 
     * @param op
     * @param retainedSelOutputCols
     */
    private void handleChildren(SelectOperator op,
        List<String> retainedSelOutputCols) {
      for(Operator<? extends Serializable> child: op.getChildOperators()) {
        if (child instanceof ReduceSinkOperator) {
          pruneReduceSinkOperator(retainedSelOutputCols, (ReduceSinkOperator)child);
        }else if (child instanceof FilterOperator){
          //filter operator has the same output columns as its parent
          for(Operator<? extends Serializable> filterChild: child.getChildOperators()){
            if (filterChild instanceof ReduceSinkOperator)
              pruneReduceSinkOperator(retainedSelOutputCols, (ReduceSinkOperator)filterChild);
          }
        }
      }
    }

    private void pruneReduceSinkOperator(List<String> retainedSelOpOutputCols,
        ReduceSinkOperator child) {
      ReduceSinkOperator reduce = (ReduceSinkOperator) child;
      reduceSinkDesc reduceConf = reduce.getConf();
      ArrayList<String> originalValueOutputColNames = reduceConf
          .getOutputValueColumnNames();
      java.util.ArrayList<exprNodeDesc> originalValueEval = reduceConf
          .getValueCols();
      ArrayList<String> newOutputColNames = new ArrayList<String>();
      java.util.ArrayList<exprNodeDesc> newValueEval = new ArrayList<exprNodeDesc>();
      for (int i = 0; i < originalValueEval.size(); i++) {
        boolean retain = false;
        List<String> current = originalValueEval.get(i).getCols();
        if (current != null) {
          for (int j = 0; j < current.size(); j++) {
            if (retainedSelOpOutputCols.contains(current.get(j))) {
              retain = true;
              break;
            }
          }
        }
        if (retain) {
          newOutputColNames.add(originalValueOutputColNames.get(i));
          newValueEval.add(originalValueEval.get(i));
        }
      }
      reduceConf.setOutputValueColumnNames(newOutputColNames);
      reduceConf.setValueCols(newValueEval);
    }

  }

  /**
   * The Factory method to get the ColumnPrunerSelectProc class.
   * @return ColumnPrunerSelectProc
   */
  public static ColumnPrunerSelectProc getSelectProc() {
    return new ColumnPrunerSelectProc();
  }
  
}
