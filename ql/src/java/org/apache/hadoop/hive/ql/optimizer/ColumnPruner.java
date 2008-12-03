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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.DefaultDispatcher;
import org.apache.hadoop.hive.ql.parse.Dispatcher;
import org.apache.hadoop.hive.ql.parse.DefaultOpGraphWalker;
import org.apache.hadoop.hive.ql.parse.OpGraphWalker;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.OperatorProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.ql.parse.Rule;

/**
 * Implementation of one of the rule-based optimization steps. ColumnPruner gets the current operator tree. The \
 * tree is traversed to find out the columns used 
 * for all the base tables. If all the columns for a table are not used, a select is pushed on top of that table 
 * (to select only those columns). Since this 
 * changes the row resolver, the tree is built again. This can be optimized later to patch the tree. 
 */
public class ColumnPruner implements Transform {
  protected ParseContext pGraphContext;
  private HashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;


  /**
   * empty constructor
   */
	public ColumnPruner() {
    pGraphContext = null;
	}

	/**
	 * Whether some column pruning needs to be done
	 * @param op Operator for the base table
	 * @param colNames columns needed by the query
	 * @return boolean
	 */
  private boolean pushSelect(Operator<? extends Serializable> op, List<String> colNames) {
    if (pGraphContext.getOpParseCtx().get(op).getRR().getColumnInfos().size() == colNames.size()) return false;
    return true;
  }

  /**
   * update the map between operator and row resolver
   * @param op operator being inserted
   * @param rr row resolver of the operator
   * @return
   */
  @SuppressWarnings("nls")
  private Operator<? extends Serializable> putOpInsertMap(Operator<? extends Serializable> op, RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    pGraphContext.getOpParseCtx().put(op, ctx);
    return op;
  }

  /**
   * insert a select to include only columns needed by the query
   * @param input operator for the base table
   * @param colNames columns needed
   * @return
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genSelectPlan(Operator input, List<String> colNames) 
    throws SemanticException {

    RowResolver inputRR  = pGraphContext.getOpParseCtx().get(input).getRR();
    RowResolver outputRR = new RowResolver();
    ArrayList<exprNodeDesc> col_list = new ArrayList<exprNodeDesc>();
    
    // Iterate over the selects
    for (int pos = 0; pos < colNames.size(); pos++) {
      String   internalName = colNames.get(pos);
      String[] colName      = inputRR.reverseLookup(internalName);
      ColumnInfo in = inputRR.get(colName[0], colName[1]);
      outputRR.put(colName[0], colName[1], 
                   new ColumnInfo((Integer.valueOf(pos)).toString(), in.getType()));
      col_list.add(new exprNodeColumnDesc(in.getType(), internalName));
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
      new selectDesc(col_list), new RowSchema(outputRR.getColumnInfos()), input), outputRR);

    return output;
  }

  /**
   * reset parse context
   * @param pctx parse context
   */
  private void resetParseContext(ParseContext pctx) {
    pctx.getAliasToPruner().clear();
    pctx.getAliasToSamplePruner().clear();
    pctx.getLoadTableWork().clear();
    pctx.getLoadFileWork().clear();
    Iterator<Operator<? extends Serializable>> iter = pctx.getOpParseCtx().keySet().iterator();
    while (iter.hasNext()) {
      Operator<? extends Serializable> op = iter.next();
      if ((!pctx.getTopOps().containsValue(op)) && (!pctx.getTopSelOps().containsValue(op)))
        iter.remove();
    }
  }
	
  /**
   * Transform the query tree. For each table under consideration, check if all columns are needed. If not, 
   * only select the operators needed at the beginning and proceed 
   * @param pactx the current parse context
   */
	public ParseContext transform(ParseContext pactx) throws SemanticException {
    this.pGraphContext = pactx;
    this.opToParseCtxMap = pGraphContext.getOpParseCtx();

    boolean done = true;    

    // generate pruned column list for all relevant operators
    ColumnPrunerProcessor cpp = new ColumnPrunerProcessor(opToParseCtxMap);
    Dispatcher disp = new DefaultDispatcher(cpp);
    OpGraphWalker ogw = new ColumnPrunerWalker(disp);
    ogw.startWalking(pGraphContext.getTopOps().values());

    // create a new select operator if any of input tables' columns can be pruned
    for (String alias_id : pGraphContext.getTopOps().keySet()) {
      Operator<? extends Serializable> topOp = pGraphContext.getTopOps().get(alias_id);

      List<String> colNames = cpp.getPrunedColList(topOp);
      // do we need to push a SELECT - all the columns of the table are not used
      if (pushSelect(topOp, colNames)) {
        topOp.setChildOperators(null);
        // Generate a select and make it a child of the table scan
        Operator select = genSelectPlan(topOp, colNames);
        pGraphContext.getTopSelOps().put(alias_id, select);
        done = false;
      }
    }

    // a select was pushed on top of the table. The old plan is no longer valid. Generate the plan again.
    // The current tables and the select pushed above (after column pruning) are maintained in the parse context.
    if (!done) {
      SemanticAnalyzer sem = (SemanticAnalyzer)SemanticAnalyzerFactory.get(pGraphContext.getConf(), pGraphContext.getParseTree());

      resetParseContext(pGraphContext);
      sem.init(pGraphContext);
      QB qb = new QB(null, null, false);

      sem.doPhase1(pGraphContext.getParseTree(), qb, sem.initPhase1Ctx());
      sem.getMetaData(qb);
      sem.genPlan(qb);
      pGraphContext = sem.getParseContext();
    }	
    return pGraphContext;
	}

	/**
	 * Column pruner processor
	 **/
	public static class ColumnPrunerProcessor implements OperatorProcessor {
	  private  Map<Operator<? extends Serializable>,List<String>> prunedColLists = 
	    new HashMap<Operator<? extends Serializable>, List<String>>();
	  private HashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;
	    
    public ColumnPrunerProcessor(HashMap<Operator<? extends Serializable>, OpParseContext> opToParseContextMap) {
	    this.opToParseCtxMap = opToParseContextMap;
	  }

    /**
     * @return the prunedColLists
     */
    public List<String> getPrunedColList(Operator<? extends Serializable> op) {
      return prunedColLists.get(op);
    }

	  private List<String> genColLists(Operator<? extends Serializable> curOp) throws SemanticException {
	    List<String> colList = new ArrayList<String>();
	    if(curOp.getChildOperators() != null) {
	      for(Operator<? extends Serializable> child: curOp.getChildOperators())
	        colList = Utilities.mergeUniqElems(colList, prunedColLists.get(child));
	    }
	    return colList;
	  }

    public void process(FilterOperator op, OperatorProcessorContext ctx) throws SemanticException {
	    exprNodeDesc condn = op.getConf().getPredicate();
	    // get list of columns used in the filter
	    List<String> cl = condn.getCols();
	    // merge it with the downstream col list
	    prunedColLists.put(op, Utilities.mergeUniqElems(genColLists(op), cl));
	  }

    public void process(GroupByOperator op, OperatorProcessorContext ctx) throws SemanticException {
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

	    prunedColLists.put(op, colLists);
	  }

    public void process(Operator<? extends Serializable> op, OperatorProcessorContext ctx) throws SemanticException {
	    prunedColLists.put(op, genColLists(op));
	  }

    public void process(ReduceSinkOperator op, OperatorProcessorContext ctx) throws SemanticException {
	    RowResolver redSinkRR = opToParseCtxMap.get(op).getRR();
	    reduceSinkDesc conf = op.getConf();
	    List<Operator<? extends Serializable>> childOperators = op.getChildOperators();
	    List<Operator<? extends Serializable>> parentOperators = op.getParentOperators();
	    List<String> childColLists = new ArrayList<String>();

	    for(Operator<? extends Serializable> child: childOperators)
	      childColLists = Utilities.mergeUniqElems(childColLists, prunedColLists.get(child));

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

	    prunedColLists.put(op, colLists);
	  }

    public void process(SelectOperator op, OperatorProcessorContext ctx) throws SemanticException {
	    List<String> cols = new ArrayList<String>();

	    if(op.getChildOperators() != null) {
	      for(Operator<? extends Serializable> child: op.getChildOperators()) {
	        // If one of my children is a FileSink or Script, return all columns.
	        // Without this break, a bug in ReduceSink to Extract edge column pruning will manifest
	        // which should be fixed before remove this
	        if ((child instanceof FileSinkOperator) || (child instanceof ScriptOperator)) {
	          prunedColLists.put(op, getColsFromSelectExpr(op));
	          return;
	        }
	        cols = Utilities.mergeUniqElems(cols, prunedColLists.get(child));
	      }
	    }

	    selectDesc conf = op.getConf();
	    if (conf.isSelectStar() && !cols.isEmpty()) {
	      // The input to the select does not matter. Go over the expressions 
	      // and return the ones which have a marked column
	      prunedColLists.put(op, getSelectColsFromChildren(op, cols));
	      return;
	    }
	    prunedColLists.put(op, getColsFromSelectExpr(op));
	  }

	  private List<String> getColsFromSelectExpr(SelectOperator op) {
	    List<String> cols = new ArrayList<String>();
	    selectDesc conf = op.getConf();
	    ArrayList<exprNodeDesc> exprList = conf.getColList();
	    for (exprNodeDesc expr : exprList)
	      cols = Utilities.mergeUniqElems(cols, expr.getCols());
	    return cols;
	  }

	  private List<String> getSelectColsFromChildren(SelectOperator op, List<String> colList) {
	    List<String> cols = new ArrayList<String>();
	    selectDesc conf = op.getConf();
	    ArrayList<exprNodeDesc> selectExprs = conf.getColList();

	    for (String col : colList) {
	      // col is the internal name i.e. position within the expression list
	      exprNodeDesc expr = selectExprs.get(Integer.parseInt(col));
	      cols = Utilities.mergeUniqElems(cols, expr.getCols());
	    }
	    return cols;
	  }
	}
	/**
	 * Walks the op tree in post order fashion (skips selects with file sink or script op children)
	 */
	public static class ColumnPrunerWalker extends DefaultOpGraphWalker {

	  public ColumnPrunerWalker(Dispatcher disp) {
      super(disp);
    }

    /**
	   * Walk the given operator
	   */
	  @Override
	  public void walk(Operator<? extends Serializable> op) throws SemanticException {
	    boolean walkChildren = true;

	    // no need to go further down for a select op with a file sink or script child
	    // since all cols are needed for these ops
	    if(op instanceof SelectOperator) {
	      for(Operator<? extends Serializable> child: op.getChildOperators()) {
	        if ((child instanceof FileSinkOperator) || (child instanceof ScriptOperator))
	          walkChildren = false;
	      }
	    }

	    if((op.getChildOperators() == null) 
	        || getDispatchedList().containsAll(op.getChildOperators()) 
	        || !walkChildren) {
	      // all children are done or no need to walk the children
	      dispatch(op, null);
	      return;
	    }
	    // move all the children to the front of queue
	    getToWalk().removeAll(op.getChildOperators());
	    getToWalk().addAll(0, op.getChildOperators());
	    // add self to the end of the queue
	    getToWalk().add(op);
	  }
	}
}
