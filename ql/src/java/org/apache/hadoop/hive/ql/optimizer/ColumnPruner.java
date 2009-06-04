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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;

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
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    
    // Iterate over the selects
    for (int pos = 0; pos < colNames.size(); pos++) {
      String   internalName = colNames.get(pos);
      String[] colName      = inputRR.reverseLookup(internalName);
      ColumnInfo in = inputRR.get(colName[0], colName[1]);
      outputRR.put(colName[0], colName[1], 
                   new ColumnInfo((Integer.valueOf(pos)).toString(), in.getType()));
      col_list.add(new exprNodeColumnDesc(in.getType(), internalName));
      colExprMap.put(Integer.toString(pos), col_list.get(pos));
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
      new selectDesc(col_list), new RowSchema(outputRR.getColumnInfos()), input), outputRR);

    output.setColumnExprMap(colExprMap);
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
    pctx.getJoinContext().clear();

    Iterator<Operator<? extends Serializable>> iter = pctx.getOpParseCtx().keySet().iterator();
    while (iter.hasNext()) {
      Operator<? extends Serializable> op = iter.next();
      if ((!pctx.getTopOps().containsValue(op)) && (!pctx.getTopSelOps().containsValue(op)))
        iter.remove();
    }
    pctx.setDestTableId(1);
    pctx.getIdToTableNameMap().clear();
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
    ColumnPrunerProcCtx cppCtx = new ColumnPrunerProcCtx(opToParseCtxMap);
    
    // create a walker which walks the tree in a DFS manner while maintaining the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "FIL%"), ColumnPrunerProcFactory.getFilterProc());
    opRules.put(new RuleRegExp("R2", "GBY%"), ColumnPrunerProcFactory.getGroupByProc());
    opRules.put(new RuleRegExp("R3", "RS%"), ColumnPrunerProcFactory.getReduceSinkProc());
    opRules.put(new RuleRegExp("R4", "SEL%"), ColumnPrunerProcFactory.getSelectProc());

    // The dispatcher fires the processor corresponding to the closest matching rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(ColumnPrunerProcFactory.getDefaultProc(), opRules, cppCtx);
    GraphWalker ogw = new ColumnPrunerWalker(disp);
   
    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);

    // create a new select operator if any of input tables' columns can be pruned
    for (String alias_id : pGraphContext.getTopOps().keySet()) {
      Operator<? extends Serializable> topOp = pGraphContext.getTopOps().get(alias_id);

      List<String> colNames = cppCtx.getPrunedColList(topOp);
      
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
      QB qb = new QB(null, null, false);
      pGraphContext.setQB(qb);
      sem.init(pGraphContext);

      sem.doPhase1(pGraphContext.getParseTree(), qb, sem.initPhase1Ctx());
      sem.getMetaData(qb);
      sem.genPlan(qb);
      pGraphContext = sem.getParseContext();
    }	
    return pGraphContext;
	}
	
	/**
	 * Walks the op tree in post order fashion (skips selects with file sink or script op children)
	 */
	public static class ColumnPrunerWalker extends DefaultGraphWalker {

	  public ColumnPrunerWalker(Dispatcher disp) {
      super(disp);
    }

    /**
	   * Walk the given operator
	   */
	  @Override
	  public void walk(Node nd) throws SemanticException {
	    boolean walkChildren = true;
	    opStack.push(nd);

	    // no need to go further down for a select op with a file sink or script child
	    // since all cols are needed for these ops
	    if(nd instanceof SelectOperator) {
	      for(Node child: nd.getChildren()) {
	        if ((child instanceof FileSinkOperator) || (child instanceof ScriptOperator))
	          walkChildren = false;
	      }
	    }

	    if((nd.getChildren() == null) 
	        || getDispatchedList().containsAll(nd.getChildren()) 
	        || !walkChildren) {
	      // all children are done or no need to walk the children
	      dispatch(nd, opStack);
	      opStack.pop();
	      return;
	    }
	    // move all the children to the front of queue
	    getToWalk().removeAll(nd.getChildren());
	    getToWalk().addAll(0, nd.getChildren());
	    // add self to the end of the queue
	    getToWalk().add(nd);
	    opStack.pop();
	  }
	}
}
