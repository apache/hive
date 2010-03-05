/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

//try to replace a bucket map join with a sorted merge map join
public class SortedMergeBucketMapJoinOptimizer implements Transform {

  private static final Log LOG = LogFactory
      .getLog(SortedMergeBucketMapJoinOptimizer.class.getName());

  public SortedMergeBucketMapJoinOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    // go through all map joins and find out all which have enabled bucket map
    // join.
    opRules.put(new RuleRegExp("R1", "MAPJOIN%"),
        getSortedMergeBucketMapjoinProc(pctx));
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  private NodeProcessor getSortedMergeBucketMapjoinProc(ParseContext pctx) {
    return new SortedMergeBucketMapjoinProc(pctx);
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        return null;
      }
    };
  }

  class SortedMergeBucketMapjoinProc implements NodeProcessor {
    ParseContext pGraphContext;

    public SortedMergeBucketMapjoinProc(ParseContext pctx) {
      this.pGraphContext = pctx;
    }

    public SortedMergeBucketMapjoinProc() {
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      if (nd instanceof SMBMapJoinOperator) {
        return null;
      }
      MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
      if (mapJoinOp.getConf().getAliasBucketFileNameMapping() == null
          || mapJoinOp.getConf().getAliasBucketFileNameMapping().size() == 0) {
        return null;
      }

      boolean tableSorted = true;
      QBJoinTree joinCxt = this.pGraphContext.getMapJoinContext()
          .get(mapJoinOp);
      if (joinCxt == null)
        return null;
      String[] srcs = joinCxt.getBaseSrc();
      int pos = 0;
      for (String src : srcs) {
        tableSorted = tableSorted
            && isTableSorted(this.pGraphContext, mapJoinOp, joinCxt, src, pos);
        pos++;
      }
      if (!tableSorted) {
        //this is a mapjoin but not suit for a sort merge bucket map join. check outer joins
        MapJoinProcessor.checkMapJoin(((MapJoinOperator) nd).getConf().getPosBigTable(), 
            ((MapJoinOperator) nd).getConf().getConds());
        return null;
      }
      // convert a bucket map join operator to a sorted merge bucket map join
      // operator
      convertToSMBJoin(mapJoinOp, srcs);
      return null;
    }

    private SMBMapJoinOperator convertToSMBJoin(MapJoinOperator mapJoinOp,
        String[] srcs) {
      SMBMapJoinOperator smbJop = new SMBMapJoinOperator(mapJoinOp);
      SMBJoinDesc smbJoinDesc = new SMBJoinDesc(mapJoinOp.getConf());
      smbJop.setConf(smbJoinDesc);
      HashMap<Byte, String> tagToAlias = new HashMap<Byte, String>();
      for (int i = 0; i < srcs.length; i++) {
        tagToAlias.put((byte) i, srcs[i]);
      }
      smbJoinDesc.setTagToAlias(tagToAlias);
      
      int indexInListMapJoinNoReducer = this.pGraphContext.getListMapJoinOpsNoReducer().indexOf(mapJoinOp);
      if(indexInListMapJoinNoReducer >= 0 ) {
        this.pGraphContext.getListMapJoinOpsNoReducer().remove(indexInListMapJoinNoReducer);
        this.pGraphContext.getListMapJoinOpsNoReducer().add(indexInListMapJoinNoReducer, smbJop);
      }

      List<? extends Operator> parentOperators = mapJoinOp.getParentOperators();
      for (int i = 0; i < parentOperators.size(); i++) {
        Operator par = parentOperators.get(i);
        int index = par.getChildOperators().indexOf(mapJoinOp);
        par.getChildOperators().remove(index);
        par.getChildOperators().add(index, smbJop);
      }
      List<? extends Operator> childOps = mapJoinOp.getChildOperators();
      for (int i = 0; i < childOps.size(); i++) {
        Operator child = childOps.get(i);
        int index = child.getParentOperators().indexOf(mapJoinOp);
        child.getParentOperators().remove(index);
        child.getParentOperators().add(index, smbJop);
      }
      return smbJop;
    }

    private boolean isTableSorted(ParseContext pctx, MapJoinOperator op,
        QBJoinTree joinTree, String alias, int pos) throws SemanticException {
      Map<String, Operator<? extends Serializable>> topOps = this.pGraphContext
          .getTopOps();
      Map<TableScanOperator, Table> topToTable = this.pGraphContext
          .getTopToTable();
      TableScanOperator tso = (TableScanOperator) topOps.get(alias);
      if (tso == null)
        return false;

      List<ExprNodeDesc> keys = op.getConf().getKeys().get((byte) pos);
      // get all join columns from join keys stored in MapJoinDesc
      List<String> joinCols = new ArrayList<String>();
      List<ExprNodeDesc> joinKeys = new ArrayList<ExprNodeDesc>();
      joinKeys.addAll(keys);
      while (joinKeys.size() > 0) {
        ExprNodeDesc node = joinKeys.remove(0);
        if (node instanceof ExprNodeColumnDesc) {
          joinCols.addAll(node.getCols());
        } else if (node instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc udfNode = ((ExprNodeGenericFuncDesc) node);
          GenericUDF udf = udfNode.getGenericUDF();
          if (!FunctionRegistry.isDeterministic(udf)) {
            return false;
          }
          joinKeys.addAll(0, udfNode.getChildExprs());
        }
      }

      Table tbl = topToTable.get(tso);
      if (tbl.isPartitioned()) {
        PrunedPartitionList prunedParts = null;
        try {
          prunedParts = PartitionPruner.prune(tbl, pGraphContext
              .getOpToPartPruner().get(tso), pGraphContext.getConf(), alias,
              pGraphContext.getPrunedPartitions());
        } catch (HiveException e) {
          LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
          throw new SemanticException(e.getMessage(), e);
        }
        boolean ret = true;
        for (Partition p : prunedParts.getConfirmedPartns()) {
          ret = ret && checkSortColsAndJoinCols(p.getSortCols(), joinCols);
          if (!ret) {
            return false;
          }
        }
        for (Partition p : prunedParts.getUnknownPartns()) {
          ret = ret && checkSortColsAndJoinCols(p.getSortCols(), joinCols);
          if (!ret) {
            return false;
          }
        }
      } else {
        return checkSortColsAndJoinCols(tbl.getSortCols(), joinCols);
      }
      return true;
    }

    private boolean checkSortColsAndJoinCols(List<Order> sortCols,
        List<String> joinCols) {
      // require all sort columns are asc, right now only support asc
      List<String> sortColNames = new ArrayList<String>();
      for (Order o : sortCols) {
        if (o.getOrder() != BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC) {
          return false;
        }
        sortColNames.add(o.getCol());
      }

      return sortColNames.containsAll(joinCols)
          && sortColNames.size() == joinCols.size();
    }
  }

}
