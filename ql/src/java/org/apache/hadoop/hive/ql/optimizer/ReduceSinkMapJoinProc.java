package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.GenTezProcContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.EdgeType;

public class ReduceSinkMapJoinProc implements NodeProcessor {

  protected transient Log LOG = LogFactory.getLog(this.getClass().getName());

  /* (non-Javadoc)
   * This processor addresses the RS-MJ case that occurs in tez on the small/hash
   * table side of things. The connection between the work that RS will be a part of
   * must be connected to the MJ work via be a broadcast edge.
   * We should not walk down the tree when we encounter this pattern because:
   * the type of work (map work or reduce work) needs to be determined
   * on the basis of the big table side because it may be a mapwork (no need for shuffle)
   * or reduce work.
   */
  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procContext, Object... nodeOutputs)
      throws SemanticException {
    GenTezProcContext context = (GenTezProcContext) procContext;
    context.preceedingWork = null;
    context.currentRootOperator = null;

    MapJoinOperator mapJoinOp = (MapJoinOperator)nd;
    Operator<? extends OperatorDesc> childOp = mapJoinOp.getChildOperators().get(0);

    ReduceSinkOperator parentRS = (ReduceSinkOperator)stack.get(stack.size() - 2);

    // remember the original parent list before we start modifying it.
    if (!context.mapJoinParentMap.containsKey(mapJoinOp)) {
      List<Operator<?>> parents = new ArrayList(mapJoinOp.getParentOperators());
      context.mapJoinParentMap.put(mapJoinOp, parents);
    }

    while (childOp != null) {
      if ((childOp instanceof ReduceSinkOperator) || (childOp instanceof FileSinkOperator)) {
        /*
         *  if there was a pre-existing work generated for the big-table mapjoin side,
         *  we need to hook the work generated for the RS (associated with the RS-MJ pattern)
         *  with the pre-existing work.
         *
         *  Otherwise, we need to associate that the reduce sink/file sink down the MJ path
         *  to be linked to the RS work (associated with the RS-MJ pattern).
         *
         */

        BaseWork myWork = context.operatorWorkMap.get(childOp);
        BaseWork parentWork = context.operatorWorkMap.get(parentRS);
          
        // set the link between mapjoin and parent vertex
        int pos = context.mapJoinParentMap.get(mapJoinOp).indexOf(parentRS);
        if (pos == -1) {
          throw new SemanticException("Cannot find position of parent in mapjoin");
        }
        LOG.debug("Mapjoin "+mapJoinOp+", pos: "+pos+" --> "+parentWork.getName());
        mapJoinOp.getConf().getParentToInput().put(pos, parentWork.getName());

        if (myWork != null) {
          // link the work with the work associated with the reduce sink that triggered this rule
          TezWork tezWork = context.currentTask.getWork();
          tezWork.connect(parentWork, myWork, EdgeType.BROADCAST_EDGE);
        } else {
          List<BaseWork> linkWorkList = context.linkOpWithWorkMap.get(childOp);
          if (linkWorkList == null) {
            linkWorkList = new ArrayList<BaseWork>();
          }
          linkWorkList.add(parentWork);
          context.linkOpWithWorkMap.put(childOp, linkWorkList);
        }

        break;
      }

      if ((childOp.getChildOperators() != null) && (childOp.getChildOperators().size() >= 1)) {
        childOp = childOp.getChildOperators().get(0);
      } else {
        break;
      }
    }

    // cut the operator tree so as to not retain connections from the parent RS downstream
    parentRS.removeChild(mapJoinOp);
    return true;
  }

}
