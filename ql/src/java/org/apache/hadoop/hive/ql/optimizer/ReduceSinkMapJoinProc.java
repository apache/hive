package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.GenTezProcContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.EdgeType;

public class ReduceSinkMapJoinProc implements NodeProcessor {

  protected transient Log LOG = LogFactory.getLog(this.getClass().getName());

  /* (non-Javadoc)
   * This processor addresses the RS-MJ case that occurs in tez on the small/hash
   * table side of things. The work that RS will be a part of must be connected 
   * to the MJ work via be a broadcast edge.
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

    BaseWork myWork = null;

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

        myWork = context.operatorWorkMap.get(childOp);
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

          // remember the output name of the reduce sink
          parentRS.getConf().setOutputName(myWork.getName());

        } else {
          List<BaseWork> linkWorkList = context.linkOpWithWorkMap.get(childOp);
          if (linkWorkList == null) {
            linkWorkList = new ArrayList<BaseWork>();
          }
          linkWorkList.add(parentWork);
          context.linkOpWithWorkMap.put(childOp, linkWorkList);

          List<ReduceSinkOperator> reduceSinks 
            = context.linkWorkWithReduceSinkMap.get(parentWork);
          if (reduceSinks == null) {
            reduceSinks = new ArrayList<ReduceSinkOperator>();
          }
          reduceSinks.add(parentRS);
          context.linkWorkWithReduceSinkMap.put(parentWork, reduceSinks);
        }

        break;
      }

      if ((childOp.getChildOperators() != null) && (childOp.getChildOperators().size() >= 1)) {
        childOp = childOp.getChildOperators().get(0);
      } else {
        break;
      }
    }

    // create the dummy operators
    List<Operator<? extends OperatorDesc>> dummyOperators =
        new ArrayList<Operator<? extends OperatorDesc>>();

    // create an new operator: HashTableDummyOperator, which share the table desc
    HashTableDummyDesc desc = new HashTableDummyDesc();
    @SuppressWarnings("unchecked")
    HashTableDummyOperator dummyOp = (HashTableDummyOperator) OperatorFactory.get(desc);
    TableDesc tbl;

    // need to create the correct table descriptor for key/value
    RowSchema rowSchema = parentRS.getParentOperators().get(0).getSchema();
    tbl = PlanUtils.getReduceValueTableDesc(PlanUtils.getFieldSchemasFromRowSchema(rowSchema, ""));
    dummyOp.getConf().setTbl(tbl);

    Map<Byte, List<ExprNodeDesc>> keyExprMap = mapJoinOp.getConf().getKeys();
    List<ExprNodeDesc> keyCols = keyExprMap.get(Byte.valueOf((byte) 0));
    StringBuffer keyOrder = new StringBuffer();
    for (ExprNodeDesc k: keyCols) {
      keyOrder.append("+");
    }
    TableDesc keyTableDesc = PlanUtils.getReduceKeyTableDesc(PlanUtils
        .getFieldSchemasFromColumnList(keyCols, "mapjoinkey"), keyOrder.toString());
    mapJoinOp.getConf().setKeyTableDesc(keyTableDesc);

    // let the dummy op be the parent of mapjoin op
    mapJoinOp.replaceParent(parentRS, dummyOp);
    List<Operator<? extends OperatorDesc>> dummyChildren =
      new ArrayList<Operator<? extends OperatorDesc>>();
    dummyChildren.add(mapJoinOp);
    dummyOp.setChildOperators(dummyChildren);
    dummyOperators.add(dummyOp);

    // cut the operator tree so as to not retain connections from the parent RS downstream
    List<Operator<? extends OperatorDesc>> childOperators = parentRS.getChildOperators();
    int childIndex = childOperators.indexOf(mapJoinOp);
    childOperators.remove(childIndex);

    // the "work" needs to know about the dummy operators. They have to be separately initialized
    // at task startup
    if (myWork != null) {
      myWork.addDummyOp(dummyOp);
    } else {
      List<Operator<?>> dummyList = dummyOperators;
      if (context.linkChildOpWithDummyOp.containsKey(childOp)) {
        dummyList = context.linkChildOpWithDummyOp.get(childOp);
      }
      dummyList.add(dummyOp);
      context.linkChildOpWithDummyOp.put(childOp, dummyList);
    }
    return true;
  }

}
