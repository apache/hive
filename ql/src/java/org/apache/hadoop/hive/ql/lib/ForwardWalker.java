package org.apache.hadoop.hive.ql.lib;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ForwardWalker extends DefaultGraphWalker {

  /**
* Constructor.
*
* @param disp
* dispatcher to call for each op encountered
*/
  public ForwardWalker(Dispatcher disp) {
    super(disp);
  }

  @SuppressWarnings("unchecked")
  protected boolean allParentsDispatched(Node nd) {
    Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
    if (op.getParentOperators() == null) {
      return true;
    }
    for (Node pNode : op.getParentOperators()) {
      if (!getDispatchedList().contains(pNode)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  protected void addAllParents(Node nd) {
    Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
    if (op.getParentOperators() == null) {
      return;
    }
    getToWalk().removeAll(op.getParentOperators());
    getToWalk().addAll(0, op.getParentOperators());
  }

  /**
* walk the current operator and its descendants.
*
* @param nd
* current operator in the graph
* @throws SemanticException
*/
  public void walk(Node nd) throws SemanticException {
    if (opStack.empty() || nd != opStack.peek()) {
      opStack.push(nd);
    }
    if (allParentsDispatched(nd)) {
      // all children are done or no need to walk the children
      if (!getDispatchedList().contains(nd)) {
        getToWalk().addAll(nd.getChildren());
        dispatch(nd, opStack);
      }
      opStack.pop();
      return;
    }
    // add children, self to the front of the queue in that order
    getToWalk().add(0, nd);
    addAllParents(nd);
  }
}