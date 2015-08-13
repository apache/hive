package org.apache.hadoop.hive.ql.lib;

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Graph walker this class takes list of starting nodes and walks them in pre-order.
 * If a rule fires up against a given node, we do not try to apply the rule
 * on its children.
 */
public class PreOrderOnceWalker extends PreOrderWalker {

  public PreOrderOnceWalker(Dispatcher disp) {
    super(disp);
  }

  /**
   * Walk the current operator and its descendants.
   * 
   * @param nd
   *          current operator in the graph
   * @throws SemanticException
   */
  @Override
  public void walk(Node nd) throws SemanticException {
    opStack.push(nd);
    dispatch(nd, opStack);

    // The rule has been applied, we bail out
    if (retMap.get(nd) != null) {
      opStack.pop();
      return;
    }

    // move all the children to the front of queue
    if (nd.getChildren() != null) {
      for (Node n : nd.getChildren()) {
        walk(n);
      }
    }

    opStack.pop();
  }

}
