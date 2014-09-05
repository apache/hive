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

import java.util.ArrayList;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;

/**
 * Expression processor factory for pruning. Each processor tries to
 * convert the expression subtree into a pruning expression.
 *
 * It can be used for partition prunner and list bucketing pruner.
 */
public abstract class PrunerExpressionOperatorFactory {

  /**
   * If all children are candidates and refer only to one table alias then this
   * expr is a candidate else it is not a candidate but its children could be
   * final candidates.
   */
  public static class GenericFuncExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ExprNodeDesc newfd = null;
      ExprNodeGenericFuncDesc fd = (ExprNodeGenericFuncDesc) nd;

      boolean unknown = false;

      if (FunctionRegistry.isOpAndOrNot(fd)) {
        // do nothing because "And" and "Or" and "Not" supports null value
        // evaluation
        // NOTE: In the future all UDFs that treats null value as UNKNOWN (both
        // in parameters and return
        // values) should derive from a common base class UDFNullAsUnknown, so
        // instead of listing the classes
        // here we would test whether a class is derived from that base class.
        // If All childs are null, set unknown to true
        boolean isAllNull = true;
        for (Object child : nodeOutputs) {
          ExprNodeDesc child_nd = (ExprNodeDesc) child;
          if (!(child_nd instanceof ExprNodeConstantDesc
              && ((ExprNodeConstantDesc) child_nd).getValue() == null)) {
            isAllNull = false;
          }
        }
        unknown = isAllNull;
      } else if (!FunctionRegistry.isDeterministic(fd.getGenericUDF())) {
        // If it's a non-deterministic UDF, set unknown to true
        unknown = true;
      } else {
        // If any child is null, set unknown to true
        for (Object child : nodeOutputs) {
          ExprNodeDesc child_nd = (ExprNodeDesc) child;
          if (child_nd instanceof ExprNodeConstantDesc
              && ((ExprNodeConstantDesc) child_nd).getValue() == null) {
            unknown = true;
          }
        }
      }

      if (unknown) {
        newfd = new ExprNodeConstantDesc(fd.getTypeInfo(), null);
      } else {
        // Create the list of children
        ArrayList<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
        for (Object child : nodeOutputs) {
          children.add((ExprNodeDesc) child);
        }
        // Create a copy of the function descriptor
        newfd = new ExprNodeGenericFuncDesc(fd.getTypeInfo(), fd.getGenericUDF(), children);
      }

      return newfd;
    }

  }

  /**
   * FieldExprProcessor.
   *
   */
  public static class FieldExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ExprNodeFieldDesc fnd = (ExprNodeFieldDesc) nd;
      boolean unknown = false;
      int idx = 0;
      ExprNodeDesc left_nd = null;
      for (Object child : nodeOutputs) {
        ExprNodeDesc child_nd = (ExprNodeDesc) child;
        if (child_nd instanceof ExprNodeConstantDesc
            && ((ExprNodeConstantDesc) child_nd).getValue() == null) {
          unknown = true;
        }
        left_nd = child_nd;
      }

      assert (idx == 0);

      ExprNodeDesc newnd = null;
      if (unknown) {
        newnd = new ExprNodeConstantDesc(fnd.getTypeInfo(), null);
      } else {
        newnd = new ExprNodeFieldDesc(fnd.getTypeInfo(), left_nd, fnd.getFieldName(),
            fnd.getIsList());
      }
      return newnd;
    }

  }


  /**
   * Processor for column expressions.
   */
  public static abstract class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ExprNodeDesc newcd = null;
      ExprNodeColumnDesc cd = (ExprNodeColumnDesc) nd;
      newcd = processColumnDesc(procCtx, cd);

      return newcd;
    }

    /**
     * Process column desc. It should be done by subclass.
     *
     * @param procCtx
     * @param cd
     * @return
     */
    protected abstract ExprNodeDesc processColumnDesc(NodeProcessorCtx procCtx,
        ExprNodeColumnDesc cd);

  }

  /**
   * Processor for constants and null expressions. For such expressions the
   * processor simply clones the exprNodeDesc and returns it.
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      if (nd instanceof ExprNodeConstantDesc) {
        return ((ExprNodeConstantDesc) nd).clone();
      } else if (nd instanceof ExprNodeNullDesc) {
        return ((ExprNodeNullDesc) nd).clone();
      }

      return new ExprNodeConstantDesc(((ExprNodeDesc)nd).getTypeInfo(), null);
    }
  }

  /**
   * Instantiate default expression processor.
   * @return
   */
  public static final NodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  /**
   * Instantiate generic function processor.
   *
   * @return
   */
  public static final NodeProcessor getGenericFuncProcessor() {
    return new GenericFuncExprProcessor();
  }

  /**
   * Instantiate field processor.
   *
   * @return
   */
  public static final NodeProcessor getFieldProcessor() {
    return new FieldExprProcessor();
  }

}
