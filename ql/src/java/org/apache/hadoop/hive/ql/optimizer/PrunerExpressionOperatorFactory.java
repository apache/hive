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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

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
  public static class GenericFuncExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprNodeGenericFuncDesc fd = (ExprNodeGenericFuncDesc) nd;
      if (!FunctionRegistry.isConsistentWithinQuery(fd.getGenericUDF())) {
        return null;
      }

      boolean argsPruned = false;
      List<ExprNodeDesc> children = new ArrayList<>(nodeOutputs.length);
      for (Object child : nodeOutputs) {
        if (child != null) {
          children.add((ExprNodeDesc) child);
        } else {
          argsPruned = true;
        }
      }

      if (!FunctionRegistry.isOpAnd(fd)) {
        if (argsPruned) {
          return null;
        } else {
          return nd;
        }
      } else {
        if (children.isEmpty()) {
          return null;
        } else if (children.size() == 1) {
          return children.get(0);
        } else {
          // Create a copy of the function descriptor
          return new ExprNodeGenericFuncDesc(fd.getTypeInfo(), fd.getGenericUDF(), children);
        }
      }
    }
  }

  /**
   * FieldExprProcessor.
   *
   */
  public static class FieldExprProcessor implements SemanticNodeProcessor {

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
  public static abstract class ColumnExprProcessor implements SemanticNodeProcessor {

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
  public static class DefaultExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      if (nd instanceof ExprNodeConstantDesc) {
        return ((ExprNodeConstantDesc) nd).clone();
      }

      return new ExprNodeConstantDesc(((ExprNodeDesc)nd).getTypeInfo(), null);
    }
  }

  /**
   * Instantiate default expression processor.
   * @return
   */
  public static final SemanticNodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  /**
   * Instantiate generic function processor.
   *
   * @return
   */
  public static final SemanticNodeProcessor getGenericFuncProcessor() {
    return new GenericFuncExprProcessor();
  }

  /**
   * Instantiate field processor.
   *
   * @return
   */
  public static final SemanticNodeProcessor getFieldProcessor() {
    return new FieldExprProcessor();
  }

}
