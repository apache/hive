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
package org.apache.hadoop.hive.ql.optimizer.unionproc;

import java.io.Serializable;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext.UnionParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Operator factory for union processing.
 */
public final class UnionProcFactory {

  private UnionProcFactory() {
    // prevent instantiation
  }

  public static int getPositionParent(UnionOperator union, Stack<Node> stack) {
    int pos = 0;
    int size = stack.size();
    assert size >= 2 && stack.get(size - 1) == union;
    Operator<? extends Serializable> parent = (Operator<? extends Serializable>) stack
        .get(size - 2);
    List<Operator<? extends Serializable>> parUnion = union
        .getParentOperators();
    pos = parUnion.indexOf(parent);
    assert pos < parUnion.size();
    return pos;
  }

  /**
   * MapRed subquery followed by Union.
   */
  public static class MapRedUnion implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      UnionOperator union = (UnionOperator) nd;
      UnionProcContext ctx = (UnionProcContext) procCtx;

      // find the branch on which this processor was invoked
      int pos = getPositionParent(union, stack);
      UnionParseContext uCtx = ctx.getUnionParseContext(union);
      if (uCtx == null) {
        uCtx = new UnionParseContext(union.getConf().getNumInputs());
      }

      ctx.setMapOnlySubq(false);
      uCtx.setMapOnlySubq(pos, false);
      uCtx.setRootTask(pos, false);
      ctx.setUnionParseContext(union, uCtx);
      return null;
    }
  }

  /**
   * Map-only subquery followed by Union.
   */
  public static class MapUnion implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      UnionOperator union = (UnionOperator) nd;
      UnionProcContext ctx = (UnionProcContext) procCtx;

      // find the branch on which this processor was invoked
      int pos = getPositionParent(union, stack);
      UnionParseContext uCtx = ctx.getUnionParseContext(union);
      if (uCtx == null) {
        uCtx = new UnionParseContext(union.getConf().getNumInputs());
      }

      uCtx.setMapOnlySubq(pos, true);
      uCtx.setRootTask(pos, true);
      ctx.setUnionParseContext(union, uCtx);
      return null;
    }
  }

  /**
   * Map-join subquery followed by Union.
   */
  public static class MapJoinUnion implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      UnionOperator union = (UnionOperator) nd;
      UnionProcContext ctx = (UnionProcContext) procCtx;

      // find the branch on which this processor was invoked
      int pos = getPositionParent(union, stack);
      UnionParseContext uCtx = ctx.getUnionParseContext(union);
      if (uCtx == null) {
        uCtx = new UnionParseContext(union.getConf().getNumInputs());
      }

      uCtx.setMapJoinSubq(pos, true);
      ctx.setUnionParseContext(union, uCtx);
      return null;
    }
  }

  /**
   * Union subquery followed by Union.
   */
  public static class UnknownUnion implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      UnionOperator union = (UnionOperator) nd;
      UnionProcContext ctx = (UnionProcContext) procCtx;

      // find the branch on which this processor was invoked
      int pos = getPositionParent(union, stack);
      UnionParseContext uCtx = ctx.getUnionParseContext(union);
      if (uCtx == null) {
        uCtx = new UnionParseContext(union.getConf().getNumInputs());
      }

      uCtx.setMapOnlySubq(pos, true);
      uCtx.setRootTask(pos, false);
      ctx.setUnionParseContext(union, uCtx);
      return null;
    }
  }

  /**
   * Default processor.
   */
  public static class NoUnion implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return null;
    }
  }

  public static NodeProcessor getMapRedUnion() {
    return new MapRedUnion();
  }

  public static NodeProcessor getMapUnion() {
    return new MapUnion();
  }

  public static NodeProcessor getMapJoinUnion() {
    return new MapJoinUnion();
  }

  public static NodeProcessor getUnknownUnion() {
    return new UnknownUnion();
  }

  public static NodeProcessor getNoUnion() {
    return new NoUnion();
  }

}
