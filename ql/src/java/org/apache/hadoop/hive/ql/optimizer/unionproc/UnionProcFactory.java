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
package org.apache.hadoop.hive.ql.optimizer.unionproc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext.UnionParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

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
    Operator<? extends OperatorDesc> parent =
      (Operator<? extends OperatorDesc>) stack.get(size - 2);
    List<Operator<? extends OperatorDesc>> parUnion = union
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
      int start = stack.size() - 2;
      UnionOperator parentUnionOperator = null;
      while (start >= 0) {
        Operator<? extends OperatorDesc> parent =
          (Operator<? extends OperatorDesc>) stack.get(start);
        if (parent instanceof UnionOperator) {
          parentUnionOperator = (UnionOperator) parent;
          break;
        }
        start--;
      }
      assert parentUnionOperator != null;

      // default to false
      boolean mapOnly = false;
      boolean rootTask = false;
      UnionParseContext parentUCtx =
          ctx.getUnionParseContext(parentUnionOperator);
      if (parentUCtx != null && parentUCtx.allMapOnlySubQSet()) {
        mapOnly = parentUCtx.allMapOnlySubQ();
        rootTask = parentUCtx.allRootTasks();
      }

      uCtx.setMapOnlySubq(pos, mapOnly);

      uCtx.setRootTask(pos, rootTask);
      ctx.setUnionParseContext(union, uCtx);
      return null;
    }
  }

  /**
   * Union followed by no processing.
   * This is to optimize queries of the type:
   * select * from (subq1 union all subq2 ...)x;
   * where at least one of the queries involve a map-reduce job.
   * There is no need for a union in this scenario - it involves an extra
   * write and read for the final output without this optimization.
   * Queries of the form:
   *   select x.c1 from (subq1 union all subq2 ...)x where filter(x.c2);
   * can be transformed to:
   *   select * from (subq1 where filter union all subq2 where filter ...)x;
   * and then optimized.
   */
  public static class UnionNoProcessFile implements NodeProcessor {

    private void pushOperatorsAboveUnion(UnionOperator union,
      Stack<Node> stack, int pos) throws SemanticException {
      // Clone all the operators between union and filescan, and push them above
      // the union. Remove the union (the tree below union gets delinked after that)
      try {
        List<Operator<? extends OperatorDesc>> parents =
          union.getParentOperators();
        int numParents = parents.size();
        for (Operator<? extends OperatorDesc> parent : parents) {
          parent.setChildOperators(null);
        }

        for (; pos < stack.size() - 1; pos++) {
          Operator<? extends OperatorDesc> originalOp =
            (Operator<? extends OperatorDesc>)stack.get(pos);

          for (int p = 0; p < numParents; p++) {
            OperatorDesc cloneDesc = (OperatorDesc)originalOp.getConf().clone();

            RowSchema origSchema = originalOp.getSchema();
            Map<String, ExprNodeDesc> origColExprMap = originalOp.getColumnExprMap();

            Operator<? extends OperatorDesc> cloneOp =
              OperatorFactory.getAndMakeChild(cloneDesc,
                origSchema == null ? null : new RowSchema(origSchema), 
                origColExprMap == null ? null : new HashMap(origColExprMap), 
                parents.get(p));
            parents.set(p, cloneOp);
          }
        }

        // FileSink cannot be simply cloned - it requires some special processing.
        // Sub-queries for the union will be processed as independent map-reduce jobs
        // possibly running in parallel. Those sub-queries cannot write to the same
        // directory. Clone the filesink, but create a sub-directory in the final path
        // for each sub-query. Also, these different filesinks need to be linked to each other
        FileSinkOperator fileSinkOp = (FileSinkOperator)stack.get(pos);
        // For file sink operator, change the directory name
        Path parentDirName = fileSinkOp.getConf().getDirName();

        // Clone the fileSinkDesc of the final fileSink and create similar fileSinks at
        // each parent
        List<FileSinkDesc> fileDescLists = new ArrayList<FileSinkDesc>();

        for (Operator<? extends OperatorDesc> parent : parents) {
          FileSinkDesc fileSinkDesc = (FileSinkDesc) fileSinkOp.getConf().clone();
          fileSinkDesc.setDirName(new Path(parentDirName, AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + parent.getIdentifier()));
          fileSinkDesc.setLinkedFileSink(true);
          if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
            Utilities.FILE_OP_LOGGER.trace("Created LinkedFileSink for union " + fileSinkDesc.getDirName()
                + "; parent " + parentDirName);
          }
          parent.setChildOperators(null);
          Operator<? extends OperatorDesc> tmpFileSinkOp =
            OperatorFactory.getAndMakeChild(fileSinkDesc, parent.getSchema(), parent);
          tmpFileSinkOp.setChildOperators(null);
          fileDescLists.add(fileSinkDesc);
        }

        for (FileSinkDesc fileDesc : fileDescLists) {
          fileDesc.setLinkedFileSinkDesc(fileDescLists);
        }

        // delink union
        union.setChildOperators(null);
        union.setParentOperators(null);
      } catch (Exception e) {
        throw new SemanticException(e.getMessage());
      }
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FileSinkOperator fileSinkOp   = (FileSinkOperator)nd;

      // Has this filesink already been processed
      if (fileSinkOp.getConf().isLinkedFileSink()) {
        return null;
      }

      int size = stack.size();
      int pos = size - 2;
      UnionOperator union = null;

      // Walk the tree. As long as the operators between the union and the filesink
      // do not involve a reducer, and they can be pushed above the union, it makes
      // sense to push them above the union, and remove the union. An interface
      // has been added to the operator 'supportUnionRemoveOptimization' to denote whether
      // this operator can be removed.
      while (pos >= 0) {
        Operator<? extends OperatorDesc> operator =
          (Operator<? extends OperatorDesc>)stack.get(pos);

        // (1) Because we have operator.supportUnionRemoveOptimization() for
        // true only in SEL and FIL operators,
        // this rule will actually only match UNION%(SEL%|FIL%)*FS%
        // (2) The assumption here is that, if
        // operator.getChildOperators().size() > 1, we are going to have
        // multiple FS operators, i.e., multiple inserts.
        // Current implementation does not support this. More details, please
        // see HIVE-9217.
        if (operator.getChildOperators() != null && operator.getChildOperators().size() > 1) {
          return null;
        }
        // Break if it encountered a union
        if (operator instanceof UnionOperator) {
          union = (UnionOperator)operator;
          break;
        }

        if (!operator.supportUnionRemoveOptimization()) {
          return null;
        }
        pos--;
      }

      UnionProcContext ctx = (UnionProcContext) procCtx;
      UnionParseContext uCtx = ctx.getUnionParseContext(union);

      // No need for this if all sub-queries are map-only queries
      // If all the queries are map-only, anyway the query is most optimized
      if ((uCtx != null) && (uCtx.allMapOnlySubQ())) {
        return null;
      }

      pos++;
      pushOperatorsAboveUnion(union, stack, pos);
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

  public static NodeProcessor getUnknownUnion() {
    return new UnknownUnion();
  }

  public static NodeProcessor getNoUnion() {
    return new NoUnion();
  }

  public static NodeProcessor getUnionNoProcessFile() {
    return new UnionNoProcessFile();
  }
}
