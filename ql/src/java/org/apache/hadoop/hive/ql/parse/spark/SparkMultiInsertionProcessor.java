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

package org.apache.hadoop.hive.ql.parse.spark;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;


public class SparkMultiInsertionProcessor implements NodeProcessor {

  private Set<Operator<?>> processed = new HashSet<Operator<?>>();

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {
    Operator<?> op = (Operator<?>) nd;
    GenSparkProcContext context = (GenSparkProcContext) procCtx;

    if (context.opToParentMap.containsKey(op)) {
      splitPlan(op, context);
      context.opToParentMap.remove(op);
    }

    return null;
  }

  /**
   * Split two tasks by creating a temporary file between them.
   *
   * @param op the select operator encountered
   * @param context processing context
   */
  @SuppressWarnings("nls")
  private void splitPlan(Operator<?> op, GenSparkProcContext context)
      throws SemanticException {
    Preconditions.checkArgument(op.getNumParent() == 1,
        "AssertionError: expecting operator " + op + " to have only one parent," +
            " but found multiple parents : " + op.getParentOperators());
    // nested multi-insertion shouldn't happen.
    SparkTask parentTask = context.defaultTask;
    SparkTask childTask = (SparkTask) TaskFactory.get(
        new SparkWork(context.conf.getVar(HiveConf.ConfVars.HIVEQUERYID)), context.conf);

    GenSparkUtils utils = GenSparkUtils.getUtils();
    ParseContext parseCtx = context.parseContext;
    parentTask.addDependentTask(childTask);

    // Generate the temporary file name
    Operator<?> parent = context.opToParentMap.get(op);

    Path taskTmpDir;
    TableDesc tt_desc;

    if (processed.add(parent)) {
      taskTmpDir = parseCtx.getContext().getMRTmpPath();
      tt_desc = PlanUtils.getIntermediateFileTableDesc(PlanUtils
          .getFieldSchemasFromRowSchema(parent.getSchema(), "temporarycol"));
      createTempFS(parent, taskTmpDir, tt_desc, parseCtx);
    } else {
      FileSinkOperator fs = (FileSinkOperator) parent.getChildOperators().get(0);
      tt_desc = fs.getConf().getTableInfo();
      taskTmpDir = fs.getConf().getDirName();
    }

    TableScanOperator tableScan = createTempTS(parent, op, parseCtx);
    String streamDesc = taskTmpDir.toUri().toString();
    context.opToTaskMap.put(tableScan, childTask);
    context.tempTS.add(tableScan);
    MapWork mapWork = utils.createMapWork(tableScan, childTask.getWork(), streamDesc, tt_desc);
    context.rootToWorkMap.put(tableScan, mapWork);
  }

  private void createTempFS(Operator<? extends OperatorDesc> parent,
      Path taskTmpDir, TableDesc tt_desc, ParseContext parseCtx) {
    boolean compressIntermediate =
        parseCtx.getConf().getBoolVar(HiveConf.ConfVars.COMPRESSINTERMEDIATE);
    FileSinkDesc desc = new FileSinkDesc(taskTmpDir, tt_desc, compressIntermediate);
    if (compressIntermediate) {
      desc.setCompressCodec(parseCtx.getConf().getVar(
          HiveConf.ConfVars.COMPRESSINTERMEDIATECODEC));
      desc.setCompressType(parseCtx.getConf().getVar(
          HiveConf.ConfVars.COMPRESSINTERMEDIATETYPE));
    }
    Operator<? extends OperatorDesc> fileSinkOp = GenMapRedUtils.putOpInsertMap(OperatorFactory
        .get(desc, parent.getSchema()), null, parseCtx);

    // Connect parent to fileSinkOp
    parent.setChildOperators(Utilities.makeList(fileSinkOp));
    fileSinkOp.setParentOperators(Utilities.makeList(parent));
  }

  private TableScanOperator createTempTS(Operator<? extends OperatorDesc> parent,
                                         Operator<? extends OperatorDesc> child,
                                         ParseContext parseCtx) {
    // Create a dummy TableScanOperator for the file generated through fileSinkOp
    RowResolver parentRowResolver =
        parseCtx.getOpParseCtx().get(parent).getRowResolver();
    TableScanOperator tableScanOp = (TableScanOperator) GenMapRedUtils.putOpInsertMap(
        GenMapRedUtils.createTemporaryTableScanOperator(parent.getSchema()),
        parentRowResolver, parseCtx);

    tableScanOp.setChildOperators(Utilities.makeList(child));
    child.setParentOperators(Utilities.makeList(tableScanOp));

    return tableScanOp;
  }

}
