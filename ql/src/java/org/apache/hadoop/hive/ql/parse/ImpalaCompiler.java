/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.ImpalaQueryOperator;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.AnalyzeRewriteContext;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaCompiledPlan;
import org.apache.hadoop.hive.ql.plan.impala.work.ImpalaWork;
import org.apache.impala.thrift.TExecRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.metadata.HiveUtils.unparseIdentifier;

/**
 * Creates a single task that encapsulates all work and context required for Impala execution.
 */
public class ImpalaCompiler extends TaskCompiler {
    protected static final Logger LOG = LoggerFactory.getLogger(ImpalaCompiler.class);

    /* When isPlanned is true, a fully planned ExecRequest is expected, otherwise we expect only a query string */
    private boolean isPlanned;
    /* Number of rows fetch from Impala per fetch when streaming */
    private long requestedFetchSize;

    ImpalaCompiler(boolean isPlanned, long requestedFetchSize) {
        this.isPlanned = isPlanned;
        this.requestedFetchSize = requestedFetchSize;
    }

    @Override
    protected void generateTaskTree(List<Task<?>> rootTasks, ParseContext pCtx,
            List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {
        // CDPD-6976: Add Perf logging for Impala Execution
        ImpalaWork work;
        Preconditions.checkArgument(mvTask.size() <= 1, "ImpalaCompiler expects at most 1 MoveTask");
        MoveTask moveTask = mvTask.size() == 0 ? null : (MoveTask) mvTask.get(0);
        if (isPlanned) {
            boolean computeStats = pCtx.getQueryProperties().isAnalyzeCommand() ||
                pCtx.getQueryProperties().isAnalyzeRewrite();
            if (computeStats) {
                // Compute statistics statements are sent to the Impala engine
                // as a SQL statement.
                // We need to create a fetch operator since only Impala returns
                // results for this statement
                createFetchTask(pCtx);
                work = ImpalaWork.createPlannedWork(generateComputeStatsStatement(pCtx, inputs),
                    pCtx.getFetchTask(), requestedFetchSize);
            } else {
                MoveWork moveWork = moveTask == null ? null : (MoveWork) moveTask.getWork();
                // This is the common path for a planned query
                work = ImpalaWork.createPlannedWork(getFinalizedCompiledPlan(pCtx, moveWork), pCtx.getQueryState().getQueryString(),
                    pCtx.getFetchTask(), requestedFetchSize);
            }
        } else {
            // CDPD-7172: Investigate security implications of Impala query execution mode
            work = ImpalaWork.createQuery(pCtx.getQueryState().getQueryString(), pCtx.getFetchTask(), requestedFetchSize);
        }

        Task task = TaskFactory.get(work);
        if (isPlanned && moveTask != null && !work.getCompiledPlan().getIsExplain()) {
            task.addDependentTask(moveTask);
        }
        rootTasks.add(task);
    }

    /**
     * This method creates a fetch task for planned query path.
     * For instance, compute stats returns a row with a single 'summary' string
     * field after completion (see {@link ColumnStatsSemanticAnalyzer#analyze}).
     * However, compute stats query does not return any result in Hive,
     * thus the plan does not contain a fetch task. We solve that mismatch
     * using this method. Note that the fetch task will use the result schema
     * as it was defined during semantic analysis.
     */
    private void createFetchTask(ParseContext pCtx) {
        FetchWork fetch = new FetchWork(new ArrayList<>(), new ArrayList<>(), null);
        fetch.setSink(pCtx.getFetchSink());
        FetchTask fetchTask = (FetchTask) TaskFactory.get(fetch);
        pCtx.setFetchTask(fetchTask);
    }

    /*
     * The translation is as follows:
     * - No column stats:
     *     ANALYZE TABLE `tab` COMPUTE STATISTICS
     *     -> COMPUTE STATS `tab` ()
     * - All columns:
     *     ANALYZE TABLE `tab` COMPUTE STATISTICS FOR COLUMNS
     *     -> COMPUTE STATS `tab`
     * - Subset of columns:
     *    ANALYZE TABLE `tab` COMPUTE STATISTICS FOR COLUMNS `col1`, `col2`
     *     -> COMPUTE STATS `tab` (`col1`, `col2`)
     */
    private String generateComputeStatsStatement(ParseContext pCtx, Set<ReadEntity> inputs) {
        Table table = inputs.iterator().next().getTable();
        StringBuilder sb = new StringBuilder();
        sb.append("COMPUTE STATS ");
        sb.append(unparseIdentifier(table.getDbName(), conf));
        sb.append(".");
        sb.append(unparseIdentifier(table.getTableName(), conf));
        AnalyzeRewriteContext columnStatsCtx = pCtx.getAnalyzeRewrite();
        if (columnStatsCtx != null && columnStatsCtx.getColName() != null) {
            Set<String> colStats = new HashSet<>(columnStatsCtx.getColName());
            boolean append = table.getCols().stream()
                .anyMatch(col -> !colStats.contains(col.getName()));
            if (append) {
              sb.append(" (");
              sb.append(columnStatsCtx.getColName()
                  .stream()
                  .map(colName -> unparseIdentifier(colName, conf))
                  .collect(Collectors.joining(", ")));
              sb.append(")");
            }
        } else {
            sb.append(" ()");
        }
        return sb.toString();
    }

    @Override
    protected void optimizeOperatorPlan(ParseContext pCtx, Set<ReadEntity> inputs,
            Set<WriteEntity> outputs) {
    }

    @Override
    protected void decideExecMode(List<Task<?>> rootTasks, Context ctx,
            GlobalLimitCtx globalLimitCtx) {
    }

    @Override
    protected void optimizeTaskPlan(List<Task<?>> rootTasks, ParseContext pCtx,
            Context ctx) {
    }

    @Override
    protected void setInputFormat(Task<?> rootTask) {
    }

    private ImpalaQueryOperator getImpalaQueryOp(ParseContext pCtx) {
        Collection<Operator<?>> tableScanOps = Lists.newArrayList(pCtx.getTopOps().values());
        Set<ImpalaQueryOperator> fsOps = OperatorUtils.findOperators(tableScanOps, ImpalaQueryOperator.class);
        if (pCtx.getLoadTableWork().size() > 1) {
          throw new RuntimeException("Multi-table inserts not supported for Impala");
        }
        if (fsOps.isEmpty()) {
            throw new RuntimeException("No ImpalaQueryOperator found in the ImpalaCompiler");
        }
        // We expect only a single ImpalaQueryOperator
        Preconditions.checkState(fsOps.size() == 1);
        return fsOps.iterator().next();
    }

    private ImpalaCompiledPlan getFinalizedCompiledPlan(ParseContext pCtx, MoveWork moveWork) throws SemanticException {
      // The primary intent of this method is to examine the MoveWork generated by the TaskCompiler and use the
      // information to deduce where the MoveTask expects the output of the Impala sink. We also propagate the
      // desired writeId. Deferring this work until now allows up to pick up any modifications the compiler
      // does to sinks and sources (such as for CTAS statements).
      // Normal query result location is determined by the sink location in FinkSinkDesc.
      ImpalaCompiledPlan plan = getImpalaQueryOp(pCtx).getCompiledPlan();
      Path destination = null;
      long writeId = -1;
      boolean isOverwrite = false;
      if (moveWork != null) {
        LoadFileDesc lfd = moveWork.getLoadFileWork();
        LoadTableDesc ltd = moveWork.getLoadTableWork();
        LoadMultiFilesDesc lmfd = moveWork.getLoadMultiFilesWork();
        Preconditions.checkState(lmfd == null, "ImpalaCompiler does not support LoadMultiFiles work");
        Preconditions.checkState((lfd != null) ^ (ltd != null), "ImpalaCompiler expects one of LoadFileDesc or LoadTableDesc work");

        if (lfd != null) {
          destination = lfd.getSourcePath();
          writeId = lfd.getWriteId();
          LOG.debug("Using LoadFileDesc to finalize Impala compiled plan: destination: {} writeId: {}",
              destination, writeId);
        }

        if (ltd != null) {
          writeId = ltd.getWriteId();
          destination = ltd.getSourcePath();
          isOverwrite = ltd.isInsertOverwrite();
          LOG.debug("Using LoadTableDesc to finalize Impala compiled plan: destination: {} isOverwrite: {}, writeId: {}",
              destination, isOverwrite, writeId);
        }
      }

      try {
        plan.createExecRequest(destination, isOverwrite, writeId);
      } catch (Exception e) {
        throw new SemanticException(e);
      }
      return plan;
    }

    @Override
    protected void genColumnStats(List<Task<?>> rootTasks, ParseContext pCtx,
            final Set<ReadEntity> inputs, final Set<WriteEntity> outputs) {
        Preconditions.checkArgument(pCtx.getQueryProperties().isAnalyzeRewrite(),
            "Only explicit column stats computation is supported for Impala");
        // Add partitions to output write entities
        Table table = inputs.iterator().next().getTable();
        if (table.isPartitioned() && !pCtx.getOpToPartList().isEmpty()) {
            PrunedPartitionList ppl =
                pCtx.getOpToPartList().entrySet().iterator().next().getValue();
            for (Partition partn : ppl.getPartitions()) {
                LOG.trace("adding part: " + partn);
                outputs.add(new WriteEntity(partn, WriteEntity.WriteType.DDL_NO_LOCK));
            }
        }
    }
}
