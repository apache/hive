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
package org.apache.hadoop.hive.impala.parse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.impala.exec.ImpalaQueryOperator;
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
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec.SpecType;
import org.apache.hadoop.hive.ql.parse.ColumnStatsSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.GlobalLimitCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.impala.plan.ImpalaCompiledPlan;
import org.apache.hadoop.hive.impala.work.ImpalaWork;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.metadata.HiveUtils.unparseIdentifier;

/**
 * Creates a single task that encapsulates all work and context required for Impala execution.
 */
public class ImpalaCompiler extends TaskCompiler {
    protected static final Logger LOG = LoggerFactory.getLogger(ImpalaCompiler.class);

    /* Number of rows fetch from Impala per fetch when streaming */
    private final long requestedFetchSize;

    public ImpalaCompiler(long requestedFetchSize) {
        this.requestedFetchSize = requestedFetchSize;
    }

    @Override
    protected void generateTaskTree(List<Task<?>> rootTasks, ParseContext pCtx,
            List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {
        // CDPD-6976: Add Perf logging for Impala Execution
        ImpalaWork work;
        Preconditions.checkArgument(mvTask.size() <= 1, "ImpalaCompiler expects at most 1 MoveTask");
        MoveTask moveTask = mvTask.size() == 0 ? null : (MoveTask) mvTask.get(0);
        boolean computeStats = pCtx.getQueryProperties().isAnalyzeCommand() ||
            pCtx.getQueryProperties().isAnalyzeRewrite();
        boolean dropStats = pCtx.getQueryProperties().isDropStatsCommand();
        if (computeStats || dropStats) {
            // Compute and drop statistics statements are sent to the Impala engine
            // as a SQL statement.
            // We need to create a fetch operator since only Impala returns
            // results for this statement
            createFetchTask(pCtx);
            if (computeStats) {
              work = ImpalaWork.createPlannedWork(generateComputeStatsStatement(pCtx, inputs),
                  pCtx.getFetchTask(), requestedFetchSize, generateInvalidateTableMetadataStatement(pCtx, inputs));
            } else {
              work = ImpalaWork.createPlannedWork(generateDropStatsStatement(pCtx, inputs),
                  pCtx.getFetchTask(), requestedFetchSize, generateInvalidateTableMetadataStatement(pCtx, inputs));
            }
        } else {
            MoveWork moveWork = moveTask == null ? null : moveTask.getWork();
            ImpalaCompiledPlan impalaCompiledPlan = getFinalizedCompiledPlan(pCtx, moveWork);
            Preconditions.checkNotNull(impalaCompiledPlan, "Impala compiled plan cannot be null");
            boolean submitExplainToBackend = !conf.getBoolVar(ConfVars.HIVE_IN_TEST) && impalaCompiledPlan.getIsExplain();
            // This is the common path for a planned query
            work = ImpalaWork.createPlannedWork(impalaCompiledPlan, pCtx.getQueryState(),
                pCtx.getFetchTask(), requestedFetchSize, submitExplainToBackend, pCtx.getContext());
        }

        Task<ImpalaWork> task = TaskFactory.get(work);
        if (moveTask != null) {
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
     * - Incremental stats:
     *     ANALYZE TABLE `tab` COMPUTE INCREMENTAL STATISTICS
     *     -> COMPUTE INCREMENTAL STATS `tab`
     * - Incremental stats with partition spec:
     *     ANALYZE TABLE `tab` PARTITION(`part_col`='part_val') COMPUTE INCREMENTAL STATISTICS
     *     -> COMPUTE INCREMENTAL STATS `tab` PARTITION (`ss_sold_date_sk`='part_val')
     */
    private String generateComputeStatsStatement(ParseContext pCtx, Set<ReadEntity> inputs) {
        boolean incremental = pCtx.getQueryProperties().isIncrementalStats();
        Table table = inputs.iterator().next().getTable();
        StringBuilder sb = new StringBuilder();
        sb.append("COMPUTE ");
        if (incremental) {
          sb.append("INCREMENTAL ");
        }
        sb.append("STATS ");
        sb.append(unparseIdentifier(table.getDbName(), conf));
        sb.append(".");
        sb.append(unparseIdentifier(table.getTableName(), conf));
        // Partition columns (if they have been specified)
        if (table.getTableSpec() != null && table.getTableSpec().specType == SpecType.STATIC_PARTITION) {
            List<FieldSchema> partCols = table.getPartCols();
            Map<String, String> partitionSpec = table.getTableSpec().partSpec;
            sb.append(" PARTITION (");
            sb.append(partCols
                .stream()
                .map(fs -> unparseIdentifier(fs.getName(), conf) + "=" + genPartValueString(fs, partitionSpec))
                .collect(Collectors.joining(", ")));
            sb.append(")");
        }
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
        } else if (!incremental) {
            sb.append(" ()");
        }
        return sb.toString();
    }

    // This method is taken from CDPD-23225, we will unify both once it is pushed
    private String genPartValueString(FieldSchema fs, Map<String, String> partitionSpec) {
        String partVal = partitionSpec.get(fs.getName());
        String partType = fs.getType().toLowerCase();
        StringBuilder sb = new StringBuilder();
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(partType);
        switch (pti.getTypeName()) {
        // TODD: Remove the partition column data types
        // not supported in Impala
        case serdeConstants.BOOLEAN_TYPE_NAME: // NOT supported ?
        case serdeConstants.INT_TYPE_NAME: // supported
        case serdeConstants.BIGINT_TYPE_NAME: // supported
        case serdeConstants.CHAR_TYPE_NAME:
        case serdeConstants.FLOAT_TYPE_NAME: // supported
        case serdeConstants.DOUBLE_TYPE_NAME: // supported
        case serdeConstants.TINYINT_TYPE_NAME: // supported
        case serdeConstants.SMALLINT_TYPE_NAME: // supported
        case serdeConstants.BINARY_TYPE_NAME: // NOT supported
            sb.append(partVal);
            break;
        case serdeConstants.STRING_TYPE_NAME: // supported
        case serdeConstants.DATE_TYPE_NAME: // supported
        case serdeConstants.TIMESTAMP_TYPE_NAME: // NOT supported
        case serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME:
        case serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME:
            sb.append("\"");
            sb.append(partVal);
            sb.append("\"");
            break;
        default:
            throw new RuntimeException("Unknown data type " + pti.getTypeName()
                + " for partition column: " + fs.getName());
        }
        return sb.toString();
    }

    /*
     * The translation is as follows:
     * - All stats:
     *     DROP STATISTICS `tab`
     *     -> DROP STATS `tab`
     * - Incremental stats:
     *     DROP INCREMENTAL STATISTICS `tab`
     *     -> DROP INCREMENTAL STATS `tab`
     * - Incremental stats with partition spec:
     *     DROP INCREMENTAL STATISTICS `tab` PARTITION(`part_col`='part_val')
     *     -> DROP INCREMENTAL STATS `tab` PARTITION (`ss_sold_date_sk`='part_val')
     */
    private String generateDropStatsStatement(ParseContext pCtx, Set<ReadEntity> inputs) {
        boolean incremental = pCtx.getQueryProperties().isIncrementalStats();
        Table table = inputs.iterator().next().getTable();
        StringBuilder sb = new StringBuilder();
        sb.append("DROP ");
        if (incremental) {
          sb.append("INCREMENTAL ");
        }
        sb.append("STATS ");
        sb.append(unparseIdentifier(table.getDbName(), conf));
        sb.append(".");
        sb.append(unparseIdentifier(table.getTableName(), conf));
        // Partition columns (if they have been specified)
        if (table.getTableSpec() != null && table.getTableSpec().specType == SpecType.STATIC_PARTITION) {
            List<FieldSchema> partCols = table.getPartCols();
            Map<String, String> partitionSpec = table.getTableSpec().partSpec;
            sb.append(" PARTITION (");
            sb.append(partCols
                .stream()
                .map(fs -> unparseIdentifier(fs.getName(), conf) + "=" + genPartValueString(fs, partitionSpec))
                .collect(Collectors.joining(", ")));
            sb.append(")");
        }
        return sb.toString();
    }

    private String generateInvalidateTableMetadataStatement(ParseContext pCtx, Set<ReadEntity> inputs) {
        Table table = inputs.iterator().next().getTable();
        StringBuilder sb = new StringBuilder();
        sb.append("INVALIDATE METADATA ");
        sb.append(unparseIdentifier(table.getDbName(), conf));
        sb.append(".");
        sb.append(unparseIdentifier(table.getTableName(), conf));
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
