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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.ContentSummaryInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.InputEstimator;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * Tries to convert simple fetch query to single fetch task, which fetches rows directly
 * from location of table/partition.
 */
public class SimpleFetchOptimizer extends Transform {

  private final Logger LOG = LoggerFactory.getLogger(SimpleFetchOptimizer.class.getName());

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Map<String, TableScanOperator> topOps = pctx.getTopOps();
    if (pctx.getQueryProperties().isQuery() && !pctx.getQueryProperties().isAnalyzeCommand()
        && topOps.size() == 1) {
      // no join, no groupby, no distinct, no lateral view, no subq,
      // no CTAS or insert, not analyze command, and single sourced.
      String alias = (String) pctx.getTopOps().keySet().toArray()[0];
      TableScanOperator topOp = pctx.getTopOps().values().iterator().next();
      try {
        FetchTask fetchTask = optimize(pctx, alias, topOp);
        if (fetchTask != null) {
          pctx.setFetchTask(fetchTask);
        }
      } catch (Exception e) {
        LOG.error("Failed to transform", e);
        if (e instanceof SemanticException) {
          throw (SemanticException) e;
        }
        throw new SemanticException(e.getMessage(), e);
      }
    }
    return pctx;
  }

  // returns non-null FetchTask instance when succeeded
  @SuppressWarnings("unchecked")
  private FetchTask optimize(ParseContext pctx, String alias, TableScanOperator source)
      throws Exception {
    String mode = HiveConf.getVar(
        pctx.getConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION);

    boolean aggressive = "more".equals(mode);
    final int limit = pctx.getQueryProperties().getOuterQueryLimit();
    // limit = 0 means that we do not need any task.
    if (limit == 0) {
      return null;
    }
    FetchData fetch = checkTree(aggressive, pctx, alias, source);
    if (fetch != null && checkThreshold(fetch, limit, pctx)) {
      FetchWork fetchWork = fetch.convertToWork();
      FetchTask fetchTask = (FetchTask) TaskFactory.get(fetchWork);
      fetchTask.setCachingEnabled(HiveConf.getBoolVar(pctx.getConf(),
              HiveConf.ConfVars.HIVE_FETCH_TASK_CACHING));
      fetchWork.setSink(fetch.completed(pctx, fetchWork));
      fetchWork.setSource(source);
      fetchWork.setLimit(limit);
      return fetchTask;
    }
    return null;
  }

  private boolean checkThreshold(FetchData data, int limit, ParseContext pctx) throws Exception {
    boolean cachingEnabled = HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CACHING);
    if (!cachingEnabled) {
      if (limit > 0) {
        if (data.hasOnlyPruningFilter()) {
          // partitioned table + query has only pruning filters
          return true;
        } else if (data.isPartitioned() == false && data.isFiltered() == false) {
          // unpartitioned table + no filters
          return true;
        }
        // fall through
      }
      Operator child = data.scanOp.getChildOperators().get(0);
      if(child instanceof SelectOperator) {
        // select *, constant and casts can be allowed without a threshold check
        if (checkExpressions((SelectOperator)child)) {
          return true;
        }
      }
    }
    // if caching is enabled we apply the treshold in all cases
    long threshold = HiveConf.getLongVar(pctx.getConf(),
        HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION_THRESHOLD);
    if (threshold < 0) {
      return true;
    }
    return data.isDataLengthWithInThreshold(pctx, threshold);
  }

  // all we can handle is LimitOperator, FilterOperator SelectOperator and final FS
  //
  // for non-aggressive mode (minimal)
  // 1. sampling is not allowed
  // 2. for partitioned table, all filters should be targeted to partition column
  // 3. SelectOperator should use only simple cast/column access
  private FetchData checkTree(boolean aggressive, ParseContext pctx, String alias,
      TableScanOperator ts) throws HiveException {
    SplitSample splitSample = pctx.getNameToSplitSample().get(alias);
    if (!aggressive && splitSample != null) {
      return null;
    }
    if (!aggressive && ts.getConf().getTableSample() != null) {
      return null;
    }
    Table table = ts.getConf().getTableMetadata();
    if (table == null) {
      return null;
    }
    ReadEntity parent = PlanUtils.getParentViewInfo(alias, pctx.getViewAliasToInput());
    if (!table.isPartitioned()) {
      FetchData fetch = new FetchData(ts, parent, table, splitSample);
      return checkOperators(fetch, aggressive, false);
    }

    boolean bypassFilter = false;
    if (HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVE_OPT_PPD)) {
      ExprNodeDesc pruner = pctx.getOpToPartPruner().get(ts);
      if (PartitionPruner.onlyContainsPartnCols(table, pruner)) {
        bypassFilter = !pctx.getPrunedPartitions(alias, ts).hasUnknownPartitions();
      }
    }

    boolean onlyPruningFilter = bypassFilter;
    Operator<?> op = ts;
    while (onlyPruningFilter) {
      if (op instanceof FileSinkOperator || op.getChildOperators() == null) {
        break;
      } else if (op.getChildOperators().size() != 1) {
        onlyPruningFilter = false;
        break;
      } else {
        op = op.getChildOperators().get(0);
      }

      if (op instanceof FilterOperator) {
        ExprNodeDesc predicate = ((FilterOperator) op).getConf().getPredicate();
        if (predicate instanceof ExprNodeConstantDesc
                && serdeConstants.BOOLEAN_TYPE_NAME.equals(predicate.getTypeInfo().getTypeName())) {
          continue;
        } else if (PartitionPruner.onlyContainsPartnCols(table, predicate)) {
          continue;
        } else {
          onlyPruningFilter = false;
        }
      }
    }

    if (!aggressive && !onlyPruningFilter) {
      return null;
    }

    PrunedPartitionList partitions = pctx.getPrunedPartitions(alias, ts);
    FetchData fetch = new FetchData(ts, parent, table, partitions, splitSample, onlyPruningFilter);
    return checkOperators(fetch, aggressive, bypassFilter);
  }

  private FetchData checkOperators(FetchData fetch, boolean aggressive, boolean bypassFilter) {
    if (aggressive) {
      return isConvertible(fetch) ? fetch : null;
    }
    return checkOperators(fetch, fetch.scanOp, bypassFilter);
  }

  private FetchData checkOperators(FetchData fetch, TableScanOperator ts, boolean bypassFilter) {
    if (ts.getChildOperators().size() != 1) {
      return null;
    }
    Operator<?> op = ts.getChildOperators().get(0);
    for (; ; op = op.getChildOperators().get(0)) {
      if (op instanceof SelectOperator) {
        if (!checkExpressions((SelectOperator) op)) {
          return null;
        }
        continue;
      }

      if (!(op instanceof LimitOperator || (op instanceof FilterOperator && bypassFilter))) {
        break;
      }

      if (op.getChildOperators() == null || op.getChildOperators().size() != 1) {
        return null;
      }

      if (op instanceof FilterOperator) {
        fetch.setFiltered(true);
      }
    }

    if (op instanceof FileSinkOperator) {
      fetch.fileSink = op;
      return fetch;
    }

    return null;
  }

  private boolean checkExpressions(SelectOperator op) {
    SelectDesc desc = op.getConf();
    if (desc.isSelectStar() || desc.isSelStarNoCompute()) {
      return true;
    }
    for (ExprNodeDesc expr : desc.getColList()) {
      if (!checkExpression(expr)) {
        return false;
      }
    }
    return true;
  }

  private boolean checkExpression(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeConstantDesc ||
        expr instanceof ExprNodeColumnDesc) {
      return true;
    }

    if (expr instanceof ExprNodeGenericFuncDesc) {
      GenericUDF udf = ((ExprNodeGenericFuncDesc) expr).getGenericUDF();
      if (udf instanceof GenericUDFToBinary || udf instanceof GenericUDFToChar
          || udf instanceof GenericUDFToDate || udf instanceof GenericUDFToDecimal
          || udf instanceof GenericUDFToUnixTimeStamp || udf instanceof GenericUDFToUtcTimestamp
          || udf instanceof GenericUDFToVarchar) {
        return expr.getChildren().size() == 1 && checkExpression(expr.getChildren().get(0));
      }
    }
    return false;
  }

  private boolean isConvertible(FetchData fetch) {
    return isConvertible(fetch, fetch.scanOp, new HashSet<Operator<?>>());
  }

  private boolean isConvertible(FetchData fetch, Operator<?> operator, Set<Operator<?>> traversed) {
    if (operator instanceof ReduceSinkOperator || operator instanceof CommonJoinOperator
        || operator instanceof ScriptOperator) {
      return false;
    }

    if (operator instanceof FilterOperator) {
      fetch.setFiltered(true);
    }

    if (!traversed.add(operator)) {
      return true;
    }
    if (operator.getNumChild() == 0) {
      if (operator instanceof FileSinkOperator) {
        fetch.fileSink = operator;
        return true;
      }
      return false;
    }
    for (Operator<?> child : operator.getChildOperators()) {
      if (!traversed.containsAll(child.getParentOperators())){
        continue;
      }
      if (!isConvertible(fetch, child, traversed)) {
        return false;
      }
    }
    return true;
  }

  enum Status {
    PASS,
    FAIL,
    UNAVAILABLE
  }

  private class FetchData {

    // source table scan
    private final TableScanOperator scanOp;
    private final ReadEntity parent;

    private final Table table;
    private final SplitSample splitSample;
    private final PrunedPartitionList partsList;
    private final Set<ReadEntity> inputs = new LinkedHashSet<ReadEntity>();
    private final boolean onlyPruningFilter;

    // this is always non-null when conversion is completed
    private Operator<?> fileSink;
    private boolean filtered;

    private FetchData(TableScanOperator scanOp, ReadEntity parent, Table table, SplitSample splitSample) {
      this.scanOp = scanOp;
      this.parent = parent;
      this.table = table;
      this.partsList = null;
      this.splitSample = splitSample;
      this.onlyPruningFilter = false;
    }

    private FetchData(TableScanOperator scanOp, ReadEntity parent, Table table, PrunedPartitionList partsList,
        SplitSample splitSample, boolean bypassFilter) {
      this.scanOp = scanOp;
      this.parent = parent;
      this.table = table;
      this.partsList = partsList;
      this.splitSample = splitSample;
      this.onlyPruningFilter = bypassFilter;
    }

    /*
     * all filters were executed during partition pruning
     */
    public final boolean hasOnlyPruningFilter() {
      return this.onlyPruningFilter;
    }

    public final boolean isPartitioned() {
      return this.table.isPartitioned();
    }

    /* there are filter operators in the pipeline */
    public final boolean isFiltered() {
      return this.filtered;
    }

    public final void setFiltered(boolean filtered) {
      this.filtered = filtered;
    }

    private FetchWork convertToWork() throws HiveException {
      inputs.clear();
      Utilities.addSchemaEvolutionToTableScanOperator(table, scanOp);
      TableDesc tableDesc = Utilities.getTableDesc(table);
      if (!table.isPartitioned()) {
        inputs.add(new ReadEntity(table, parent, !table.isView() && parent == null));
        FetchWork work = new FetchWork(table.getPath(), tableDesc);
        PlanUtils.configureInputJobPropertiesForStorageHandler(work.getTblDesc());
        work.setSplitSample(splitSample);
        return work;
      }
      List<Path> listP = new ArrayList<Path>();
      List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

      for (Partition partition : partsList.getNotDeniedPartns()) {
        inputs.add(new ReadEntity(partition, parent, parent == null));
        listP.add(partition.getDataLocation());
        partP.add(Utilities.getPartitionDescFromTableDesc(tableDesc, partition, true));
      }
      Table sourceTable = partsList.getSourceTable();
      inputs.add(new ReadEntity(sourceTable, parent, parent == null));
      TableDesc table = Utilities.getTableDesc(sourceTable);
      FetchWork work = new FetchWork(listP, partP, table);
      if (!work.getPartDesc().isEmpty()) {
        PartitionDesc part0 = work.getPartDesc().get(0);
        PlanUtils.configureInputJobPropertiesForStorageHandler(part0.getTableDesc());
        work.setSplitSample(splitSample);
      }
      return work;
    }

    // this optimizer is for replacing FS to temp+fetching from temp with
    // single direct fetching, which means FS is not needed any more when conversion completed.
    // rows forwarded will be received by ListSinkOperator, which is replacing FS
    private ListSinkOperator completed(ParseContext pctx, FetchWork work) {
      for (ReadEntity input : inputs) {
        PlanUtils.addInput(pctx.getSemanticInputs(), input);
      }
      return replaceFSwithLS(fileSink, work.getSerializationNullFormat());
    }

    private boolean isDataLengthWithInThreshold(ParseContext pctx, final long threshold)
        throws Exception {
      if (splitSample != null && splitSample.getTotalLength() != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Threshold " + splitSample.getTotalLength() + " exceeded for pseudoMR mode");
        }
        return (threshold - splitSample.getTotalLength()) > 0;
      }

      Status status = checkThresholdWithMetastoreStats(table, partsList, threshold);
      if (status.equals(Status.PASS)) {
        return true;
      } else if (status.equals(Status.FAIL)) {
        return false;
      } else {
        LOG.info("Cannot fetch stats from metastore for table: {}. Falling back to filesystem scan..",
          table.getCompleteName());
        // metastore stats is unavailable, fallback to old way
        final JobConf jobConf = new JobConf(pctx.getConf());
        Utilities.setColumnNameList(jobConf, scanOp, true);
        Utilities.setColumnTypeList(jobConf, scanOp, true);
        HiveStorageHandler handler = table.getStorageHandler();
        if (handler instanceof InputEstimator) {
          InputEstimator estimator = (InputEstimator) handler;
          TableDesc tableDesc = Utilities.getTableDesc(table);
          PlanUtils.configureInputJobPropertiesForStorageHandler(tableDesc);
          Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf);
          long len = estimator.estimate(jobConf, scanOp, threshold).getTotalLength();
          LOG.debug("Threshold {} exceeded for pseudoMR mode", len);
          return (threshold - len) > 0;
        }
        if (table.isNonNative()) {
          return true; // nothing can be done
        }
        if (!table.isPartitioned()) {
          long len = getPathLength(jobConf, table.getPath(), table.getInputFormatClass(), threshold);
          LOG.debug("Threshold {} exceeded for pseudoMR mode", len);
          return (threshold - len) > 0;
        }
        final AtomicLong total = new AtomicLong(0);
        //TODO: use common thread pool later?
        int threadCount = HiveConf.getIntVar(pctx.getConf(),
          HiveConf.ConfVars.HIVE_STATS_GATHER_NUM_THREADS);
        final ExecutorService pool = (threadCount > 0) ?
          Executors.newFixedThreadPool(threadCount,
            new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("SimpleFetchOptimizer-FileLength-%d").build()) : null;
        try {
          List<Future> futures = Lists.newLinkedList();
          for (final Partition partition : partsList.getNotDeniedPartns()) {
            final Path path = partition.getDataLocation();
            if (pool != null) {
              futures.add(pool.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                  long len = getPathLength(jobConf, path, partition.getInputFormatClass(), threshold);
                  LOG.trace(path + ", length=" + len);
                  return total.addAndGet(len);
                }
              }));
            } else {
              total.addAndGet(getPathLength(jobConf, path, partition.getInputFormatClass(), threshold));
            }
          }
          if (pool != null) {
            pool.shutdown();
            for (Future<Long> future : futures) {
              long totalLen = future.get();
              if ((threshold - totalLen) <= 0) {
                // early exit, as getting file lengths can be expensive in object stores.
                return false;
              }
            }
          }
          return (threshold - total.get()) >= 0;
        } finally {
          LOG.info("Data set size=" + total.get() + ", threshold=" + threshold);
          if (pool != null) {
            pool.shutdownNow();
          }
        }
      }
    }

    // This method gets the basic stats from metastore for table/partitions. This will make use of the statistics from
    // AnnotateWithStatistics optimizer when available. If execution engine is tez, AnnotateWithStatistics
    // optimization is applied only during physical compilation because of DPP changing the stats. In such case, we
    // we will get the basic stats from metastore. When statistics is absent in metastore we will use the fallback of
    // scanning the filesystem to get file lengths.
    private Status checkThresholdWithMetastoreStats(final Table table, final PrunedPartitionList partsList,
      final long threshold) {
      Status status = Status.UNAVAILABLE;
      if (table != null && !table.isPartitioned()) {
        long dataSize = StatsUtils.getTotalSize(table);
        if (dataSize <= 0) {
          LOG.warn("Cannot determine basic stats for table: {} from metastore. Falling back.", table.getCompleteName());
          return Status.UNAVAILABLE;
        }

        status = (threshold - dataSize) >= 0 ? Status.PASS : Status.FAIL;
      } else if (table != null && table.isPartitioned() && partsList != null) {
        List<Long> dataSizes = StatsUtils.getBasicStatForPartitions(table, partsList.getNotDeniedPartns(),
          StatsSetupConst.TOTAL_SIZE);
        long totalDataSize = StatsUtils.getSumIgnoreNegatives(dataSizes);
        if (totalDataSize <= 0) {
          LOG.warn("Cannot determine basic stats for partitioned table: {} from metastore. Falling back.",
            table.getCompleteName());
          return Status.UNAVAILABLE;
        }

        status = (threshold - totalDataSize) >= 0 ? Status.PASS : Status.FAIL;
      }

      if (status == Status.PASS && MetaStoreUtils.isExternalTable(table.getTTable())) {
        // External table should also check the underlying file size.
        LOG.warn("Table {} is external table, falling back to filesystem scan.", table.getCompleteName());
        status = Status.UNAVAILABLE;
      }
      return status;
    }

    private long getPathLength(JobConf conf, Path path,
        Class<? extends InputFormat> clazz, long threshold)
        throws IOException {
      if (ContentSummaryInputFormat.class.isAssignableFrom(clazz)) {
        InputFormat input = HiveInputFormat.getInputFormatFromCache(clazz, conf);
        return ((ContentSummaryInputFormat)input).getContentSummary(path, conf).getLength();
      } else {
        FileSystem fs = path.getFileSystem(conf);
        try {
          long length = 0;
          RemoteIterator<LocatedFileStatus> results = fs.listFiles(path, true);
          // No need to iterate more, when threshold is reached
          // (beneficial especially for object stores)
          while (length <= threshold && results.hasNext()) {
            length += results.next().getLen();
          }
          LOG.trace("length=" + length + ", threshold=" + threshold);
          return length;
        } catch (FileNotFoundException e) {
          return 0;
        }
      }
    }
  }

  public static ListSinkOperator replaceFSwithLS(Operator<?> fileSink, String nullFormat) {
    ListSinkDesc desc = new ListSinkDesc(nullFormat);
    ListSinkOperator sink = (ListSinkOperator) OperatorFactory.get(
        fileSink.getCompilationOpContext(), desc);

    sink.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
    Operator<? extends OperatorDesc> parent = fileSink.getParentOperators().get(0);
    sink.getParentOperators().add(parent);
    parent.replaceChild(fileSink, sink);
    fileSink.setParentOperators(null);
    return sink;
  }
}
