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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
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
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
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
public class SimpleFetchOptimizer implements Transform {

  private final Log LOG = LogFactory.getLog(SimpleFetchOptimizer.class.getName());

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Map<String, Operator<? extends OperatorDesc>> topOps = pctx.getTopOps();
    if (pctx.getQueryProperties().isQuery() && !pctx.getQueryProperties().isAnalyzeCommand()
        && topOps.size() == 1) {
      // no join, no groupby, no distinct, no lateral view, no subq,
      // no CTAS or insert, not analyze command, and single sourced.
      String alias = (String) pctx.getTopOps().keySet().toArray()[0];
      Operator<?> topOp = (Operator<?>) pctx.getTopOps().values().toArray()[0];
      if (topOp instanceof TableScanOperator) {
        try {
          FetchTask fetchTask = optimize(pctx, alias, (TableScanOperator) topOp);
          if (fetchTask != null) {
            pctx.setFetchTask(fetchTask);
          }
        } catch (Exception e) {
          // Has to use full name to make sure it does not conflict with
          // org.apache.commons.lang.StringUtils
          LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
          if (e instanceof SemanticException) {
            throw (SemanticException) e;
          }
          throw new SemanticException(e.getMessage(), e);
        }
      }
    }
    return pctx;
  }

  // returns non-null FetchTask instance when succeeded
  @SuppressWarnings("unchecked")
  private FetchTask optimize(ParseContext pctx, String alias, TableScanOperator source)
      throws Exception {
    String mode = HiveConf.getVar(
        pctx.getConf(), HiveConf.ConfVars.HIVEFETCHTASKCONVERSION);

    boolean aggressive = "more".equals(mode);
    final int limit = pctx.getQueryProperties().getOuterQueryLimit();
    FetchData fetch = checkTree(aggressive, pctx, alias, source);
    if (fetch != null && checkThreshold(fetch, limit, pctx)) {
      FetchWork fetchWork = fetch.convertToWork();
      FetchTask fetchTask = (FetchTask) TaskFactory.get(fetchWork, pctx.getConf());
      fetchWork.setSink(fetch.completed(pctx, fetchWork));
      fetchWork.setSource(source);
      fetchWork.setLimit(limit);
      return fetchTask;
    }
    return null;
  }

  private boolean checkThreshold(FetchData data, int limit, ParseContext pctx) throws Exception {
    if (limit > 0) {
      if (data.hasOnlyPruningFilter()) {
        /* partitioned table + query has only pruning filters */
        return true;
      } else if (data.isPartitioned() == false && data.isFiltered() == false) {
        /* unpartitioned table + no filters */
        return true;
      }
      /* fall through */
    }
    long threshold = HiveConf.getLongVar(pctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONTHRESHOLD);
    if (threshold < 0) {
      return true;
    }
    long remaining = threshold;
    remaining -= data.getInputLength(pctx, remaining);
    if (remaining < 0) {
      LOG.info("Threshold " + remaining + " exceeded for pseudoMR mode");
      return false;
    }
    return true;
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
    if (HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVEOPTPPD)) {
      ExprNodeDesc pruner = pctx.getOpToPartPruner().get(ts);
      if (PartitionPruner.onlyContainsPartnCols(table, pruner)) {
        bypassFilter = !pctx.getPrunedPartitions(alias, ts).hasUnknownPartitions();
      }
    }
    if (!aggressive && !bypassFilter) {
      return null;
    }
    PrunedPartitionList partitions = pctx.getPrunedPartitions(alias, ts);
    FetchData fetch = new FetchData(ts, parent, table, partitions, splitSample, bypassFilter);
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
        || operator instanceof ScriptOperator || operator instanceof UDTFOperator) {
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

    private long getInputLength(ParseContext pctx, long remaining) throws Exception {
      if (splitSample != null && splitSample.getTotalLength() != null) {
        return splitSample.getTotalLength();
      }
      if (splitSample != null) {
        return splitSample.getTargetSize(calculateLength(pctx, splitSample.estimateSourceSize(remaining)));
      }
      return calculateLength(pctx, remaining);
    }

    private long calculateLength(ParseContext pctx, long remaining) throws Exception {
      JobConf jobConf = new JobConf(pctx.getConf());
      Utilities.setColumnNameList(jobConf, scanOp, true);
      Utilities.setColumnTypeList(jobConf, scanOp, true);
      HiveStorageHandler handler = table.getStorageHandler();
      if (handler instanceof InputEstimator) {
        InputEstimator estimator = (InputEstimator) handler;
        TableDesc tableDesc = Utilities.getTableDesc(table);
        PlanUtils.configureInputJobPropertiesForStorageHandler(tableDesc);
        Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf);
        return estimator.estimate(jobConf, scanOp, remaining).getTotalLength();
      }
      if (table.isNonNative()) {
        return 0; // nothing can be done
      }
      if (!table.isPartitioned()) {
        return getFileLength(jobConf, table.getPath(), table.getInputFormatClass());
      }
      long total = 0;
      for (Partition partition : partsList.getNotDeniedPartns()) {
        Path path = partition.getDataLocation();
        total += getFileLength(jobConf, path, partition.getInputFormatClass());
        if (total > remaining) {
          break;
        }
      }
      return total;
    }

    // from Utilities.getInputSummary()
    private long getFileLength(JobConf conf, Path path, Class<? extends InputFormat> clazz)
        throws IOException {
      ContentSummary summary;
      if (ContentSummaryInputFormat.class.isAssignableFrom(clazz)) {
        InputFormat input = HiveInputFormat.getInputFormatFromCache(clazz, conf);
        summary = ((ContentSummaryInputFormat)input).getContentSummary(path, conf);
      } else {
        FileSystem fs = path.getFileSystem(conf);
        try {
          summary = fs.getContentSummary(path);
        } catch (FileNotFoundException e) {
          return 0;
        }
      }
      return summary.getLength();
    }
  }

  public static ListSinkOperator replaceFSwithLS(Operator<?> fileSink, String nullFormat) {
    ListSinkDesc desc = new ListSinkDesc(nullFormat);
    ListSinkOperator sink = (ListSinkOperator) OperatorFactory.get(desc);

    sink.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
    Operator<? extends OperatorDesc> parent = fileSink.getParentOperators().get(0);
    sink.getParentOperators().add(parent);
    parent.replaceChild(fileSink, sink);
    fileSink.setParentOperators(null);
    return sink;
  }
}
