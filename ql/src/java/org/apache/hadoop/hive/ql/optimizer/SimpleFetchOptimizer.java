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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;

/**
 * Tries to convert simple fetch query to single fetch task, which fetches rows directly
 * from location of table/partition.
 */
public class SimpleFetchOptimizer implements Transform {

  private final Log LOG = LogFactory.getLog(SimpleFetchOptimizer.class.getName());

  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Map<String, Operator<? extends Serializable>> topOps = pctx.getTopOps();
    if (pctx.getQB().isSimpleSelectQuery() && topOps.size() == 1) {
      // no join, no groupby, no distinct, no lateral view, no subq,
      // no CTAS or insert, not analyze command, and single sourced.
      String alias = (String) pctx.getTopOps().keySet().toArray()[0];
      Operator topOp = (Operator) pctx.getTopOps().values().toArray()[0];
      if (topOp instanceof TableScanOperator) {
        try {
          FetchTask fetchTask = optimize(pctx, alias, (TableScanOperator) topOp);
          if (fetchTask != null) {
            pctx.setFetchTask(fetchTask);
          }
        } catch (HiveException e) {
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
      throws HiveException {
    String mode = HiveConf.getVar(
        pctx.getConf(), HiveConf.ConfVars.HIVEFETCHTASKCONVERSION);

    boolean aggressive = "more".equals(mode);
    FetchData fetch = checkTree(aggressive, pctx, alias, source);
    if (fetch != null) {
      int limit = pctx.getQB().getParseInfo().getOuterQueryLimit();
      FetchWork fetchWork = fetch.convertToWork();
      FetchTask fetchTask = (FetchTask) TaskFactory.get(fetchWork, pctx.getConf());
      fetchWork.setSink(fetch.completed(pctx, fetchWork));
      fetchWork.setSource(source);
      fetchWork.setLimit(limit);
      return fetchTask;
    }
    return null;
  }

  // all we can handle is LimitOperator, FilterOperator SelectOperator and final FS
  //
  // for non-aggressive mode (minimal)
  // 1. samping is not allowed
  // 2. for partitioned table, all filters should be targeted to partition column
  // 3. SelectOperator should be select star
  private FetchData checkTree(boolean aggressive, ParseContext pctx, String alias,
      TableScanOperator ts) throws HiveException {
    SplitSample splitSample = pctx.getNameToSplitSample().get(alias);
    if (!aggressive && splitSample != null) {
      return null;
    }
    QB qb = pctx.getQB();
    if (!aggressive && qb.hasTableSample(alias)) {
      return null;
    }

    Table table = qb.getMetaData().getAliasToTable().get(alias);
    if (table == null) {
      return null;
    }
    if (!table.isPartitioned()) {
      return checkOperators(new FetchData(table, splitSample), ts, aggressive, false);
    }

    boolean bypassFilter = false;
    if (HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVEOPTPPD)) {
      ExprNodeDesc pruner = pctx.getOpToPartPruner().get(ts);
      bypassFilter = PartitionPruner.onlyContainsPartnCols(table, pruner);
    }
    if (aggressive || bypassFilter) {
      PrunedPartitionList pruned = pctx.getPrunedPartitions(alias, ts);
      if (aggressive || pruned.getUnknownPartns().isEmpty()) {
        bypassFilter &= pruned.getUnknownPartns().isEmpty();
        return checkOperators(new FetchData(pruned, splitSample), ts, aggressive, bypassFilter);
      }
    }
    return null;
  }

  private FetchData checkOperators(FetchData fetch, TableScanOperator ts, boolean aggresive,
      boolean bypassFilter) {
    if (ts.getChildOperators().size() != 1) {
      return null;
    }
    Operator<?> op = ts.getChildOperators().get(0);
    for (; ; op = op.getChildOperators().get(0)) {
      if (aggresive) {
        if (!(op instanceof LimitOperator || op instanceof FilterOperator
            || op instanceof SelectOperator)) {
          break;
        }
      } else if (!(op instanceof LimitOperator || (op instanceof FilterOperator && bypassFilter)
          || (op instanceof SelectOperator && ((SelectOperator) op).getConf().isSelectStar()))) {
        break;
      }
      if (op.getChildOperators() == null || op.getChildOperators().size() != 1) {
        return null;
      }
    }
    if (op instanceof FileSinkOperator) {
      fetch.fileSink = op;
      return fetch;
    }
    return null;
  }

  private class FetchData {

    private final Table table;
    private final SplitSample splitSample;
    private final PrunedPartitionList partsList;
    private final HashSet<ReadEntity> inputs = new HashSet<ReadEntity>();

    // this is always non-null when conversion is completed
    private Operator<?> fileSink;

    private FetchData(Table table, SplitSample splitSample) {
      this.table = table;
      this.partsList = null;
      this.splitSample = splitSample;
    }

    private FetchData(PrunedPartitionList partsList, SplitSample splitSample) {
      this.table = null;
      this.partsList = partsList;
      this.splitSample = splitSample;
    }

    private FetchWork convertToWork() throws HiveException {
      inputs.clear();
      if (table != null) {
        inputs.add(new ReadEntity(table));
        String path = table.getPath().toString();
        FetchWork work = new FetchWork(path, Utilities.getTableDesc(table));
        PlanUtils.configureInputJobPropertiesForStorageHandler(work.getTblDesc());
        work.setSplitSample(splitSample);
        return work;
      }
      List<String> listP = new ArrayList<String>();
      List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

      for (Partition partition : partsList.getNotDeniedPartns()) {
        inputs.add(new ReadEntity(partition));
        listP.add(partition.getPartitionPath().toString());
        partP.add(Utilities.getPartitionDesc(partition));
      }
      TableDesc table = Utilities.getTableDesc(partsList.getSourceTable());
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
      pctx.getSemanticInputs().addAll(inputs);
      ListSinkOperator sink = new ListSinkOperator();
      sink.setConf(new ListSinkDesc(work.getSerializationNullFormat()));
      sink.setParentOperators(new ArrayList<Operator<? extends Serializable>>());
      Operator<? extends Serializable> parent = fileSink.getParentOperators().get(0);
      sink.getParentOperators().add(parent);
      parent.replaceChild(fileSink, sink);
      fileSink.setParentOperators(null);
      return sink;
    }
  }
}
