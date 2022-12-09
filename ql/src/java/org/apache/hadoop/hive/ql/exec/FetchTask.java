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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * FetchTask implementation.
 **/
public class FetchTask extends Task<FetchWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private int maxRows = 100;
  private FetchOperator fetch;
  private ListSinkOperator sink;
  private List fetchedData;
  private int currentRow;
  private int totalRows;
  private static final Logger LOG = LoggerFactory.getLogger(FetchTask.class);
  JobConf job = null;
  private boolean cachingEnabled = false;

  public FetchTask() {
    super();
  }

  public void setValidWriteIdList(String writeIdStr) {
    fetch.setValidWriteIdList(writeIdStr);
  }

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
    super.initialize(queryState, queryPlan, taskQueue, context);
    work.initializeForFetch(context.getOpContext());
    fetchedData = new ArrayList<>();

    try {
      // Create a file system handle
      job = new JobConf(conf);
      initFetch();
    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error("Initialize failed", e);
      throw new RuntimeException(e);
    }
  }

  private List<VirtualColumn> getVirtualColumns(Operator<?> ts) {
    if (ts instanceof TableScanOperator && ts.getConf() != null) {
      return ((TableScanOperator) ts).getConf().getVirtualCols();
    }
    return null;
  }

  @Override
  public int execute() {
    if (cachingEnabled) {
      executeInner(fetchedData);
    }
    return 0;
  }

  /**
   * Return the tableDesc of the fetchWork.
   */
  public TableDesc getTblDesc() {
    return work.getTblDesc();
  }

  /**
   * Return the maximum number of rows returned by fetch.
   */
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * Set the maximum number of rows returned by fetch.
   */
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public boolean fetch(List res) {
    if (cachingEnabled) {
      if (currentRow >= fetchedData.size()) {
        return false;
      }
      int toIndex = Math.min(fetchedData.size(), currentRow + maxRows);
      res.addAll(fetchedData.subList(currentRow, toIndex));
      currentRow = toIndex;
      return true;
    } else {
      return executeInner(res);
    }
  }

  public boolean isFetchFrom(FileSinkDesc fs) {
    return fs.getFinalDirName().equals(work.getTblDir());
  }

  @Override
  public StageType getType() {
    return StageType.FETCH;
  }

  @Override
  public String getName() {
    return "FETCH";
  }

  /**
   * Clear the Fetch Operator.
   *
   * @throws HiveException
   */
  public void clearFetch() throws HiveException {
    if (fetch != null) {
      fetch.clearFetchContext();
    }
    fetchedData.clear();
  }

  public void resetFetch() throws HiveException {
    if (cachingEnabled) {
      currentRow = 0;
    } else {
      clearFetch();
      initFetch();
    }
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }

  private boolean executeInner(List target) {
    sink.reset(target);
    int rowsRet;
    if (cachingEnabled) {
      rowsRet = work.getLimit() >= 0 ? work.getLimit() : Integer.MAX_VALUE;
    } else {
      rowsRet = work.getLeastNumRows();
      if (rowsRet <= 0) {
        rowsRet = work.getLimit() >= 0 ? Math.min(work.getLimit() - totalRows, maxRows) : maxRows;
      }
    }

    try {
      if (rowsRet <= 0 || work.getLimit() == totalRows) {
        fetch.clearFetchContext();
        return false;
      }
      boolean fetched = false;
      while (sink.getNumRows() < rowsRet) {
        if (!fetch.pushRow()) {
          if (work.getLeastNumRows() > 0) {
            throw new HiveException("leastNumRows check failed");
          }

          // Closing the operator can sometimes yield more rows (HIVE-11892)
          fetch.closeOperator();

          return fetched;
        }
        fetched = true;
      }
      return true;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getClass().getName() + ":" + e.getMessage(),
          "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    } finally {
      totalRows += sink.getNumRows();
    }
  }

  private void initFetch() throws HiveException {
    Operator<?> source = work.getSource();
    if (source instanceof TableScanOperator) {
      TableScanOperator ts = (TableScanOperator) source;
      // push down projections
      ColumnProjectionUtils.appendReadColumns(job, ts.getNeededColumnIDs(), ts.getNeededColumns(),
          ts.getNeededNestedColumnPaths(), ts.getConf().hasVirtualCols());
      // push down filters and as of information
      HiveInputFormat.pushFiltersAndAsOf(job, ts, null);

      AcidUtils.setAcidOperationalProperties(job, ts.getConf().isTranscationalTable(),
          ts.getConf().getAcidOperationalProperties());
    }
    sink = work.getSink();
    fetch = new FetchOperator(work, job, source, getVirtualColumns(source));
    source.initialize(conf, new ObjectInspector[]{ fetch.getOutputObjectInspector() });
    totalRows = 0;
    ExecMapper.setDone(false);
  }

  public void setCachingEnabled(boolean cachingEnabled) {
    this.cachingEnabled = cachingEnabled;
  }

  public boolean isCachingEnabled() {
    return cachingEnabled;
  }
}