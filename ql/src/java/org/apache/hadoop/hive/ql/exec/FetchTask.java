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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

/**
 * FetchTask implementation.
 **/
public class FetchTask extends Task<FetchWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private int maxRows = 100;
  private FetchOperator fetch;
  private ListSinkOperator sink;
  private int totalRows;

  private static transient final Log LOG = LogFactory.getLog(FetchTask.class);

  public FetchTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);
    work.initializeForFetch();

    try {
      // Create a file system handle
      JobConf job = new JobConf(conf, ExecDriver.class);

      Operator<?> source = work.getSource();
      if (source instanceof TableScanOperator) {
        TableScanOperator ts = (TableScanOperator) source;
        HiveInputFormat.pushFilters(job, ts);
        ColumnProjectionUtils.appendReadColumnIDs(job, ts.getNeededColumnIDs());
      }
      sink = work.getSink();
      fetch = new FetchOperator(work, job, source, getVirtualColumns(source));
      source.initialize(conf, new ObjectInspector[]{fetch.getOutputObjectInspector()});

    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  private List<VirtualColumn> getVirtualColumns(Operator<?> ts) {
    if (ts instanceof TableScanOperator && ts.getConf() != null) {
      return ((TableScanOperator)ts).getConf().getVirtualCols();
    }
    return null;
  }

  @Override
  public int execute(DriverContext driverContext) {
    assert false;
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

  @Override
  public boolean fetch(ArrayList<String> res) throws IOException, CommandNeedRetryException {
    sink.reset(res);
    try {
      int rowsRet = work.getLeastNumRows();
      if (rowsRet <= 0) {
        rowsRet = work.getLimit() >= 0 ? Math.min(work.getLimit() - totalRows, maxRows) : maxRows;
      }
      if (rowsRet <= 0) {
        fetch.clearFetchContext();
        return false;
      }
      boolean fetched = false;
      while (sink.getNumRows() < rowsRet) {
        if (!fetch.pushRow()) {
          if (work.getLeastNumRows() > 0) {
            throw new CommandNeedRetryException();
          }
          return fetched;
        }
        fetched = true;
      }
      return true;
    } catch (CommandNeedRetryException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      totalRows += sink.getNumRows();
    }
  }

  @Override
  public StageType getType() {
    return StageType.FETCH;
  }

  @Override
  public String getName() {
    return "FETCH";
  }

  @Override
  protected void localizeMRTmpFilesImpl(Context ctx) {
    String s = work.getTblDir();
    if ((s != null) && ctx.isMRTmpFileURI(s)) {
      work.setTblDir(ctx.localizeMRTmpFileURI(s));
    }

    ArrayList<String> ls = work.getPartDir();
    if (ls != null) {
      ctx.localizePaths(ls);
    }
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
  }
}
