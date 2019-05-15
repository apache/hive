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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.stats.BasicStatsNoJobTask;
import org.apache.hadoop.hive.ql.stats.BasicStatsTask;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor;
import org.apache.hadoop.hive.ql.stats.IStatsProcessor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * StatsTask implementation.
 **/

public class StatsTask extends Task<StatsWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static transient final Logger LOG = LoggerFactory.getLogger(StatsTask.class);

  public StatsTask() {
    super();
  }

  List<IStatsProcessor> processors = new ArrayList<>();

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, DriverContext ctx,
      CompilationOpContext opContext) {
    super.initialize(queryState, queryPlan, ctx, opContext);

    if (work.getBasicStatsWork() != null) {
      BasicStatsTask task = new BasicStatsTask(conf, work.getBasicStatsWork());
      task.followedColStats = work.hasColStats();
      processors.add(0, task);
    } else if (work.isFooterScan()) {
      BasicStatsNoJobTask t = new BasicStatsNoJobTask(conf, work.getBasicStatsNoJobWork());
      processors.add(0, t);
    }
    if (work.hasColStats()) {
      processors.add(new ColStatsProcessor(work.getColStats(), conf));
    }

    for (IStatsProcessor p : processors) {
      p.initialize(opContext);
    }
  }


  @Override
  public int execute(DriverContext driverContext) {
    if (driverContext.getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }
    if (work.isAggregating() && work.isFooterScan()) {
      throw new RuntimeException("Can not have both basic stats work and stats no job work!");
    }
    int ret = 0;
    try {

      if (work.isFooterScan()) {
        work.getBasicStatsNoJobWork().setPartitions(work.getPartitions());
      }

      Hive db = getHive();
      Table tbl = getTable(db);

      for (IStatsProcessor task : processors) {
        task.setDpPartSpecs(dpPartSpecs);
        ret = task.process(db, tbl);
        if (ret != 0) {
          return ret;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to run stats task", e);
      setException(e);
      return 1;
    }
    return 0;
  }


  private Table getTable(Hive db) throws SemanticException, HiveException {
    Table tbl = work.getTable();
    // FIXME for ctas this is still needed because location is not set sometimes
    if (tbl.getSd().getLocation() == null) {
      tbl = db.getTable(work.getFullTableName());
    }
    return tbl;
  }

  @Override
  public StageType getType() {
    return StageType.STATS;
  }

  @Override
  public String getName() {
    return "STATS TASK";
  }

  private Collection<Partition> dpPartSpecs;

  @Override
  protected void receiveFeed(FeedType feedType, Object feedValue) {
    // this method should be called by MoveTask when there are dynamic
    // partitions generated
    if (feedType == FeedType.DYNAMIC_PARTITIONS) {
      dpPartSpecs = (Collection<Partition>) feedValue;
    }
  }

  public static ExecutorService newThreadPool(HiveConf conf) {
    int numThreads = HiveConf.getIntVar(conf, ConfVars.HIVE_STATS_GATHER_NUM_THREADS);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("StatsNoJobTask-Thread-%d").build());
    LOG.info("Initialized threadpool for stats computation with {} threads", numThreads);
    return executor;
  }
}
