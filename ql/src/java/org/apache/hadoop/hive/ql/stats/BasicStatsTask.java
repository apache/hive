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


package org.apache.hadoop.hive.ql.stats;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.apache.hadoop.hive.common.StatsSetupConst.DELETE_COUNT;
import static org.apache.hadoop.hive.common.StatsSetupConst.INSERT_COUNT;
import static org.apache.hadoop.hive.common.StatsSetupConst.UPDATE_COUNT;

/**
 * StatsTask implementation. StatsTask mainly deals with "collectable" stats. These are
 * stats that require data scanning and are collected during query execution (unless the user
 * explicitly requests data scanning just for the purpose of stats computation using the "ANALYZE"
 * command. All other stats are computed directly by the MetaStore. The rationale being that the
 * MetaStore layer covers all Thrift calls and provides better guarantees about the accuracy of
 * those stats.
 **/
public class BasicStatsTask implements Serializable, IStatsProcessor {

  private static final long serialVersionUID = 1L;
  private static transient final Logger LOG = LoggerFactory.getLogger(BasicStatsTask.class);

  private Table table;
  private Collection<Partition> dpPartSpecs;
  public boolean followedColStats;
  private BasicStatsWork work;
  private HiveConf conf;

  protected transient LogHelper console;

  public BasicStatsTask(HiveConf conf, BasicStatsWork work) {
    super();
    dpPartSpecs = null;
    this.conf = conf;
    console = new LogHelper(LOG);
    this.work = work;
  }

  @Override
  public int process(Hive db, Table tbl) throws Exception {

    LOG.info("Executing stats task");
    table = tbl;
    return aggregateStats(db, tbl);
  }

  @Override
  public void initialize(CompilationOpContext opContext) {
  }

  public StageType getType() {
    return StageType.STATS;
  }

  public String getName() {
    return "STATS";
  }

  private static class BasicStatsProcessor {

    private Partish partish;
    private List<FileStatus> partfileStatus;
    private boolean isMissingAcidState = false;
    private BasicStatsWork work;
    private boolean followedColStats1;
    private Map<String, String> providedBasicStats;

    public BasicStatsProcessor(Partish partish, BasicStatsWork work, HiveConf conf, boolean followedColStats2) {
      this.partish = partish;
      this.work = work;
      followedColStats1 = followedColStats2;
      Table table = partish.getTable();
      if (table.isNonNative() && table.getStorageHandler().canProvideBasicStatistics()) {
        providedBasicStats = table.getStorageHandler().getBasicStatistics(partish);
      }
    }

    public Object process(StatsAggregator statsAggregator) throws HiveException, MetaException {
      Partish p = partish;
      Map<String, String> parameters = p.getPartParameters();
      if (work.isTargetRewritten()) {
        StatsSetupConst.setBasicStatsState(parameters, StatsSetupConst.TRUE);
      }

      // work.getTableSpecs() == null means it is not analyze command
      // and then if it is not followed by column stats, we should clean
      // column stats
      // FIXME: move this to ColStat related part
      if (!work.isExplicitAnalyze() && !followedColStats1) {
        StatsSetupConst.clearColumnStatsState(parameters);
      }

      if (partfileStatus == null && providedBasicStats == null) {
        // This may happen if ACID state is absent from config.
        String spec =  partish.getPartition() == null ? partish.getTable().getTableName()
            :  partish.getPartition().getSpec().toString();
        LOG.warn("Partition/partfiles is null for: " + spec);
        if (isMissingAcidState) {
          MetaStoreServerUtils.clearQuickStats(parameters);
          return p.getOutput();
        }
        return null;
      }

      // The collectable stats for the aggregator needs to be cleared.
      // For example, if a file is being loaded, the old number of rows are not valid
      // XXX: makes no sense for me... possibly not needed anymore
      if (work.isClearAggregatorStats()) {
        // we choose to keep the invalid stats and only change the setting.
        StatsSetupConst.setBasicStatsState(parameters, StatsSetupConst.FALSE);
      }

      if (providedBasicStats == null) {
        MetaStoreServerUtils.populateQuickStats(partfileStatus, parameters);

        if (statsAggregator != null) {
          // Update stats for transactional tables (MM, or full ACID with overwrite), even
          // though we are marking stats as not being accurate.
          if (StatsSetupConst.areBasicStatsUptoDate(parameters) || p.isTransactionalTable()) {
            String prefix = getAggregationPrefix(p.getTable(), p.getPartition());
            updateStats(statsAggregator, parameters, prefix);
          }
        }
      } else {
        parameters.putAll(providedBasicStats);
      }

      return p.getOutput();
    }

    public void collectFileStatus(Warehouse wh, HiveConf conf) throws MetaException, IOException {
      if (providedBasicStats == null) {
        if (!partish.isTransactionalTable()) {
          partfileStatus = wh.getFileStatusesForSD(partish.getPartSd());
        } else {
          Path path = new Path(partish.getPartSd().getLocation());
          partfileStatus = AcidUtils.getAcidFilesForStats(partish.getTable(), path, conf, null);
          isMissingAcidState = true;
        }
      }
    }

    private void updateStats(StatsAggregator statsAggregator, Map<String, String> parameters,
        String aggKey) throws HiveException {
      for (String statType : StatsSetupConst.STATS_REQUIRE_COMPUTE) {
        String value = statsAggregator.aggregateStats(aggKey, statType);
        if (value != null && !value.isEmpty()) {
          long longValue = Long.parseLong(value);

          if (!work.isTargetRewritten()) {
            String originalValue = parameters.get(statType);
            if (originalValue != null) {
              longValue += Long.parseLong(originalValue); // todo: invalid + valid = invalid
            }
          }
          parameters.put(statType, String.valueOf(longValue));
        }
      }
    }

  }

  private static class TransactionalStatsProcessor {
    private final Hive db;
    private final Partish partish;

    private TransactionalStatsProcessor(Hive db, Partish partish) {
      this.db = db;
      this.partish = partish;
    }

    private long toLong(String value) {
      if (value == null || value.isEmpty()) {
        return 0;
      }

      return Long.parseLong(value);
    }

    public void process(StatsAggregator statsAggregator) throws HiveException, MetaException {
      if (statsAggregator == null) {
        return;
      }

      if (partish.isTransactionalTable()) {
        String prefix = getAggregationPrefix(partish.getTable(), partish.getPartition());
        long insertCount = toLong(statsAggregator.aggregateStats(prefix, INSERT_COUNT));
        long updateCount = toLong(statsAggregator.aggregateStats(prefix, UPDATE_COUNT));
        long deleteCount = toLong(statsAggregator.aggregateStats(prefix, DELETE_COUNT));

        if (insertCount > 0 || updateCount > 0 || deleteCount > 0) {
          UpdateTransactionalStatsRequest request = new UpdateTransactionalStatsRequest();
          request.setTableId(partish.getTable().getTTable().getId());
          request.setInsertCount(insertCount);
          request.setUpdatedCount(updateCount);
          request.setDeletedCount(deleteCount);
          db.updateTransactionalStatistics(request);
        }
      }
    }
  }

  private int aggregateStats(Hive db, Table tbl) {

    StatsAggregator statsAggregator = null;
    int ret = 0;
    StatsCollectionContext scc = null;
    EnvironmentContext environmentContext = null;
    environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);

    try {
      // Stats setup:
      final Warehouse wh = new Warehouse(conf);
      if (!getWork().getNoStatsAggregator() && !getWork().isNoScanAnalyzeCommand()) {
        try {
          scc = getContext();
          statsAggregator = createStatsAggregator(scc, conf);
        } catch (HiveException e) {
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
            throw e;
          }
          console.printError(ErrorMsg.STATS_SKIPPING_BY_ERROR.getErrorCodedMsg(e.toString()));
        }
      }

      List<Partition> partitions = getPartitionsList(db);

      String tableFullName = table.getDbName() + "." + table.getTableName();

      List<Partish> partishes = new ArrayList<>();

      if (partitions == null) {
        Partish p;
        partishes.add(p = new Partish.PTable(table));

        BasicStatsProcessor basicStatsProcessor = new BasicStatsProcessor(p, work, conf, followedColStats);
        basicStatsProcessor.collectFileStatus(wh, conf);
        Table res = (Table) basicStatsProcessor.process(statsAggregator);
        if (res == null) {
          return 0;
        }
        db.alterTable(tableFullName, res, environmentContext, true);

        TransactionalStatsProcessor transactionalStatsProcessor = new TransactionalStatsProcessor(db, p);
        transactionalStatsProcessor.process(statsAggregator);

        if (conf.getBoolVar(ConfVars.TEZ_EXEC_SUMMARY)) {
          console.printInfo("Table " + tableFullName + " stats: [" + toString(p.getPartParameters()) + ']');
        } else {
          LOG.info("Table " + tableFullName + " stats: [" + toString(p.getPartParameters()) + ']');
        }

        // The table object is assigned to the latest table object.
        // So that it can be used by ColStatsProcessor.
        // This is only required for unpartitioned tables.
        tbl.setTTable(res.getTTable());

      } else {
        // Partitioned table:
        // Need to get the old stats of the partition
        // and update the table stats based on the old and new stats.

        List<Partition> updates = new ArrayList<Partition>();

        final ExecutorService pool = buildBasicStatsExecutor();

        final List<Future<Void>> futures = Lists.newLinkedList();
        List<BasicStatsProcessor> processors = Lists.newLinkedList();
        List<TransactionalStatsProcessor> transactionalStatsProcessors = Lists.newLinkedList();

        try {
          for(final Partition partn : partitions) {
            Partish p;
            BasicStatsProcessor bsp = new BasicStatsProcessor(p = new Partish.PPart(table, partn), work, conf, followedColStats);
            processors.add(bsp);
            transactionalStatsProcessors.add(new TransactionalStatsProcessor(db, p));

            futures.add(pool.submit(new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                bsp.collectFileStatus(wh, conf);
                return null;
              }
            }));
          }
          pool.shutdown();
          for (Future<Void> future : futures) {
            future.get();
          }
        } catch (InterruptedException e) {
          LOG.debug("Cancelling " + futures.size() + " file stats lookup tasks");
          //cancel other futures
          for (Future future : futures) {
            future.cancel(true);
          }
          // Fail the query if the stats are supposed to be reliable
          if (work.isStatsReliable()) {
            ret = 1;
          }
        } finally {
          if (pool != null) {
            pool.shutdownNow();
          }
          LOG.debug("Finished getting file stats of all partitions!");
        }

        for (BasicStatsProcessor basicStatsProcessor : processors) {
          Object res = basicStatsProcessor.process(statsAggregator);
          if (res == null) {
            LOG.info("Partition " + basicStatsProcessor.partish.getPartition().getSpec() + " stats: [0]");
            continue;
          }
          updates.add((Partition) res);
          if (conf.getBoolVar(ConfVars.TEZ_EXEC_SUMMARY)) {
            console.printInfo("Partition " + basicStatsProcessor.partish.getPartition().getSpec() + " stats: [" + toString(basicStatsProcessor.partish.getPartParameters()) + ']');
          } else {
            LOG.info("Partition " + basicStatsProcessor.partish.getPartition().getSpec() + " stats: [" + toString(basicStatsProcessor.partish.getPartParameters()) + ']');
          }
        }

        if (!updates.isEmpty()) {
          db.alterPartitions(tableFullName, updates, environmentContext, true);
        }

        for (TransactionalStatsProcessor transactionalStatsProcessor : transactionalStatsProcessors) {
          transactionalStatsProcessor.process(statsAggregator);
        }

        if (work.isStatsReliable() && updates.size() != processors.size()) {
          LOG.info("Stats should be reliadble...however seems like there were some issue.. => ret 1");
          ret = 1;
        }
      }

    } catch (Exception e) {
      console.printInfo("[Warning] could not update stats.",
          "Failed with exception " + e.getMessage() + "\n"
              + StringUtils.stringifyException(e));

      // Fail the query if the stats are supposed to be reliable
      if (work.isStatsReliable()) {
        ret = 1;
      }
    } finally {
      if (statsAggregator != null) {
        statsAggregator.closeConnection(scc);
      }
    }
    // The return value of 0 indicates success,
    // anything else indicates failure
    return ret;
  }

  private BasicStatsWork getWork() {
    return work;
  }

  private ExecutorService buildBasicStatsExecutor() {
    //Get the file status up-front for all partitions. Beneficial in cases of blob storage systems
    int poolSize = conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 1);
    // In case thread count is set to 0, use single thread.
    poolSize = Math.max(poolSize, 1);
    final ExecutorService pool = Executors.newFixedThreadPool(poolSize, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("stats-updater-thread-%d").build());
    LOG.debug("Getting file stats of all partitions. threadpool size:" + poolSize);
    return pool;
  }

  private StatsAggregator createStatsAggregator(StatsCollectionContext scc, HiveConf conf) throws HiveException {
    String statsImpl = HiveConf.getVar(conf, HiveConf.ConfVars.HIVESTATSDBCLASS);
    StatsFactory factory = StatsFactory.newFactory(statsImpl, conf);
    if (factory == null) {
      throw new HiveException(ErrorMsg.STATSPUBLISHER_NOT_OBTAINED.getErrorCodedMsg());
    }
    // initialize stats publishing table for noscan which has only stats task
    // the rest of MR task following stats task initializes it in ExecDriver.java
    StatsPublisher statsPublisher = factory.getStatsPublisher();
    if (!statsPublisher.init(scc)) { // creating stats table if not exists
      throw new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
    }

    // manufacture a StatsAggregator
    StatsAggregator statsAggregator = factory.getStatsAggregator();
    if (!statsAggregator.connect(scc)) {
      throw new HiveException(ErrorMsg.STATSAGGREGATOR_CONNECTION_ERROR.getErrorCodedMsg(statsImpl));
    }
    return statsAggregator;
  }

  private StatsCollectionContext getContext() throws HiveException {

    StatsCollectionContext scc = new StatsCollectionContext(conf);
    Task sourceTask = getWork().getSourceTask();
    if (sourceTask == null) {
      throw new HiveException(ErrorMsg.STATSAGGREGATOR_SOURCETASK_NULL.getErrorCodedMsg());
    }
    scc.setTask(sourceTask);
    scc.setStatsTmpDir(this.getWork().getStatsTmpDir());
    return scc;
  }


  private String toString(Map<String, String> parameters) {
    StringBuilder builder = new StringBuilder();
    for (String statType : StatsSetupConst.SUPPORTED_STATS) {
      String value = parameters.get(statType);
      if (value != null) {
        if (builder.length() > 0) {
          builder.append(", ");
        }
        builder.append(statType).append('=').append(value);
      }
    }
    return builder.toString();
  }

  /**
   * Get the list of partitions that need to update statistics.
   * TODO: we should reuse the Partitions generated at compile time
   * since getting the list of partitions is quite expensive.
   *
   * @return a list of partitions that need to update statistics.
   * @throws HiveException
   */
  private List<Partition> getPartitionsList(Hive db) throws HiveException {
    if (work.getLoadFileDesc() != null) {
      return null; //we are in CTAS, so we know there are no partitions
    }

    if (work.getTableSpecs() != null) {

      // ANALYZE command
      TableSpec tblSpec = work.getTableSpecs();
      table = tblSpec.tableHandle;
      if (!table.isPartitioned()) {
        return null;
      }
      // get all partitions that match with the partition spec
      return tblSpec.partitions != null ? unmodifiableList(tblSpec.partitions) : emptyList();
    } else if (work.getLoadTableDesc() != null) {

      // INSERT OVERWRITE command
      LoadTableDesc tbd = work.getLoadTableDesc();
      table = db.getTable(tbd.getTable().getTableName());
      if (!table.isPartitioned()) {
        return null;
      }
      DynamicPartitionCtx dpCtx = tbd.getDPCtx();
      if (dpCtx != null && dpCtx.getNumDPCols() > 0) { // dynamic partitions
        // If no dynamic partitions are generated, dpPartSpecs may not be initialized
        if (dpPartSpecs != null) {
          // Reload partition metadata because another BasicStatsTask instance may have updated the stats.
          List<String> partNames = dpPartSpecs.stream().map(Partition::getName).collect(Collectors.toList());
          return db.getPartitionsByNames(table, partNames);
        }
      } else { // static partition
        return singletonList(db.getPartition(table, tbd.getPartitionSpec(), false));
      }
    }
    return emptyList();
  }

  public Collection<Partition> getDpPartSpecs() {
    return dpPartSpecs;
  }

  @Override
  public void setDpPartSpecs(Collection<Partition> dpPartSpecs) {
    this.dpPartSpecs = dpPartSpecs;
  }

  public static String getAggregationPrefix(Table table, Partition partition) throws MetaException {
    String prefix = getAggregationPrefix0(table, partition);
    String aggKey = prefix.endsWith(Path.SEPARATOR) ? prefix : prefix + Path.SEPARATOR;
    return aggKey;
  }

  private static String getAggregationPrefix0(Table table, Partition partition) throws MetaException {

    // prefix is of the form dbName.tblName
    String prefix = FileUtils.escapePathName(table.getDbName()).toLowerCase() + "." +
        FileUtils.escapePathName(table.getTableName()).toLowerCase();
    // FIXME: this is a secret contract; reusein getAggrKey() creates a more closer relation to the StatsGatherer
    // prefix = work.getAggKey();
    if (partition != null) {
      return Utilities.join(prefix, Warehouse.makePartPath(partition.getSpec()));
    }
    return prefix;
  }
}
