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

package org.apache.hadoop.hive.metastore.handler;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.UpdateTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.HMSHandler.getPartValsFromName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

@SuppressWarnings("unused")
@RequestHandler(requestBody = SetPartitionsStatsRequest.class)
public class SetAggrStatsHandler
    extends AbstractRequestHandler<SetPartitionsStatsRequest, SetAggrStatsHandler.SetAggrStatsResult> {
  private static final Logger LOG = LoggerFactory.getLogger(SetAggrStatsHandler.class);
  private RawStore ms;
  private String catName;
  private String dbName;
  private String tableName;
  private Table t;
  private boolean needMerge;
  private Configuration conf;

  SetAggrStatsHandler(IHMSHandler handler, SetPartitionsStatsRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.needMerge = request.isSetNeedMerge() && request.isNeedMerge();
    this.conf = handler.getConf();
    this.ms = handler.getMS();
    List<ColumnStatistics> csNews = request.getColStats();
    if (csNews != null && !csNews.isEmpty()) {
      ColumnStatistics firstColStats = csNews.get(0);
      ColumnStatisticsDesc statsDesc = firstColStats.getStatsDesc();
      this.catName = normalizeIdentifier(statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf));
      this.dbName = normalizeIdentifier(statsDesc.getDbName());
      this.tableName = normalizeIdentifier(statsDesc.getTableName());
      this.t = ms.getTable(catName, dbName, tableName);
      if (statsDesc.isIsTblLevel() && request.getColStatsSize() != 1) {
        // there should be only one ColumnStatistics
        throw new MetaException(
            "Expecting only 1 ColumnStatistics for table's column stats, but find " + request.getColStatsSize());
      }
    }
  }

  @Override
  protected SetAggrStatsResult execute() throws TException, IOException {
    boolean ret = true;
    List<ColumnStatistics> csNews = request.getColStats();
    if (csNews == null || csNews.isEmpty()) {
      return new SetAggrStatsResult(true);
    }
    // figure out if it is table level or partition level
    ColumnStatistics firstColStats = csNews.get(0);
    ColumnStatisticsDesc statsDesc = firstColStats.getStatsDesc();
    List<String> colNames = new ArrayList<>();
    for (ColumnStatisticsObj obj : firstColStats.getStatsObj()) {
      colNames.add(obj.getColName());
    }
    if (statsDesc.isIsTblLevel()) {
      if (needMerge) {
        return new SetAggrStatsResult(updateTableColumnStatsWithMerge(colNames));
      } else {
        // This is the overwrite case, we do not care about the accuracy.
        return new SetAggrStatsResult(updateTableColumnStatsInternal(firstColStats,
            request.getValidWriteIdList(), request.getWriteId()));
      }
    } else {
      // partition level column stats merging
      // note that we may have two or more duplicate partition names.
      // see autoColumnStats_2.q under TestMiniLlapLocalCliDriver
      Map<String, ColumnStatistics> newStatsMap = new HashMap<>();
      for (ColumnStatistics csNew : csNews) {
        String partName = csNew.getStatsDesc().getPartName();
        if (newStatsMap.containsKey(partName)) {
          MetaStoreServerUtils.mergeColStats(csNew, newStatsMap.get(partName));
        }
        newStatsMap.put(partName, csNew);
      }

      if (needMerge) {
        ret = updatePartColumnStatsWithMerge(colNames, newStatsMap);
      } else { // No merge.
        // We don't short-circuit on errors here anymore. That can leave acid stats invalid.
        if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL)) {
          ret = updatePartitionColStatsInBatch(newStatsMap,
              request.getValidWriteIdList(), request.getWriteId());
        } else {
          MTable mTable = ms.ensureGetMTable(catName, dbName, tableName);
          for (Map.Entry<String, ColumnStatistics> entry : newStatsMap.entrySet()) {
            // We don't short-circuit on errors here anymore. That can leave acid stats invalid.
            ret = updatePartitonColStatsInternal(mTable, entry.getValue(),
                request.getValidWriteIdList(), request.getWriteId()) && ret;
          }
        }
      }
    }
    return new SetAggrStatsResult(ret);
  }

  private boolean updateTableColumnStatsWithMerge(List<String> colNames) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    ColumnStatistics firstColStats = request.getColStats().get(0);
    ms.openTransaction();
    boolean isCommitted = false, result = false;
    try {
      ColumnStatistics csOld = ms.getTableColumnStatistics(catName, dbName, tableName, colNames,
          request.getEngine(), request.getValidWriteIdList());
      // we first use the valid stats list to prune the stats
      boolean isInvalidTxnStats = csOld != null
          && csOld.isSetIsStatsCompliant() && !csOld.isIsStatsCompliant();
      if (isInvalidTxnStats) {
        // No columns can be merged; a shortcut for getMergableCols.
        firstColStats.setStatsObj(Lists.newArrayList());
      } else {
        MetaStoreServerUtils.getMergableCols(firstColStats, t.getParameters());
        // we merge those that can be merged
        if (csOld != null && csOld.getStatsObjSize() != 0 && !firstColStats.getStatsObj().isEmpty()) {
          MetaStoreServerUtils.mergeColStats(firstColStats, csOld);
        }
      }

      if (!firstColStats.getStatsObj().isEmpty()) {
        result = updateTableColumnStatsInternal(firstColStats,
            request.getValidWriteIdList(), request.getWriteId());
      } else if (isInvalidTxnStats) {
        // For now because the stats state is such as it is, we will invalidate everything.
        // Overall the sematics here are not clear - we could invalide only some columns, but does
        // that make any physical sense? Could query affect some columns but not others?
        t.setWriteId(request.getWriteId());
        StatsSetupConst.clearColumnStatsState(t.getParameters());
        StatsSetupConst.setBasicStatsState(t.getParameters(), StatsSetupConst.FALSE);
        ms.alterTable(catName, dbName, tableName, t, request.getValidWriteIdList());
      } else {
        // TODO: why doesn't the original call for non acid tables invalidate the stats?
        LOG.debug("All the column stats are not accurate to merge.");
        result = true;
      }

      ms.commitTransaction();
      isCommitted = true;
    } finally {
      if (!isCommitted) {
        ms.rollbackTransaction();
      }
    }
    return result;
  }

  private boolean updateTableColumnStatsInternal(ColumnStatistics colStats,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    normalizeColStatsInput(colStats);

    Map<String, String> parameters = null;
    ms.openTransaction();
    boolean committed = false;
    try {
      parameters = ms.updateTableColumnStatistics(colStats, validWriteIds, writeId);
      if (parameters != null) {
        Table tableObj = ms.getTable(colStats.getStatsDesc().getCatName(),
            colStats.getStatsDesc().getDbName(),
            colStats.getStatsDesc().getTableName(), validWriteIds);
        if (!handler.getTransactionalListeners().isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
              EventMessage.EventType.UPDATE_TABLE_COLUMN_STAT,
              new UpdateTableColumnStatEvent(colStats, tableObj, parameters,
                  writeId, handler));
        }
        if (!handler.getListeners().isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
              EventMessage.EventType.UPDATE_TABLE_COLUMN_STAT,
              new UpdateTableColumnStatEvent(colStats, tableObj, parameters,
                  writeId, handler));
        }
      }
      committed = ms.commitTransaction();
    } finally {
      if (!committed) {
        ms.rollbackTransaction();
      }
    }

    return parameters != null;
  }

  private boolean updatePartColumnStatsWithMerge(
      List<String> colNames, Map<String, ColumnStatistics> newStatsMap)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    ms.openTransaction();
    boolean isCommitted = false, result = true;
    try {
      // a single call to get all column stats for all partitions
      List<String> partitionNames = new ArrayList<>();
      partitionNames.addAll(newStatsMap.keySet());
      List<ColumnStatistics> csOlds = ms.getPartitionColumnStatistics(catName, dbName, tableName,
          partitionNames, colNames, request.getEngine(), request.getValidWriteIdList());
      if (newStatsMap.values().size() != csOlds.size()) {
        // some of the partitions miss stats.
        LOG.debug("Some of the partitions miss stats.");
      }
      Map<String, ColumnStatistics> oldStatsMap = new HashMap<>();
      for (ColumnStatistics csOld : csOlds) {
        oldStatsMap.put(csOld.getStatsDesc().getPartName(), csOld);
      }

      // another single call to get all the partition objects
      List<Partition> partitions = ms.getPartitionsByNames(catName, dbName, tableName, partitionNames);
      Map<String, Partition> mapToPart = new HashMap<>();
      for (int index = 0; index < partitionNames.size(); index++) {
        mapToPart.put(partitionNames.get(index), partitions.get(index));
      }

      MTable mTable = ms.ensureGetMTable(catName, dbName, tableName);
      Map<String, ColumnStatistics> statsMap =  new HashMap<>();
      boolean useDirectSql = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL);
      for (Map.Entry<String, ColumnStatistics> entry : newStatsMap.entrySet()) {
        ColumnStatistics csNew = entry.getValue();
        ColumnStatistics csOld = oldStatsMap.get(entry.getKey());
        boolean isInvalidTxnStats = csOld != null
            && csOld.isSetIsStatsCompliant() && !csOld.isIsStatsCompliant();
        Partition part = mapToPart.get(entry.getKey());
        if (isInvalidTxnStats) {
          // No columns can be merged; a shortcut for getMergableCols.
          csNew.setStatsObj(Lists.newArrayList());
        } else {
          // we first use getParameters() to prune the stats
          MetaStoreServerUtils.getMergableCols(csNew, part.getParameters());
          // we merge those that can be merged
          if (csOld != null && csOld.getStatsObjSize() != 0 && !csNew.getStatsObj().isEmpty()) {
            MetaStoreServerUtils.mergeColStats(csNew, csOld);
          }
        }

        if (!csNew.getStatsObj().isEmpty()) {
          // We don't short-circuit on errors here anymore. That can leave acid stats invalid.
          if (useDirectSql) {
            statsMap.put(csNew.getStatsDesc().getPartName(), csNew);
          } else {
            result = updatePartitonColStatsInternal(mTable, csNew,
                request.getValidWriteIdList(), request.getWriteId()) && result;
          }
        } else if (isInvalidTxnStats) {
          // For now because the stats state is such as it is, we will invalidate everything.
          // Overall the sematics here are not clear - we could invalide only some columns, but does
          // that make any physical sense? Could query affect some columns but not others?
          part.setWriteId(request.getWriteId());
          StatsSetupConst.clearColumnStatsState(part.getParameters());
          StatsSetupConst.setBasicStatsState(part.getParameters(), StatsSetupConst.FALSE);
          ms.alterPartition(catName, dbName, tableName, part.getValues(), part,
              request.getValidWriteIdList());
          result = false;
        } else {
          // TODO: why doesn't the original call for non acid tables invalidate the stats?
          LOG.debug("All the column stats " + csNew.getStatsDesc().getPartName()
              + " are not accurate to merge.");
        }
      }
      ms.commitTransaction();
      isCommitted = true;
      // updatePartitionColStatsInBatch starts/commit transaction internally. As there is no write or select for update
      // operations is done in this transaction, it is safe to commit it before calling updatePartitionColStatsInBatch.
      if (!statsMap.isEmpty()) {
        updatePartitionColStatsInBatch(statsMap,  request.getValidWriteIdList(), request.getWriteId());
      }
    } finally {
      if (!isCommitted) {
        ms.rollbackTransaction();
      }
    }
    return result;
  }

  private boolean updatePartitionColStatsInBatch(Map<String, ColumnStatistics> statsMap,
      String validWriteIds, long writeId)
      throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {

    if (statsMap.size() == 0) {
      return false;
    }

    long start = System.currentTimeMillis();
    Map<String, ColumnStatistics> newStatsMap = new HashMap<>();
    long numStats = 0;
    long numStatsMax = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.JDBC_MAX_BATCH_SIZE);
    try {
      for (Map.Entry entry : statsMap.entrySet()) {
        ColumnStatistics colStats = (ColumnStatistics) entry.getValue();
        normalizeColStatsInput(colStats);
        assert catName.equalsIgnoreCase(colStats.getStatsDesc().getCatName());
        assert dbName.equalsIgnoreCase(colStats.getStatsDesc().getDbName());
        assert tableName.equalsIgnoreCase(colStats.getStatsDesc().getTableName());
        newStatsMap.put((String) entry.getKey(), colStats);
        numStats += colStats.getStatsObjSize();

        if (newStatsMap.size() >= numStatsMax) {
          updatePartitionColStatsForOneBatch(t, newStatsMap, validWriteIds, writeId);
          newStatsMap.clear();
          numStats = 0;
        }
      }
      if (numStats != 0) {
        updatePartitionColStatsForOneBatch(t, newStatsMap, validWriteIds, writeId);
      }
    } finally {
      long end = System.currentTimeMillis();
      float sec = (end - start) / 1000F;
      LOG.info("updatePartitionColStatsInBatch took " + sec + " seconds for " + statsMap.size() + " stats");
    }
    return true;
  }

  public boolean updatePartitonColStatsInternal(MTable mTable, ColumnStatistics colStats,
      String validWriteIds, long writeId)
      throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
    normalizeColStatsInput(colStats);
    ColumnStatisticsDesc csd = colStats.getStatsDesc();

    Map<String, String> parameters;
    List<String> partVals;
    boolean committed = false;
    ms.openTransaction();

    try {
      partVals = getPartValsFromName(t, csd.getPartName());
      parameters = ms.updatePartitionColumnStatistics(t, mTable, colStats, partVals, validWriteIds, writeId);
      if (parameters != null) {
        if (!handler.getTransactionalListeners().isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
              EventMessage.EventType.UPDATE_PARTITION_COLUMN_STAT,
              new UpdatePartitionColumnStatEvent(colStats, partVals, parameters, t,
                  writeId, handler));
        }
        if (!handler.getListeners().isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
              EventMessage.EventType.UPDATE_PARTITION_COLUMN_STAT,
              new UpdatePartitionColumnStatEvent(colStats, partVals, parameters, t,
                  writeId, handler));
        }
      }
      committed = ms.commitTransaction();
    } finally {
      if (!committed) {
        ms.rollbackTransaction();
      }
    }
    return parameters != null;
  }

  private void normalizeColStatsInput(ColumnStatistics colStats) {
    // TODO: is this really needed? this code is propagated from HIVE-1362 but most of it is useless.
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
    statsDesc.setCatName(statsDesc.isSetCatName() ? statsDesc.getCatName().toLowerCase() : getDefaultCatalog(conf));
    statsDesc.setDbName(statsDesc.getDbName().toLowerCase());
    statsDesc.setTableName(statsDesc.getTableName().toLowerCase());
    statsDesc.setPartName(statsDesc.getPartName());
    long time = System.currentTimeMillis() / 1000;
    statsDesc.setLastAnalyzed(time);

    for (ColumnStatisticsObj statsObj : colStats.getStatsObj()) {
      statsObj.setColName(statsObj.getColName().toLowerCase());
      statsObj.setColType(statsObj.getColType().toLowerCase());
    }
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(colStats.getStatsObj());
  }

  private void updatePartitionColStatsForOneBatch(Table tbl, Map<String, ColumnStatistics> statsMap,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    Map<String, Map<String, String>> result = ms.updatePartitionColumnStatisticsInBatch(statsMap, tbl,
        handler.getTransactionalListeners(), validWriteIds, writeId);
    if (result != null && result.size() != 0 && handler.getListeners() != null) {
      // The normal listeners, unlike transaction listeners are not using the same transactions used by the update
      // operations. So there is no need of keeping them within the same transactions. If notification to one of
      // the listeners failed, then even if we abort the transaction, we can not revert the notifications sent to the
      // other listeners.
      for (Map.Entry entry : result.entrySet()) {
        Map<String, String> parameters = (Map<String, String>) entry.getValue();
        ColumnStatistics colStats = statsMap.get(entry.getKey());
        List<String> partVals = getPartValsFromName(tbl, colStats.getStatsDesc().getPartName());
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
            EventMessage.EventType.UPDATE_PARTITION_COLUMN_STAT,
            new UpdatePartitionColumnStatEvent(colStats, partVals, parameters,
                tbl, writeId, handler));
      }
    }
  }


  @Override
  protected String getMessagePrefix() {
    return "SetAggrStatsHandler [" + id + "] -  aggregating stats for " +
        TableName.getQualified(catName, dbName, tableName) + ":";
  }

  public record SetAggrStatsResult(boolean success) implements Result {

  }
}
