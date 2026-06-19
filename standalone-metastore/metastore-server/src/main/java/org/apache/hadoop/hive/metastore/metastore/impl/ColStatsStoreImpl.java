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

package org.apache.hadoop.hive.metastore.metastore.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.jdo.Query;
import javax.jdo.datastore.JDOConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.PersistenceManagerProvider;
import org.apache.hadoop.hive.metastore.QueryWrapper;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.StatObjectConverter;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionFilterMode;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.directsql.DirectSqlDeleteStats;
import org.apache.hadoop.hive.metastore.directsql.MetaStoreDirectSql;
import org.apache.hadoop.hive.metastore.metastore.GetHelper;
import org.apache.hadoop.hive.metastore.metastore.GetListHelper;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.metastore.iface.ColStatsStore;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.RetryingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.Batchable.NO_BATCHING;
import static org.apache.hadoop.hive.metastore.ObjectStore.verifyStatsChangeCtx;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifiers;

public class ColStatsStoreImpl extends RawStoreAware implements ColStatsStore {
  private static final Logger LOG = LoggerFactory.getLogger(ColStatsStoreImpl.class);

  private DatabaseProduct dbType;
  protected int batchSize = NO_BATCHING;
  private boolean areTxnStatsSupported = false;
  private Configuration conf;

  @Override
  public void setBaseStore(RawStore store) {
    super.setBaseStore(store);
    this.dbType = PersistenceManagerProvider.getDatabaseProduct();
    this.batchSize = MetastoreConf.getIntVar(store.getConf(),
        MetastoreConf.ConfVars.RAWSTORE_PARTITION_BATCH_SIZE);
    this.areTxnStatsSupported = MetastoreConf.getBoolVar(baseStore.getConf(),
        MetastoreConf.ConfVars.HIVE_TXN_STATS_ENABLED);
    this.conf = store.getConf();
  }
  
  @Override
  public List<TableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException {
    return new GetListHelper<TableName, TableName> (this, null) {
      @Override
      protected List<TableName> getSqlResult() throws MetaException {
        return getDirectSql().getTableNamesWithStats();
      }

      @Override
      protected boolean canUseJdoQuery() throws MetaException {
        return false;
      }

      @Override
      protected List<TableName> getJdoResult() throws MetaException {
        throw new UnsupportedOperationException("UnsupportedOperationException"); // TODO: implement?
      }
    }.run(false);
  }

  @Override
  public Map<String, List<String>> getPartitionColsWithStats(TableName tableName)
      throws MetaException, NoSuchObjectException {
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    return new GetHelper<TableName, Map<String, List<String>>>(this, null) {
      @Override
      protected Map<String, List<String>> getSqlResult() throws MetaException {
        try {
          return getDirectSql().getColAndPartNamesWithStats(catName, dbName, tblName);
        } catch (Throwable ex) {
          LOG.error("DirectSQL failed", ex);
          throw new MetaException(ex.getMessage());
        }
      }

      @Override
      protected boolean canUseJdoQuery() throws MetaException {
        return false;
      }

      @Override
      protected Map<String, List<String>> getJdoResult() throws MetaException {
        throw new UnsupportedOperationException("UnsupportedOperationException"); // TODO: implement?
      }

      @Override
      protected String describeResult() {
        return results.size() + " partitions";
      }
    }.run(false);
  }

  @Override
  public List<TableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException {
    return new GetListHelper<TableName, TableName>(this, null) {
      @Override
      protected List<TableName> getSqlResult() throws MetaException {
        return getDirectSql().getAllTableNamesForStats();
      }

      @Override
      protected List<TableName> getJdoResult() throws MetaException {
        List<TableName> result = new ArrayList<>();
        String paramStr = "", whereStr = "";
        for (int i = 0; i < MetaStoreDirectSql.STATS_TABLE_TYPES.length; ++i) {
          if (i != 0) {
            paramStr += ", ";
            whereStr += "||";
          }
          paramStr += "java.lang.String tt" + i;
          whereStr += " tableType == tt" + i;
        }
        Query query = pm.newQuery(MTable.class, whereStr);
        query.declareParameters(paramStr);
        Collection<MTable> tbls = (Collection<MTable>) query.executeWithArray(
            query, MetaStoreDirectSql.STATS_TABLE_TYPES);
        pm.retrieveAll(tbls);
        for (MTable tbl : tbls) {
          result.add(new TableName(
              tbl.getDatabase().getCatalogName(), tbl.getDatabase().getName(), tbl.getTableName()));
        }
        return result;
      }
    }.run(false);
  }

  private void writeMTableColumnStatistics(Table table, MTableColumnStatistics mStatsObj,
      MTableColumnStatistics oldStats) throws MetaException {

    Preconditions.checkState(baseStore.isActiveTransaction());

    String colName = mStatsObj.getColName();

    LOG.info("Updating table level column statistics for table={} colName={}",
        Warehouse.getCatalogQualifiedTableName(table), colName);
    validateTableCols(table, Lists.newArrayList(colName));

    if (oldStats != null) {
      StatObjectConverter.setFieldsIntoOldStats(mStatsObj, oldStats);
    } else {
      pm.makePersistent(mStatsObj);
    }
  }

  private void writeMPartitionColumnStatistics(Table table, Partition partition,
      MPartitionColumnStatistics mStatsObj, MPartitionColumnStatistics oldStats) {
    String catName = mStatsObj.getPartition().getTable().getDatabase().getCatalogName();
    String dbName = mStatsObj.getPartition().getTable().getDatabase().getName();
    String tableName = mStatsObj.getPartition().getTable().getTableName();
    String partName = mStatsObj.getPartition().getPartitionName();
    String colName = mStatsObj.getColName();

    Preconditions.checkState(this.baseStore.isActiveTransaction());

    LOG.info("Updating partition level column statistics for table=" +
        TableName.getQualified(catName, dbName, tableName) +
        " partName=" + partName + " colName=" + colName);

    boolean foundCol = false;
    List<FieldSchema> colList = partition.getSd().getCols();
    for (FieldSchema col : colList) {
      if (col.getName().equals(mStatsObj.getColName())) {
        foundCol = true;
        break;
      }
    }

    if (!foundCol) {
      LOG.warn("Column " + colName + " for which stats gathering is requested doesn't exist.");
    }

    if (oldStats != null) {
      StatObjectConverter.setFieldsIntoOldStats(mStatsObj, oldStats);
    } else {
      pm.makePersistent(mStatsObj);
    }
  }

  @Override
  public Map<String, String> updateTableColumnStatistics(ColumnStatistics colStats, String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
    long start = System.currentTimeMillis();
    String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf);
    // DataNucleus objects get detached all over the place for no (real) reason.
    // So let's not use them anywhere unless absolutely necessary.
    MTable mTable = baseStore.ensureGetMTable(catName, statsDesc.getDbName(), statsDesc.getTableName());
    int maxRetries = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_S4U_NOWAIT_MAX_RETRIES);
    long sleepInterval = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.METASTORE_S4U_NOWAIT_RETRY_SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
    Map<String, String> result = new RetryingExecutor<>(maxRetries, () -> {
      AtomicReference<Exception> exceptionRef = new AtomicReference<>();
      String savePoint = "uts_" + ThreadLocalRandom.current().nextInt(10000) + "_" + System.nanoTime();
      setTransactionSavePoint(savePoint);
      executePlainSQL(
          sqlGenerator.addForUpdateNoWait("SELECT \"TBL_ID\" FROM \"TBLS\" WHERE \"TBL_ID\" = " + mTable.getId()),
          true,
          exception -> {
            rollbackTransactionToSavePoint(savePoint);
            exceptionRef.set(exception);
          });
      if (exceptionRef.get() != null) {
        throw new RetryingExecutor.RetryException(exceptionRef.get());
      }
      pm.refresh(mTable);
      Table table = convertToTable(mTable, conf);
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }

      Map<String, MTableColumnStatistics> oldStats = Maps.newHashMap();
      List<MTableColumnStatistics> stats = getMTableColumnStatistics(table, colNames, colStats.getEngine());
      for (MTableColumnStatistics cStat : stats) {
        oldStats.put(cStat.getColName(), cStat);
      }

      for (ColumnStatisticsObj statsObj : statsObjs) {
        MTableColumnStatistics mStatsObj = StatObjectConverter.convertToMTableColumnStatistics(mTable, statsDesc,
            statsObj, colStats.getEngine());
        writeMTableColumnStatistics(table, mStatsObj, oldStats.get(statsObj.getColName()));
        // There is no need to add colname again, otherwise we will get duplicate colNames.
      }

      // Set the table properties
      // No need to check again if it exists.
      String dbname = table.getDbName();
      String name = table.getTableName();
      MTable oldt = mTable;
      Map<String, String> newParams = new HashMap<>(table.getParameters());
      StatsSetupConst.setColumnStatsState(newParams, colNames);
      boolean isTxn = TxnUtils.isTransactionalTable(oldt.getParameters());
      if (isTxn) {
        if (!areTxnStatsSupported) {
          StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
        } else {
          String errorMsg = verifyStatsChangeCtx(TableName.getDbTable(dbname, name), oldt.getParameters(), newParams,
              writeId, validWriteIds, true);
          if (errorMsg != null) {
            throw new MetaException(errorMsg);
          }
          if (!isCurrentStatsValidForTheQuery(oldt, validWriteIds, true)) {
            // Make sure we set the flag to invalid regardless of the current value.
            StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
            LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the table " + dbname + "." + name);
          }
          oldt.setWriteId(writeId);
        }
      }
      oldt.setParameters(newParams);
      return newParams;
    }).onRetry(e -> e instanceof RetryingExecutor.RetryException)
        .commandName("updateTableColumnStatistics").sleepInterval(sleepInterval, interval ->
            ThreadLocalRandom.current().nextLong(sleepInterval) + 30).run();

    LOG.debug("{} updateTableColumnStatistics took {}ms",
        new TableName(catName, statsDesc.getDbName(), statsDesc.getTableName()),
        System.currentTimeMillis() - start);
    return result;
  }

  @Override
  public Map<String, String> updatePartitionColumnStatistics(Table table, MTable mTable, ColumnStatistics colStats,
      List<String> partVals, String validWriteIds, long writeId)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    long start = System.currentTimeMillis();
    List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
    String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf);
    MPartition mPartition =
        baseStore.ensureGetMPartition(new TableName(catName, statsDesc.getDbName(), statsDesc.getTableName()), partVals);
    if (mPartition == null) {
      throw new NoSuchObjectException("Partition for which stats is gathered doesn't exist.");
    }

    List<String> colNames = new ArrayList<>();
    for(ColumnStatisticsObj statsObj : statsObjs) {
      colNames.add(statsObj.getColName());
    }
    int maxRetries = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_S4U_NOWAIT_MAX_RETRIES);
    long sleepInterval = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.METASTORE_S4U_NOWAIT_RETRY_SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
    Map<String, String> result = new RetryingExecutor<>(maxRetries, () -> {
      AtomicReference<Exception> exceptionRef = new AtomicReference<>();
      String savePoint = "ups_" + ThreadLocalRandom.current().nextInt(10000) + "_" + System.nanoTime();
      setTransactionSavePoint(savePoint);
      executePlainSQL(sqlGenerator.addForUpdateNoWait(
              "SELECT \"PART_ID\" FROM \"PARTITIONS\" WHERE \"PART_ID\" = " + mPartition.getId()),
          true,
          exception -> {
            rollbackTransactionToSavePoint(savePoint);
            exceptionRef.set(exception);
          });
      if (exceptionRef.get() != null) {
        throw new RetryingExecutor.RetryException(exceptionRef.get());
      }
      pm.refresh(mPartition);
      Partition partition = convertToPart(catName, statsDesc.getDbName(), statsDesc.getTableName(),
          mPartition, TxnUtils.isAcidTable(table), conf);
      Map<String, MPartitionColumnStatistics> oldStats = Maps.newHashMap();
      List<MPartitionColumnStatistics> stats =
          getMPartitionColumnStatistics(table, Lists.newArrayList(statsDesc.getPartName()), colNames, colStats.getEngine());
      for (MPartitionColumnStatistics cStat : stats) {
        oldStats.put(cStat.getColName(), cStat);
      }

      for (ColumnStatisticsObj statsObj : statsObjs) {
        MPartitionColumnStatistics mStatsObj = StatObjectConverter.convertToMPartitionColumnStatistics(mPartition,
            statsDesc, statsObj, colStats.getEngine());
        writeMPartitionColumnStatistics(table, partition, mStatsObj, oldStats.get(statsObj.getColName()));
      }

      Map<String, String> newParams = new HashMap<>(mPartition.getParameters());
      StatsSetupConst.setColumnStatsState(newParams, colNames);
      boolean isTxn = TxnUtils.isTransactionalTable(table);
      if (isTxn) {
        if (!areTxnStatsSupported) {
          StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
        } else {
          String errorMsg = verifyStatsChangeCtx(
              TableName.getDbTable(statsDesc.getDbName(), statsDesc.getTableName()), mPartition.getParameters(),
              newParams, writeId, validWriteIds, true);
          if (errorMsg != null) {
            throw new MetaException(errorMsg);
          }
          if (!isCurrentStatsValidForTheQuery(mPartition.getParameters(), mPartition.getWriteId(), validWriteIds, true)) {
            // Make sure we set the flag to invalid regardless of the current value.
            StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
            LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the partition: {}, {} ",
                new TableName(catName, statsDesc.getDbName(), statsDesc.getTableName()), statsDesc.getPartName());
          }
          mPartition.setWriteId(writeId);
        }
      }
      mPartition.setParameters(newParams);
      return newParams;
    }).onRetry(e -> e instanceof RetryingExecutor.RetryException)
        .commandName("updatePartitionColumnStatistics").sleepInterval(sleepInterval, interval ->
            ThreadLocalRandom.current().nextLong(sleepInterval) + 30).run();
    LOG.debug("{} updatePartitionColumnStatistics took {}ms",
        new TableName(catName, statsDesc.getDbName(), statsDesc.getTableName()),
        System.currentTimeMillis() - start);
    return result;
  }

  @Override
  public Map<String, Map<String, String>> updatePartitionColumnStatisticsInBatch(
      Map<String, ColumnStatistics> partColStatsMap,
      Table tbl,
      List<TransactionalMetaStoreEventListener> listeners,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {

    return new GetHelper<TableName, Map<String, Map<String, String>>>(this, null) {
      @Override
      protected String describeResult() {
        return "Map of partition key to column stats if successful";
      }
      @Override
      protected Map<String, Map<String, String>> getSqlResult()
          throws MetaException {
        return getDirectSql().updatePartitionColumnStatisticsBatch(partColStatsMap, tbl,
            listeners, validWriteIds, writeId);
      }

      @Override
      protected boolean canUseJdoQuery() throws MetaException {
        return false;
      }

      @Override
      protected Map<String, Map<String, String>> getJdoResult()
          throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        throw new UnsupportedOperationException("Cannot update partition column statistics with JDO, make sure direct SQL is enabled");
      }
    }.run(false);
  }

  private List<MTableColumnStatistics> getMTableColumnStatistics(Table table, List<String> colNames, String engine)
      throws MetaException {

    Preconditions.checkState(baseStore.isActiveTransaction());

    if (colNames.isEmpty()) {
      return Collections.emptyList();
    }

    validateTableCols(table, colNames);

    List<MTableColumnStatistics> result = Collections.emptyList();
    Query query = pm.newQuery(MTableColumnStatistics.class);
    result =
        Batchable.runBatched(batchSize, colNames, new Batchable<String, MTableColumnStatistics>() {
          @Override
          public List<MTableColumnStatistics> run(List<String> input)
              throws MetaException {
            StringBuilder filter =
                new StringBuilder("table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3 && engine == t4 && (");
            StringBuilder paramStr = new StringBuilder(
                "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4");
            Object[] params = new Object[input.size() + 4];
            params[0] = table.getTableName();
            params[1] = table.getDbName();
            params[2] = table.getCatName();
            params[3] = engine;
            for (int i = 0; i < input.size(); ++i) {
              filter.append((i == 0) ? "" : " || ").append("colName == c").append(i);
              paramStr.append(", java.lang.String c").append(i);
              params[i + 4] = input.get(i);
            }
            filter.append(")");
            query.setFilter(filter.toString());
            query.declareParameters(paramStr.toString());
            List<MTableColumnStatistics> paritial = (List<MTableColumnStatistics>) query.executeWithArray(params);
            pm.retrieveAll(paritial);
            return paritial;
          }
        });

    if (result.size() > colNames.size()) {
      throw new MetaException("Unexpected " + result.size() + " statistics for "
          + colNames.size() + " columns");
    }
    return new ArrayList<>(result);
  }

  @Override
  public List<ColumnStatistics> getTableColumnStatistics(
      TableName tblName,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    String catName = normalizeIdentifier(tblName.getCat());
    String dbName = normalizeIdentifier(tblName.getDb());
    String tableName = normalizeIdentifier(tblName.getTable());
    // Note: this will get stats without verifying ACID.
    List<ColumnStatistics> result = new ArrayList<>();
    Query query = pm.newQuery(MTableColumnStatistics.class);
    query.setFilter("table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    query.setResult("DISTINCT engine");
    Collection names = (Collection) query.execute(tableName, dbName, catName);
    List<String> engines = new ArrayList<>();
    for (Iterator i = names.iterator(); i.hasNext();) {
      engines.add((String) i.next());
    }
    for (String e : engines) {
      ColumnStatistics cs = getTableColumnStatisticsInternal(
          new TableName(catName, dbName, tableName), colNames, e);
      if (cs != null) {
        result.add(cs);
      }
    }
    return result;
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(
      TableName tableName,
      List<String> colNames,
      String engine) throws MetaException, NoSuchObjectException {
    // Note: this will get stats without verifying ACID.
    return getTableColumnStatisticsInternal(tableName, colNames, engine);
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(
      TableName tableName,
      List<String> colNames,
      String engine,
      String writeIdList) throws MetaException, NoSuchObjectException {
    // If the current stats in the metastore doesn't comply with
    // the isolation level of the query, set No to the compliance flag.
    Boolean isCompliant = null;
    String catName = normalizeIdentifier(tableName.getCat());
    String dbName = normalizeIdentifier(tableName.getDb());
    String tblName = normalizeIdentifier(tableName.getTable());
    if (writeIdList != null) {
      MTable table = baseStore.ensureGetMTable(catName, dbName, tblName);
      isCompliant = !TxnUtils.isTransactionalTable(table.getParameters())
          || (areTxnStatsSupported && isCurrentStatsValidForTheQuery(table, writeIdList, false));
    }
    ColumnStatistics stats = getTableColumnStatisticsInternal(
        new TableName(catName, dbName, tblName), colNames, engine);
    if (stats != null && isCompliant != null) {
      stats.setIsStatsCompliant(isCompliant);
    }
    return stats;
  }

  protected ColumnStatistics getTableColumnStatisticsInternal(
      TableName tableName, final List<String> colNames, String engine) throws MetaException, NoSuchObjectException {
    final boolean enableBitVector = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR);
    final boolean enableKll = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_KLL);
    return new ObjectStore.GetStatHelper(normalizeIdentifier(catName), normalizeIdentifier(dbName),
        normalizeIdentifier(tableName), allowSql, allowJdo, null) {
      @Override
      protected ColumnStatistics getSqlResult(ObjectStore.GetHelper<ColumnStatistics> ctx) throws MetaException {
        return directSqlAggrStats.getTableStats(catName, dbName, tblName, colNames, engine, enableBitVector, enableKll);
      }

      @Override
      protected ColumnStatistics getJdoResult(ObjectStore.GetHelper<ColumnStatistics> ctx) throws MetaException {

        List<MTableColumnStatistics> mStats = getMTableColumnStatistics(getTable(), colNames, engine);
        if (mStats.isEmpty()) {
          return null;
        }
        // LastAnalyzed is stored per column, but thrift object has it per
        // multiple columns. Luckily, nobody actually uses it, so we will set to
        // lowest value of all columns for now.
        ColumnStatisticsDesc desc = StatObjectConverter.getTableColumnStatisticsDesc(mStats.get(0));
        List<ColumnStatisticsObj> statObjs = new ArrayList<>(mStats.size());
        for (MTableColumnStatistics mStat : mStats) {
          if (desc.getLastAnalyzed() > mStat.getLastAnalyzed()) {
            desc.setLastAnalyzed(mStat.getLastAnalyzed());
          }
          statObjs.add(StatObjectConverter.getColumnStatisticsObj(mStat, enableBitVector, enableKll));
          Deadline.checkTimeout();
        }
        ColumnStatistics colStat = new ColumnStatistics(desc, statObjs);
        colStat.setEngine(engine);
        return colStat;
      }
    }.run(true);
  }

  @Override
  public List<List<ColumnStatistics>> getPartitionColumnStatistics(TableName tblName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    String catName = normalizeIdentifier(tblName.getCat());
    String dbName = normalizeIdentifier(tblName.getDb());
    String tableName = normalizeIdentifier(tblName.getTable());
    // Note: this will get stats without verifying ACID.
    List<List<ColumnStatistics>> result = new ArrayList<>();
    Query query = pm.newQuery(MPartitionColumnStatistics.class);
    query.setFilter("partition.table.tableName == t1 && partition.table.database.name == t2 && partition.table.database.catalogName == t3");
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    query.setResult("DISTINCT engine");
    Collection names = (Collection) query.execute(tableName, dbName, catName);
    List<String> engines = new ArrayList<>();
    for (Iterator i = names.iterator(); i.hasNext();) {
      engines.add((String) i.next());
    }
    for (String e : engines) {
      List<ColumnStatistics> cs = getPartitionColumnStatisticsInternal(
          new TableName(catName, dbName, tableName), partNames, colNames, e);
      if (cs != null) {
        result.add(cs);
      }
    }
    return result;
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(TableName tableName,
      List<String> partNames, List<String> colNames, String engine) throws MetaException, NoSuchObjectException {
    // Note: this will get stats without verifying ACID.
    if (CollectionUtils.isEmpty(partNames) || CollectionUtils.isEmpty(colNames)) {
      LOG.debug("PartNames and/or ColNames are empty");
      return Collections.emptyList();
    }
    return getPartitionColumnStatisticsInternal(tableName, partNames, colNames, engine);
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(
      TableName tableName,
      List<String> partNames, List<String> colNames,
      String engine, String writeIdList)
      throws MetaException, NoSuchObjectException {
    if (CollectionUtils.isEmpty(partNames) || CollectionUtils.isEmpty(colNames)) {
      LOG.debug("PartNames and/or ColNames are empty");
      return Collections.emptyList();
    }
    List<ColumnStatistics> allStats = getPartitionColumnStatisticsInternal(
        catName, dbName, tableName, partNames, colNames, engine, true, true);
    if (writeIdList != null) {
      if (!areTxnStatsSupported) {
        for (ColumnStatistics cs : allStats) {
          cs.setIsStatsCompliant(false);
        }
      } else {
        // TODO: this could be improved to get partitions in bulk
        for (ColumnStatistics cs : allStats) {
          MPartition mpart = ensureGetMPartition(new TableName(catName, dbName, tableName),
              Warehouse.getPartValuesFromPartName(cs.getStatsDesc().getPartName()));
          if (mpart == null
              || !isCurrentStatsValidForTheQuery(mpart.getParameters(), mpart.getWriteId(), writeIdList, false)) {
            if (mpart != null) {
              LOG.debug("The current metastore transactional partition column statistics for {}.{}.{} "
                      + "(write ID {}) are not valid for current query ({} {})", dbName, tableName,
                  mpart.getPartitionName(), mpart.getWriteId(), writeIdList);
            }
            cs.setIsStatsCompliant(false);
          } else {
            cs.setIsStatsCompliant(true);
          }
        }
      }
    }
    return allStats;
  }

  protected List<ColumnStatistics> getPartitionColumnStatisticsInternal(
     TableName tableName, final List<String> partNames, final List<String> colNames,
      String engine) throws MetaException, NoSuchObjectException {
    final boolean enableBitVector = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR);
    final boolean enableKll = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_KLL);
    return new ObjectStore.GetListHelper<ColumnStatistics>(catName, dbName, tableName, allowSql, allowJdo) {
      @Override
      protected List<ColumnStatistics> getSqlResult(
          ObjectStore.GetHelper<List<ColumnStatistics>> ctx) throws MetaException {
        return directSqlAggrStats.getPartitionStats(
            catName, dbName, tblName, partNames, colNames, engine, enableBitVector, enableKll);
      }
      @Override
      protected List<ColumnStatistics> getJdoResult(ObjectStore.GetHelper<List<ColumnStatistics>> ctx)
          throws MetaException, NoSuchObjectException {
        List<MPartitionColumnStatistics> mStats =
            getMPartitionColumnStatistics(getTable(), partNames, colNames, engine);
        List<ColumnStatistics> result = new ArrayList<>(Math.min(mStats.size(), partNames.size()));
        String lastPartName = null;
        List<ColumnStatisticsObj> curList = null;
        ColumnStatisticsDesc csd = null;
        for (int i = 0; i <= mStats.size(); ++i) {
          boolean isLast = i == mStats.size();
          MPartitionColumnStatistics mStatsObj = isLast ? null : mStats.get(i);
          String partName = isLast ? null : mStatsObj.getPartition().getPartitionName();
          if (isLast || !partName.equals(lastPartName)) {
            if (i != 0) {
              ColumnStatistics colStat = new ColumnStatistics(csd, curList);
              colStat.setEngine(engine);
              result.add(colStat);
            }
            if (isLast) {
              continue;
            }
            csd = StatObjectConverter.getPartitionColumnStatisticsDesc(mStatsObj);
            curList = new ArrayList<>(colNames.size());
          }
          curList.add(StatObjectConverter.getColumnStatisticsObj(mStatsObj, enableBitVector, enableKll));
          lastPartName = partName;
          Deadline.checkTimeout();
        }
        return result;
      }
    }.run(true);
  }

  @Override
  public AggrStats get_aggr_stats_for(TableName tableName,
      final List<String> partNames, final List<String> colNames,
      String engine, String writeIdList) throws MetaException, NoSuchObjectException {
    // If the current stats in the metastore doesn't comply with
    // the isolation level of the query, return null.
    if (writeIdList != null) {
      if (partNames == null || partNames.isEmpty()) {
        return null;
      }

      Table table = getTable(catName, dbName, tblName);
      boolean isTxn = TxnUtils.isTransactionalTable(table.getParameters());
      if (isTxn && !areTxnStatsSupported) {
        return null;
      }
      GetPartitionsFilterSpec fs = new GetPartitionsFilterSpec();
      fs.setFilterMode(PartitionFilterMode.BY_NAMES);
      fs.setFilters(partNames);
      GetProjectionsSpec ps = new GetProjectionsSpec();
      ps.setIncludeParamKeyPattern(StatsSetupConst.COLUMN_STATS_ACCURATE + '%');
      ps.setFieldList(Lists.newArrayList("writeId", "parameters", "values"));
      List<Partition> parts = getPartitionSpecsByFilterAndProjection(table, ps, fs);

      // Loop through the given "partNames" list
      // checking isolation-level-compliance of each partition column stats.
      for (Partition part : parts) {

        if (!isCurrentStatsValidForTheQuery(part.getParameters(), part.getWriteId(), writeIdList, false)) {
          String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
          LOG.debug("The current metastore transactional partition column "
                  + "statistics for {}.{}.{} is not valid for the current query",
              dbName, tblName, partName);
          return null;
        }
      }
    }
    return get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, engine);
  }

  @Override
  public AggrStats get_aggr_stats_for(TableName tableName,
      final List<String> partNames, final List<String> colNames, String engine)
      throws MetaException, NoSuchObjectException {
    final boolean useDensityFunctionForNDVEstimation = MetastoreConf.getBoolVar(conf,
        MetastoreConf.ConfVars.STATS_NDV_DENSITY_FUNCTION);
    final double ndvTuner = MetastoreConf.getDoubleVar(conf, MetastoreConf.ConfVars.STATS_NDV_TUNER);
    final boolean enableBitVector = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR);
    final boolean enableKll = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_KLL);
    return new ObjectStore.GetHelper<AggrStats>(catName, dbName, tblName, true, false) {
      @Override
      protected AggrStats getSqlResult(ObjectStore.GetHelper<AggrStats> ctx)
          throws MetaException {
        return directSql.aggrColStatsForPartitions(catName, dbName, tblName, partNames,
            colNames, engine, useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector, enableKll);
      }
      @Override
      protected AggrStats getJdoResult(ObjectStore.GetHelper<AggrStats> ctx)
          throws MetaException, NoSuchObjectException {
        // This is fast path for query optimizations, if we can find this info
        // quickly using
        // directSql, do it. No point in failing back to slow path here.
        throw new MetaException("Jdo path is not implemented for stats aggr.");
      }
      @Override
      protected String describeResult() {
        return null;
      }
    }.run(true);
  }

  @Override
  public List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    final boolean enableBitVector = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR);
    final boolean enableKll = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_KLL);
    return new ObjectStore.GetHelper<List<MetaStoreServerUtils.ColStatsObjWithSourceInfo>>(
        catName, dbName, null, true, false) {
      @Override
      protected List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getSqlResult(
          ObjectStore.GetHelper<List<MetaStoreServerUtils.ColStatsObjWithSourceInfo>> ctx) throws MetaException {
        return directSqlAggrStats.getColStatsForAllTablePartitions(catName, dbName, enableBitVector, enableKll);
      }

      @Override
      protected List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getJdoResult(
          ObjectStore.GetHelper<List<MetaStoreServerUtils.ColStatsObjWithSourceInfo>> ctx)
          throws MetaException, NoSuchObjectException {
        // This is fast path for query optimizations, if we can find this info
        // quickly using directSql, do it. No point in failing back to slow path
        // here.
        throw new MetaException("Jdo path is not implemented for getPartitionColStatsForDatabase.");
      }

      @Override
      protected String describeResult() {
        return null;
      }
    }.run(true);
  }

  private List<MPartitionColumnStatistics> getMPartitionColumnStatistics(Table table, List<String> partNames,
      List<String> colNames, String engine) throws MetaException {
    boolean committed = false;

    try {
      openTransaction();
      // We are not going to verify SD for each partition. Just verify for the
      // table. TODO: we need verify the partition column instead
      try {
        validateTableCols(table, colNames);
      } catch (MetaException me) {
        LOG.warn("The table does not have the same column definition as its partition.");
      }
      List<MPartitionColumnStatistics> result = Collections.emptyList();
      try (Query query = pm.newQuery(MPartitionColumnStatistics.class)) {
        String paramStr = "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4";
        String filter = "partition.table.tableName == t1 && partition.table.database.name == t2 && partition.table.database.catalogName == t3 && engine == t4 && (";
        Object[] params = new Object[colNames.size() + partNames.size() + 4];
        int i = 0;
        params[i++] = table.getTableName();
        params[i++] = table.getDbName();
        params[i++] = table.isSetCatName() ? table.getCatName() : getDefaultCatalog(conf);
        params[i++] = engine;
        int firstI = i;
        for (String s : partNames) {
          filter += ((i == firstI) ? "" : " || ") + "partition.partitionName == p" + i;
          paramStr += ", java.lang.String p" + i;
          params[i++] = s;
        }
        filter += ") && (";
        firstI = i;
        for (String s : colNames) {
          filter += ((i == firstI) ? "" : " || ") + "colName == c" + i;
          paramStr += ", java.lang.String c" + i;
          params[i++] = s;
        }
        filter += ")";
        query.setFilter(filter);
        query.declareParameters(paramStr);
        query.setOrdering("partition.partitionName ascending");
        result = (List<MPartitionColumnStatistics>) query.executeWithArray(params);
        pm.retrieveAll(result);
        result = new ArrayList<>(result);
      } catch (Exception ex) {
        LOG.error("Error retrieving statistics via jdo", ex);
        throw new MetaException(ex.getMessage());
      }
      committed = commitTransaction();
      return result;
    } finally {
      if (!committed) {
        rollbackTransaction();
        return Collections.emptyList();
      }
    }
  }

  @Override
  public void deleteAllPartitionColumnStatistics(TableName tn, String writeIdList) {

    String catName = tn.getCat();
    String dbName = tn.getDb();
    String tableName = tn.getTable();

    Query query = null;
    dbName = org.apache.commons.lang3.StringUtils.defaultString(dbName, Warehouse.DEFAULT_DATABASE_NAME);
    catName = normalizeIdentifier(catName);
    if (tableName == null) {
      throw new RuntimeException("Table name is null.");
    }
    boolean ret = false;
    try {
      openTransaction();
      MTable mTable = getMTable(catName, dbName, tableName);

      query = pm.newQuery(MPartitionColumnStatistics.class);

      String filter = "partition.table.database.name == t2 && partition.table.tableName == t3 && partition.table.database.catalogName == t4";
      String parameters = "java.lang.String t2, java.lang.String t3, java.lang.String t4";

      query.setFilter(filter);
      query.declareParameters(parameters);

      Long number = query.deletePersistentAll(normalizeIdentifier(dbName), normalizeIdentifier(tableName),
          normalizeIdentifier(catName));

      new ObjectStore.GetHelper<Integer>(catName, dbName, tableName, true, true) {
        private final MetaStoreDirectSql.SqlFilterForPushdown filter = new MetaStoreDirectSql.SqlFilterForPushdown();

        @Override
        protected String describeResult() {
          return "Partition count";
        }

        @Override
        protected boolean canUseDirectSql(ObjectStore.GetHelper<Integer> ctx) throws MetaException {
          return true;
        }

        @Override
        protected Integer getSqlResult(ObjectStore.GetHelper<Integer> ctx) throws MetaException {
          directSql.deleteColumnStatsState(getTable().getId());
          return 0;
        }

        @Override
        protected Integer getJdoResult(ObjectStore.GetHelper<Integer> ctx) throws MetaException, NoSuchObjectException {
          try {
            List<Partition> parts = getPartitions(catName, dbName, tableName,
                GetPartitionsArgs.getAllPartitions());
            for (Partition part : parts) {
              Partition newPart = new Partition(part);
              StatsSetupConst.clearColumnStatsState(newPart.getParameters());
              alterPartition(catName, dbName, tableName, part.getValues(), newPart, writeIdList);
            }
            return parts.size();
          } catch (InvalidObjectException e) {
            LOG.error("error updating parts", e);
            return -1;
          }
        }
      }.run(true);

      ret = commitTransaction();
    } catch (Exception e) {
      LOG.error("Couldn't clear stats for table", e);
    } finally {
      rollbackAndCleanup(ret, query);
    }
  }

  @Override
  public boolean deletePartitionColumnStatistics(TableName tableName,
      List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    if (partNames == null || partNames.isEmpty()) {
      throw new InvalidInputException("No partition specified for dropping the statistics");
    }
    dbName = org.apache.commons.lang3.StringUtils.defaultString(dbName, Warehouse.DEFAULT_DATABASE_NAME);
    catName = normalizeIdentifier(catName);
    List<String> cols = normalizeIdentifiers(colNames);
    return new ObjectStore.GetHelper<Boolean>(catName, dbName, tableName, true, true) {
      @Override
      protected String describeResult() {
        return "delete partition column stats";
      }
      @Override
      protected Boolean getSqlResult(ObjectStore.GetHelper<Boolean> ctx) throws MetaException {
        DirectSqlDeleteStats deleteStats = new DirectSqlDeleteStats(directSql, pm);
        return deleteStats.deletePartitionColumnStats(catName, dbName, tableName, partNames, cols, engine);
      }
      @Override
      protected Boolean getJdoResult(ObjectStore.GetHelper<Boolean> ctx)
          throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return deletePartitionColumnStatisticsViaJdo(catName, dbName, tableName, partNames, cols, engine);
      }
    }.run(false);
  }

  private boolean deletePartitionColumnStatisticsViaJdo(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean ret = false;
    String database = org.apache.commons.lang3.StringUtils.defaultString(dbName,
        Warehouse.DEFAULT_DATABASE_NAME);
    String catalog = normalizeIdentifier(catName);
    Batchable<String, Void> b = new Batchable<String, Void>() {
      @Override
      public List<Void> run(List<String> input) throws Exception {
        Query query = pm.newQuery(MPartitionColumnStatistics.class);
        addQueryAfterUse(query);
        String filter;
        String parameters;
        if (colNames != null && !colNames.isEmpty()) {
          filter = "t1.contains(partition.partitionName) && partition.table.database.name == t2 && partition.table.tableName == t3 && "
              + "t4.contains(colName) && partition.table.database.catalogName == t5" + (engine != null ? " && engine == t6" : "");
          parameters = "java.util.Collection t1, java.lang.String t2, java.lang.String t3, "
              + "java.util.Collection t4, java.lang.String t5" + (engine != null ? ", java.lang.String t6" : "");
        } else {
          filter = "t1.contains(partition.partitionName) && partition.table.database.name == t2 && partition.table.tableName == t3 && " +
              "partition.table.database.catalogName == t4" + (engine != null ? " && engine == t5" : "");
          parameters = "java.util.Collection t1, java.lang.String t2, java.lang.String t3, java.lang.String t4" + (engine != null ? ", java.lang.String t5" : "");
        }
        query.setFilter(filter);
        query.declareParameters(parameters);
        List<Object> params = new ArrayList<>();
        params.add(input);
        params.add(normalizeIdentifier(database));
        params.add(normalizeIdentifier(tableName));
        if (colNames != null && !colNames.isEmpty()) {
          params.add(colNames);
        }
        params.add(catalog);
        if (engine != null) {
          params.add(engine);
        }
        List<MPartitionColumnStatistics> mStatsObjColl =
            (List<MPartitionColumnStatistics>) query.executeWithArray(params.toArray());
        pm.retrieveAll(mStatsObjColl);
        if (mStatsObjColl != null) {
          pm.deletePersistentAll(mStatsObjColl);
        }
        return null;
      }
    };
    try {
      Batchable.runBatched(batchSize, partNames, b);
    } finally {
      b.closeAllQueries();
    }

    Batchable.runBatched(batchSize, partNames, new Batchable<String, Void>() {
      @Override
      public List<Void> run(List<String> input) throws MetaException {
        Pair<Query, Map<String, String>> queryWithParams = getPartQueryWithParams(pm, catalog, database, tableName,
            input);
        try (QueryWrapper qw = new QueryWrapper(queryWithParams.getLeft())) {
          qw.setResultClass(MPartition.class);
          qw.setClass(MPartition.class);
          List<MPartition> mparts = (List<MPartition>) qw.executeWithMap(queryWithParams.getRight());
          for (MPartition mPart : mparts) {
            Map<String, String> params = mPart.getParameters();
            if (params != null && params.containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
              if (colNames == null || colNames.isEmpty()) {
                StatsSetupConst.clearColumnStatsState(params);
              } else {
                StatsSetupConst.removeColumnStatsState(params, colNames);
              }
              mPart.setParameters(params);
            }
          }
        }
        return Collections.emptyList();
      }
    });
    return ret;
  }

  @Override
  public boolean deleteTableColumnStatistics(TableName tableName,
      List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    dbName = org.apache.commons.lang3.StringUtils.defaultString(dbName, Warehouse.DEFAULT_DATABASE_NAME);
    if (tableName == null) {
      throw new InvalidInputException("Table name is null.");
    }
    List<String> cols = normalizeIdentifiers(colNames);
    return new ObjectStore.GetHelper<Boolean>(catName, dbName, tableName, true, true) {
      @Override
      protected String describeResult() {
        return "delete table column stats";
      }
      @Override
      protected Boolean getSqlResult(ObjectStore.GetHelper<Boolean> ctx) throws MetaException {
        DirectSqlDeleteStats deleteStats = new DirectSqlDeleteStats(directSql, pm);
        return deleteStats.deleteTableColumnStatistics(getTable(), cols, engine);
      }
      @Override
      protected Boolean getJdoResult(ObjectStore.GetHelper<Boolean> ctx)
          throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return deleteTableColumnStatisticsViaJdo(catName, dbName, tableName, cols, engine);
      }
    }.run(true);
  }

  private boolean deleteTableColumnStatisticsViaJdo(String catName, String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean ret = false;
    Query query = null;
    try {
      openTransaction();
      List<MTableColumnStatistics> mStatsObjColl;
      // Note: this does not verify ACID state; called internally when removing cols/etc.
      //       Also called via an unused metastore API that checks for ACID tables.
      query = pm.newQuery(MTableColumnStatistics.class);
      String filter;
      String parameters;
      if (colNames != null && !colNames.isEmpty()) {
        filter = "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3 && t4.contains(colName)" + (engine != null ? " && engine == t5" : "");
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.util.Collection t4" + (engine != null ? ", java.lang.String t5" : "");
      } else {
        filter = "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3" + (engine != null ? " && engine == t4" : "");
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3" + (engine != null ? ", java.lang.String t4" : "");
      }

      query.setFilter(filter);
      query.declareParameters(parameters);
      List<Object> params = new ArrayList<>();
      params.add(normalizeIdentifier(tableName));
      params.add(normalizeIdentifier(dbName));
      params.add(catName == null ? null : normalizeIdentifier(catName));
      if (colNames != null && !colNames.isEmpty()) {
        params.add(colNames);
      }
      if (engine != null) {
        params.add(engine);
      }
      mStatsObjColl = (List<MTableColumnStatistics>) query.executeWithArray(params.toArray());
      pm.retrieveAll(mStatsObjColl);
      if (mStatsObjColl != null) {
        pm.deletePersistentAll(mStatsObjColl);
      }

      MTable mTable = getMTable(catName, dbName, tableName);
      if (mTable != null) {
        Map<String, String> tableParams = mTable.getParameters();
        if (tableParams != null && tableParams.containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
          if (colNames == null || colNames.isEmpty()) {
            StatsSetupConst.clearColumnStatsState(tableParams);
          } else {
            StatsSetupConst.removeColumnStatsState(tableParams, colNames);
          }
          mTable.setParameters(tableParams);
        }
      }
      ret = commitTransaction();
    } finally {
      rollbackAndCleanup(ret, query);
    }
    return ret;
  }

  private void executePlainSQL(String sql,
      boolean atLeastOneRecord,
      Consumer<Exception> exceptionConsumer)
      throws SQLException, MetaException {
    String s = dbType.getPrepareTxnStmt();
    assert pm.currentTransaction().isActive();
    JDOConnection jdoConn = pm.getDataStoreConnection();
    Connection conn = (Connection) jdoConn.getNativeConnection();
    try (Statement statement = conn.createStatement()) {
      if (s != null) {
        statement.execute(s);
      }
      try {
        statement.execute(sql);
        try (ResultSet rs = statement.getResultSet()) {
          // sqlserver needs rs.next for validating the s4u nowait
          if (atLeastOneRecord && !rs.next()) {
            throw new MetaException("At least one record but none is returned from the query: " + sql);
          }
        }
      } catch (SQLException e) {
        if (exceptionConsumer != null) {
          exceptionConsumer.accept(e);
        } else {
          throw e;
        }
      }
    } finally {
      jdoConn.close();
    }
  }

  /**
   * Return true if the current statistics in the Metastore is valid
   * for the query of the given "txnId" and "queryValidWriteIdList".
   *
   * Note that a statistics entity is valid iff
   * the stats is written by the current query or
   * the conjunction of the following two are true:
   * ~ COLUMN_STATE_ACCURATE(CSA) state is true
   * ~ Isolation-level (snapshot) compliant with the query
   * @param tbl                    MTable of the stats entity
   * @param queryValidWriteIdList  valid writeId list of the query
   * @Precondition   "tbl" should be retrieved from the TBLS table.
   */
  private boolean isCurrentStatsValidForTheQuery(MTable tbl, String queryValidWriteIdList,
      boolean isCompleteStatsWriter) throws MetaException {
    return isCurrentStatsValidForTheQuery(tbl.getParameters(), tbl.getWriteId(),
        queryValidWriteIdList, isCompleteStatsWriter);
  }

  /**
   * Return true if the current statistics in the Metastore is valid
   * for the query of the given "txnId" and "queryValidWriteIdList".
   *
   * Note that a statistics entity is valid iff
   * the stats is written by the current query or
   * the conjunction of the following two are true:
   * ~ COLUMN_STATE_ACCURATE(CSA) state is true
   * ~ Isolation-level (snapshot) compliant with the query
   * @param queryValidWriteIdList  valid writeId list of the query
   */
  // TODO: move to somewhere else
  public static boolean isCurrentStatsValidForTheQuery(
      Map<String, String> statsParams, long statsWriteId, String queryValidWriteIdList,
      boolean isCompleteStatsWriter) throws MetaException {

    // Note: can be changed to debug/info to verify the calls.
    LOG.debug("isCurrentStatsValidForTheQuery with stats write ID {}; query {}; writer: {} params {}",
        statsWriteId, queryValidWriteIdList, isCompleteStatsWriter, statsParams);
    // return true since the stats does not seem to be transactional.
    if (statsWriteId < 1) {
      return true;
    }
    // This COLUMN_STATS_ACCURATE(CSA) state checking also includes the case that the stats is
    // written by an aborted transaction but TXNS has no entry for the transaction
    // after compaction. Don't check for a complete stats writer - it may replace invalid stats.
    if (!isCompleteStatsWriter && !StatsSetupConst.areBasicStatsUptoDate(statsParams)) {
      return false;
    }

    if (queryValidWriteIdList != null) { // Can be null when stats are being reset to invalid.
      ValidWriteIdList list4TheQuery = ValidReaderWriteIdList.fromValue(queryValidWriteIdList);
      // Just check if the write ID is valid. If it's valid (i.e. we are allowed to see it),
      // that means it cannot possibly be a concurrent write. If it's not valid (we are not
      // allowed to see it), that means it's either concurrent or aborted, same thing for us.
      if (list4TheQuery.isWriteIdValid(statsWriteId)) {
        return true;
      }
      // Updater is also allowed to overwrite stats from aborted txns, as long as they are not concurrent.
      if (isCompleteStatsWriter && list4TheQuery.isWriteIdAborted(statsWriteId)) {
        return true;
      }
    }

    return false;
  }

  private abstract class GetStatHelper extends GetHelper<TableName, ColumnStatistics> {
    public GetStatHelper(TableName tableName, RawStoreAware baseStore) throws MetaException {
      super(baseStore, tableName);
    }

    @Override
    protected String describeResult() {
      return "statistics for " + (results == null ? 0 : results.getStatsObjSize()) + " columns";
    }
  }

  @VisibleForTesting
  public void validateTableCols(Table table, List<String> colNames) throws MetaException {
    List<FieldSchema> colList = table.getSd().getCols();
    for (String colName : colNames) {
      boolean foundCol = false;
      for (FieldSchema mCol : colList) {
        if (mCol.getName().equals(colName)) {
          foundCol = true;
          break;
        }
      }
      if (!foundCol) {
        throw new MetaException("Column " + colName + " doesn't exist in table "
            + table.getTableName() + " in database " + table.getDbName());
      }
    }
  }

}
