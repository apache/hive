/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive.metastore.task;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.NoMutex;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.TableFetcher;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergHouseKeeperService implements MetastoreTaskThread {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergHouseKeeperService.class);

  private Configuration conf;
  private TxnStore txnHandler;
  private boolean shouldUseMutex;
  private ExecutorService deleteExecutorService = null;

  // table cache to avoid making repeated requests for the same Iceberg tables more than once per day
  private final Cache<TableName, Table> tableCache = Caffeine.newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(1, TimeUnit.DAYS)
      .build();

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.ICEBERG_TABLE_EXPIRY_INTERVAL, unit);
  }

  @Override
  public void run() {
    LOG.debug("Running IcebergHouseKeeperService...");

    String catalogName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ICEBERG_TABLE_EXPIRY_CATALOG_NAME);
    String dbPattern = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ICEBERG_TABLE_EXPIRY_DATABASE_PATTERN);
    String tablePattern = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ICEBERG_TABLE_EXPIRY_TABLE_PATTERN);

    TxnStore.MutexAPI mutex = shouldUseMutex ? txnHandler.getMutexAPI() : new NoMutex();

    try (AutoCloseable closeable = mutex.acquireLock(TxnStore.MUTEX_KEY.IcebergHouseKeeper.name())) {
      expireTables(catalogName, dbPattern, tablePattern);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void expireTables(String catalogName, String dbPattern, String tablePattern) {
    try (IMetaStoreClient msc = new HiveMetaStoreClient(conf)) {
      // TODO: HIVE-28952 – modify TableFetcher to return HMS Table API objects directly,
      // avoiding the need for subsequent msc.getTable calls to fetch each matched table individually
      List<TableName> tables = getTableFetcher(msc, catalogName, dbPattern, tablePattern).getTables();

      LOG.debug("{} candidate tables found", tables.size());

      for (TableName table : tables) {
        try {
          expireSnapshotsForTable(getIcebergTable(table, msc));
        } catch (Exception e) {
          LOG.error("Exception while running iceberg expiry service on catalog/db/table: {}/{}/{}",
              catalogName, dbPattern, tablePattern, e);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error while getting tables from metastore", e);
    }
  }

  @VisibleForTesting
  TableFetcher getTableFetcher(IMetaStoreClient msc, String catalogName, String dbPattern, String tablePattern) {
    return new TableFetcher.Builder(msc, catalogName, dbPattern, tablePattern).tableTypes(
            "EXTERNAL_TABLE")
        .tableCondition(
            hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS + "table_type like \"ICEBERG\" ")
        .build();
  }

  private Table getIcebergTable(TableName tableName, IMetaStoreClient msc) {
    return tableCache.get(tableName, key -> {
      LOG.debug("Getting iceberg table from metastore as it's not present in table cache: {}", tableName);
      GetTableRequest request = new GetTableRequest(tableName.getDb(), tableName.getTable());
      try {
        return IcebergTableUtil.getTable(conf, msc.getTable(request));
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Deletes snapshots of an Iceberg table, using the number of threads defined by the
   * Hive config HIVE_ICEBERG_EXPIRE_SNAPSHOT_NUMTHREADS.
   * This is largely equivalent to the HiveIcebergStorageHandler.expireSnapshotWithDefaultParams method.
   *
   * @param icebergTable the iceberg Table reference
   */
  private void expireSnapshotsForTable(Table icebergTable) {
    ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots();
    if (deleteExecutorService != null) {
      expireSnapshots.executeDeleteWith(deleteExecutorService);
    }
    expireSnapshots.commit();
  }

  @Override
  public void enforceMutex(boolean enableMutex) {
    this.shouldUseMutex = enableMutex;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    txnHandler = TxnUtils.getTxnStore(conf);

    int numThreads = conf.getInt(HiveConf.ConfVars.HIVE_ICEBERG_EXPIRE_SNAPSHOT_NUMTHREADS.varname,
        HiveConf.ConfVars.HIVE_ICEBERG_EXPIRE_SNAPSHOT_NUMTHREADS.defaultIntVal);
    if (numThreads > 0) {
      LOG.info("Will expire Iceberg snapshots using an executor service with {} threads", numThreads);
      deleteExecutorService = IcebergTableUtil.newDeleteThreadPool("iceberg-housekeeper-service", numThreads);
    }
  }
}
