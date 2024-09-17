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
package org.apache.hadoop.hive.metastore;

import com.codahale.metrics.Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Map;

/**
 * Report metrics of metadata added, deleted by this Hive Metastore.
 * The listener is only attached when the metrics are enabled.
 */
public class HMSMetricsListener extends MetaStoreEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(HMSMetricsListener.class);

  private Counter createdDatabases, deletedDatabases, createdTables, deletedTables, createdParts,
      deletedParts;

  public HMSMetricsListener(Configuration config) {
    super(config);
    createdDatabases = Metrics.getOrCreateCounter(MetricsConstants.CREATE_TOTAL_DATABASES);
    deletedDatabases = Metrics.getOrCreateCounter(MetricsConstants.DELETE_TOTAL_DATABASES);
    createdTables = Metrics.getOrCreateCounter(MetricsConstants.CREATE_TOTAL_TABLES);
    deletedTables = Metrics.getOrCreateCounter(MetricsConstants.DELETE_TOTAL_TABLES);
    createdParts = Metrics.getOrCreateCounter(MetricsConstants.CREATE_TOTAL_PARTITIONS);
    deletedParts = Metrics.getOrCreateCounter(MetricsConstants.DELETE_TOTAL_PARTITIONS);
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    Metrics.getOrCreateGauge(MetricsConstants.TOTAL_DATABASES).incrementAndGet();
    createdDatabases.inc();
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    Metrics.getOrCreateGauge(MetricsConstants.TOTAL_DATABASES).decrementAndGet();
    deletedDatabases.inc();
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    Metrics.getOrCreateGauge(MetricsConstants.TOTAL_TABLES).incrementAndGet();
    createdTables.inc();
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    Metrics.getOrCreateGauge(MetricsConstants.TOTAL_TABLES).decrementAndGet();
    deletedTables.inc();
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    Metrics.getOrCreateGauge(MetricsConstants.TOTAL_PARTITIONS).decrementAndGet();
    deletedParts.inc();
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    Metrics.getOrCreateGauge(MetricsConstants.TOTAL_PARTITIONS).incrementAndGet();
    createdParts.inc();
  }

  @Override
  public void onAllocWriteId(AllocWriteIdEvent allocWriteIdEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
    if (MetastoreConf.getBoolVar(getConf(), MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON) && isNoAutoCompactSet(allocWriteIdEvent)) {
      int numOfWritesToDisabledCompactionTable = Metrics.getOrCreateGauge(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE).incrementAndGet();
      if (numOfWritesToDisabledCompactionTable >= MetastoreConf.getIntVar(getConf(), MetastoreConf.ConfVars.COMPACTOR_NUMBER_OF_DISABLED_COMPACTION_TABLES_THRESHOLD)) {
        LOGGER.warn("There has been a write to table " + allocWriteIdEvent.getDbName() + "." + allocWriteIdEvent.getTableName() + " where auto-compaction is disabled \"no_auto_compact\"=\"true\".");
      }
    }
  }

  private Boolean isNoAutoCompactSet(AllocWriteIdEvent allocWriteIdEvent) throws MetaException {
    String catalog = MetaStoreUtils.getDefaultCatalog(getConf());
    String dbName = allocWriteIdEvent.getDbName();
    String tableName = allocWriteIdEvent.getTableName();

    RawStore rawStore;
    if (allocWriteIdEvent.getIHMSHandler() != null) {
      rawStore = allocWriteIdEvent.getIHMSHandler().getMS();
    } else {
      rawStore = HMSHandler.getMSForConf(getConf());
    }
    Map<String, String> dbParameters;
    try {
      dbParameters = rawStore.getDatabase(catalog, dbName).getParameters();
    } catch (NoSuchObjectException e) {
      LOGGER.error("Unable to find database " + dbName + ", " + e.getMessage());
      throw new MetaException(String.valueOf(e));
    }
    Table table = rawStore.getTable(catalog, dbName, tableName);
    // In the case of CTAS, the table is created after write ids are allocated, so we'll skip metrics collection.
    if (table != null) {
      return MetaStoreUtils.isNoAutoCompactSet(dbParameters, table.getParameters());
    }
    return false;
  }
}
