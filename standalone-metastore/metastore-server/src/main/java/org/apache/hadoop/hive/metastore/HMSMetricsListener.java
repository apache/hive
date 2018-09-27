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
import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Report metrics of metadata added, deleted by this Hive Metastore.
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
}
