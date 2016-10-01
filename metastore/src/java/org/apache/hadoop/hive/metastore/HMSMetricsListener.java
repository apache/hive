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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Report metrics of metadata added, deleted by this Hive Metastore.
 */
public class HMSMetricsListener extends MetaStoreEventListener {

  public static final Logger LOGGER = LoggerFactory.getLogger(HMSMetricsListener.class);
  private Metrics metrics;

  public HMSMetricsListener(Configuration config, Metrics metrics) {
    super(config);
    this.metrics = metrics;
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
    incrementCounterInternal(MetricsConstant.CREATE_TOTAL_DATABASES);
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    incrementCounterInternal(MetricsConstant.DELETE_TOTAL_DATABASES);
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    incrementCounterInternal(MetricsConstant.CREATE_TOTAL_TABLES);
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    incrementCounterInternal(MetricsConstant.DELETE_TOTAL_TABLES);
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    incrementCounterInternal(MetricsConstant.DELETE_TOTAL_PARTITIONS);
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    incrementCounterInternal(MetricsConstant.CREATE_TOTAL_PARTITIONS);
  }

  private void incrementCounterInternal(String name) {
    if (metrics != null) {
      metrics.incrementCounter(name);
    }
  }
}
