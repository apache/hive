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
package org.apache.hadoop.hive.metastore.metastore.iface;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.metastore.MetaDescriptor;
import org.apache.hadoop.hive.metastore.metastore.impl.ColStatsStoreImpl;
import org.apache.hadoop.hive.metastore.model.MTable;

@MetaDescriptor(alias = "stats", defaultImpl = ColStatsStoreImpl.class)
public interface ColStatsStore {
  /** Persists the given column statistics object to the metastore
   * @param colStats object to persist
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException No such table.
   * @throws MetaException error accessing the RDBMS.
   * @throws InvalidObjectException the stats object is invalid
   * @throws InvalidInputException unable to record the stats for the table
   */
  Map<String, String> updateTableColumnStatistics(ColumnStatistics colStats, String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  Map<String, String> updatePartitionColumnStatistics(Table table, MTable mTable,
      ColumnStatistics statsObj, List<String> partVals,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;
}
