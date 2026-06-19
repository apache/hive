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

package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsMetricType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData;

public class CompactionMetricsDataConverter {

  public static CompactionMetricsDataStruct dataToStruct(CompactionMetricsData data) throws MetaException {
    CompactionMetricsDataStruct struct = new CompactionMetricsDataStruct();
    struct.setDbname(data.getDbName());
    struct.setTblname(data.getTblName());
    struct.setPartitionname(data.getPartitionName());
    struct.setType(dbCompactionMetricType2ThriftType(data.getMetricType()));
    struct.setMetricvalue(data.getMetricValue());
    struct.setVersion(data.getVersion());
    struct.setThreshold(data.getThreshold());
    return struct;
  }

  public static CompactionMetricsData structToData(CompactionMetricsDataStruct struct) throws MetaException {
    return new CompactionMetricsData.Builder()
        .dbName(struct.getDbname())
        .tblName(struct.getTblname())
        .partitionName(struct.getPartitionname())
        .metricType(thriftCompactionMetricType2DbType(struct.getType()))
        .metricValue(struct.getMetricvalue())
        .version(struct.getVersion())
        .threshold(struct.getThreshold())
        .build();
  }

  public static CompactionMetricsMetricType dbCompactionMetricType2ThriftType(CompactionMetricsData.MetricType type)
      throws MetaException {
    switch (type) {
    case NUM_DELTAS:
      return CompactionMetricsMetricType.NUM_DELTAS;
    case NUM_SMALL_DELTAS:
      return CompactionMetricsMetricType.NUM_SMALL_DELTAS;
    case NUM_OBSOLETE_DELTAS:
      return CompactionMetricsMetricType.NUM_OBSOLETE_DELTAS;
    default:
      throw new MetaException("Unexpected metric type " + type);
    }
  }

  public static CompactionMetricsData.MetricType thriftCompactionMetricType2DbType(CompactionMetricsMetricType type)
      throws MetaException {
    switch (type) {
    case NUM_DELTAS:
      return CompactionMetricsData.MetricType.NUM_DELTAS;
    case NUM_SMALL_DELTAS:
      return CompactionMetricsData.MetricType.NUM_SMALL_DELTAS;
    case NUM_OBSOLETE_DELTAS:
      return CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS;
    default:
      throw new MetaException("Unexpected metric type " + type);
    }
  }

}
