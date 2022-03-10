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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple factory class, which returns an instance of {@link QueryCompactor}.
 */
final class QueryCompactorFactory {
  static final private Logger LOG = LoggerFactory.getLogger(QueryCompactorFactory.class.getName());

  /**
   * Factory class, no need to expose constructor.
   */
  private QueryCompactorFactory() {
  }

  /**
   * Get an instance of {@link QueryCompactor}. At the moment the following implementors can be fetched:
   * <p>
   * {@link MajorQueryCompactor} - handles query based major compaction
   * <br>
   * {@link MinorQueryCompactor} - handles query based minor compaction
   * <br>
   * {@link MmMajorQueryCompactor} - handles query based major compaction for micro-managed tables
   * <br>
   * {@link MmMinorQueryCompactor} - handles query based minor compaction for micro-managed tables
   * <br>
   * </p>
   * @param table the table, on which the compaction should be running, must be not null.
   * @param configuration the hive configuration, must be not null.
   * @param compactionInfo provides insight about the type of compaction, must be not null.
   * @return {@link QueryCompactor} or null.
   */
  static QueryCompactor getQueryCompactor(Table table, HiveConf configuration, CompactionInfo compactionInfo)
      throws HiveException {
    if (!AcidUtils.isInsertOnlyTable(table.getParameters())
        && HiveConf.getBoolVar(configuration, HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED)) {
      if (!"tez".equalsIgnoreCase(HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE))) {
        LOG.info("Query-based compaction is only supported on tez. Falling back to MR compaction.");
        return null;
      }
      if (compactionInfo.isMajorCompaction()) {
        return new MajorQueryCompactor();
      } else {
        return new MinorQueryCompactor();
      }
    }
    if (AcidUtils.isInsertOnlyTable(table.getParameters())) {
      if (!configuration.getBoolVar(HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM)) {
        throw new HiveException(
            "Insert only compaction is disabled. Set hive.compactor.compact.insert.only to true to enable it.");
      }
      if (compactionInfo.isMajorCompaction()) {
        return new MmMajorQueryCompactor();
      } else {
        return new MmMinorQueryCompactor();
      }
    }

    return null;
  }

}
