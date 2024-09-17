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
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple factory class, which returns an instance of {@link QueryCompactor}.
 */
public final class CompactorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CompactorFactory.class.getName());
  private static final String ICEBERG_MAJOR_QUERY_COMPACTOR_CLASS = "org.apache.iceberg.mr.hive.compaction.IcebergMajorQueryCompactor";

  private static final CompactorFactory INSTANCE = new CompactorFactory();

  static CompactorFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Factory class, no need to expose constructor.
   */
  private CompactorFactory() {
  }

  /**
   * Get an instance of {@link Compactor}. At the moment the following implementors can be fetched:
   * <p>
   * {@link MajorQueryCompactor} - handles query based major compaction
   * <br>
   * {@link MinorQueryCompactor} - handles query based minor compaction
   * <br>
   * {@link MmMajorQueryCompactor} - handles query based major compaction for micro-managed tables
   * <br>
   * {@link MmMinorQueryCompactor} - handles query based minor compaction for micro-managed tables
   * <br>
   * {@link MRCompactor} - handles MR based minor, major, or rebalance compaction
   * <br>
   * {@link RebalanceQueryCompactor} - handles query based rebalance compaction
   * <br>
   * </p>
   * @param msc The {@link IMetaStoreClient} instance is used only by the {@link MRCompactor}.
   * @param table the table, on which the compaction should be running, must be not null.
   * @param configuration the hive configuration, must be not null.
   * @param compactionInfo provides insight about the type of compaction, must be not null.
   * @return {@link QueryCompactor} or null.
   */
  public CompactorPipeline getCompactorPipeline(Table table, HiveConf configuration, CompactionInfo compactionInfo, IMetaStoreClient msc)
      throws HiveException {
    if (AcidUtils.isFullAcidTable(table.getParameters())) {
      if (!"tez".equalsIgnoreCase(HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE)) ||
          !HiveConf.getBoolVar(configuration, HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED)) {
        if (CompactionType.REBALANCE.equals(compactionInfo.type)) {
          throw new HiveException("Rebalancing compaction is only supported in Tez, and via Query based compaction. " +
              "Set hive.execution.engine=tez and hive.compactor.crud.query.based=true to enable it.");
        }
        if (!"tez".equalsIgnoreCase(HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE)) &&
            HiveConf.getBoolVar(configuration, HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED)) {
          LOG.warn("Query-based compaction is enabled, but it is only supported on tez. Falling back to MR compaction.");
        }
        return new CompactorPipeline(new MRCompactor(msc));
      }
      switch (compactionInfo.type) {
        case MINOR:
          return new CompactorPipeline(new MergeCompactor())
                  .addCompactor(new MinorQueryCompactor());
        case MAJOR:
          return new CompactorPipeline(new MergeCompactor())
                  .addCompactor(new MajorQueryCompactor());
        case REBALANCE:
          return new CompactorPipeline(new RebalanceQueryCompactor());
      }
    } else if (AcidUtils.isInsertOnlyTable(table.getParameters())) {
      if (!configuration.getBoolVar(HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM)) {
        throw new HiveException(
            "Insert only compaction is disabled. Set hive.compactor.compact.insert.only=true to enable it.");
      }
      switch (compactionInfo.type) {
        case MINOR:
          return new CompactorPipeline(new MergeCompactor())
                  .addCompactor(new MmMinorQueryCompactor());
        case MAJOR:
          return new CompactorPipeline(new MergeCompactor())
                  .addCompactor(new MmMajorQueryCompactor());
        default:
          throw new HiveException(
              compactionInfo.type.name() + " compaction is not supported on insert only tables.");
      }
    } else if (MetaStoreUtils.isIcebergTable(table.getParameters())) {
      switch (compactionInfo.type) {
        case MAJOR:

          try {
            Class<? extends QueryCompactor> icebergMajorQueryCompactor = (Class<? extends QueryCompactor>)
                Class.forName(ICEBERG_MAJOR_QUERY_COMPACTOR_CLASS, true, 
                    Utilities.getSessionSpecifiedClassLoader());

            return new CompactorPipeline(icebergMajorQueryCompactor.newInstance());
          }
          catch (Exception e) {
            throw new HiveException("Failed instantiating and calling Iceberg compactor");
          }
        default:
          throw new HiveException(
                  compactionInfo.type.name() + " compaction is not supported on Iceberg tables.");
      }
    }
    throw new HiveException("Only transactional tables can be compacted, " + table.getTableName() + "is not suitable " +
        "for compaction!");
  }

}
