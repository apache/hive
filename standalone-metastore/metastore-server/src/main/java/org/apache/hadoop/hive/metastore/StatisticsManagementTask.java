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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

/**
 * Statistics management task responsible for periodic auto-deletion of table and partition column
 * statistics based on a configured retention interval.
 *
 * <p>When {@code metastore.statistics.auto.deletion} is enabled, this task scans
 * {@code TAB_COL_STATS} and {@code PART_COL_STATS} for rows whose {@code lastAnalyzed} timestamp
 * is older than {@code metastore.statistics.retention.period}, and deletes them.
 * Individual tables may opt out by setting the table property
 * {@value #STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY} to any non-null value.
 */
public class StatisticsManagementTask extends ObjectStore implements MetastoreTaskThread {

    private static final Logger LOG = LoggerFactory.getLogger(StatisticsManagementTask.class);

    /**
     * Table property key that, when present on a table, excludes it from automatic statistics
     * deletion regardless of the global retention setting.
     */
    public static final String STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY =
            "statistics.auto.deletion.exclude";

    private static final Lock LOCK = new ReentrantLock();

    private Configuration conf;

    @Override
    public long runFrequency(TimeUnit unit) {
        return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COLUMN_STATISTICS_MANAGEMENT_TASK_FREQUENCY, unit);
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = new Configuration(configuration);
        super.setConf(configuration);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void run() {
        LOG.debug("Auto statistics deletion started. Cleaning up table/partition column statistics over the retention period.");
        long retentionMillis =
                MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COLUMN_STATISTICS_RETENTION_PERIOD, TimeUnit.MILLISECONDS);
        if (retentionMillis <= 0 || !MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COLUMN_STATISTICS_AUTO_DELETION)) {
            LOG.info("Statistics auto deletion is set to off currently.");
            return;
        }
        if (!LOCK.tryLock()) {
            return;
        }
        try {
            long now = System.currentTimeMillis();
            long lastAnalyzedThreshold = (now - retentionMillis) / 1000;
            PersistenceManager pm = getPersistenceManager();
            boolean committed = false;
            openTransaction();
            try {
                try (IMetaStoreClient msc = new HiveMetaStoreClient(conf)) {
                    deleteExpiredTableColStats(pm, msc, lastAnalyzedThreshold);
                    deleteExpiredPartitionColStats(pm, msc, lastAnalyzedThreshold);
                }
                committed = commitTransaction();
            } finally {
                if (!committed) {
                    rollbackTransaction();
                }
            }
        } catch (Exception e) {
            LOG.error("Error during statistics auto deletion", e);
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * Deletes expired table-level column statistics from {@code TAB_COL_STATS}.
     * Tables with the {@value #STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY} property set are skipped.
     *
     * @param pm                   the JDO persistence manager to use for the query
     * @param msc                  the metastore client used to issue delete requests
     * @param lastAnalyzedThreshold epoch seconds; rows with lastAnalyzed below this value are expired
     * @throws Exception if the JDO query or the delete request fails
     */
    private void deleteExpiredTableColStats(PersistenceManager pm, IMetaStoreClient msc,
                                            long lastAnalyzedThreshold) throws Exception {
        Query tblQuery = null;
        try {
            tblQuery = pm.newQuery(MTableColumnStatistics.class);
            tblQuery.setFilter("lastAnalyzed < threshold");
            tblQuery.declareParameters("long threshold");
            // partitionName does not exist on MTableColumnStatistics; omitted here
            tblQuery.setResult(
                    "table.database.name, "
                            + "table.tableName, "
                            + "table.parameters.get(\"" + STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY + "\")");
            @SuppressWarnings("unchecked")
            List<Object[]> tblRows = (List<Object[]>) tblQuery.execute(lastAnalyzedThreshold);
            for (Object[] row : tblRows) {
                String dbName = (String) row[0];
                String tblName = (String) row[1];
                String excludeVal = (String) row[2];
                if (excludeVal != null) {
                    LOG.info("Skipping auto deletion of table stats for {}.{} due to exclude property.",
                            dbName, tblName);
                    continue;
                }
                DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tblName);
                request.setEngine("hive");
                request.setTableLevel(true);
                msc.deleteColumnStatistics(request);
            }
        } finally {
            if (tblQuery != null) {
                tblQuery.closeAll();
            }
        }
    }

    /**
     * Deletes expired partition-level column statistics from {@code PART_COL_STATS}.
     * Tables with the {@value #STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY} property set are skipped.
     *
     * @param pm                   the JDO persistence manager to use for the query
     * @param msc                  the metastore client used to issue delete requests
     * @param lastAnalyzedThreshold epoch seconds; rows with lastAnalyzed below this value are expired
     * @throws Exception if the JDO query or the delete request fails
     */
    private void deleteExpiredPartitionColStats(PersistenceManager pm, IMetaStoreClient msc,
                                                long lastAnalyzedThreshold) throws Exception {
        Query partQuery = null;
        try {
            partQuery = pm.newQuery(MPartitionColumnStatistics.class);
            partQuery.setFilter("lastAnalyzed < threshold");
            partQuery.declareParameters("long threshold");
            // project via partition navigation to reach partitionName and the table exclude property
            partQuery.setResult(
                    "partition.table.database.name, "
                            + "partition.table.tableName, "
                            + "partition.partitionName, "
                            + "partition.table.parameters.get(\"" + STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY + "\")");
            @SuppressWarnings("unchecked")
            List<Object[]> partRows = (List<Object[]>) partQuery.execute(lastAnalyzedThreshold);
            for (Object[] row : partRows) {
                String dbName = (String) row[0];
                String tblName = (String) row[1];
                String partName = (String) row[2];
                String excludeVal = (String) row[3];
                if (excludeVal != null) {
                    LOG.info("Skipping auto deletion of partition stats for {}.{} due to exclude property.",
                            dbName, tblName);
                    continue;
                }
                DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tblName);
                request.setEngine("hive");
                request.setTableLevel(false);
                request.addToPart_names(partName);
                msc.deleteColumnStatistics(request);
            }
        } finally {
            if (partQuery != null) {
                partQuery.closeAll();
            }
        }
    }
}