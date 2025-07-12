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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.TimeValidator;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

/**
 * Statistics management task is primarily responsible for auto deletion of table column stats based on a certain frequency
 *
 * If some table column statistics are older than the period time, they should be deleted automatically
 * Statistics Retention - If "partition.retention.period" table property is set with retention interval, when this
 * metastore task runs periodically, it will drop partitions with age (creation time) greater than retention period.
 * Dropping partitions after retention period will also delete the data in that partition.
 *
 */
public class StatisticsManagementTask implements MetastoreTaskThread {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticsManagementTask.class);

    // global
    public static final String STATISTICS_AUTO_DELETION = "statistics.auto.deletion";
    public static final String STATISTICS_RETENTION_PERIOD = "statistics.retention.period";

    // The 2 configs for users to set in the conf
    // this is an optional table property, if this property does not exist for a table, then it is not excluded
    public static final String STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY = "statistics.auto.deletion.exclude";

    private static final Lock lock = new ReentrantLock();

    // these are just for testing
    private static int completedAttempts;
    private static int skippedAttempts;

    private Configuration conf;

    @Override
    public long runFrequency(TimeUnit unit) {
        return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.STATISTICS_MANAGEMENT_TASK_FREQUENCY, unit);
    }

    @Override
    public void setConf(Configuration configuration) {
        // we modify conf in setupConf(), so we make a copy
        conf = new Configuration(configuration);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    // what needs to be included in this run() method:
    // get the "lastAnalyzed" information from TAB_COL_STATS and find all the tables need to be deleted
    // delete all column stats
    @Override
    public void run() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Auto statistics deletion started. Cleaning up table/partition column statistics over the retention period.");
        }
        long retentionMillis = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars. STATISTICS_RETENTION_PERIOD, TimeUnit.MILLISECONDS);
        if (retentionMillis <= 0 || !MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATISTICS_AUTO_DELETION)) {
            LOG.info("Statistics auto deletion is set to off currently.");
            return;
        }
        if (lock.tryLock()) {
            skippedAttempts = 0;
            String qualifiedTableName = null;
            IMetaStoreClient msc = null;
            try {
                // Get retention period in conf in milliseconds; default is 365 days.
                long now = System.currentTimeMillis();
                long lastAnalyzedThreshold = (now - retentionMillis) / 1000;

                // Get all databases from metastore
                List<String> databases = msc.getAllDatabases();
                RawStore ms = HMSHandler.getMSForConf(conf);
                PersistenceManager pm = ((ObjectStore) ms).getPersistenceManager();
                Query q = pm.newQuery("SELECT FROM org.apache.hadoop.hive.metastore.model.MTableColumnStatistics");
                q.setFilter("lastAnalyzed < " + lastAnalyzedThreshold);
                List<MTableColumnStatistics> results = (List<MTableColumnStatistics>) q.execute();

                for (MTableColumnStatistics stat : results) {
                    String dbName = stat.getTable().getDatabase().getName();
                    String tblName = stat.getTable().getTableName();
                    Map<String, String> tblParams = stat.getTable().getParameters();
                    if (tblParams != null && tblParams.getOrDefault(STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY, null) != null) {
                        LOG.info("Skipping table {}.{} due to exclude property.", dbName, tblName);
                        continue;
                    }
                    /**
                    // if this table contains "lastAnalyzed" in table property, we process the auto stats deletion
                    long lastAnalyzed = stat.getLastAnalyzed();
                    // lastAnalyzed is in unit seconds, switch it to milliseconds
                    lastAnalyzed *= 1000;
                    **/

                    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tblName);
                    request.setEngine("hive");
                    boolean isPartitioned = stat.getTable().getPartitionKeys() != null && !stat.getTable().getPartitionKeys().isEmpty();
                    // Delete table-level column statistics
                    if (!isPartitioned) {
                        request.setTableLevel(true);
                    } else {
                        request.setTableLevel(false);
                    }
                    msc.deleteColumnStatistics(request);
                }
            } catch (Exception e) {
                LOG.error("Error during statistics auto deletion", e);
            }
        }
    }

}
