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
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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

    // what needs to be included in this run() method
    // find all the tables need to be deleted
    // delete all column stats
    @Override
    public void run() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Auto statistics deletion started. Cleaning up table/partition column statistics over the retention period.");
        }
        if (!MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATISTICS_AUTO_DELETION)) {
            LOG.info("Statistics auto deletion is set to off currently.");
            return;
        }
        if (lock.tryLock()) {
            skippedAttempts = 0;
            String qualifiedTableName = null;
            IMetaStoreClient msc = null;
            try {
                // Get retention period in conf in milliseconds; default is 365 days.
                long retentionMillis = 24 * 60 * 60 * 1000 * MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.STATISTICS_RETENTION_PERIOD);
                long now = System.currentTimeMillis();

                // Get all databases from metastore
                List<String> databases = msc.getAllDatabases();
                for (String dbName : databases) {
                    // Get all tables in the database
                    List<String> tableNames = msc.getAllTables(dbName);
                    for (String tableName : tableNames) {
                        Table table = msc.getTable(dbName, tableName);
                        // Check if the table has the exclude property set
                        Map<String, String> tblParams = table.getParameters();
                        // if this table needs to be excluded from this auto stats deletion, then skip this table
                        if (tblParams != null && tblParams.getOrDefault(STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY, null) != null) {
                            LOG.info("Skipping table {}.{} due to exclude property.", dbName, tableName);
                            continue;
                        }
                        // if this table contains "lastAnalyzed" in table property, we process the auto stats deletion
                        if (tblParams != null && tblParams.containsKey("lastAnalyzed")) {
                            // lastAnalyzed is in unit seconds
                            long lastAnalyzed = 1000 * Long.parseLong(tblParams.get("lastAnalyzed"));
                            if (now - lastAnalyzed < retentionMillis) {
                                LOG.info("Skipping table {}.{} because column stats do not exceed retention period.", dbName, tableName);
                                continue;
                            } else {
                                DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tableName);
                                request.setEngine("hive");
                                boolean isPartitioned = table.getPartitionKeysSize() > 0;
                                // Delete table-level column statistics
                                if (!isPartitioned) {
                                    request.setTableLevel(true);
                                } else {
                                    request.setTableLevel(false);
                                }
                                msc.deleteColumnStatistics(request);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error during statistics auto deletion", e);
            }
        }
    }


    private void setupMsckPathInvalidation() {
        // if invalid partition directory appears, we just skip and move on. We don't want partition management to throw
        // when invalid path is encountered as these are background threads. We just want to skip and move on. Users will
        // have to fix the invalid paths via external means.
        conf.set(MetastoreConf.ConfVars.MSCK_PATH_VALIDATION.getVarname(), "skip");
    }

    private static class MsckThread implements Runnable {
        private MsckInfo msckInfo;
        private Configuration conf;
        private String qualifiedTableName;
        private CountDownLatch countDownLatch;

        MsckThread(MsckInfo msckInfo, Configuration conf, String qualifiedTableName, CountDownLatch countDownLatch) {
            this.msckInfo = msckInfo;
            this.conf = conf;
            this.qualifiedTableName = qualifiedTableName;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                Msck msck = new Msck( true, true);
                msck.init(conf);
                msck.repair(msckInfo);
            } catch (Exception e) {
                LOG.error("Exception while running partition discovery task for table: " + qualifiedTableName, e);
            } finally {
                // there is no recovery from exception, so we always count down and retry in next attempt
                countDownLatch.countDown();
            }
        }
    }

    @VisibleForTesting
    public static int getSkippedAttempts() {
        return skippedAttempts;
    }

    @VisibleForTesting
    public static int getCompletedAttempts() {
        return completedAttempts;
    }
}
