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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

/**
 * Statistics management task is primarily responsible for auto deletion of table column stats based on a certain frequency
 *
 * If some table or partition column statistics are older than the configured retention interval
 * (MetastoreConf.ConfVars.STATISTICS_RETENTION_PERIOD), they are deleted when this metastore task runs periodically.
 */
public class StatisticsManagementTask implements MetastoreTaskThread {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticsManagementTask.class);

    // The 2 configs for users to set in the conf
    // this is an optional table property, if this property does not exist for a table, then it is not excluded
    public static final String STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY = "statistics.auto.deletion.exclude";

    private static final Lock lock = new ReentrantLock();

    private Configuration conf;

    @Override
    public long runFrequency(TimeUnit unit) {
        return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.STATISTICS_MANAGEMENT_TASK_FREQUENCY, unit);
    }

    @Override
    public void setConf(Configuration configuration) {
        // we modify conf in setupConf(), so we make a copy
        this.conf = configuration;
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
        LOG.debug("Auto statistics deletion started. Cleaning up table/partition column statistics over the retention period.");
        long retentionMillis = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars. STATISTICS_RETENTION_PERIOD, TimeUnit.MILLISECONDS);
        if (retentionMillis <= 0 || !MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATISTICS_AUTO_DELETION)) {
            LOG.info("Statistics auto deletion is set to off currently.");
            return;
        }
        if (!lock.tryLock()) {
            return;
        }
        try {
            long now = System.currentTimeMillis();
            long lastAnalyzedThreshold = (now - retentionMillis) / 1000;

            String filter = "lastAnalyzed < threshold";
            String paramStr = "long threshold";
            try (IMetaStoreClient msc = new HiveMetaStoreClient(conf)) {
                RawStore ms = HMSHandler.getMSForConf(conf);
                PersistenceManager pm = ((ObjectStore) ms).getPersistenceManager();

                Query q = null;
                try {
                    q = pm.newQuery(MTableColumnStatistics.class);
                    q.setFilter(filter);
                    q.declareParameters(paramStr);
                    // only fetch required fields, avoid loading heavy MTable objects
                    q.setResult(
                            "table.database.name, " +
                                    "table.tableName, " +
                                    "partitionName, " +
                                    "table.parameters.get(\"" + STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY + "\")"
                    );
                    @SuppressWarnings("unchecked")
                    List<Object[]> rows = (List<Object[]>) q.execute(lastAnalyzedThreshold);

                    for (Object[] row : rows) {
                        String dbName = (String) row[0];
                        String tblName = (String) row[1];
                        String partName = (String) row[2];     // can be null for table-level stats
                        String excludeVal = (String) row[3];   // can be null

                        // exclude check uses projected param value
                        if (excludeVal != null) {
                            LOG.info("Skipping auto deletion of stats for table {}.{} due to STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY property being set on the table.", dbName, tblName);
                            continue;
                        }
                        DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tblName);
                        request.setEngine("hive");

                        // decide tableLevel based on whether this stat row is table-level or partition-level
                        // avoids loading table partition keys / MTable
                        request.setTableLevel(partName == null);
                        msc.deleteColumnStatistics(request);
                    }
                } finally {
                    if (q != null) {
                        q.closeAll();
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error during statistics auto deletion", e);
        } finally {
            lock.unlock();
        }
    }

}
