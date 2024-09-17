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

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *  Updates table/partition statistics.
 *  Intended to run after a successful compaction.
 */
public final class StatsUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(StatsUpdater.class);
    /**
     * This doesn't throw any exceptions because we don't want the Compaction to appear as failed
     * if stats gathering fails since this prevents Cleaner from doing it's job and if there are
     * multiple failures, auto initiated compactions will stop which leads to problems that are
     * much worse than stale stats.
     *
     * todo: longer term we should write something COMPACTION_QUEUE.CQ_META_INFO.  This is a binary
     * field so need to figure out the msg format and how to surface it in SHOW COMPACTIONS, etc
     *
     * @param ci Information about the compaction being run
     * @param hiveConf The hive configuration object
     * @param userName The user to run the statistic collection with
     * @param compactionQueueName The name of the compaction queue
     */
    public void gatherStats(CompactionInfo ci, HiveConf hiveConf,
                            String userName, String compactionQueueName,
                            IMetaStoreClient msc) {
        try {
            if (msc == null) {
                throw new IllegalArgumentException("Metastore client is missing");
            }

            HiveConf conf = new HiveConf(hiveConf);
            //so that Driver doesn't think it's already in a transaction
            conf.unset(ValidTxnList.VALID_TXNS_KEY);

            //e.g. analyze table page_view partition(dt='10/15/2014',country=’US’)
            // compute statistics for columns viewtime
            StringBuilder sb = new StringBuilder("analyze table ")
                    .append(StatsUtils.getFullyQualifiedTableName(ci.dbname, ci.tableName));
            if (ci.partName != null) {
                sb.append(" partition(");
                Map<String, String> partitionColumnValues = Warehouse.makeEscSpecFromName(ci.partName);
                for (Map.Entry<String, String> ent : partitionColumnValues.entrySet()) {
                    sb.append(ent.getKey()).append("='").append(ent.getValue()).append("',");
                }
                sb.setLength(sb.length() - 1); //remove trailing ,
                sb.append(")");
            }
            sb.append(" compute statistics");
            if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER) && ci.isMajorCompaction()) {
                List<String> columnList = msc.findColumnsWithStats(CompactionInfo.compactionInfoToStruct(ci));
                if (!columnList.isEmpty()) {
                    sb.append(" for columns ").append(String.join(",", columnList));
                }
            } else {
                sb.append(" noscan");
            }
            LOG.info(ci + ": running '" + sb + "'");
            if (compactionQueueName != null && compactionQueueName.length() > 0) {
                conf.set(TezConfiguration.TEZ_QUEUE_NAME, compactionQueueName);
            }
            SessionState sessionState = DriverUtils.setUpSessionState(conf, userName, true);
            DriverUtils.runOnDriver(conf, sessionState, sb.toString());
        } catch (Throwable t) {
            LOG.error(ci + ": gatherStats(" + ci.dbname + "," + ci.tableName + "," + ci.partName +
                    ") failed due to: " + t.getMessage(), t);
        }
    }
}
