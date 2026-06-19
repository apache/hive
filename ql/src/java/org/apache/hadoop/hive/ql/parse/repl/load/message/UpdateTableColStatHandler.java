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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.messaging.UpdateTableColumnStatMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.Serializable;
import java.util.List;

/**
 * UpdateTableColStatHandler
 * Target(Load) side handler for table stat update event
 */
public class UpdateTableColStatHandler extends AbstractMessageHandler {
    @Override
    public List<Task<?>> handle(Context context)
            throws SemanticException {
        UpdateTableColumnStatMessage utcsm =
                deserializer.getUpdateTableColumnStatMessage(context.dmd.getPayload());

        // Update tablename and database name in the statistics object
        ColumnStatistics colStats = utcsm.getColumnStatistics();
        // In older version of hive, engine might not have set.
        if (colStats.getEngine() == null) {
            colStats.setEngine(org.apache.hadoop.hive.conf.Constants.HIVE_ENGINE);
        }
        ColumnStatisticsDesc colStatsDesc = colStats.getStatsDesc();
        colStatsDesc.setDbName(context.dbName);
        if (!context.isDbNameEmpty()) {
            updatedMetadata.set(context.dmd.getEventTo().toString(), context.dbName,
                    colStatsDesc.getTableName(), null);
        }

        try {
            return ReplUtils.addTasksForLoadingColStats(colStats, context.hiveConf, updatedMetadata,
                    utcsm.getTableObject(), utcsm.getWriteId(), context.getDumpDirectory(),
                    context.getMetricCollector());
        } catch(Exception e) {
            throw new SemanticException(e);
        }
    }
}
