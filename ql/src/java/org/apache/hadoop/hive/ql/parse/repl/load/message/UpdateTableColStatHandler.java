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
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * UpdateTableColStatHandler
 * Target(Load) side handler for table stat update event
 */
public class UpdateTableColStatHandler extends AbstractMessageHandler {
    @Override
    public List<Task<? extends Serializable>> handle(Context context)
            throws SemanticException {
        UpdateTableColumnStatMessage utcsm =
                deserializer.getUpdateTableColumnStatMessage(context.dmd.getPayload());

        // Update tablename and database name in the statistics object
        ColumnStatistics colStats = utcsm.getColumnStatistics();
        ColumnStatisticsDesc colStatsDesc = colStats.getStatsDesc();
        colStatsDesc.setDbName(context.dbName);
        if (!context.isTableNameEmpty()) {
          colStatsDesc.setTableName(context.tableName);
        }
        if (!context.isDbNameEmpty()) {
            updatedMetadata.set(context.dmd.getEventTo().toString(), context.dbName,
                    context.tableName, null);
        }

      // TODO: For txn stats update, ColumnStatsUpdateTask.execute()->Hive
      // .setPartitionColumnStatistics expects a valid writeId allocated by the current txn and
      // also, there should be a table snapshot. But, it won't be there as update from
      // ReplLoadTask which doesn't have a write id allocated. Need to check this further.
        return Collections.singletonList(TaskFactory.get(new ColumnStatsUpdateWork(colStats),
                context.hiveConf));
    }
}
