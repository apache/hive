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
package org.apache.hadoop.hive.ql.security.authorization;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.common.StatsSetupConst;

public class HiveCustomStorageHandlerUtils {

    public static final String WRITE_OPERATION_CONFIG_PREFIX = "file.sink.write.operation.";

    public static final String WRITE_OPERATION_IS_SORTED = "file.sink.write.operation.sorted.";

    public static final String MERGE_TASK_ENABLED = "file.sink.merge.task.enabled.";

    public static String getTablePropsForCustomStorageHandler(Map<String, String> tableProperties) {
        StringBuilder properties = new StringBuilder();
        for (Map.Entry<String,String> serdeMap : tableProperties.entrySet()) {
            if (!serdeMap.getKey().equalsIgnoreCase(serdeConstants.SERIALIZATION_FORMAT) &&
                    !serdeMap.getKey().equalsIgnoreCase(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
                properties.append(serdeMap.getValue().replaceAll("\\s","").replaceAll("\"","")); //replace space and double quotes if any in the values to avoid URI syntax exception
                properties.append("/");
            }
        }
        return properties.toString();
    }

    /**
     * @param table the HMS table
     * @return a map of table properties combined with the serde properties, if any
     */
    public static Map<String, String> getTableProperties(Table table) {
        Map<String, String> tblProps = new HashMap<>(table.getParameters());
        Optional.ofNullable(table.getSd().getSerdeInfo().getParameters())
            .ifPresent(tblProps::putAll);
        return tblProps;
    }

    public static Context.Operation getWriteOperation(UnaryOperator<String> ops, String tableName) {
        String operation = ops.apply(WRITE_OPERATION_CONFIG_PREFIX + tableName);
        return operation == null ? null : Context.Operation.valueOf(operation);
    }

    public static void setWriteOperation(Configuration conf, String tableName, Context.Operation operation) {
        if (conf == null || tableName == null) {
            return;
        }

        conf.set(WRITE_OPERATION_CONFIG_PREFIX + tableName, operation.name());
    }

    public static void setWriteOperationIsSorted(Configuration conf, String tableName, boolean isSorted) {
        if (conf == null || tableName == null) {
            return;
        }

        conf.set(WRITE_OPERATION_IS_SORTED + tableName, Boolean.toString(isSorted));
    }

    public static boolean getWriteOperationIsSorted(UnaryOperator<String> ops, String tableName) {
        String operation = ops.apply(WRITE_OPERATION_IS_SORTED + tableName);
        return Boolean.parseBoolean(operation);
    }

    public static void setMergeTaskEnabled(Configuration conf, String tableName, boolean isMerge) {
        if (conf == null || tableName == null) {
            return;
        }

        conf.set(MERGE_TASK_ENABLED + tableName, Boolean.toString(isMerge));
    }

    public static boolean isMergeTaskEnabled(UnaryOperator<String> ops, String tableName) {
        String operation = ops.apply(MERGE_TASK_ENABLED + tableName);
        return Boolean.parseBoolean(operation);
    }
}
