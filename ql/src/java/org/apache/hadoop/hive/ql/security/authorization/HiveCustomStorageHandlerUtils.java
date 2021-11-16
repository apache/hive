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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.common.StatsSetupConst;

public class HiveCustomStorageHandlerUtils {

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
}
