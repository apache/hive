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

package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.oracle.query;

import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.storage.core.Storable;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.oracle.exception.OracleQueryException;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.AbstractStorableUpdateQuery;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OracleUpdateQuery extends AbstractStorableUpdateQuery {

    private Map<Schema.Field,Object> whereClauseColumnToValueMap = new HashMap<>();

    public OracleUpdateQuery(Storable storable) {
        super(storable);
        whereClauseColumnToValueMap = createWhereClauseColumnToValueMap();
    }

    @Override
    protected String createParameterizedSql() {

        List<String> whereClauseColumns = new LinkedList<>();
        for (Map.Entry<Schema.Field, Object> columnKeyValue : whereClauseColumnToValueMap.entrySet()) {
            if (columnKeyValue.getKey().getType() == Schema.Type.STRING) {
                String stringValue = (String) columnKeyValue.getValue();
                if ((stringValue).length() > 4000) {
                    throw new OracleQueryException(String.format("Column \"%s\" of the table \"%s\" is compared against a value \"%s\", " +
                                    "which is greater than 4k characters",
                            columnKeyValue.getKey().getName(), tableName, stringValue));
                } else
                    whereClauseColumns.add(String.format(" to_char(\"%s\") = ?", columnKeyValue.getKey().getName()));
            } else {
                whereClauseColumns.add(String.format(" \"%s\" = ?", columnKeyValue.getKey().getName()));
            }
        }

        String sql = "UPDATE \"" + tableName + "\" SET "
                + join(getColumnNames(columns, "\"%s\" = ?"), ", ")
                + " WHERE " + join(whereClauseColumns, " AND ");
        LOG.debug("Sql '{}'", sql);
        return sql;
    }

    private Map<Schema.Field, Object> createWhereClauseColumnToValueMap() {
        Map<Schema.Field, Object> bindingMap = getBindings().stream().
                collect(Collectors.toMap(keyValuePair -> keyValuePair.getKey(), keyValuePair -> keyValuePair.getValue()));
        return whereFields.stream().collect(Collectors.toMap(f -> f, f -> bindingMap.get(f)));
    }
}
