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
import org.apache.hadoop.hive.registry.storage.core.OrderByField;
import org.apache.hadoop.hive.registry.storage.core.StorableKey;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.oracle.exception.OracleQueryException;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.AbstractSelectQuery;
import org.apache.hadoop.hive.registry.storage.core.search.SearchQuery;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OracleSelectQuery extends AbstractSelectQuery {

    public OracleSelectQuery(String nameSpace) {
        super(nameSpace);
    }

    public OracleSelectQuery(StorableKey storableKey) {
        super(storableKey);
    }

    public OracleSelectQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace, orderByFields);
    }

    public OracleSelectQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey, orderByFields);
    }

    public OracleSelectQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery, schema);
    }

    @Override
    protected String getParameterizedSql() {
        String sql = "SELECT * FROM \"" + tableName + "\"";
        if (columns != null) {
            List<String> whereClauseColumns = new LinkedList<>();
            for (Map.Entry<Schema.Field, Object> columnKeyValue : primaryKey.getFieldsToVal().entrySet()) {
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
            sql += " WHERE " + join(whereClauseColumns, " AND ");
        }
        LOG.debug(sql);
        return sql;
    }

    @Override
    protected String orderBySql() {
        String sql = "";
        if (orderByFields != null && !orderByFields.isEmpty()) {
            sql += join(orderByFields.stream()
                    .map(x -> " ORDER BY \"" + x.getFieldName() + "\" " + (x.isDescending() ? "DESC" : "ASC"))
                    .collect(Collectors.toList()), ",");
        }
        return sql;
    }

    @Override
    protected String fieldEncloser() {
        return "\"";
    }
}
