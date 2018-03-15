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
package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.mysql.query;

import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.storage.core.OrderByField;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.AbstractSelectQuery;
import org.apache.hadoop.hive.registry.storage.core.search.SearchQuery;
import org.apache.hadoop.hive.registry.storage.core.StorableKey;

import java.util.List;
import java.util.stream.Collectors;

public class MySqlSelectQuery extends AbstractSelectQuery {

    public MySqlSelectQuery(String nameSpace) {
        super(nameSpace);
    }

    public MySqlSelectQuery(StorableKey storableKey) {
        super(storableKey);
    }

    public MySqlSelectQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace, orderByFields);
    }

    public MySqlSelectQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey, orderByFields);
    }

    public MySqlSelectQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery, schema);
    }

    @Override
    protected String fieldEncloser() {
        return "`";
    }

    @Override
    protected String getParameterizedSql() {
        String sql = "SELECT * FROM " + tableName;
        //where clause is defined by columns specified in the PrimaryKey
        if (columns != null) {
            sql += " WHERE " + join(getColumnNames(columns, "`%s` = ?"), " AND ");
        }
        LOG.debug(sql);
        return sql;
    }

    @Override
    protected String orderBySql() {
        String sql = "";
        if (orderByFields != null && !orderByFields.isEmpty()) {
            sql += join(orderByFields.stream()
                    .map(x -> " ORDER BY `" + x.getFieldName() + "` " + (x.isDescending() ? "DESC" : "ASC"))
                    .collect(Collectors.toList()), ",");
        }
        return sql;
    }
}
