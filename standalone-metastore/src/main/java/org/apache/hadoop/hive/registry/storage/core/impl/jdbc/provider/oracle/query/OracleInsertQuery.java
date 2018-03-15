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
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.AbstractStorableSqlQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class OracleInsertQuery extends AbstractStorableSqlQuery {


    public OracleInsertQuery(Storable storable) {
        super(storable);
    }

    protected Collection<String> getColumnNames(Collection<Schema.Field> columns, final String formatter) {
        Collection<String> collection = new ArrayList<>();
        for (Schema.Field field : columns) {
            if (!field.getName().equalsIgnoreCase("id") || getStorableId() != null) {
                String fieldName = formatter == null ? field.getName() : String.format(formatter, field.getName());
                collection.add(fieldName);
            }
        }
        return collection;
    }

    @Override
    protected String createParameterizedSql() {
        Collection<String> columnNames = getColumnNames(columns, "\"%s\"");
        String sql = "INSERT INTO \"" + tableName + "\" ("
                + join(columnNames, ", ")
                + ") VALUES ( " + getBindVariables("?,", columnNames.size()) + ")";
        LOG.debug(sql);
        return sql;
    }

    @Override
    public List<Schema.Field> getColumns() {
        List<Schema.Field> cols = super.getColumns();
        if (getStorableId() == null) {
            return cols.stream()
                    .filter(f -> !f.getName().equalsIgnoreCase("id"))
                    .collect(Collectors.toList());
        }
        return cols;
    }

    private Long getStorableId() {
        try {
            return getStorable().getId();
        } catch (UnsupportedOperationException ex) {
            // ignore
        }
        return null;
    }
}
