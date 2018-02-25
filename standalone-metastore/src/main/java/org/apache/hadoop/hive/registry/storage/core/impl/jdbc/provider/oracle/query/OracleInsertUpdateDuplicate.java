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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OracleInsertUpdateDuplicate extends AbstractStorableSqlQuery {

    public OracleInsertUpdateDuplicate(Storable storable) {
        super(storable);
    }

    @Override
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

    /*
        Insert or Update query with an example :-

        MERGE INTO employee M
        USING (SELECT 1 AS id, 'Matt' AS name, '5' AS experience FROM dual) N
          ON (M.id = N.id)
        WHEN MATCHED THEN UPDATE
           SET M.name = N.name ,
           M.experience = N.experience
        WHEN NOT MATCHED THEN
          INSERT( id, name, experience)
          VALUES(N.id, N.name, N.experience);
     */
    @Override
    protected String createParameterizedSql() {
        Collection<String> columnNames = getColumnNames(columns, "\"%s\"");
        List<String> primaryColumnNames = getPrimaryColumns(getStorable(), "%s");
        List<String> nonPrimaryColumnNames = getNonPrimaryColumns(getStorable(), "%s");
        String sql = " MERGE INTO \"" + tableName + "\" t1 USING " +
                "( SELECT " + join(getColumnNames(columns, "? as \"%s\""), ",") +
                " FROM dual ) t2 ON ( " + join(primaryColumnNames.stream().map(pCol ->
                String.format("t1.\"%s\"=t2.\"%s\"", pCol, pCol)).collect(Collectors.toList()), " AND ") + " )" +
                ( nonPrimaryColumnNames.size() == 0 ? "" :
                " WHEN MATCHED THEN UPDATE SET " + join(nonPrimaryColumnNames.stream().map(col ->
                String.format("t1.\"%s\"=t2.\"%s\"", col, col)).collect(Collectors.toList()), " , ") ) +
                " WHEN NOT MATCHED THEN INSERT (" + join(columnNames, ",") + ") VALUES (" +
                getBindVariables("?,", columnNames.size()) + ")";
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

    private List<String> getPrimaryColumns(Storable storable, final String formatter) {
        return storable.getPrimaryKey().getFieldsToVal().keySet().stream().map(
                colField -> String.format(formatter, colField.getName())).collect(Collectors.toList());
    }

    private List<String> getNonPrimaryColumns(Storable storable, final String formatter) {
        Set<String> primaryKeySet = new HashSet();
        primaryKeySet.addAll(getPrimaryColumns(storable, "%s"));
        Collection<String> columnNames = getColumnNames(columns, "%s");
        return columnNames.stream().filter(colField -> !primaryKeySet.contains(colField)).collect(Collectors.toList());
    }
}
