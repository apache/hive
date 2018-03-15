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

package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.storage.core.PrimaryKey;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public abstract class AbstractSqlQuery implements SqlQuery {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractSqlQuery.class);
    protected List<Schema.Field> columns;
    protected String tableName;
    protected PrimaryKey primaryKey;
    private String sql;

    /** This method must be overridden and must return parameterized sql */
    protected abstract String createParameterizedSql();

    @Override
    public String getParametrizedSql() {
        if (sql == null) {
            sql = createParameterizedSql();
        }
        return sql;
    }

    @Override
    public List<Schema.Field> getColumns() {
        return columns;
    }

    @Override
    public String getNamespace() {
        return tableName;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    // ==== helper methods used in the query construction process ======

    protected String join(Collection<String> in, String separator) {
        return Joiner.on(separator).join(in);
    }

    /**
     * @param num number of times to repeat the pattern
     * @return bind variables repeated num times
     */
    protected String getBindVariables(String pattern, int num) {
        return StringUtils.chop(StringUtils.repeat(pattern, num));
    }

    /**
     * if formatter != null applies the formatter to the column names. Examples of output are:
     * <br>
     * formatter == null ==&gt; [colName1, colName2]
     * <br>
     * formatter == "%s = ?" ==&gt; [colName1 = ?, colName2 = ?]
     */
    protected Collection<String> getColumnNames(Collection<Schema.Field> columns, final String formatter) {
        return Collections2.transform(columns, new Function<Schema.Field, String>() {
            @Override
            public String apply(Schema.Field field) {
                return formatter == null ? field.getName() : String.format(formatter, field.getName());
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractSqlQuery that = (AbstractSqlQuery) o;

        if (columns != null ? !columns.equals(that.columns) : that.columns != null) return false;
        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;
        return !(primaryKey != null ? !primaryKey.equals(that.primaryKey) : that.primaryKey != null);

    }

    @Override
    public int hashCode() {
        int result = columns != null ? columns.hashCode() : 0;
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        result = 31 * result + (primaryKey != null ? primaryKey.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AbstractSqlQuery{" +
                "columns=" + columns +
                ", tableName='" + tableName + '\'' +
                ", primaryKey=" + primaryKey +
                ", sql='" + sql + '\'' +
                '}';
    }
}
