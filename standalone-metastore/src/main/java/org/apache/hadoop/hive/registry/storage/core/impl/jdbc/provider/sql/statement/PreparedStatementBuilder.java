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

package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.statement;

import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.storage.core.exception.MalformedQueryException;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.config.ExecutionConfig;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.AbstractStorableKeyQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.AbstractStorableSqlQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.AbstractStorableUpdateQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.SqlQuery;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Prepares a {@link PreparedStatement} from a {@link SqlQuery} object. The parameters are replaced
 * with calls to method {@code getPreparedStatement}, which returns the {@link PreparedStatement} ready to be executed
 *
 * @see #getPreparedStatement(SqlQuery)
 */
public class PreparedStatementBuilder {
    private static final Logger log = LoggerFactory.getLogger(PreparedStatementBuilder.class);
    private final Connection connection;
    private PreparedStatement preparedStatement;
    private final SqlQuery sqlBuilder;
    private final StorageDataTypeContext storageDataTypeContext;
    private final ExecutionConfig config;
    private int numPrepStmtParams;                          // Number of prepared statement parameters

    /**
     * Creates a {@link PreparedStatement} for which calls to method {@code getPreparedStatement}
     * return the {@link PreparedStatement} ready to be executed
     *
     * @param connection Connection used to prepare the statement
     * @param config Configuration that needs to be passed to the {@link PreparedStatement}
     * @param sqlBuilder Sql builder object for which to build the {@link PreparedStatement}
     * @param returnGeneratedKeys Whether statement has option 'Statement.RETURN_GENERATED_KEYS' or not.
     * @throws SQLException
     */
    protected PreparedStatementBuilder(Connection connection, ExecutionConfig config, StorageDataTypeContext storageDataTypeContext,
                                       SqlQuery sqlBuilder, boolean returnGeneratedKeys) throws SQLException {
        this.connection = connection;
        this.config = config;
        this.sqlBuilder = sqlBuilder;
        this.storageDataTypeContext = storageDataTypeContext;
        setPreparedStatement(returnGeneratedKeys);
        setNumPrepStmtParams();
    }

    /**
     * Creates a {@link PreparedStatement} for which calls to method {@code getPreparedStatement}
     * return the {@link PreparedStatement} ready to be executed
     *
     * @param connection Connection used to prepare the statement
     * @param config Configuration that needs to be passed to the {@link PreparedStatement}
     * @param sqlBuilder Sql builder object for which to build the {@link PreparedStatement}
     * @throws SQLException
     */
    public static PreparedStatementBuilder of(Connection connection, ExecutionConfig config, StorageDataTypeContext storageDataTypeContext,
                                              SqlQuery sqlBuilder) throws SQLException {
        return new PreparedStatementBuilder(connection, config, storageDataTypeContext, sqlBuilder, false);
    }

    /**
     * Creates a {@link PreparedStatement} for which calls to method {@code getPreparedStatement}
     * return the {@link PreparedStatement} ready to be executed.
     * Please note that Statement.RETURN_GENERATED_KEYS is set while initializing {@link PreparedStatement}.
     *
     * @param connection Connection used to prepare the statement
     * @param config Configuration that needs to be passed to the {@link PreparedStatement}
     * @param sqlBuilder Sql builder object for which to build the {@link PreparedStatement}
     * @throws SQLException
     */
    public static PreparedStatementBuilder supportReturnGeneratedKeys(Connection connection, ExecutionConfig config, StorageDataTypeContext storageDataTypeContext,
                                                                      SqlQuery sqlBuilder) throws SQLException {
        return new PreparedStatementBuilder(connection, config, storageDataTypeContext, sqlBuilder, true);
    }

    /** Creates the prepared statement with the parameters in place to be replaced */
    private void setPreparedStatement(boolean returnGeneratedKeys) throws SQLException {
        final String parameterizedSql = sqlBuilder.getParametrizedSql();
        log.debug("Creating prepared statement for parameterized sql [{}]", parameterizedSql);

        final PreparedStatement preparedStatement;
        if (returnGeneratedKeys) {
            preparedStatement = connection.prepareStatement(parameterizedSql, Statement.RETURN_GENERATED_KEYS);
        } else {
            preparedStatement = connection.prepareStatement(parameterizedSql);
        }

        final int queryTimeoutSecs = config.getQueryTimeoutSecs();
        if (queryTimeoutSecs > 0) {
            preparedStatement.setQueryTimeout(queryTimeoutSecs);
        }
        this.preparedStatement = preparedStatement;
    }

    private void setNumPrepStmtParams() {
        Pattern p = Pattern.compile("[?]");
        Matcher m = p.matcher(sqlBuilder.getParametrizedSql());
        int groupCount = 0;
        while (m.find()) {
            groupCount++;
        }
        log.debug("{} ? query parameters found for {} ", groupCount, sqlBuilder.getParametrizedSql());

        assertIsNumColumnsMultipleOfNumParameters(sqlBuilder, groupCount);

        numPrepStmtParams = groupCount;
    }

    // Used to assert that data passed in is valid
    private void assertIsNumColumnsMultipleOfNumParameters(SqlQuery sqlBuilder, int groupCount) {
        final List<Schema.Field> columns = sqlBuilder.getColumns();
        boolean isMultiple;

        if (sqlBuilder instanceof AbstractStorableUpdateQuery) {
            isMultiple = (groupCount % ((AbstractStorableUpdateQuery) sqlBuilder).getBindings().size()) == 0;
        } else if (columns == null || columns.size() == 0) {
            isMultiple = groupCount == 0;
        } else {
            isMultiple = ((groupCount % sqlBuilder.getColumns().size()) == 0);
        }

        if (!isMultiple) {
            throw new MalformedQueryException("Number of columns must be a multiple of the number of query parameters");
        }
    }

    /**
     * Replaces parameters from {@link SqlQuery} and returns a {@code getPreparedStatement} ready to be executed
     *
     * @param sqlBuilder The {@link SqlQuery} for which to get the {@link PreparedStatement}.
     *                   This parameter must be of the same type of the {@link SqlQuery} used to construct this object.
     * @return The prepared statement with the parameters values set and ready to be executed
     * */
    public PreparedStatement getPreparedStatement(SqlQuery sqlBuilder) throws SQLException {
        // If more types become available consider subclassing instead of going with this approach, which was chosen here for simplicity
        if (sqlBuilder instanceof AbstractStorableUpdateQuery) {
            setStorableUpdatePreparedStatement((AbstractStorableUpdateQuery)sqlBuilder);
        } else if (sqlBuilder instanceof AbstractStorableKeyQuery) {
            setStorableKeyPreparedStatement(sqlBuilder);
        } else if (sqlBuilder instanceof AbstractStorableSqlQuery) {
            setStorablePreparedStatement(sqlBuilder);
        }
        log.debug("Successfully prepared statement [{}]", preparedStatement);
        return preparedStatement;
    }

    private void setStorableKeyPreparedStatement(SqlQuery sqlBuilder) throws SQLException {
        final List<Schema.Field> columns = sqlBuilder.getColumns();

        if (columns != null) {
            final int len = columns.size();
            Map<Schema.Field, Object> columnsToValues = sqlBuilder.getPrimaryKey().getFieldsToVal();

            for (int j = 0; j < numPrepStmtParams; j++) {
                Schema.Field column = columns.get(j % len);
                Schema.Type javaType = column.getType();
                storageDataTypeContext.setPreparedStatementParams(preparedStatement, javaType, j + 1, columnsToValues.get(column));
            }
        }
    }

    private void setStorableUpdatePreparedStatement(AbstractStorableUpdateQuery updateQuery) throws SQLException {
        List<Pair<Schema.Field, Object>> bindings = updateQuery.getBindings();
        for (int i = 0; i < bindings.size(); i++) {
            Pair<Schema.Field, Object> binding = bindings.get(i);
            Schema.Type javaType = binding.getKey().getType();
            storageDataTypeContext.setPreparedStatementParams(preparedStatement, javaType, i + 1, binding.getValue());
        }
    }

    private void setStorablePreparedStatement(SqlQuery sqlBuilder) throws SQLException {
        final List<Schema.Field> columns = sqlBuilder.getColumns();

        if (columns != null) {
            final int len = columns.size();
            final Map columnsToValues = ((AbstractStorableSqlQuery)sqlBuilder).getStorable().toMap();

            for (int j = 0; j < numPrepStmtParams; j++) {
                Schema.Field column = columns.get(j % len);
                Schema.Type javaType = column.getType();
                String columnName = column.getName();
                storageDataTypeContext.setPreparedStatementParams(preparedStatement, javaType, j + 1, columnsToValues.get(columnName));
            }
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return preparedStatement.getMetaData();
    }

    @Override
    public String toString() {
        return "PreparedStatementBuilder{" +
                "sqlBuilder=" + sqlBuilder +
                ", numPrepStmtParams=" + numPrepStmtParams +
                ", connection=" + connection +
                ", preparedStatement=" + preparedStatement +
                ", config=" + config +
                '}';
    }
}
