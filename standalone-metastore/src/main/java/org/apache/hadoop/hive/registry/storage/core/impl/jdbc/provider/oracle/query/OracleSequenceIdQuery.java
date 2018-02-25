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

import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.config.ExecutionConfig;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.oracle.statement.OracleDataTypeContext;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.AbstractSqlQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OracleSequenceIdQuery {
    private static final Logger log = LoggerFactory.getLogger(OracleSequenceIdQuery.class);
    private static final String nextValueFunction = "nextval";
    private final String namespace;
    private final OracleDataTypeContext oracleDatabaseStorageContext;
    private final int queryTimeoutSecs;

    public OracleSequenceIdQuery(String namespace, int queryTimeoutSecs, OracleDataTypeContext oracleDatabaseStorageContext) {
        this.namespace = namespace;
        this.queryTimeoutSecs = queryTimeoutSecs;
        this.oracleDatabaseStorageContext = oracleDatabaseStorageContext;
    }

    public Long getNextID(Connection connection) {

        OracleSqlQuery nextValueQuery = new OracleSqlQuery(String.format("SELECT \"%s\".%s from DUAL", namespace.toUpperCase(), nextValueFunction));
        Long nextId = 0l;

        try {
            ResultSet selectResultSet = PreparedStatementBuilder.of(connection, new ExecutionConfig(queryTimeoutSecs), oracleDatabaseStorageContext, nextValueQuery).getPreparedStatement(nextValueQuery).executeQuery();
            if (selectResultSet.next()) {
                nextId = selectResultSet.getLong(nextValueFunction);
            } else {
                throw new RuntimeException("No sequence-id created for the current sequence of [" + namespace + "]");
            }
            log.debug("Generated sequence id [{}] for [{}]", nextId, namespace);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        return nextId;
    }

    static class OracleSqlQuery extends AbstractSqlQuery {

        private String sql;

        public OracleSqlQuery(String sql) {
            this.sql = sql;
        }

        @Override
        protected String createParameterizedSql() {
            return sql;
        }
    }
}
