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

package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

public class OracleConnectorProvider extends AbstractJDBCConnectorProvider {
    private static Logger LOG = LoggerFactory.getLogger(OracleConnectorProvider.class);
    private static final String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver".intern();

    public OracleConnectorProvider(String dbName, DataConnector dataConn) {
        super(dbName, dataConn, DRIVER_CLASS);
        driverClassName = DRIVER_CLASS;
    }

    @Override protected ResultSet fetchTableMetadata(String tableName) throws MetaException {
        ResultSet rs = null;
        try {
            rs = getConnection().getMetaData().getColumns(null, scoped_db, tableName, null);
        } catch (Exception ex) {
            LOG.warn("Could not retrieve table names from remote datasource, cause:" + ex.getMessage());
            throw new MetaException("Could not retrieve table names from remote datasource, cause:" + ex);
        }
        return rs;
    }

    @Override protected ResultSet fetchTableNames() throws MetaException {
        ResultSet rs = null;
        try {
            rs = getConnection().getMetaData().getTables(null, scoped_db, null, new String[] { "TABLE" });
        } catch (SQLException sqle) {
            LOG.warn("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
            throw new MetaException("Could not retrieve table names from remote datasource, cause:" + sqle);
        }
        return rs;
    }

    @Override protected String getCatalogName() {
        return null;
    }

    @Override protected String getDatabaseName() {
        return scoped_db;
    }

    protected String getDataType(String dbDataType, int size) {
        String mappedType = super.getDataType(dbDataType, size);
        if (!mappedType.equalsIgnoreCase(ColumnType.VOID_TYPE_NAME)) {
            return mappedType;
        }

        // map any db specific types here.
        //TODO: Large Objects (LOB), Interval data types of oracle needs to be supported.
        switch (dbDataType.toLowerCase())
        {
            case "varchar2":
            case "nchar":
            case "nvarchar2":
                mappedType = ColumnType.VARCHAR_TYPE_NAME + wrapSize(size);
                break;
            case "raw":
            case "long raw":
                mappedType = ColumnType.STRING_TYPE_NAME;
                break;
            case "number":
                mappedType =  ColumnType.INT_TYPE_NAME;
                break;
            default:
                mappedType = ColumnType.VOID_TYPE_NAME;
        }
        return mappedType;
    }
}
