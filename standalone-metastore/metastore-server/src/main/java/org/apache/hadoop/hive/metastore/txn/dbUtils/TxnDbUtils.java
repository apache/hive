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
package org.apache.hadoop.hive.metastore.txn.dbUtils;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class TxnDbUtils {

    QueryRunner qr = new QueryRunner();
    public TxnDbUtils() {
    }

    public <T> T executeQuery(Connection dbConn, String query, ResultSetHandler<?> resultSetHandler,
                              List<?> params) throws SQLException {

        return params.isEmpty() ? (T) qr.query(dbConn, query, resultSetHandler) :
                (T) qr.query(dbConn, query, resultSetHandler, params.toArray());
    }

    public <T> T insertQuery(Connection dbConn, String query, ResultSetHandler<?> resultSetHandler
            , Object[] params) throws SQLException {

        if (params != null)
            return (T) qr.insert(dbConn, query, resultSetHandler, params);
        else
            return (T) qr.insert(dbConn, query, resultSetHandler);

    }

    public Object updateQuery(Connection dbConn, String query,
                              Object[] params) throws SQLException {

        return params != null || params.length ==0 ? qr.update(dbConn, query, params) : qr.update(dbConn, query);
    }

   /* public <T> List<T> queryColumn(Connection dbConn, String sql, String columnName, Object[] params) throws SQLException {
        List<T> returnList = null;
        ColumnListHandler<T> resultSetHandler = new ColumnListHandler<T>(columnName);
        returnList = new QueryRunner().query(dbConn, sql, resultSetHandler, params);
        return returnList;
    }*/
}
