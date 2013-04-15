/**
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
package org.apache.hadoop.hive.ql.profiler;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.OperatorHookContext;

public class HiveProfilerUtils {
  public static void createTableIfNonExistent(HiveProfilerConnectionInfo info,
      String createTable) throws Exception {
    Connection conn = info.getConnection();
    Statement stmt = conn.createStatement();
    stmt.setQueryTimeout(info.getTimeout());
    DatabaseMetaData dbm = conn.getMetaData();
    ResultSet rs = dbm.getTables(null, null, info.getTableName(), null);
    boolean tblExists = rs.next();
    if(!tblExists) {
      stmt.executeUpdate(createTable);
      stmt.close();
    }
  }

  public static boolean closeConnection(HiveProfilerConnectionInfo info) throws SQLException{
    info.getConnection().close();
    // In case of derby, explicitly shutdown the database otherwise it reports error when
    // trying to connect to the same JDBC connection string again.
    if (info.getDbClass().equalsIgnoreCase("jdbc:derby")) {
      try {
        // The following closes the derby connection. It throws an exception that has to be caught
        // and ignored.
        DriverManager.getConnection(info.getConnectionString() + ";shutdown=true");
      } catch (Exception e) {
        // Do nothing because we know that an exception is thrown anyway.
      }
    }
    return true;
  }

  public static String getLevelAnnotatedName(OperatorHookContext opHookContext) {
    Operator parent = opHookContext.getParentOperator();
    if (parent != null && parent instanceof MapOperator) {
      parent = null;
    }
    Operator op = opHookContext.getOperator();
    String parentOpName = parent == null ? "" : parent.getName();
    String parentOpId = parent == null ? "main()" : parent.getOperatorId();
    String levelAnnoName = parentOpId + " ==> " + op.getOperatorId();
    return levelAnnoName;
  }
}
