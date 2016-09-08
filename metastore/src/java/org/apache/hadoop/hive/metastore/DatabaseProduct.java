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

package org.apache.hadoop.hive.metastore;

import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;

/** Database product infered via JDBC. */
public enum DatabaseProduct {
  DERBY, MYSQL, POSTGRES, ORACLE, SQLSERVER, OTHER;

  /**
   * Determine the database product type
   * @param conn database connection
   * @return database product type
   */
  public static DatabaseProduct determineDatabaseProduct(String productName) throws SQLException {
    if (productName == null) {
      return OTHER;
    }
    productName = productName.toLowerCase();
    if (productName.contains("derby")) {
      return DERBY;
    } else if (productName.contains("microsoft sql server")) {
      return SQLSERVER;
    } else if (productName.contains("mysql")) {
      return MYSQL;
    } else if (productName.contains("oracle")) {
      return ORACLE;
    } else if (productName.contains("postgresql")) {
      return POSTGRES;
    } else {
      return OTHER;
    }
  }

  public static boolean isDeadlock(DatabaseProduct dbProduct, SQLException e) {
    return e instanceof SQLTransactionRollbackException
        || ((dbProduct == MYSQL || dbProduct == POSTGRES || dbProduct == SQLSERVER)
            && e.getSQLState().equals("40001"))
        || (dbProduct == POSTGRES && e.getSQLState().equals("40P01"))
        || (dbProduct == ORACLE && (e.getMessage().contains("deadlock detected")
            || e.getMessage().contains("can't serialize access for this transaction")));
  }

  /**
   * Whether the RDBMS has restrictions on IN list size (explicit, or poor perf-based).
   */
  public static boolean needsInBatching(DatabaseProduct dbType) {
    return dbType == ORACLE || dbType == SQLSERVER;
  }

  /**
   * Whether the RDBMS has a bug in join and filter operation order described in DERBY-6358.
   */
  public static boolean hasJoinOperationOrderBug(DatabaseProduct dbType) {
    return dbType == DERBY || dbType == ORACLE;
  }
};
