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
package org.apache.hadoop.hive.metastore.txn.jdbc.commands;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.function.Function;

import static org.apache.hadoop.hive.metastore.DatabaseProduct.DbType.MYSQL;

public class RemoveDuplicateCompleteTxnComponentsCommand implements ParameterizedCommand {
  
  private RemoveDuplicateCompleteTxnComponentsCommand() {}
  
  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return null;
  }

  //language=SQL
  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    switch (databaseProduct.dbType) {
      case MYSQL:
      case SQLSERVER:
        return "DELETE tc " +
            "FROM \"COMPLETED_TXN_COMPONENTS\" tc " +
            "INNER JOIN (" +
            "    SELECT \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\"," +
            "        MAX(\"CTC_WRITEID\") highestWriteId," +
            "        MAX(CASE WHEN \"CTC_UPDATE_DELETE\" = 'Y' THEN \"CTC_WRITEID\" END) updateWriteId" +
            "    FROM \"COMPLETED_TXN_COMPONENTS\"" +
            "    GROUP BY \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\"" +
            ") c ON " +
            "      tc.\"CTC_DATABASE\" = c.\"CTC_DATABASE\" " +
            "      AND tc.\"CTC_TABLE\" = c.\"CTC_TABLE\"" +
            "      AND (tc.\"CTC_PARTITION\" = c.\"CTC_PARTITION\" OR (tc.\"CTC_PARTITION\" IS NULL AND c.\"CTC_PARTITION\" IS NULL)) " +
            "WHERE tc.\"CTC_WRITEID\" < c.\"highestWriteId\" " +
            (MYSQL == databaseProduct.dbType ?
                "  AND NOT tc.\"CTC_WRITEID\" <=> c.updateWriteId" :
                "  AND (tc.\"CTC_WRITEID\" != c.updateWriteId OR c.updateWriteId IS NULL)");
        
      case DERBY: 
      case ORACLE:
      case POSTGRES:
      case CUSTOM:
        String existsSubQuery =   
          "EXISTS (" +
          "    SELECT 1" +
          "    FROM \"COMPLETED_TXN_COMPONENTS\" c" +
          "    WHERE tc.\"CTC_DATABASE\" = c.\"CTC_DATABASE\"" +
          "      AND tc.\"CTC_TABLE\" = c.\"CTC_TABLE\"" +
          "      AND %s" +
          "      AND (tc.\"CTC_UPDATE_DELETE\" = 'N' OR c.\"CTC_UPDATE_DELETE\" = 'Y')" +
          "      AND tc.\"CTC_WRITEID\" < c.\"CTC_WRITEID\"" +
          ")";
        
        return new StringBuilder()
          .append("DELETE FROM \"COMPLETED_TXN_COMPONENTS\" tc WHERE ") 
          .append(String.format(existsSubQuery, 
              "tc.\"CTC_PARTITION\" = c.\"CTC_PARTITION\""))
          .append(" OR ")
          .append(String.format(existsSubQuery, 
              "tc.\"CTC_PARTITION\" IS NULL AND c.\"CTC_PARTITION\" IS NULL"))
          .toString();
     
      default:
        String msg = "Unknown database product: " + databaseProduct.dbType;
        throw new MetaException(msg);
    }
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource();
  }
  
  public static RemoveDuplicateCompleteTxnComponentsCommand INSTANCE = new RemoveDuplicateCompleteTxnComponentsCommand();
  
}