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
package org.apache.hadoop.hive.metastore.txn.jdbc.queries;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GetMaxAllocatedTableWriteIdHandler implements QueryHandler<MaxAllocatedTableWriteIdResponse> {

  private final MaxAllocatedTableWriteIdRequest rqst;

  public GetMaxAllocatedTableWriteIdHandler(MaxAllocatedTableWriteIdRequest rqst) {
    this.rqst = rqst;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "SELECT \"NWI_NEXT\" FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = :dbName AND \"NWI_TABLE\" = :tableName";
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("dbName", rqst.getDbName())
        .addValue("tableName", rqst.getTableName());
  }

  @Override
  public MaxAllocatedTableWriteIdResponse extractData(ResultSet rs) throws SQLException, DataAccessException {
    long maxWriteId = 0l;
    if (rs.next()) {
      // The row contains the nextId not the previously allocated
      maxWriteId = rs.getLong(1) - 1;
    }
    return new MaxAllocatedTableWriteIdResponse(maxWriteId);
  }
}
