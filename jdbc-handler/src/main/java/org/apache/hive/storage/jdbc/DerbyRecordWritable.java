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
package org.apache.hive.storage.jdbc;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * DerbyRecordWritable writes serialized row by row data to the underlying database.
 */
public class DerbyRecordWritable extends DBRecordWritable {

  public DerbyRecordWritable(int numColumns) {
    this.columnValues = new Object[numColumns];
  }
  @Override
  public void write(PreparedStatement statement) throws SQLException {
    if (columnValues == null) {
      throw new SQLException("No data available to be written");
    }
    ParameterMetaData parameterMetaData = statement.getParameterMetaData();
    for (int i = 0; i < columnValues.length; i++) {
      Object value = columnValues[i];
      if ((parameterMetaData.getParameterType(i + 1) == Types.CHAR) && value != null && value instanceof Boolean) {
        value = ((Boolean) value).booleanValue() ? "1" : "0";
      }
      statement.setObject(i + 1, value);
    }
  }
}
