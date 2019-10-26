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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;

/**
 * DBRecordWritable writes serialized row by row data to the underlying database.
 */
public class DBRecordWritable implements Writable,
        org.apache.hadoop.mapreduce.lib.db.DBWritable {

  private Object[] columnValues;

  public DBRecordWritable() {
  }

  public DBRecordWritable(int numColumns) {
    this.columnValues = new Object[numColumns];
  }

  public void clear() {
    Arrays.fill(columnValues, null);
  }

  public void set(int i, Object columnObject) {
    columnValues[i] = columnObject;
  }

  @Override
  public void readFields(ResultSet rs) throws SQLException {
    // do nothing
  }

  @Override
  public void write(PreparedStatement statement) throws SQLException {
    if (columnValues == null) {
      throw new SQLException("No data available to be written");
    }
    for (int i = 0; i < columnValues.length; i++) {
      statement.setObject(i + 1, columnValues[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // do nothing
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // do nothing
  }

}
