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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JdbcRecordWriter is wrapper class to write data to the underlying database.
 */
public class JdbcRecordWriter implements RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcRecordWriter.class);

  @SuppressWarnings("rawtypes")
  private final DBRecordWriter dbRecordWriter;

  @SuppressWarnings("rawtypes")
  public JdbcRecordWriter(DBRecordWriter writer) {
    this.dbRecordWriter = writer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Writable w) throws IOException {
    dbRecordWriter.write((DBRecordWritable) w, null);
  }

  @Override
  public void close(boolean abort) throws IOException {
    if (abort) {
      Connection conn = dbRecordWriter.getConnection();
      try {
        conn.rollback();
      } catch (SQLException ex) {
        LOG.warn("Failed to perform rollback on connection", ex);
      } finally {
        try {
          conn.close();
        } catch (SQLException ex) {
          throw new IOException(ex.getMessage());
        }
      }
    } else {
      dbRecordWriter.close(null);
    }
  }

}
