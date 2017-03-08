/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.dao;

import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An iterator that allows iterating through a SQL resultset. Includes methods to clear up resources.
 */
public class JdbcRecordIterator implements Iterator<Map<String, String>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordIterator.class);

  private Connection conn;
  private PreparedStatement ps;
  private ResultSet rs;


  public JdbcRecordIterator(Connection conn, PreparedStatement ps, ResultSet rs) {
    this.conn = conn;
    this.ps = ps;
    this.rs = rs;
  }


  @Override
  public boolean hasNext() {
    try {
      return rs.next();
    }
    catch (Exception se) {
      LOGGER.warn("hasNext() threw exception", se);
      return false;
    }
  }


  @Override
  public Map<String, String> next() {
    try {
      ResultSetMetaData metadata = rs.getMetaData();
      int numColumns = metadata.getColumnCount();
      Map<String, String> record = new HashMap<String, String>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        String key = metadata.getColumnName(i + 1);
        String value = rs.getString(i + 1);
        if (value == null) {
          value = NullWritable.get().toString();
        }
        record.put(key, value);
      }

      return record;
    }
    catch (Exception e) {
      LOGGER.warn("next() threw exception", e);
      return null;
    }
  }


  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not supported");
  }


  /**
   * Release all DB resources
   */
  public void close() {
    try {
      rs.close();
      ps.close();
      conn.close();
    }
    catch (Exception e) {
      LOGGER.warn("Caught exception while trying to close database objects", e);
    }
  }

}
