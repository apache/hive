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

package org.apache.hadoop.hive.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.io.BytesWritable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * HiveQueryResultSet.
 *
 */
public class HiveQueryResultSet extends HiveBaseResultSet {
  private HiveInterface client;
  private DynamicSerDe ds;

  private int maxRows = 0;
  private int rowsFetched = 0;

  @SuppressWarnings("unchecked")
  public HiveQueryResultSet(HiveInterface client, int maxRows) throws SQLException {
    this.client = client;
    row = new ArrayList();
    this.maxRows = maxRows;
    initDynamicSerde();
  }

  @SuppressWarnings("unchecked")
  public HiveQueryResultSet(HiveInterface client) throws SQLException {
    this(client, 0);
  }

  /**
   * Instantiate the dynamic serde used to deserialize the result row.
   */
  private void initDynamicSerde() throws SQLException {
    try {
      Schema fullSchema = client.getThriftSchema();
      List<FieldSchema> schema = fullSchema.getFieldSchemas();
      columnNames = new ArrayList<String>();
      columnTypes = new ArrayList<String>();

      String serDDL;

      if ((schema != null) && (!schema.isEmpty())) {
        serDDL = new String("struct result { ");
        for (int pos = 0; pos < schema.size(); pos++) {
          if (pos != 0) {
            serDDL = serDDL.concat(",");
          }
          columnTypes.add(schema.get(pos).getType());
          columnNames.add(schema.get(pos).getName());
          serDDL = serDDL.concat(schema.get(pos).getType());
          serDDL = serDDL.concat(" ");
          serDDL = serDDL.concat(schema.get(pos).getName());
        }
        serDDL = serDDL.concat("}");
      } else {
        serDDL = new String("struct result { string empty }");
      }

      ds = new DynamicSerDe();
      Properties dsp = new Properties();
      dsp.setProperty(Constants.SERIALIZATION_FORMAT,
          org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class
          .getName());
      dsp.setProperty(
          org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME,
          "result");
      dsp.setProperty(Constants.SERIALIZATION_DDL, serDDL);
      dsp.setProperty(Constants.SERIALIZATION_LIB, ds.getClass().toString());
      dsp.setProperty(Constants.FIELD_DELIM, "9");
      ds.initialize(new Configuration(), dsp);
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage());
    }
  }

  public void close() throws SQLException {
    client = null;
  }

  /**
   * Moves the cursor down one row from its current position.
   * 
   * @see java.sql.ResultSet#next()
   * @throws SQLException
   *           if a database access error occurs.
   */
  public boolean next() throws SQLException {
    if (maxRows > 0 && rowsFetched >= maxRows) {
      return false;
    }

    String rowStr = "";
    try {
      rowStr = (String) client.fetchOne();
      rowsFetched++;
      if (!"".equals(rowStr)) {
        Object o = ds.deserialize(new BytesWritable(rowStr.getBytes()));
        row = (ArrayList<?>) o;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Error retrieving next row");
    }
    // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
    return !"".equals(rowStr);
  }

}
