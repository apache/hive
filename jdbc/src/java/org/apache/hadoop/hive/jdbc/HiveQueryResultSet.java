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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.io.BytesWritable;

/**
 * HiveQueryResultSet.
 *
 */
public class HiveQueryResultSet extends HiveBaseResultSet {

  public static final Log LOG = LogFactory.getLog(HiveQueryResultSet.class);

  private HiveInterface client;
  private SerDe serde;

  private int maxRows = 0;
  private int rowsFetched = 0;

  public HiveQueryResultSet(HiveInterface client, int maxRows) throws SQLException {
    this.client = client;
    this.maxRows = maxRows;
    initSerde();
    row = Arrays.asList(new Object[columnNames.size()]);
  }

  public HiveQueryResultSet(HiveInterface client) throws SQLException {
    this(client, 0);
  }

  /**
   * Instantiate the serde used to deserialize the result rows.
   */
  private void initSerde() throws SQLException {
    try {
      Schema fullSchema = client.getSchema();
      List<FieldSchema> schema = fullSchema.getFieldSchemas();
      columnNames = new ArrayList<String>();
      columnTypes = new ArrayList<String>();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if ((schema != null) && (!schema.isEmpty())) {
        for (int pos = 0; pos < schema.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
          columnNames.add(schema.get(pos).getName());
          columnTypes.add(schema.get(pos).getType());
          namesSb.append(schema.get(pos).getName());
          typesSb.append(schema.get(pos).getType());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        LOG.info("Column names: " + names);
        props.setProperty(Constants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        LOG.info("Column types: " + types);
        props.setProperty(Constants.LIST_COLUMN_TYPES, types);
      }
      serde.initialize(new Configuration(), props);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage());
    }
  }

  @Override
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetched row string: " + rowStr);
      }

      if (!"".equals(rowStr)) {
        StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
        List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
        Object data = serde.deserialize(new BytesWritable(rowStr.getBytes()));

        assert row.size() == fieldRefs.size() : row.size() + ", " + fieldRefs.size();
        for (int i = 0; i < fieldRefs.size(); i++) {
          StructField fieldRef = fieldRefs.get(i);
          ObjectInspector oi = fieldRef.getFieldObjectInspector();
          Object obj = soi.getStructFieldData(data, fieldRef);
          row.set(i, convertLazyToJava(obj, oi));
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Deserialized row: " + row);
        }
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Error retrieving next row");
    }
    // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
    return !"".equals(rowStr);
  }

  /**
   * Convert a LazyObject to a standard Java object in compliance with JDBC 3.0 (see JDBC 3.0
   * Specification, Table B-3: Mapping from JDBC Types to Java Object Types).
   *
   * This method is kept consistent with {@link HiveResultSetMetaData#hiveTypeToSqlType}.
   */
  private static Object convertLazyToJava(Object o, ObjectInspector oi) {
    Object obj = ObjectInspectorUtils.copyToStandardObject(o, oi, ObjectInspectorCopyOption.JAVA);

    // for now, expose non-primitive as a string
    // TODO: expose non-primitive as a structured object while maintaining JDBC compliance
    if (obj != null && oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      obj = obj.toString();
    }

    return obj;
  }
}
