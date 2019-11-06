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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An iterator that allows iterating through a SQL resultset. Includes methods to clear up resources.
 */
public class JdbcRecordIterator implements Iterator<Map<String, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordIterator.class);

  private Connection conn;
  private PreparedStatement ps;
  private ResultSet rs;
  private String[] hiveColumnNames;
  List<TypeInfo> hiveColumnTypesList;

  public JdbcRecordIterator(Connection conn, PreparedStatement ps, ResultSet rs, Configuration conf) {
    this.conn = conn;
    this.ps = ps;
    this.rs = rs;
    String fieldNamesProperty;
    String fieldTypesProperty;
    if (conf.get(Constants.JDBC_TABLE) != null && conf.get(Constants.JDBC_QUERY) != null) {
      fieldNamesProperty = Preconditions.checkNotNull(conf.get(Constants.JDBC_QUERY_FIELD_NAMES));
      fieldTypesProperty = Preconditions.checkNotNull(conf.get(Constants.JDBC_QUERY_FIELD_TYPES));
    } else {
      fieldNamesProperty = Preconditions.checkNotNull(conf.get(serdeConstants.LIST_COLUMNS));
      fieldTypesProperty = Preconditions.checkNotNull(conf.get(serdeConstants.LIST_COLUMN_TYPES));
    }
    hiveColumnNames = fieldNamesProperty.trim().split(",");
    hiveColumnTypesList = TypeInfoUtils.getTypeInfosFromTypeString(fieldTypesProperty);
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
  public Map<String, Object> next() {
    try {
      Map<String, Object> record = new HashMap<String, Object>(hiveColumnNames.length);
      for (int i = 0; i < hiveColumnNames.length; i++) {
        String key = hiveColumnNames[i];
        Object value = null;
        if (!(hiveColumnTypesList.get(i) instanceof PrimitiveTypeInfo)) {
          throw new RuntimeException("date type of column " + hiveColumnNames[i] + ":" +
                  hiveColumnTypesList.get(i).getTypeName() + " is not supported");
        }
        try {
          switch (((PrimitiveTypeInfo) hiveColumnTypesList.get(i)).getPrimitiveCategory()) {
            case INT:
            case SHORT:
            case BYTE:
              value = rs.getInt(i + 1);
              break;
            case LONG:
              value = rs.getLong(i + 1);
              break;
            case FLOAT:
              value = rs.getFloat(i + 1);
              break;
            case DOUBLE:
              value = rs.getDouble(i + 1);
              break;
            case DECIMAL:
              value = rs.getBigDecimal(i + 1);
              break;
            case BOOLEAN:
              value = rs.getBoolean(i + 1);
              break;
            case CHAR:
            case VARCHAR:
            case STRING:
              value = rs.getString(i + 1);
              break;
            case DATE:
              value = rs.getDate(i + 1);
              break;
            case TIMESTAMP:
              value = rs.getTimestamp(i + 1);
              break;
            default:
              LOGGER.error("date type of column " + hiveColumnNames[i] + ":" +
                      ((PrimitiveTypeInfo) hiveColumnTypesList.get(i)).getPrimitiveCategory() +
                      " is not supported");
              value = null;
              break;
          }
          if (value != null && !rs.wasNull()) {
            record.put(key, value);
          } else {
            record.put(key, null);
          }
        } catch (SQLDataException e) {
          record.put(key, null);
        }

      }

      return record;
    }
    catch (Exception e) {
      LOGGER.warn("next() threw exception", e);
      if (e instanceof SQLException){
        throw new RuntimeException(e);
      }
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
