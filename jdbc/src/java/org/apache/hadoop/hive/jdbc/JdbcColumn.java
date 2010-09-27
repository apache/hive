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

/**
 * Column metadata.
 */
public class JdbcColumn {
  private final String columnName;
  private final String tableName;
  private final String tableCatalog;
  private final String type;
  private final String comment;
  private final int ordinalPos;

  JdbcColumn(String columnName, String tableName, String tableCatalog
          , String type, String comment, int ordinalPos) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.tableCatalog = tableCatalog;
    this.type = type;
    this.comment = comment;
    this.ordinalPos = ordinalPos;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableCatalog() {
    return tableCatalog;
  }

  public String getType() {
    return type;
  }

  public Integer getSqlType() throws SQLException {
    return HiveResultSetMetaData.hiveTypeToSqlType(type);
  }

  public Integer getColumnSize() {
    if (type.equalsIgnoreCase("string")) {
      return Integer.MAX_VALUE;
    } else if (type.equalsIgnoreCase("tinyint")) {
      return 3;
    } else if (type.equalsIgnoreCase("smallint")) {
      return 5;
    } else if (type.equalsIgnoreCase("int")) {
      return 10;
    } else if (type.equalsIgnoreCase("bigint")) {
      return 19;
    } else if (type.equalsIgnoreCase("float")) {
      return 12;
    } else if (type.equalsIgnoreCase("double")) {
      return 22;
    } else { // anything else including boolean is null
      return null;
    }
  }

  public Integer getNumPrecRadix() {
    if (type.equalsIgnoreCase("tinyint")) {
      return 10;
    } else if (type.equalsIgnoreCase("smallint")) {
      return 10;
    } else if (type.equalsIgnoreCase("int")) {
      return 10;
    } else if (type.equalsIgnoreCase("bigint")) {
      return 10;
    } else if (type.equalsIgnoreCase("float")) {
      return 2;
    } else if (type.equalsIgnoreCase("double")) {
      return 2;
    } else { // anything else including boolean and string is null
      return null;
    }
  }

  public Integer getDecimalDigits() {
    if (type.equalsIgnoreCase("tinyint")) {
      return 0;
    } else if (type.equalsIgnoreCase("smallint")) {
      return 0;
    } else if (type.equalsIgnoreCase("int")) {
      return 0;
    } else if (type.equalsIgnoreCase("bigint")) {
      return 0;
    } else { // anything else including float and double is null
      return null;
    }
  }

  public String getComment() {
    return comment;
  }

  public int getOrdinalPos() {
    return ordinalPos;
  }
}
