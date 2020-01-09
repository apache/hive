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

package org.apache.hadoop.hive.ql.plan.impala.funcmapper;

import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.thrift.TPrimitiveType;

import com.google.common.collect.Lists;

/**
 * class used to define the Impala ColumnType.
 * As defined, this only handles SCALAR types.
 * Once we support other types, we may have to
 * restructure this code.
 */
public class ImpalaTypeConverter {
  static public TPrimitiveType getTPrimitiveType(String stringTypeName) throws HiveException {
    if (stringTypeName.equals("tinyint")) {
      return TPrimitiveType.TINYINT;
    }
    if (stringTypeName.equals("smallint")) {
      return TPrimitiveType.SMALLINT;
    }
    if (stringTypeName.equals("int")) {
      return TPrimitiveType.INT;
    }
    if (stringTypeName.equals("bigint")) {
      return TPrimitiveType.BIGINT;
    }
    if (stringTypeName.equals("string")) {
      return TPrimitiveType.STRING;
    }
    if (stringTypeName.equals("float")) {
      return TPrimitiveType.FLOAT;
    }
    if (stringTypeName.equals("double")) {
      return TPrimitiveType.DOUBLE;
    }
    if (stringTypeName.startsWith("decimal")) {
      return TPrimitiveType.DECIMAL;
    }
    if (stringTypeName.equals("char")) {
      return TPrimitiveType.CHAR;
    }
    if (stringTypeName.equals("varchar")) {
      return TPrimitiveType.VARCHAR;
    }
    if (stringTypeName.equals("timestamp")) {
      return TPrimitiveType.TIMESTAMP;
    }
    if (stringTypeName.equals("date")) {
      return TPrimitiveType.DATE;
    }
    throw new HiveException("Current type name " + stringTypeName + " not supported yet.");
  }

  static public SqlTypeName getSqlTypeName(TPrimitiveType primitiveType) throws HiveException {
    switch (primitiveType) {
    case TINYINT:
      return SqlTypeName.TINYINT;
    case SMALLINT:
      return SqlTypeName.SMALLINT;
    case INT:
      return SqlTypeName.INTEGER;
    case BIGINT:
      return SqlTypeName.BIGINT;
    case STRING:
      return SqlTypeName.VARCHAR;
    case BOOLEAN:
      return SqlTypeName.BOOLEAN;
    case FLOAT:
      return SqlTypeName.FLOAT;
    case DOUBLE:
      return SqlTypeName.DOUBLE;
    case DECIMAL:
      return SqlTypeName.DECIMAL;
    case CHAR:
      return SqlTypeName.CHAR;
    case VARCHAR:
      return SqlTypeName.VARCHAR;
    case TIMESTAMP:
      return SqlTypeName.TIMESTAMP;
    case DATE:
      return SqlTypeName.DATE;
    default:
      throw new HiveException("Current primitive type name " + primitiveType + " not supported yet.");
    }
  }

  static public TPrimitiveType getTPrimitiveType(SqlTypeName calciteTypeName) throws HiveException {
    switch (calciteTypeName) {
    case TINYINT:
      return TPrimitiveType.TINYINT;
    case SMALLINT:
      return TPrimitiveType.SMALLINT;
    case INTEGER:
      return TPrimitiveType.INT;
    case BIGINT:
      return TPrimitiveType.BIGINT;
    case VARCHAR:
      return TPrimitiveType.STRING;
    case BOOLEAN:
      return TPrimitiveType.BOOLEAN;
    case FLOAT:
      return TPrimitiveType.FLOAT;
    case DOUBLE:
      return TPrimitiveType.DOUBLE;
    case DECIMAL:
      return TPrimitiveType.DECIMAL;
    case CHAR:
      return TPrimitiveType.CHAR;
    case TIMESTAMP:
      return TPrimitiveType.TIMESTAMP;
    case DATE:
      return TPrimitiveType.DATE;
    default:
      throw new HiveException("TPrimitiveType " + calciteTypeName + "  not supported yet.");
    }
  }

  static public List<SqlTypeName> getSqlTypeNames(TPrimitiveType[] primitiveTypes)
      throws HiveException  {
    List<SqlTypeName> result = Lists.newArrayList();
    for (TPrimitiveType primitiveType : primitiveTypes) {
      result.add(getSqlTypeName(primitiveType));
    }
    return result;
  }

  static public List<SqlTypeName> getSqlTypeNamesFromNodes(List<RexNode> rexNodes) {
    List<SqlTypeName> result = Lists.newArrayList();
    for (RexNode r : rexNodes) {
      result.add(r.getType().getSqlTypeName());
    }
    return result;
  }

}
