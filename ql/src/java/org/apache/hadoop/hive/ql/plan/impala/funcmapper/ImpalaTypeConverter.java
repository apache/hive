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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TPrimitiveType;

import com.google.common.collect.Lists;

/**
 * class used to define the Impala ColumnType.
 * As defined, this only handles SCALAR types.
 * Once we support other types, we may have to
 * restructure this code.
 */
public class ImpalaTypeConverter {

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
    if (primitiveTypes == null) {
      return result;
    }
    for (TPrimitiveType primitiveType : primitiveTypes) {
      result.add(getSqlTypeName(primitiveType));
    }
    return result;
  }

  static public List<SqlTypeName> getSqlTypeNamesFromNodes(List<RexNode> rexNodes) {
    List<SqlTypeName> result = Lists.newArrayList();
    for (RexNode r : rexNodes) {
      SqlTypeName sqlTypeName = r.getType().getSqlTypeName();
      result.add(r.getType().getSqlTypeName());
    }
    return result;
  }

  static public List<Type> getImpalaTypesList(TPrimitiveType[] argTypes) {
    List<Type> types = Lists.newArrayList();
    if (argTypes == null) {
      return types;
    }
    for (TPrimitiveType argType : argTypes) {
      types.add(getImpalaType(argType));
    }
    return types;
  }

  static public Type getImpalaType(TPrimitiveType argType, int precision, int scale) {
    // Char, varchar, ldecimal, and fixed_uda_intermediate contain precisions and need to be
    // treated separately.
    switch (argType) {
      case CHAR:
        return ScalarType.createCharType(precision);
      case VARCHAR:
        return ScalarType.createVarcharType(precision);
      case DECIMAL:
        return ScalarType.createDecimalType(precision, scale);
      case FIXED_UDA_INTERMEDIATE:
        return ScalarType.createFixedUdaIntermediateType(precision);
      default:
        return ScalarType.createType(PrimitiveType.fromThrift(argType));
    }
  }

  static public Type getImpalaType(TPrimitiveType argType) {
    // Char and decimal contain precisions and need to be treated separately from
    // the rest. The precisions for this case are unknown though, as we are only given
    // a "primitivetype'.
    if (argType == TPrimitiveType.CHAR) {
      // -1 is just a placeholder. This is only called for Impala function signatures
      return ScalarType.createCharType(-1);
    } else if (argType == TPrimitiveType.VARCHAR) {
      // -1 is just a placeholder. This is only called for Impala function signatures
      return ScalarType.createVarcharType(-1);
    } else if (argType == TPrimitiveType.DECIMAL) {
      // Unknown precision and scale, just create the wild card.  This is only for
      // Impala Function signatures.
      return ScalarType.createWildCardDecimalType();
    }
    return ScalarType.createType(PrimitiveType.fromThrift(argType));
  }

  static public Type[] getImpalaTypes(TPrimitiveType[] argTypes) {
    if (argTypes == null) {
      return null;
    }
    return getImpalaTypesList(argTypes).toArray(new Type[1]);
  }

  static public Type getImpalaType(RelDataType relDataType) throws HiveException {
    if (relDataType.getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
      return ScalarType.createDecimalType(relDataType.getPrecision(),
          relDataType.getScale());
    }
    TPrimitiveType tPrimitiveType = getTPrimitiveType(relDataType.getSqlTypeName());
    PrimitiveType primitiveType = PrimitiveType.fromThrift(tPrimitiveType);
    return ScalarType.getDefaultScalarType(primitiveType);
  }


}
