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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.ImpalaTypeSystemImpl;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TPrimitiveType;

import java.util.List;
import java.util.Map;

/**
 * A utility class that holds methods that convert impala types to Calcite
 * types and vice versa.
 *
 * One important distinction is the different classes used here. On the Impala side,
 * there are:
 * - Normalized Type names (e.g. Type.BOOLEAN).  These are pre-created types. This becomes
 * important for types with precisions like decimal, char, and varchar.  The Type.DECIMAL
 * (and char and varchar) do not have any precision (or scale) associated with them.  In
 * the function signatures, all precisions are allowed, so this type is used when describing
 * them.
 * - types with precisions.  These also use Types (also of type ScalarType), but the precision
 * (and scale) is included with the datatype.
 * 
 * On the Calcite side, there are:
 * - Normalized RelDataTypes.  While theoretically we should have been able to use SqlTypeName to
 * have the same purpose as the Impala default dataypes, there is no SqlTypeName.STRING. Therefore,
 * we needed to resort to RelDataTypes for this purpose.
 * - types with precisions. The normal RelDataType is used.
 */
public class ImpalaTypeConverter {

  // Maps Impala default types to Calcite default types.
  private static Map<Type, RelDataType> impalaToCalciteMap;

  static {
    RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl()));
    RelDataTypeFactory factory = rexBuilder.getTypeFactory();
    ImmutableMap.Builder<Type, RelDataType> map = ImmutableMap.builder();
    map.put(Type.BOOLEAN, factory.createSqlType(SqlTypeName.BOOLEAN));
    map.put(Type.TINYINT, factory.createSqlType(SqlTypeName.TINYINT));
    map.put(Type.SMALLINT, factory.createSqlType(SqlTypeName.SMALLINT));
    map.put(Type.INT, factory.createSqlType(SqlTypeName.INTEGER));
    map.put(Type.BIGINT, factory.createSqlType(SqlTypeName.BIGINT));
    map.put(Type.FLOAT, factory.createSqlType(SqlTypeName.FLOAT));
    map.put(Type.DOUBLE, factory.createSqlType(SqlTypeName.DOUBLE));
    map.put(Type.TIMESTAMP, factory.createSqlType(SqlTypeName.TIMESTAMP));
    map.put(Type.DATE, factory.createSqlType(SqlTypeName.DATE));
    map.put(Type.DECIMAL, factory.createSqlType(SqlTypeName.DECIMAL));
    map.put(Type.CHAR, factory.createSqlType(SqlTypeName.CHAR, 1));
    map.put(Type.VARCHAR, factory.createSqlType(SqlTypeName.VARCHAR, 1));
    map.put(Type.STRING, factory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE));
    impalaToCalciteMap = map.build();
  }

  // helper function to handle translation of lists.
  public static List<Type> getNormalizedImpalaTypes(List<RelDataType> relDataTypes) {
    return Lists.transform(relDataTypes, ImpalaTypeConverter::getNormalizedImpalaType);
  }

  /** 
   * Return the default impala type given a reldatatype that potentially has precision.
   */
  public static Type getNormalizedImpalaType(RelDataType relDataType) {
    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
      return Type.BIGINT;
    }
    switch (sqlTypeName) {
      case VARCHAR:
        return relDataType.getPrecision() == Integer.MAX_VALUE ? Type.STRING : Type.VARCHAR;
      case CHAR:
        return Type.CHAR;
      case DECIMAL:
        return Type.DECIMAL;
      case BOOLEAN:
        return Type.BOOLEAN;
      case TINYINT:
        return Type.TINYINT;
      case SMALLINT:
        return Type.SMALLINT;
      case INTEGER:
        return Type.INT;
      case BIGINT:
        return Type.BIGINT;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DATE:
        return Type.DATE;
      case SYMBOL:
        return null;
      case NULL:
        return Type.NULL;
      default:
        throw new RuntimeException("Unknown SqlTypeName " + sqlTypeName + " to convert to Impala.");
    }
  }

  /**
   * Given RexNode operands, return the normalized RelDataType associated with it. See
   * the class description on the definition of normalized.
   */
  public static List<RelDataType> getNormalizedImpalaTypeList(List<RexNode> rexNodes) {
    return Lists.transform(rexNodes, ImpalaTypeConverter::getNormalizedImpalaType);
  }

  public static RelDataType getNormalizedImpalaType(RexNode node) {
    SqlTypeName sqlTypeName = node.getType().getSqlTypeName();
    if (node instanceof RexLiteral &&
        SqlTypeName.CHAR_TYPES.contains(sqlTypeName)) {
      return getInterpretationType((RexLiteral) node);
    } else if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
      return node.getType();
    } else {
      Type impalaType = getNormalizedImpalaType(node.getType());
      return getRelDataType(impalaType);
    }
  }

  // helper function to handle translation of lists.
  public static List<RelDataType> getRelDataTypes(List<Type> impalaTypes) {
    return Lists.transform(impalaTypes, ImpalaTypeConverter::getRelDataType);
  }

  /**
   * Get the normalized RelDataType given an impala type.
   */
  public static RelDataType getRelDataType(Type impalaType) {
    if (impalaType == null) {
      return null;
    }
    TPrimitiveType primitiveType = impalaType.getPrimitiveType().toThrift();
    Type normalizedImpalaType = getImpalaType(primitiveType);
    return impalaToCalciteMap.get(normalizedImpalaType);
  }

  // helper function to handle translation of lists.
  public static List<Type> createImpalaTypes(List<RelDataType> relDataTypes) {
    return Lists.transform(relDataTypes, ImpalaTypeConverter::createImpalaType);
  }

  /**
   * Create a new impala type given a relDataType
   */
  public static Type createImpalaType(RelDataType relDataType) {
    // First retrieve the normalized impala type
    if (relDataType.getSqlTypeName() == SqlTypeName.VARCHAR &&
        relDataType.getPrecision() == Integer.MAX_VALUE) {
      return Type.STRING;
    }
    Type impalaType = getType(relDataType.getSqlTypeName());
    // create the impala type given the normalized type, precision, and scale.
    return createImpalaType(impalaType, relDataType.getPrecision(), relDataType.getScale());
  }

  public static Type createImpalaType(Type impalaType, int precision, int scale) {
    TPrimitiveType primitiveType = impalaType.getPrimitiveType().toThrift();
    // Char, varchar, decimal, and fixed_uda_intermediate contain precisions and need to be
    // treated separately.
    switch (primitiveType) {
      case CHAR:
        return ScalarType.createCharType(precision);
      case VARCHAR:
        return ScalarType.createVarcharType(precision);
      case DECIMAL:
        return ScalarType.createDecimalType(precision, scale);
      case FIXED_UDA_INTERMEDIATE:
        return ScalarType.createFixedUdaIntermediateType(precision);
      default:
        return impalaType;
    }
  }

  /**
   * Create list of types given primitive types.
   * Primitive types should not be exposed outside of this class.
   */
  static List<Type> getImpalaTypesList(TPrimitiveType[] argTypes) {
    List<Type> types = Lists.newArrayList();
    if (argTypes == null) {
      return types;
    }
    for (TPrimitiveType argType : argTypes) {
      types.add(getImpalaType(argType));
    }
    return types;
  }

  /**
   * Create Impala types given primitive types.
   * Primitive types should not be exposed outside of this class.
   */
  static Type getImpalaType(TPrimitiveType argType) {
    // Char and decimal contain precisions and need to be treated separately from
    // the rest. The precisions for this case are unknown though, as we are only given
    // a "primitivetype'.
    switch (argType) {
      case CHAR:
        return Type.CHAR;
      case VARCHAR:
        return Type.VARCHAR;
      case DECIMAL:
        return Type.DECIMAL;
      case BOOLEAN:
        return Type.BOOLEAN;
      case TINYINT:
        return Type.TINYINT;
      case SMALLINT:
        return Type.SMALLINT;
      case INT:
        return Type.INT;
      case BIGINT:
        return Type.BIGINT;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DATE:
        return Type.DATE;
      case STRING:
        return Type.STRING;
      case FIXED_UDA_INTERMEDIATE:
        return Type.FIXED_UDA_INTERMEDIATE;
      case NULL_TYPE:
        return Type.NULL;
      default:
        throw new RuntimeException("Unknown type " + argType);
    }
  }

  /**
   * Get the RelDataType from the RexLiteral.  WHen the literal is a char
   * type, the RelDataType does not hold the real type information, we have
   * to dig a bit deeper into its interpretation type.
   */
  private static RelDataType getInterpretationType(RexLiteral rexLiteral) {
    switch (rexLiteral.getType().getSqlTypeName()) {
      case CHAR:
        return impalaToCalciteMap.get(Type.CHAR);
      case VARCHAR:
        if (rexLiteral.getType().getPrecision() == Integer.MAX_VALUE) {
          return impalaToCalciteMap.get(Type.STRING);
        }
        return impalaToCalciteMap.get(Type.VARCHAR);
    }
    throw new RuntimeException("Not a char/varchar/string type: " + rexLiteral.getType());
  }

  private static Type getType(SqlTypeName calciteTypeName) {
    switch (calciteTypeName) {
      case TINYINT:
        return Type.TINYINT;
      case SMALLINT:
        return Type.SMALLINT;
      case INTEGER:
        return Type.INT;
      case BIGINT:
        return Type.BIGINT;
      case VARCHAR:
        return Type.VARCHAR;
      case BOOLEAN:
        return Type.BOOLEAN;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case DECIMAL:
        return Type.DECIMAL;
      case CHAR:
        return Type.CHAR;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DATE:
        return Type.DATE;
      case NULL:
        return Type.NULL;
      default:
        throw new RuntimeException("Type " + calciteTypeName + "  not supported yet.");
    }
  }
}
