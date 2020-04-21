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
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * TODO: Jira CDPD-8258 has been filed for this file. The sole method in here,
 * "decimalOf()" exists in Calcite 1.21 which is already in use oin hive master
 * branch. This code should be removed when we upgrade.
 */
public class Calcite2302 {
  static RelDataType decimalOf(RelDataTypeFactory dtFactory, RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    assert typeName != null;
    switch (typeName) {
    case DECIMAL:
      return type;
    case TINYINT:
      return dtFactory.createSqlType(SqlTypeName.DECIMAL, 3, 0);
    case SMALLINT:
      return dtFactory.createSqlType(SqlTypeName.DECIMAL, 5, 0);
    case INTEGER:
      return dtFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0);
    case BIGINT:
      // the default max precision is 19, so this is actually DECIMAL(19, 0)
      // but derived system can override the max precision/scale.
      return dtFactory.createSqlType(SqlTypeName.DECIMAL, 38, 0);
    case REAL:
      return dtFactory.createSqlType(SqlTypeName.DECIMAL, 14, 7);
    case FLOAT:
      return dtFactory.createSqlType(SqlTypeName.DECIMAL, 14, 7);
    case DOUBLE:
      // the default max precision is 19, so this is actually DECIMAL(19, 15)
      // but derived system can override the max precision/scale.
      return dtFactory.createSqlType(SqlTypeName.DECIMAL, 30, 15);
    default:
      // default precision and scale.
      return dtFactory.createSqlType(SqlTypeName.DECIMAL);
    }
  }
}
