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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;

/**
 * Factory for row expressions created from JSON files.
 */
class HiveRexJsonBuilder extends RexBuilder {
  HiveRexJsonBuilder() {
    super(new HiveTypeFactory());
  }

  @Override
  public RexNode makeLiteral(Object value, RelDataType type, boolean allowCast, boolean trim) {
    // VARCHAR will always come back as CHAR if we don't allow a cast
    allowCast = SqlTypeName.VARCHAR.equals(type.getSqlTypeName()) || allowCast;
    if (SqlTypeName.CHAR_TYPES.contains(type.getSqlTypeName())) {
      value = RexNodeExprFactory.makeHiveUnicodeString((String) value);
    }
    return super.makeLiteral(value, type, allowCast, trim);
  }
}
