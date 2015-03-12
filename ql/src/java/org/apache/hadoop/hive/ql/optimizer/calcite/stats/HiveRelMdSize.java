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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HiveRelMdSize extends RelMdSize {

  protected static final Log LOG  = LogFactory.getLog(HiveRelMdSize.class.getName());

  private static final HiveRelMdSize INSTANCE = new HiveRelMdSize();

  public static final RelMetadataProvider SOURCE =
          ReflectiveRelMetadataProvider.reflectiveSource(INSTANCE,
                  BuiltInMethod.AVERAGE_COLUMN_SIZES.method,
                  BuiltInMethod.AVERAGE_ROW_SIZE.method);

  //~ Constructors -----------------------------------------------------------

  private HiveRelMdSize() {}

  //~ Methods ----------------------------------------------------------------

  public Double averageTypeValueSize(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
    case TINYINT:
      return 1d;
    case SMALLINT:
      return 2d;
    case INTEGER:
    case FLOAT:
    case REAL:
    case DATE:
    case TIME:
      return 4d;
    case BIGINT:
    case DOUBLE:
    case TIMESTAMP:
    case INTERVAL_DAY_TIME:
    case INTERVAL_YEAR_MONTH:
      return 8d;
    case BINARY:
      return (double) type.getPrecision();
    case VARBINARY:
      return Math.min((double) type.getPrecision(), 100d);
    case CHAR:
      return (double) type.getPrecision() * BYTES_PER_CHARACTER;
    case VARCHAR:
      // Even in large (say VARCHAR(2000)) columns most strings are small
      return Math.min((double) type.getPrecision() * BYTES_PER_CHARACTER, 100d);
    case ROW:
      Double average = 0.0;
      for (RelDataTypeField field : type.getFieldList()) {
        average += averageTypeValueSize(field.getType());
      }
      return average;
    default:
      return null;
    }
  }

}
