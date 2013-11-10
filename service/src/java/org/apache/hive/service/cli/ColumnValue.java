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

package org.apache.hive.service.cli;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hive.service.cli.thrift.TBoolValue;
import org.apache.hive.service.cli.thrift.TByteValue;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TDoubleValue;
import org.apache.hive.service.cli.thrift.TI16Value;
import org.apache.hive.service.cli.thrift.TI32Value;
import org.apache.hive.service.cli.thrift.TI64Value;
import org.apache.hive.service.cli.thrift.TStringValue;

/**
 * ColumnValue.
 *
 */
public class ColumnValue {

  public static final TColumnValue NULL = new TColumnValue();

  static {
    NULL.setStringVal(new TStringValue());
  }

  // TODO: replace this with a non-Thrift implementation
  private final TColumnValue tColumnValue;

  public ColumnValue(TColumnValue tColumnValue) {
    this.tColumnValue = new TColumnValue(tColumnValue);
  }

  private static boolean isNull(Object value) {
    return (value == null);
  }

  public static ColumnValue booleanValue(Boolean value) {
    TBoolValue tBoolValue = new TBoolValue();
    if (value != null) {
      tBoolValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.boolVal(tBoolValue));
  }

  public static ColumnValue byteValue(Byte value) {
    TByteValue tByteValue = new TByteValue();
    if (value != null) {
      tByteValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.byteVal(tByteValue));
  }

  public static ColumnValue shortValue(Short value) {
    TI16Value tI16Value = new TI16Value();
    if (value != null) {
      tI16Value.setValue(value);
    }
    return new ColumnValue(TColumnValue.i16Val(tI16Value));
  }

  public static ColumnValue intValue(Integer value) {
    TI32Value tI32Value = new TI32Value();
    if (value != null) {
      tI32Value.setValue(value);
    }
    return new ColumnValue(TColumnValue.i32Val(tI32Value));
  }

  public static ColumnValue longValue(Long value) {
    TI64Value tI64Value = new TI64Value();
    if (value != null) {
      tI64Value.setValue(value);
    }
    return new ColumnValue(TColumnValue.i64Val(tI64Value));
  }

  public static ColumnValue floatValue(Float value) {
    TDoubleValue tDoubleValue = new TDoubleValue();
    if (value != null) {
      tDoubleValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.doubleVal(tDoubleValue));
  }

  public static ColumnValue doubleValue(Double value) {
    TDoubleValue tDoubleValue = new TDoubleValue();
    if (value != null) {
      tDoubleValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.doubleVal(tDoubleValue));
  }

  public static ColumnValue stringValue(String value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.stringVal(tStringValue));
  }

  public static ColumnValue stringValue(HiveChar value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return new ColumnValue(TColumnValue.stringVal(tStringValue));
  }

  public static ColumnValue stringValue(HiveVarchar value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return new ColumnValue(TColumnValue.stringVal(tStringValue));
  }

  public static ColumnValue dateValue(Date value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return new ColumnValue(TColumnValue.stringVal(tStringValue));
  }

  public static ColumnValue timestampValue(Timestamp value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return new ColumnValue(TColumnValue.stringVal(tStringValue));
  }

  public static ColumnValue stringValue(HiveDecimal value) {
    TStringValue tStrValue = new TStringValue();
    if (value != null) {
      tStrValue.setValue(value.toString());
    }
    return new ColumnValue(TColumnValue.stringVal(tStrValue));
  }

  public static ColumnValue newColumnValue(Type type, Object value) {
    switch (type) {
    case BOOLEAN_TYPE:
      return booleanValue((Boolean)value);
    case TINYINT_TYPE:
      return byteValue((Byte)value);
    case SMALLINT_TYPE:
      return shortValue((Short)value);
    case INT_TYPE:
      return intValue((Integer)value);
    case BIGINT_TYPE:
      return longValue((Long)value);
    case FLOAT_TYPE:
      return floatValue((Float)value);
    case DOUBLE_TYPE:
      return doubleValue((Double)value);
    case STRING_TYPE:
      return stringValue((String)value);
    case CHAR_TYPE:
      return stringValue((HiveChar)value);
    case VARCHAR_TYPE:
      return stringValue((HiveVarchar)value);
    case DATE_TYPE:
      return dateValue((Date)value);
    case TIMESTAMP_TYPE:
      return timestampValue((Timestamp)value);
    case DECIMAL_TYPE:
      return stringValue(((HiveDecimal)value));
    case BINARY_TYPE:
    case ARRAY_TYPE:
    case MAP_TYPE:
    case STRUCT_TYPE:
    case UNION_TYPE:
    case USER_DEFINED_TYPE:
      return stringValue((String)value);
    default:
      return null;
    }
  }

  public TColumnValue toTColumnValue() {
    return new TColumnValue(tColumnValue);
  }

}
