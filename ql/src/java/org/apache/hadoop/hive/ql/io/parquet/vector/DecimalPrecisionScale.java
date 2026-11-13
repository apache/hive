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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

/**
 * Decimal precision and scale of a column, and the decimal64 store shared by the per-value and dictionary paths.
 */
final class DecimalPrecisionScale {

  private final Type type;
  private final TypeInfo hiveType;

  DecimalPrecisionScale(Type type, TypeInfo hiveType) {
    this.type = type;
    this.hiveType = hiveType;
  }

  /**
   * Fill a {@link DecimalColumnVector} at the Parquet file (logical-type) precision/scale: the scale
   * {@link PerValueUpdater#readDecimal} reads the unscaled bytes at, carried per row by the HiveDecimal.
   */
  void fillDecimalPrecisionScale(DecimalColumnVector c) {
    DecimalTypeInfo dti = getDecimalTypeInfo();
    c.precision = (short) dti.getPrecision();
    c.scale = (short) dti.getScale();
  }

  /**
   * Fill a long-backed {@link Decimal64ColumnVector} at the Hive (table) scale -- the scale every
   * consumer reads {@code c.vector} at, and the only scale at which the unscaled value fits the long.
   * NOT the Parquet file scale from {@link #getDecimalTypeInfo()}: under schema evolution that
   * scale can be larger (e.g. a DECIMAL(38,37) file read as DECIMAL(16,8)) and would overflow.
   */
  void fillDecimal64PrecisionScale(Decimal64ColumnVector c) {
    DecimalTypeInfo dti = hiveType instanceof DecimalTypeInfo hiveDti ? hiveDti : getDecimalTypeInfo();
    c.precision = (short) dti.getPrecision();
    c.scale = (short) dti.getScale();
  }

  /**
   * Decimal precision/scale for this column: from the Parquet decimal logical type when present,
   * otherwise from the Hive type (Parquet stores it as a non-decimal physical type but HMS reports
   * decimal).
   */
  DecimalTypeInfo getDecimalTypeInfo() {
    if (type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation d) {
      return TypeInfoFactory.getDecimalTypeInfo(d.getPrecision(), d.getScale());
    } else if (TypeInfoUtils.getBaseName(hiveType.getTypeName())
        .equalsIgnoreCase(serdeConstants.DECIMAL_TYPE_NAME)) {
      return (DecimalTypeInfo) hiveType;
    }
    throw new UnsupportedOperationException(
        "The underlying Parquet type cannot be converted to Hive Decimal type: " + type);
  }

  /**
   * Store one decimal64 value into {@code c[rowId]} from {@code reader}, NULLing the entry when the
   * value is out of range. {@code fast} selects the identity fast path (raw unscaled long) over the
   * HiveDecimal/byte[] slow path. {@code id >= 0} reads that dictionary entry; a negative {@code id}
   * reads the current page value. Shared by the page ({@link PerValueUpdater#readDecimal64}) and dictionary
   * ({@link DictionaryValues.Decimal64s}) decode loops.
   */
  static void setDecimal64Value(Decimal64ColumnVector c, int rowId, boolean fast,
      ParquetDataColumnReader reader, int id, short valueScale) {
    c.isNull[rowId] = false;
    boolean stored;
    if (fast) {
      // Identity fast path: store the raw unscaled long directly (no HiveDecimal/byte[] per row).
      long v = id >= 0 ? reader.readDecimal64(id) : reader.readDecimal64();
      stored = reader.isValid();
      if (stored) {
        c.vector[rowId] = v;
      }
    } else {
      // set() enforces the column precision/scale and marks the entry NULL if the value does not
      // fit (e.g. schema-evolved data whose larger file scale can't be held at the column scale).
      byte[] bytes = id >= 0 ? reader.readDecimal(id) : reader.readDecimal();
      stored = reader.isValid();
      if (stored) {
        c.set(rowId, bytes, valueScale);
        stored = !c.isNull[rowId];
      }
    }
    if (!stored) {
      c.vector[rowId] = 0;
      PerValueUpdater.setNullValue(c, rowId);
    }
  }
}
