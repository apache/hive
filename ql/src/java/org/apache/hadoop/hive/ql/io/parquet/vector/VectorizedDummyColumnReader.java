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

package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A dummy vectorized parquet reader used for schema evolution.
 * If a default value is provided, it returns that value for the entire batch.
 * Otherwise, it returns nulls.
 */
public class VectorizedDummyColumnReader extends BaseVectorizedColumnReader {

  private final Object defaultValue;

  public VectorizedDummyColumnReader(Object defaultValue) {
    super();
    this.defaultValue = defaultValue;
  }

  @Override
  public void readBatch(int total, ColumnVector col, TypeInfo typeInfo) throws IOException {

    // Case 1: No default → (all nulls)
    if (defaultValue == null) {
      Arrays.fill(col.isNull, true);
      col.noNulls = false;
      col.isRepeating = true;
      return;
    }

    // Case 2: We have a default → fill with constant value
    col.isRepeating = true;
    col.noNulls = true;
    col.isNull[0] = false;

    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        fillPrimitive(col, (PrimitiveTypeInfo) typeInfo, defaultValue);
        break;

      case STRUCT:
      case LIST:
      case MAP:
        throw new IOException(
          "Default values for complex types not supported yet in DummyColumnReader: " +
            typeInfo.getTypeName());

      default:
        throw new IOException(
          "Unsupported type category in DummyColumnReader: " +
            typeInfo.getCategory());
    }
  }

  /* -------------------------
     Primitive/leaf-type filler
     ------------------------- */
  private void fillPrimitive(ColumnVector col, PrimitiveTypeInfo ti, Object value)
      throws IOException {

    switch (ti.getPrimitiveCategory()) {

      case BOOLEAN:
        ((LongColumnVector) col).vector[0] = ((Boolean) value) ? 1 : 0;
        return;

      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        ((LongColumnVector) col).vector[0] = ((Number) value).longValue();
        return;

      case FLOAT:
      case DOUBLE:
        ((DoubleColumnVector) col).vector[0] = ((Number) value).doubleValue();
        return;

      case STRING:
      case VARCHAR:
      case CHAR:
        byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
        ((BytesColumnVector) col).setRef(0, bytes, 0, bytes.length);
        return;

      case DECIMAL:
        DecimalColumnVector dcv = (DecimalColumnVector) col;
        dcv.set(0, HiveDecimal.create(value.toString()));
        return;

      case TIMESTAMP: {
        TimestampColumnVector tcv = (TimestampColumnVector) col;

        long micros = (Long) value;
        long seconds = micros / 1_000_000L;
        long nanos = (micros % 1_000_000L) * 1000L;
        tcv.time[0] = seconds * 1000L;
        tcv.nanos[0] = (int) nanos;

        return;
      }

      case DATE: {
        LongColumnVector lcv = (LongColumnVector) col;
        lcv.vector[0] = ((Number) value).intValue();
        return;
      }

      default:
        throw new IOException(
          "Unsupported primitive type in DummyColumnReader: "
            + ti.getPrimitiveCategory());
    }
  }
}
