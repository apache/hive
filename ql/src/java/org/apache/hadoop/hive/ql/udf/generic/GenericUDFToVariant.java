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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a value to a variant, preserving the value's type as the corresponding variant
 * primitive; the typed counterpart of parse_json for types JSON cannot express (binary,
 * date, timestamp, decimal). Mirrors Spark's cast to variant: primitive, array and variant
 * inputs are accepted; struct and map inputs are not. Timestamps are truncated to
 * microseconds, the variant format's precision.
 */
@Description(name = "variant", value = "_FUNC_(x) - Converts x to a VARIANT, preserving its type", extended = """
    Example:
      > SELECT CAST(unhex('DEADBEEF') AS VARIANT);
      "3q2+7w==\"""")
public class GenericUDFToVariant extends GenericUDF {
  private static final String VARIANT_TYPE_NAME = VariantObjectInspector.get().getTypeName();

  private transient ObjectInspector inputOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException("variant requires one argument");
    }
    checkSupported(arguments[0]);
    inputOI = arguments[0];
    return ObjectInspectorFactory.getVariantObjectInspector();
  }

  private static void checkSupported(ObjectInspector oi) throws UDFArgumentException {
    boolean supported = switch (oi.getCategory()) {
      case PRIMITIVE -> switch (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
          ((PrimitiveObjectInspector) oi).getPrimitiveCategory())) {
        case INTERVAL_GROUP, UNKNOWN_GROUP -> false;
        default -> true;
      };
      case LIST -> {
        checkSupported(((ListObjectInspector) oi).getListElementObjectInspector());
        yield true;
      }
      case STRUCT -> VARIANT_TYPE_NAME.equals(oi.getTypeName());
      default -> false;
    };
    if (!supported) {
      throw new UDFArgumentTypeException(0, "Cannot cast " + oi.getTypeName() + " to VARIANT");
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object value = arguments[0].get();
    if (value == null) {
      return null;
    }
    try {
      VariantBuilder builder = new VariantBuilder(false);
      append(builder, value, inputOI);
      Variant variant = builder.result();
      return List.of(variant.getMetadata(), variant.getValue());
    } catch (RuntimeException e) {
      throw new HiveException("Cannot cast value to VARIANT: " + e.getMessage(), e);
    }
  }

  private static void append(VariantBuilder builder, Object o, ObjectInspector oi) throws HiveException {
    if (o == null) {
      builder.appendNull();
      return;
    }
    switch (oi.getCategory()) {
      case PRIMITIVE -> appendPrimitive(builder, o, (PrimitiveObjectInspector) oi);
      case LIST -> appendArray(builder, o, (ListObjectInspector) oi);
      // the variant shape, guaranteed by initialize: embed as-is
      case STRUCT -> builder.appendVariant(Variant.from(((StructObjectInspector) oi).getStructFieldsDataAsList(o)));
      default -> throw new HiveException("Cannot cast " + oi.getTypeName() + " to VARIANT");
    }
  }

  private static void appendPrimitive(VariantBuilder builder, Object o, PrimitiveObjectInspector poi)
      throws HiveException {
    switch (poi.getPrimitiveCategory()) {
      case VOID -> builder.appendNull();
      case BOOLEAN -> builder.appendBoolean(((BooleanObjectInspector) poi).get(o));
      case BYTE, SHORT, INT, LONG -> builder.appendLong(PrimitiveObjectInspectorUtils.getLong(o, poi));
      case FLOAT -> builder.appendFloat(((FloatObjectInspector) poi).get(o));
      case DOUBLE -> builder.appendDouble(((DoubleObjectInspector) poi).get(o));
      case DECIMAL ->
          builder.appendDecimal(((HiveDecimalObjectInspector) poi).getPrimitiveJavaObject(o).bigDecimalValue());
      case STRING, CHAR, VARCHAR -> builder.appendString(PrimitiveObjectInspectorUtils.getString(o, poi));
      case BINARY -> builder.appendBinary(((BinaryObjectInspector) poi).getPrimitiveJavaObject(o));
      case DATE -> builder.appendDate(((DateObjectInspector) poi).getPrimitiveJavaObject(o).toEpochDay());
      case TIMESTAMP ->
          builder.appendTimestampNtz(((TimestampObjectInspector) poi).getPrimitiveJavaObject(o).toEpochMicro());
      case TIMESTAMPLOCALTZ -> {
        Instant instant = ((TimestampLocalTZObjectInspector) poi).getPrimitiveJavaObject(o)
            .getZonedDateTime().toInstant();
        builder.appendTimestamp(instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000);
      }
      default -> throw new HiveException("Cannot cast " + poi.getTypeName() + " to VARIANT");
    }
  }

  private static void appendArray(VariantBuilder builder, Object o, ListObjectInspector loi) throws HiveException {
    int start = builder.getWritePos();
    int length = loi.getListLength(o);
    List<Integer> offsets = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      offsets.add(builder.getWritePos() - start);
      append(builder, loi.getListElement(o, i), loi.getListElementObjectInspector());
    }
    builder.finishWritingArray(start, offsets);
  }

  @Override
  public String getDisplayString(String[] children) {
    return "CAST( " + children[0] + " AS VARIANT)";
  }
}
