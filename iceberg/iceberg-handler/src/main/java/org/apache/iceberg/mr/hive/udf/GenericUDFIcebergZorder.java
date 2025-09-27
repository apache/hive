/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.udf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.iceberg.util.ZOrderByteUtils;

/**
 * Hive UDF to compute the Z-order value of given input columns using Iceberg's ZOrderByteUtils.
 * Supports various primitive types and converts inputs into interleaved binary representation.
 */
@Description(name = "iceberg_zorder",
        value = "_FUNC_(value) - " +
                "Returns the z-value calculated by Iceberg ZOrderByteUtils class")
public class GenericUDFIcebergZorder extends GenericUDF {
  private PrimitiveObjectInspector[] argOIs;
  // For variable-length types (e.g., strings), how many bytes contribute to z-order
  private final int varLengthContribution = 8;
  private transient ByteBuffer[] reUseBuffer;
  private static final int MAX_OUTPUT_SIZE = Integer.MAX_VALUE;

  /**
   * Initializes the UDF, validating argument types are primitives and preparing buffers.
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentException("iceberg_zorder requires at least 2 arguments");
    }
    argOIs = new PrimitiveObjectInspector[arguments.length];
    reUseBuffer = new ByteBuffer[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      if (!(arguments[i] instanceof PrimitiveObjectInspector poi)) {
        throw new UDFArgumentTypeException(i, "Only primitive types supported for z-order");
      }
      argOIs[i] = poi;
    }
    return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
  }

  /**
   * Evaluates the UDF by converting input values to ordered bytes, interleaving them,
   * and returning the resulting Z-order binary value.
   */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    byte[][] inputs = new byte[arguments.length][];
    int totalLength = 0;

    for (int i = 0; i < arguments.length; i++) {
      byte[] orderedBytes = convertToOrderedBytes(arguments[i].get(), argOIs[i], i);
      inputs[i] = orderedBytes;
      totalLength += orderedBytes.length;
    }

    int outputLength = Math.min(totalLength, MAX_OUTPUT_SIZE);
    ByteBuffer buffer = ByteBuffer.allocate(outputLength);

    byte[] interleaved = ZOrderByteUtils.interleaveBits(inputs, outputLength, buffer);
    return new BytesWritable(interleaved);
  }

  @Override
  public String getDisplayString(String[] children) {
    return "iceberg_zorder(" + String.join(", ", children) + ")";
  }

  /**
   * Converts a single input value to its ordered byte representation based on type.
   * @return fixed-length byte arrays to be used in interleaving.
   */
  private byte[] convertToOrderedBytes(Object value, PrimitiveObjectInspector oi,
                                       int position) throws HiveException {
    if (value == null) {
      // For NULL values, we have primitive buffer size of 8 with values of 0
      return ByteBuffer.wrap(new byte[ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE]).array();
    }

    if (reUseBuffer[position] == null) {
      reUseBuffer[position] = ByteBuffer.allocate(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE);
    }
    switch (oi.getPrimitiveCategory()) {
      case BOOLEAN:
        boolean boolValue = (Boolean) oi.getPrimitiveJavaObject(value);
        return ZOrderByteUtils.intToOrderedBytes(boolValue ? 1 : 0, reUseBuffer[position]).array();

      case BYTE:
        byte byteValue = (Byte) oi.getPrimitiveJavaObject(value);
        return ZOrderByteUtils.tinyintToOrderedBytes(byteValue, reUseBuffer[position]).array();

      case SHORT:
        short shortValue = (Short) oi.getPrimitiveJavaObject(value);
        return ZOrderByteUtils.shortToOrderedBytes(shortValue, reUseBuffer[position]).array();

      case INT:
        int intValue = (Integer) oi.getPrimitiveJavaObject(value);
        return ZOrderByteUtils.intToOrderedBytes(intValue, reUseBuffer[position]).array();

      case LONG:
        long longValue = (Long) oi.getPrimitiveJavaObject(value);
        return ZOrderByteUtils.longToOrderedBytes(longValue, reUseBuffer[position]).array();

      case FLOAT:
        float floatValue = (Float) oi.getPrimitiveJavaObject(value);
        return ZOrderByteUtils.floatToOrderedBytes(floatValue, reUseBuffer[position]).array();

      case DOUBLE:
        double doubleValue = (Double) oi.getPrimitiveJavaObject(value);
        return ZOrderByteUtils.doubleToOrderedBytes(doubleValue, reUseBuffer[position]).array();

      case DATE:
        // Get data in epoch seconds and convert it to long
        Object dateValue = oi.getPrimitiveJavaObject(value);
        long dateInSeconds;
        if (dateValue instanceof java.sql.Date dd) {
          dateInSeconds = dd.getTime() / 1000L;
        } else if (dateValue instanceof org.apache.hadoop.hive.common.type.Date dd) {
          dateInSeconds = dd.toEpochSecond();
        } else {
          throw new HiveException("Unsupported DATE backing type: " + dateValue.getClass());
        }
        return ZOrderByteUtils.longToOrderedBytes(dateInSeconds, reUseBuffer[position]).array();

      case TIMESTAMP:
        Object tsValue = oi.getPrimitiveJavaObject(value);
        long tsInSeconds;
        if (tsValue instanceof org.apache.hadoop.hive.common.type.Timestamp ts) {
          tsInSeconds = ts.toEpochSecond();
        } else if (tsValue instanceof java.sql.Timestamp ts) {
          tsInSeconds = ts.getTime() / 1000L;
        } else {
          throw new HiveException("Unsupported TIMESTAMP backing type: " + tsValue.getClass());
        }
        return ZOrderByteUtils.longToOrderedBytes(tsInSeconds, reUseBuffer[position]).array();

      case CHAR:
      case VARCHAR:
      case STRING:
        String strVal = String.valueOf(oi.getPrimitiveJavaObject(value));
        return ZOrderByteUtils.stringToOrderedBytes(strVal, varLengthContribution,
                reUseBuffer[position], StandardCharsets.UTF_8.newEncoder()).array();

      default:
        throw new HiveException("Unsupported type in z-order: " + oi.getPrimitiveCategory());
    }
  }
}
