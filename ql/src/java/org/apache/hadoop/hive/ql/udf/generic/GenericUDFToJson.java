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
package org.apache.hadoop.hive.ql.udf.generic;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VariantVal;
import org.apache.hadoop.hive.serde2.typeinfo.VariantUtil;


public class GenericUDFToJson extends GenericUDF {
  private StructObjectInspector soi;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("to_json takes exactly 1 argument");
    }
    if (!(arguments[0] instanceof StructObjectInspector)) {
      throw new UDFArgumentTypeException(0, "Argument must be VARIANT (struct<metadata:binary,value:binary>)");
    }
    soi = (StructObjectInspector) arguments[0];
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object variantObj = arguments[0].get();
    if (variantObj == null) {
      return null;
    }
    VariantVal variantVal = VariantVal.from(soi.getStructFieldsDataAsList(variantObj));

    if (variantVal.getMetadata() == null || variantVal.getValue() == null) {
      return null;
    }
    try {
      return VariantJsonDecoder.toJson(variantVal.getMetadata(), variantVal.getValue());
    } catch (Exception e) {
      throw new HiveException("Error decoding variant to JSON", e);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "to_json(" + children[0] + ")";
  }

  static class VariantJsonDecoder {

    private static String toJson(byte[] metadataBytes, byte[] valueBytes) throws IOException {
      if (valueBytes == null || valueBytes.length == 0) {
        return "null";
      }
      // Parse metadata first
      List<String> dictionary = VariantUtil.parseMetadata(metadataBytes);

      StringBuilder sb = new StringBuilder();
      ByteBuffer valueBuf = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);

      decodeValue(valueBuf, dictionary, sb);
      return sb.toString();
    }

    private static void decodeValue(ByteBuffer valueBuf, List<String> dictionary, StringBuilder sb) throws IOException {
      if (!valueBuf.hasRemaining()) {
        throw new IOException("Unexpected end of value buffer");
      }

      byte valueMetadata = valueBuf.get();
      int basicType = valueMetadata & 0x03;
      int valueHeader = (valueMetadata >> 2) & 0x3F;

      switch (basicType) {
        case 0: // Primitive type
          decodePrimitive(valueHeader, valueBuf, sb);
          break;
        case 1: // Short string
          decodeShortString(valueHeader, valueBuf, sb);
          break;
        case 2: // Object
          decodeObject(valueHeader, valueBuf, dictionary, sb);
          break;
        case 3: // Array
          decodeArray(valueHeader, valueBuf, dictionary, sb);
          break;
        default:
          throw new IOException("Unknown basic type: " + basicType);
      }
    }

    private static void decodePrimitive(int primitiveHeader, ByteBuffer buf, StringBuilder sb) throws IOException {
      switch (primitiveHeader) {
        case 0: // null
          sb.append("null");
          break;
        case 1: // boolean true
          sb.append("true");
          break;
        case 2: // boolean false
          sb.append("false");
          break;
        case 3: // int8
          sb.append(buf.get());
          break;
        case 4: // int16
          sb.append(buf.getShort());
          break;
        case 5: // int32
          sb.append(buf.getInt());
          break;
        case 6: // int64
          sb.append(buf.getLong());
          break;
        case 7: // double
          sb.append(buf.getDouble());
          break;
        case 8: // decimal4
        case 9: // decimal8
        case 10: // decimal16
          decodeDecimal(primitiveHeader, buf, sb);
          break;
        case 11: // date
          decodeDate(buf, sb);
          break;
        case 12: // timestamp with timezone (MICROS)
        case 13: // timestamp without timezone (MICROS)
          decodeTimestamp(primitiveHeader, buf, sb);
          break;
        case 14: // float
          sb.append(buf.getFloat());
          break;
        case 15: // binary
          decodeBinary(buf, sb);
          break;
        case 16: // string
          decodeString(buf, sb);
          break;
        default:
          throw new IOException("Unknown primitive type: " + primitiveHeader);
      }
    }

    private static void decodeObject(int objectHeader, ByteBuffer buf, List<String> dictionary, StringBuilder sb)
        throws IOException {
      VariantUtil.decodeObject(objectHeader, buf, dictionary, sb, VariantJsonDecoder::decodeValue);
    }

    private static void decodeArray(int arrayHeader, ByteBuffer buf, List<String> dictionary, StringBuilder sb)
        throws IOException {
      VariantUtil.decodeArray(arrayHeader, buf, dictionary, sb, VariantJsonDecoder::decodeValue);
    }

    // Helper methods for specific types
    private static void decodeDecimal(int primitiveHeader, ByteBuffer buf, StringBuilder sb) throws IOException {
      int scale = buf.get() & 0xFF; // 1-byte scale (unsigned)
      BigInteger unscaled = VariantUtil.decodeDecimalUnscaled(primitiveHeader, buf);
      BigDecimal decimal = new BigDecimal(unscaled, scale);
      sb.append(decimal);
    }

    private static void decodeDate(ByteBuffer buf, StringBuilder sb) {
      sb.append("\"")
        .append(VariantUtil.decodeDate(buf))
        .append("\"");
    }

    private static void decodeTimestamp(int timestampType, ByteBuffer buf, StringBuilder sb) throws IOException {
      sb.append("\"")
        .append(VariantUtil.decodeTimestamp(timestampType, buf))
        .append("\"");
    }

    private static void decodeBinary(ByteBuffer buf, StringBuilder sb) {
      sb.append("\"")
        .append(VariantUtil.decodeBinary(buf))
        .append("\"");
    }

    private static void decodeString(ByteBuffer buf, StringBuilder sb) {
      String str = VariantUtil.decodeString(buf);
      sb.append("\"")
        .append(VariantUtil.escapeJson(str))
        .append("\"");
    }

    private static void decodeShortString(int length, ByteBuffer buf, StringBuilder sb) {
      String str = VariantUtil.decodeShortString(length, buf);
      sb.append("\"")
        .append(VariantUtil.escapeJson(str))
        .append("\"");
    }
  }
}