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

package org.apache.hadoop.hive.serde2.lazy;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * LazyUtils.
 *
 */
public final class LazyUtils {

  /**
   * Returns the digit represented by character b.
   *
   * @param b
   *          The ascii code of the character
   * @param radix
   *          The radix
   * @return -1 if it's invalid
   */
  public static int digit(int b, int radix) {
    int r = -1;
    if (b >= '0' && b <= '9') {
      r = b - '0';
    } else if (b >= 'A' && b <= 'Z') {
      r = b - 'A' + 10;
    } else if (b >= 'a' && b <= 'z') {
      r = b - 'a' + 10;
    }
    if (r >= radix) {
      r = -1;
    }
    return r;
  }

  /**
   * returns false, when the bytes definitely cannot be parsed into a base-10
   * Number (Long or a Double)
   * 
   * If it returns true, the bytes might still be invalid, but not obviously.
   */

  public static boolean isNumberMaybe(byte[] buf, int offset, int len) {
    switch (len) {
    case 0:
      return false;
    case 1:
      // space usually
      return Character.isDigit(buf[offset]);
    case 2:
      // \N or -1 (allow latter)
      return Character.isDigit(buf[offset + 1])
          || Character.isDigit(buf[offset + 0]);
    case 4:
      // null or NULL
      if (buf[offset] == 'N' || buf[offset] == 'n') {
        return false;
      }
    }
    // maybe valid - too expensive to check without a parse
    return true;
  }

  /**
   * returns false, when the bytes definitely cannot be parsed into a date/timestamp.
   * 
   * Y2k requirements and dash requirements say the string has to be at least
   * yyyy-m-m = 8 bytes or more minimum; Timestamp needs to be at least 1 byte longer,
   * but the Date check is necessary, but not sufficient.
   */
  public static boolean isDateMaybe(byte[] buf, int offset, int len) {
    // maybe valid - too expensive to check without a parse
    return len >= 8;
  }

  /**
   * Returns -1 if the first byte sequence is lexicographically less than the
   * second; returns +1 if the second byte sequence is lexicographically less
   * than the first; otherwise return 0.
   */
  public static int compare(byte[] b1, int start1, int length1, byte[] b2,
      int start2, int length2) {

    int min = Math.min(length1, length2);

    for (int i = 0; i < min; i++) {
      if (b1[start1 + i] == b2[start2 + i]) {
        continue;
      }
      if (b1[start1 + i] < b2[start2 + i]) {
        return -1;
      } else {
        return 1;
      }
    }

    if (length1 < length2) {
      return -1;
    }
    if (length1 > length2) {
      return 1;
    }
    return 0;
  }

  /**
   * Convert a UTF-8 byte array to String.
   *
   * @param bytes
   *          The byte[] containing the UTF-8 String.
   * @param start
   *          The start position inside the bytes.
   * @param length
   *          The length of the data, starting from "start"
   * @return The unicode String
   */
  public static String convertToString(byte[] bytes, int start, int length) {
    try {
      return Text.decode(bytes, start, length);
    } catch (CharacterCodingException e) {
      return null;
    }
  }

  public static byte[] trueBytes = {(byte) 't', 'r', 'u', 'e'};
  public static byte[] falseBytes = {(byte) 'f', 'a', 'l', 's', 'e'};

  /**
   * Write the bytes with special characters escaped.
   *
   * @param escaped
   *          Whether the data should be written out in an escaped way.
   * @param escapeChar
   *          If escaped, the char for prefixing special characters.
   * @param needsEscape
   *          If escaped, whether a specific character needs escaping. This
   *          array should have size of 256.
   */
  public static void writeEscaped(OutputStream out, byte[] bytes, int start,
      int len, boolean escaped, byte escapeChar, boolean[] needsEscape)
      throws IOException {
    if (escaped) {
      int end = start + len;
      for (int i = start; i <= end; i++) {
        if (i == end || needsEscape[bytes[i] & 0xFF]) {  // Converts negative byte to positive index
          if (i > start) {
            out.write(bytes, start, i - start);
          }

          if (i == end) break;

          out.write(escapeChar);
          if (bytes[i] == '\r') {
            out.write('r');
            start = i + 1;
          } else if (bytes[i] == '\n') {
            out.write('n');
            start = i + 1;
          } else {
            // the current char will be written out later.
            start = i;
          }
        }
      }
    } else {
      out.write(bytes, start, len);
    }
  }

  /**
   * Write out the text representation of a Primitive Object to a UTF8 byte
   * stream.
   *
   * @param out
   *          The UTF8 byte OutputStream
   * @param o
   *          The primitive Object
   * @param needsEscape
   *          Whether a character needs escaping. 
   */
  public static void writePrimitiveUTF8(OutputStream out, Object o,
      PrimitiveObjectInspector oi, boolean escaped, byte escapeChar,
      boolean[] needsEscape) throws IOException {

    PrimitiveObjectInspector.PrimitiveCategory category = oi.getPrimitiveCategory();
    switch (category) {
    case BOOLEAN: {
      boolean b = ((BooleanObjectInspector) oi).get(o);
      if (b) {
        out.write(trueBytes, 0, trueBytes.length);
      } else {
        out.write(falseBytes, 0, falseBytes.length);
      }
      break;
    }
    case BYTE: {
      LazyInteger.writeUTF8(out, ((ByteObjectInspector) oi).get(o));
      break;
    }
    case SHORT: {
      LazyInteger.writeUTF8(out, ((ShortObjectInspector) oi).get(o));
      break;
    }
    case INT: {
      LazyInteger.writeUTF8(out, ((IntObjectInspector) oi).get(o));
      break;
    }
    case LONG: {
      LazyLong.writeUTF8(out, ((LongObjectInspector) oi).get(o));
      break;
    }
    case FLOAT: {
      float f = ((FloatObjectInspector) oi).get(o);
      ByteBuffer b = Text.encode(String.valueOf(f));
      out.write(b.array(), 0, b.limit());
      break;
    }
    case DOUBLE: {
      double d = ((DoubleObjectInspector) oi).get(o);
      ByteBuffer b = Text.encode(String.valueOf(d));
      out.write(b.array(), 0, b.limit());
      break;
    }
    case STRING: {
      Text t = ((StringObjectInspector) oi).getPrimitiveWritableObject(o);
      writeEscaped(out, t.getBytes(), 0, t.getLength(), escaped, escapeChar,
          needsEscape);
      break;
    }
    case CHAR: {
      HiveCharWritable hc = ((HiveCharObjectInspector) oi).getPrimitiveWritableObject(o);
      Text t = hc.getPaddedValue();
      writeEscaped(out, t.getBytes(), 0, t.getLength(), escaped, escapeChar,
          needsEscape);
      break;
    }
    case VARCHAR: {
      HiveVarcharWritable hc = ((HiveVarcharObjectInspector)oi).getPrimitiveWritableObject(o);
      Text t = hc.getTextValue();
      writeEscaped(out, t.getBytes(), 0, t.getLength(), escaped, escapeChar,
          needsEscape);
      break;
    }
    case BINARY: {
      BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
      byte[] toEncode = new byte[bw.getLength()];
      System.arraycopy(bw.getBytes(), 0,toEncode, 0, bw.getLength());
      byte[] toWrite = Base64.encodeBase64(toEncode);
      out.write(toWrite, 0, toWrite.length);
      break;
    }
    case DATE: {
      LazyDate.writeUTF8(out,
          ((DateObjectInspector) oi).getPrimitiveWritableObject(o));
      break;
    }
    case TIMESTAMP: {
      LazyTimestamp.writeUTF8(out,
          ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o));
      break;
    }
    case TIMESTAMPLOCALTZ: {
      LazyTimestampLocalTZ.writeUTF8(out, ((TimestampLocalTZObjectInspector) oi).
          getPrimitiveWritableObject(o));
      break;
    }
    case INTERVAL_YEAR_MONTH: {
      LazyHiveIntervalYearMonth.writeUTF8(out,
          ((HiveIntervalYearMonthObjectInspector) oi).getPrimitiveWritableObject(o));
      break;
    }
    case INTERVAL_DAY_TIME: {
      LazyHiveIntervalDayTime.writeUTF8(out,
          ((HiveIntervalDayTimeObjectInspector) oi).getPrimitiveWritableObject(o));
      break;
    }
    case DECIMAL: {
      HiveDecimalObjectInspector decimalOI = (HiveDecimalObjectInspector) oi;
      LazyHiveDecimal.writeUTF8(out,
        decimalOI.getPrimitiveJavaObject(o), decimalOI.scale());
      break;
    }
    default: {
      throw new RuntimeException("Unknown primitive type: " + category);
    }
    }
  }

  /**
   * Write out a binary representation of a PrimitiveObject to a byte stream.
   *
   * @param out ByteStream.Output, an unsynchronized version of ByteArrayOutputStream, used as a
   *            backing buffer for the the DataOutputStream
   * @param o the PrimitiveObject
   * @param oi the PrimitiveObjectInspector
   * @throws IOException on error during the write operation
   */
  public static void writePrimitive(
      OutputStream out,
      Object o,
      PrimitiveObjectInspector oi) throws IOException {

    DataOutputStream dos = new DataOutputStream(out);

    try {
      switch (oi.getPrimitiveCategory()) {
      case BOOLEAN:
        boolean b = ((BooleanObjectInspector) oi).get(o);
        dos.writeBoolean(b);
        break;

      case BYTE:
        byte bt = ((ByteObjectInspector) oi).get(o);
        dos.writeByte(bt);
        break;

      case SHORT:
        short s = ((ShortObjectInspector) oi).get(o);
        dos.writeShort(s);
        break;

      case INT:
        int i = ((IntObjectInspector) oi).get(o);
        dos.writeInt(i);
        break;

      case LONG:
        long l = ((LongObjectInspector) oi).get(o);
        dos.writeLong(l);
        break;

      case FLOAT:
        float f = ((FloatObjectInspector) oi).get(o);
        dos.writeFloat(f);
        break;

      case DOUBLE:
        double d = ((DoubleObjectInspector) oi).get(o);
        dos.writeDouble(d);
        break;

      case BINARY: {
        BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
        out.write(bw.getBytes(), 0, bw.getLength());
        break;
      }

      case DECIMAL: {
        HiveDecimalWritable hdw = ((HiveDecimalObjectInspector) oi).getPrimitiveWritableObject(o);
        hdw.write(dos);
        break;
      }

      default:
        throw new RuntimeException("Hive internal error.");
      }
    } finally {
      // closing the underlying ByteStream should have no effect, the data should still be
      // accessible
      dos.close();
    }
  }

  public static int hashBytes(byte[] data, int start, int len) {
    int hash = 1;
    for (int i = start; i < len; i++) {
      hash = (31 * hash) + data[i];
    }
    return hash;
  }



  /**
   * gets a byte[] with copy of data from source BytesWritable
   * @param sourceBw - source BytesWritable
   */
  public static byte[] createByteArray(BytesWritable sourceBw){
    //TODO should replace with BytesWritable.copyData() once Hive
    //removes support for the Hadoop 0.20 series.
    return Arrays.copyOf(sourceBw.getBytes(), sourceBw.getLength());
  }

  /**
   * Utility function to get separator for current level used in serialization.
   * Used to get a better log message when out of bound lookup happens
   * @param separators - array of separators byte, byte at index x indicates
   *  separator used at that level
   * @param level - nesting level
   * @return separator at given level
   * @throws SerDeException
   */
  static byte getSeparator(byte[] separators, int level) throws SerDeException {
    try{
      return separators[level];
    }catch(ArrayIndexOutOfBoundsException e){
      String msg = "Number of levels of nesting supported for " +
          "LazySimpleSerde is " + (separators.length - 1) +
          " Unable to work with level " + level;
      
      String txt = ". Use %s serde property for tables using LazySimpleSerde.";
      
      if(separators.length < 9){
        msg += String.format(txt, LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS);
      } else if (separators.length < 25) {
      	msg += String.format(txt, LazySerDeParameters.SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS);
      }
      
      throw new SerDeException(msg, e);
    }
  }

  public static void copyAndEscapeStringDataToText(byte[] inputBytes, int start, int length,
      byte escapeChar, Text data) {

    // First calculate the length of the output string
    int outputLength = 0;
    for (int i = 0; i < length; i++) {
      if (inputBytes[start + i] != escapeChar) {
        outputLength++;
      } else {
        outputLength++;
        i++;
      }
    }

    // Copy the data over, so that the internal state of Text will be set to
    // the required outputLength.
    data.set(inputBytes, start, outputLength);

    // We need to copy the data byte by byte only in case the
    // "outputLength < length" (which means there is at least one escaped
    // byte.
    if (outputLength < length) {
      int k = 0;
      byte[] outputBytes = data.getBytes();
      for (int i = 0; i < length; i++) {
        byte b = inputBytes[start + i];
        if (b == escapeChar && i < length - 1) {
          ++i;
          // Check if it's '\r' or '\n'
          if (inputBytes[start + i] == 'r') {
            outputBytes[k++] = '\r';
          } else if (inputBytes[start + i] == 'n') {
            outputBytes[k++] = '\n';
          } else {
            // get the next byte
            outputBytes[k++] = inputBytes[start + i];
          }
        } else {
          outputBytes[k++] = b;
        }
      }
      assert (k == outputLength);
    }
  }

  /**
   * Return the byte value of the number string.
   *
   * @param altValue
   *          The string containing a number.
   * @param defaultVal
   *          If the altValue does not represent a number, return the
   *          defaultVal.
   */
  public static byte getByte(String altValue, byte defaultVal) {
    if (altValue != null && altValue.length() > 0) {
      try {
        return Byte.parseByte(altValue);
      } catch (NumberFormatException e) {
        return (byte) altValue.charAt(0);
      }
    }
    return defaultVal;
  }
  
  private LazyUtils() {
    // prevent instantiation
  }
}
