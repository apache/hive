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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * TimestampWritable
 * Writable equivalent of java.sq.Timestamp
 *
 * Timestamps are of the format
 *    YYYY-MM-DD HH:MM:SS.[fff...]
 *
 * We encode Unix timestamp in seconds in 4 bytes, using the MSB to signify
 * whether the timestamp has a fractional portion.
 *
 * The fractional portion is reversed, and encoded as a VInt
 * so timestamps with less precision use fewer bytes.
 *
 *      0.1    -> 1
 *      0.01   -> 10
 *      0.001  -> 100
 *
 */
public class TimestampWritable implements WritableComparable<TimestampWritable> {
  static final private Log LOG = LogFactory.getLog(TimestampWritable.class);

  static final public byte[] nullBytes = {0x0, 0x0, 0x0, 0x0};

  private static final int NO_DECIMAL_MASK = 0x7FFFFFFF;
  private static final int HAS_DECIMAL_MASK = 0x80000000;

  private static final DateFormat dateFormat =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private Timestamp timestamp = new Timestamp(0);

  /**
   * true if data is stored in timestamp field rather than byte arrays.
   *      allows for lazy conversion to bytes when necessary
   * false otherwise
   */
  private boolean bytesEmpty;
  private boolean timestampEmpty;

  /* Allow use of external byte[] for efficiency */
  private byte[] currentBytes;
  private final byte[] internalBytes = new byte[9];
  private byte[] externalBytes;
  private int offset;

  /* Reused to read VInts */
  static private final VInt vInt = new VInt();

  /* Constructors */
  public TimestampWritable() {
    Arrays.fill(internalBytes, (byte) 0x0);
    bytesEmpty = false;
    currentBytes = internalBytes;
    offset = 0;

    clearTimestamp();
  }

  public TimestampWritable(byte[] bytes, int offset) {
    set(bytes, offset);
  }

  public TimestampWritable(TimestampWritable t) {
    this(t.getBytes(), 0);
  }

  public TimestampWritable(Timestamp t) {
    set(t);
  }

  public void set(byte[] bytes, int offset) {
    externalBytes = bytes;
    this.offset = offset;
    bytesEmpty = false;
    currentBytes = externalBytes;

    clearTimestamp();
  }

  public void set(Timestamp t) {
    if (t == null) {
      timestamp.setTime(0);
      timestamp.setNanos(0);
      return;
    }
    this.timestamp = t;
    bytesEmpty = true;
    timestampEmpty = false;
  }

  public void set(TimestampWritable t) {
    if (t.bytesEmpty) {
      set(t.getTimestamp());
      return;
    }
    if (t.currentBytes == t.externalBytes) {
      set(t.currentBytes, t.offset);
    } else {
      set(t.currentBytes, 0);
    }
  }

  private void clearTimestamp() {
    timestampEmpty = true;
  }

  public void writeToByteStream(Output byteStream) {
    checkBytes();
    byteStream.write(currentBytes, offset, getTotalLength());
  }

  /**
   *
   * @return seconds corresponding to this TimestampWritable
   */
  public int getSeconds() {
    if (bytesEmpty) {
      return (int) (timestamp.getTime() / 1000);
    }
    return TimestampWritable.getSeconds(currentBytes, offset);
  }

  /**
   *
   * @return nanoseconds in this TimestampWritable
   */
  public int getNanos() {
    if (!timestampEmpty) {
      return timestamp.getNanos();
    }

    return hasDecimal() ? TimestampWritable.getNanos(currentBytes, offset+4) : 0;
  }

  /**
   *
   * @return length of serialized TimestampWritable data
   */
  private int getTotalLength() {
    return 4 + getDecimalLength();
  }

  /**
   *
   * @return number of bytes the variable length decimal takes up
   */
  private int getDecimalLength() {
    checkBytes();
    return hasDecimal() ? WritableUtils.decodeVIntSize(currentBytes[offset+4]) : 0;
  }

  public Timestamp getTimestamp() {
    if (timestampEmpty) {
      populateTimestamp();
    }
    return timestamp;
  }

  /**
   * Used to create copies of objects
   * @return a copy of the internal TimestampWritable byte[]
   */
  public byte[] getBytes() {
    checkBytes();

    int len = getTotalLength();
    byte[] b = new byte[len];

    System.arraycopy(currentBytes, offset, b, 0, len);
    return b;
  }

  /**
   * @return byte[] representation of TimestampWritable that is binary
   * sortable (4 byte seconds, 4 bytes for nanoseconds)
   */
  public byte[] getBinarySortable() {
    byte[] b = new byte[8];
    int nanos = getNanos();
    int seconds = HAS_DECIMAL_MASK | getSeconds();
    intToBytes(seconds, b, 0);
    intToBytes(nanos, b, 4);
    return b;
  }

  /**
   * Given a byte[] that has binary sortable data, initialize the internal
   * structures to hold that data
   * @param bytes
   * @param offset
   */
  public void setBinarySortable(byte[] bytes, int offset) {
    int seconds = bytesToInt(bytes, offset);
    int nanos = bytesToInt(bytes, offset+4);
    if (nanos == 0) {
      seconds &= NO_DECIMAL_MASK;
    } else {
      seconds |= HAS_DECIMAL_MASK;
    }
    intToBytes(seconds, internalBytes, 0);
    setNanosBytes(nanos, internalBytes, 4);
    currentBytes = internalBytes;
    this.offset = 0;
  }

  /**
   * The data of TimestampWritable can be stored either in a byte[]
   * or in a Timestamp object. Calling this method ensures that the byte[]
   * is populated from the Timestamp object if previously empty.
   */
  private void checkBytes() {
    if (bytesEmpty) {
      // Populate byte[] from Timestamp
      convertTimestampToBytes(timestamp, internalBytes, 0);
      offset = 0;
      currentBytes = internalBytes;
      bytesEmpty = false;
    }
  }

  /**
   *
   * @return double representation of the timestamp, accurate to nanoseconds
   */
  public double getDouble() {
    double seconds, nanos;
    if (bytesEmpty) {
      seconds = timestamp.getTime() / 1000;
      nanos = timestamp.getNanos();
    } else {
      seconds = getSeconds();
      nanos = getNanos();
    }
    return seconds + ((double) nanos) / 1000000000;
  }



  public void readFields(DataInput in) throws IOException {
    in.readFully(internalBytes, 0, 4);
    if (TimestampWritable.hasDecimal(internalBytes[0])) {
      in.readFully(internalBytes, 4, 1);
      int len = (byte) WritableUtils.decodeVIntSize(internalBytes[4]);
      in.readFully(internalBytes, 5, len-1);
    }
    currentBytes = internalBytes;
    this.offset = 0;
  }

  public void write(OutputStream out) throws IOException {
    checkBytes();
    out.write(currentBytes, offset, getTotalLength());
  }

  public void write(DataOutput out) throws IOException {
    write((OutputStream) out);
  }

  public int compareTo(TimestampWritable t) {
    checkBytes();
    int s1 = this.getSeconds();
    int s2 = t.getSeconds();
    if (s1 == s2) {
      int n1 = this.getNanos();
      int n2 = t.getNanos();
      if (n1 == n2) {
        return 0;
      }
      return n1 - n2;
    } else {
      return s1 - s2;
    }
  }

  @Override
  public boolean equals(Object o) {
    return compareTo((TimestampWritable) o) == 0;
  }

  @Override
  public String toString() {
    if (timestampEmpty) {
      populateTimestamp();
    }

    String timestampString = timestamp.toString();
    if (timestampString.length() > 19) {
      if (timestampString.length() == 21) {
        if (timestampString.substring(19).compareTo(".0") == 0) {
          return dateFormat.format(timestamp);
        }
      }
      return dateFormat.format(timestamp) + timestampString.substring(19);
    }

    return dateFormat.format(timestamp);
  }

  @Override
  public int hashCode() {
    long seconds = getSeconds();
    seconds <<= 32;
    seconds |= getNanos();
    return (int) ((seconds >>> 32) ^ seconds);
  }

  private void populateTimestamp() {
    long seconds = getSeconds();
    int nanos = getNanos();
    timestamp.setTime(seconds * 1000);
    timestamp.setNanos(nanos);
  }

  /** Static methods **/

  /**
   * Gets seconds stored as integer at bytes[offset]
   * @param bytes
   * @param offset
   * @return the number of seconds
   */
  public static int getSeconds(byte[] bytes, int offset) {
    return NO_DECIMAL_MASK & bytesToInt(bytes, offset);
  }

  public static int getNanos(byte[] bytes, int offset) {
    LazyBinaryUtils.readVInt(bytes, offset, vInt);
    int val = vInt.value;
    int len = (int) Math.floor(Math.log10(val)) + 1;

    // Reverse the value
    int tmp = 0;
    while (val != 0) {
      tmp *= 10;
      tmp += val % 10;
      val /= 10;
    }
    val = tmp;

    if (len < 9) {
      val *= Math.pow(10, 9 - len);
    }
    return val;
  }

  /**
   * Writes a Timestamp's serialized value to byte array b at
   * @param t
   * @param b
   */
  public static void convertTimestampToBytes(Timestamp t, byte[] b,
      int offset) {
    if (b.length < 9) {
      LOG.error("byte array too short");
    }
    long millis = t.getTime();
    int nanos = t.getNanos();

    boolean hasDecimal = nanos != 0 && setNanosBytes(nanos, b, offset+4);
    setSecondsBytes(millis, b, offset, hasDecimal);
  }

  /**
   * Given an integer representing seconds, write its serialized
   * value to the byte array b at offset
   * @param millis
   * @param b
   * @param offset
   * @param hasDecimal
   */
  private static void setSecondsBytes(long millis, byte[] b, int offset, boolean hasDecimal) {
    int seconds = (int) (millis / 1000);

    if (!hasDecimal) {
      seconds &= NO_DECIMAL_MASK;
    } else {
      seconds |= HAS_DECIMAL_MASK;
    }

    intToBytes(seconds, b, offset);
  }

  /**
   * Given an integer representing nanoseconds, write its serialized
   * value to the byte array b at offset
   *
   * @param nanos
   * @param b
   * @param offset
   * @return
   */
  private static boolean setNanosBytes(int nanos, byte[] b, int offset) {
    int decimal = 0;
    if (nanos != 0) {
      int counter = 0;
      while (counter < 9) {
        decimal *= 10;
        decimal += nanos % 10;
        nanos /= 10;
        counter++;
      }
    }

    LazyBinaryUtils.writeVLongToByteArray(b, offset, decimal);
    return decimal != 0;
  }

  /**
   * Interprets a float as a unix timestamp and returns a Timestamp object
   * @param f
   * @return the equivalent Timestamp object
   */
  public static Timestamp floatToTimestamp(float f) {
    return doubleToTimestamp((double) f);
  }

  public static Timestamp doubleToTimestamp(double f) {
    long seconds = (long) f;

    // We must ensure the exactness of the double's fractional portion.
    // 0.6 as the fraction part will be converted to 0.59999... and
    // significantly reduce the savings from binary serializtion
    BigDecimal bd = new BigDecimal(String.valueOf(f));
    bd = bd.subtract(new BigDecimal(seconds)).multiply(new BigDecimal(1000000000));
    int nanos = bd.intValue();

    // Convert to millis
    long millis = seconds * 1000;
    Timestamp t = new Timestamp(millis);

    // Set remaining fractional portion to nanos
    t.setNanos(nanos);
    return t;
  }

  public static void setTimestamp(Timestamp t, byte[] bytes, int offset) {
    boolean hasDecimal = hasDecimal(bytes[offset]);
    t.setTime(((long) TimestampWritable.getSeconds(bytes, offset)) * 1000);
    if (hasDecimal) {
      t.setNanos(TimestampWritable.getNanos(bytes, offset+4));
    }
  }

  public static Timestamp createTimestamp(byte[] bytes, int offset) {
    Timestamp t = new Timestamp(0);
    TimestampWritable.setTimestamp(t, bytes, offset);
    return t;
  }

  public boolean hasDecimal() {
    return hasDecimal(currentBytes[offset]);
  }

  /**
   *
   * @param b first byte in an encoded TimestampWritable
   * @return true if it has a decimal portion, false otherwise
   */
  public static boolean hasDecimal(byte b) {
    return (b >> 7) != 0;
  }

  /**
   * Writes <code>value</code> into <code>dest</code> at <code>offset</code>
   * @param value
   * @param dest
   * @param offset
   */
  private static void intToBytes(int value, byte[] dest, int offset) {
    dest[offset] = (byte) ((value >> 24) & 0xFF);
    dest[offset+1] = (byte) ((value >> 16) & 0xFF);
    dest[offset+2] = (byte) ((value >> 8) & 0xFF);
    dest[offset+3] = (byte) (value & 0xFF);
  }

  /**
   *
   * @param bytes
   * @param offset
   * @return integer represented by the four bytes in <code>bytes</code>
   *  beginning at <code>offset</code>
   */
  private static int bytesToInt(byte[] bytes, int offset) {
    return ((0xFF & bytes[offset]) << 24)
        | ((0xFF & bytes[offset+1]) << 16)
        | ((0xFF & bytes[offset+2]) << 8)
        | (0xFF & bytes[offset+3]);
  }
}
