/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.io;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;

/**
 * Writable for TimestampTZ. Copied from TimestampWritableV2.
 * After we replace {@link java.sql.Timestamp} with {@link java.time.LocalDateTime} for Timestamp,
 * it'll need a new Writable.
 * All timestamp with time zone will be serialized as UTC retaining the instant.
 * E.g. "2017-04-14 18:00:00 Asia/Shanghai" will be converted to
 * "2017-04-14 10:00:00.0 Z".
 */
public class TimestampLocalTZWritable implements WritableComparable<TimestampLocalTZWritable> {

  public static final byte[] nullBytes = {0x0, 0x0, 0x0, 0x0};
  private static final int DECIMAL_OR_SECOND_VINT_FLAG = 1 << 31;
  private static final long SEVEN_BYTE_LONG_SIGN_FLIP = 0xff80L << 48; // only need flip the MSB?

  /**
   * The maximum number of bytes required for a TimestampWritableV2
   */
  public static final int MAX_BYTES = 13;

  public static final int BINARY_SORTABLE_LENGTH = 11;

  private TimestampTZ timestampTZ = new TimestampTZ();
  private ZoneId timeZone;

  /**
   * true if data is stored in timestamptz field rather than byte arrays.
   * allows for lazy conversion to bytes when necessary
   * false otherwise
   */
  private boolean bytesEmpty = true;
  private boolean timestampTZEmpty = true;

  /* Allow use of external byte[] for efficiency */
  private byte[] currentBytes;
  private final byte[] internalBytes = new byte[MAX_BYTES];
  private byte[] externalBytes;
  private int offset;

  public TimestampLocalTZWritable() {
    this.bytesEmpty = false;
    this.currentBytes = internalBytes;
    this.offset = 0;
  }

  public TimestampLocalTZWritable(byte[] bytes, int offset, ZoneId timeZone) {
    set(bytes, offset, timeZone);
  }

  public TimestampLocalTZWritable(TimestampLocalTZWritable other) {
    this(other.getBytes(), 0, other.getTimestampTZ().getZonedDateTime().getZone());
  }

  public TimestampLocalTZWritable(TimestampTZ tstz) {
    set(tstz);
  }

  public void set(byte[] bytes, int offset, ZoneId timeZone) {
    externalBytes = bytes;
    this.offset = offset;
    this.timeZone = timeZone;
    bytesEmpty = false;
    timestampTZEmpty = true;
    currentBytes = externalBytes;
  }

  public void set(TimestampTZ tstz) {
    if (tstz == null) {
      timestampTZ.setZonedDateTime(null);
      return;
    }
    timestampTZ = tstz;
    timeZone = timestampTZ.getZonedDateTime().getZone();
    bytesEmpty = true;
    timestampTZEmpty = false;
  }

  public void set(TimestampLocalTZWritable t) {
    if (t.bytesEmpty) {
      set(t.getTimestampTZ());
    } else if (t.currentBytes == t.externalBytes) {
      set(t.currentBytes, t.offset, t.timeZone);
    } else {
      set(t.currentBytes, 0, t.timeZone);
    }
  }

  public void setTimeZone(ZoneId timeZone) {
    if (timestampTZ != null) {
      timestampTZ.setZonedDateTime(
          timestampTZ.getZonedDateTime().withZoneSameInstant(timeZone));
    }
    this.timeZone = timeZone;
  }

  public ZoneId getTimeZone() {
    return timeZone;
  }

  public TimestampTZ getTimestampTZ() {
    populateTimestampTZ();
    return timestampTZ;
  }

  /**
   * Used to create copies of objects
   *
   * @return a copy of the internal TimestampTZWritable byte[]
   */
  public byte[] getBytes() {
    checkBytes();

    int len = getTotalLength();
    byte[] b = new byte[len];

    System.arraycopy(currentBytes, offset, b, 0, len);
    return b;
  }

  /**
   * @return length of serialized TimestampTZWritable data. As a side effect, populates the internal
   * byte array if empty.
   */
  private int getTotalLength() {
    checkBytes();
    return getTotalLength(currentBytes, offset);
  }

  /**
   * The data of TimestampTZWritable can be stored either in a byte[]
   * or in a TimestampTZ object. Calling this method ensures that the byte[]
   * is populated from the TimestampTZ object if previously empty.
   */
  private void checkBytes() {
    if (bytesEmpty) {
      populateBytes();
      offset = 0;
      currentBytes = internalBytes;
      bytesEmpty = false;
    }
  }

  // Writes the TimestampTZ's serialized value to the internal byte array.
  private void populateBytes() {
    Arrays.fill(internalBytes, (byte) 0);

    long seconds = timestampTZ.getEpochSecond();
    int nanos = timestampTZ.getNanos();

    boolean hasSecondVInt = seconds < 0 || seconds > Integer.MAX_VALUE;
    boolean hasDecimal = setNanosBytes(nanos, internalBytes, offset + 4, hasSecondVInt);

    int firstInt = (int) seconds;
    if (hasDecimal || hasSecondVInt) {
      firstInt |= DECIMAL_OR_SECOND_VINT_FLAG;
    }
    intToBytes(firstInt, internalBytes, offset);
    if (hasSecondVInt) {
      LazyBinaryUtils.writeVLongToByteArray(internalBytes,
          offset + 4 + WritableUtils.decodeVIntSize(internalBytes[offset + 4]),
          seconds >> 31);
    }
  }

  private void populateTimestampTZ() {
    if (timestampTZEmpty) {
      if (bytesEmpty) {
        throw new IllegalStateException("Bytes are empty");
      }
      long seconds = getSeconds(currentBytes, offset);
      int nanos = hasDecimalOrSecondVInt(currentBytes[offset]) ? getNanos(currentBytes, offset + 4) : 0;
      timestampTZ.set(seconds, nanos, timeZone);
      timestampTZEmpty = false;
    }
  }

  public long getSeconds() {
    if (!timestampTZEmpty) {
      return timestampTZ.getEpochSecond();
    } else if (!bytesEmpty) {
      return getSeconds(currentBytes, offset);
    }
    throw new IllegalStateException("Both timestamp and bytes are empty");
  }

  public int getNanos() {
    if (!timestampTZEmpty) {
      return timestampTZ.getNanos();
    } else if (!bytesEmpty) {
      return hasDecimalOrSecondVInt(currentBytes[offset]) ? getNanos(currentBytes, offset + 4) : 0;
    }
    throw new IllegalStateException("Both timestamp and bytes are empty");
  }

  @Override
  public int compareTo(TimestampLocalTZWritable o) {
    return getTimestampTZ().compareTo(o.getTimestampTZ());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TimestampLocalTZWritable) {
      return compareTo((TimestampLocalTZWritable) o) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getTimestampTZ().hashCode();
  }

  @Override
  public String toString() {
    populateTimestampTZ();
    return timestampTZ.toString();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    checkBytes();
    dataOutput.write(currentBytes, offset, getTotalLength());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    dataInput.readFully(internalBytes, 0, 4);
    if (hasDecimalOrSecondVInt(internalBytes[0])) {
      dataInput.readFully(internalBytes, 4, 1);
      int len = (byte) WritableUtils.decodeVIntSize(internalBytes[4]);
      if (len > 1) {
        dataInput.readFully(internalBytes, 5, len - 1);
      }

      long vlong = LazyBinaryUtils.readVLongFromByteArray(internalBytes, 4);
      Preconditions.checkState(vlong >= -1000000000 && vlong <= 999999999,
          "Invalid nanos value for a TimestampTZWritable: " + vlong +
              ", expected to be between -1000000000 and 999999999.");
      if (vlong < 0) {
        // This indicates there is a second VInt containing the additional bits of the seconds
        // field.
        dataInput.readFully(internalBytes, 4 + len, 1);
        int secondVIntLen = (byte) WritableUtils.decodeVIntSize(internalBytes[4 + len]);
        if (secondVIntLen > 1) {
          dataInput.readFully(internalBytes, 5 + len, secondVIntLen - 1);
        }
      }
    }
    currentBytes = internalBytes;
    offset = 0;
    timestampTZEmpty = true;
    bytesEmpty = false;
  }

  public byte[] toBinarySortable() {
    byte[] b = new byte[BINARY_SORTABLE_LENGTH];
    int nanos = getNanos();
    // We flip the highest-order bit of the seven-byte representation of seconds to make negative
    // values come before positive ones.
    long seconds = getSeconds() ^ SEVEN_BYTE_LONG_SIGN_FLIP;
    sevenByteLongToBytes(seconds, b, 0);
    intToBytes(nanos, b, 7);
    return b;
  }

  public void fromBinarySortable(byte[] bytes, int binSortOffset, ZoneId timeZone) {
    // Flip the sign bit (and unused bits of the high-order byte) of the seven-byte long back.
    long seconds = readSevenByteLong(bytes, binSortOffset) ^ SEVEN_BYTE_LONG_SIGN_FLIP;
    int nanos = bytesToInt(bytes, binSortOffset + 7);
    timestampTZ.set(seconds, nanos, timeZone);
    timestampTZEmpty = false;
    bytesEmpty = true;
  }

  public void writeToByteStream(ByteStream.RandomAccessOutput byteStream) {
    checkBytes();
    byteStream.write(currentBytes, offset, getTotalLength());
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
  private static boolean setNanosBytes(int nanos, byte[] b, int offset, boolean hasSecondVInt) {
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

    if (hasSecondVInt || decimal != 0) {
      // We use the sign of the reversed-nanoseconds field to indicate that there is a second VInt
      // present.
      LazyBinaryUtils.writeVLongToByteArray(b, offset, hasSecondVInt ? (-decimal - 1) : decimal);
    }
    return decimal != 0;
  }

  public static void setTimestampTZ(TimestampTZ t, byte[] bytes, int offset, ZoneId timeZone) {
    long seconds = getSeconds(bytes, offset);
    int nanos = hasDecimalOrSecondVInt(bytes[offset]) ? getNanos(bytes, offset + 4) : 0;
    t.set(seconds, nanos, timeZone);
  }

  public static int getTotalLength(byte[] bytes, int offset) {
    int len = 4;
    if (hasDecimalOrSecondVInt(bytes[offset])) {
      int firstVIntLen = WritableUtils.decodeVIntSize(bytes[offset + 4]);
      len += firstVIntLen;
      if (hasSecondVInt(bytes[offset + 4])) {
        len += WritableUtils.decodeVIntSize(bytes[offset + 4 + firstVIntLen]);
      }
    }
    return len;
  }

  public static long getSeconds(byte[] bytes, int offset) {
    int firstVInt = bytesToInt(bytes, offset);
    if (firstVInt >= 0 || !hasSecondVInt(bytes[offset + 4])) {
      return firstVInt & ~DECIMAL_OR_SECOND_VINT_FLAG;
    }
    return ((long) (firstVInt & ~DECIMAL_OR_SECOND_VINT_FLAG)) |
        (LazyBinaryUtils.readVLongFromByteArray(bytes,
            offset + 4 + WritableUtils.decodeVIntSize(bytes[offset + 4])) << 31);
  }

  public static int getNanos(byte[] bytes, int offset) {
    int val = (int) LazyBinaryUtils.readVLongFromByteArray(bytes, offset);
    if (val < 0) {
      val = -val - 1;
    }
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

  private static boolean hasDecimalOrSecondVInt(byte b) {
    return b < 0;
  }

  private static boolean hasSecondVInt(byte b) {
    return WritableUtils.isNegativeVInt(b);
  }

  /**
   * Writes <code>value</code> into <code>dest</code> at <code>offset</code>
   *
   * @param value
   * @param dest
   * @param offset
   */
  private static void intToBytes(int value, byte[] dest, int offset) {
    dest[offset] = (byte) ((value >> 24) & 0xFF);
    dest[offset + 1] = (byte) ((value >> 16) & 0xFF);
    dest[offset + 2] = (byte) ((value >> 8) & 0xFF);
    dest[offset + 3] = (byte) (value & 0xFF);
  }

  /**
   * Writes <code>value</code> into <code>dest</code> at <code>offset</code> as a seven-byte
   * serialized long number.
   */
  private static void sevenByteLongToBytes(long value, byte[] dest, int offset) {
    dest[offset] = (byte) ((value >> 48) & 0xFF);
    dest[offset + 1] = (byte) ((value >> 40) & 0xFF);
    dest[offset + 2] = (byte) ((value >> 32) & 0xFF);
    dest[offset + 3] = (byte) ((value >> 24) & 0xFF);
    dest[offset + 4] = (byte) ((value >> 16) & 0xFF);
    dest[offset + 5] = (byte) ((value >> 8) & 0xFF);
    dest[offset + 6] = (byte) (value & 0xFF);
  }

  /**
   * @param bytes
   * @param offset
   * @return integer represented by the four bytes in <code>bytes</code>
   * beginning at <code>offset</code>
   */
  private static int bytesToInt(byte[] bytes, int offset) {
    return ((0xFF & bytes[offset]) << 24)
        | ((0xFF & bytes[offset + 1]) << 16)
        | ((0xFF & bytes[offset + 2]) << 8)
        | (0xFF & bytes[offset + 3]);
  }

  private static long readSevenByteLong(byte[] bytes, int offset) {
    // We need to shift everything 8 bits left and then shift back to populate the sign field.
    return (((0xFFL & bytes[offset]) << 56)
        | ((0xFFL & bytes[offset + 1]) << 48)
        | ((0xFFL & bytes[offset + 2]) << 40)
        | ((0xFFL & bytes[offset + 3]) << 32)
        | ((0xFFL & bytes[offset + 4]) << 24)
        | ((0xFFL & bytes[offset + 5]) << 16)
        | ((0xFFL & bytes[offset + 6]) << 8)) >> 8;
  }
}
