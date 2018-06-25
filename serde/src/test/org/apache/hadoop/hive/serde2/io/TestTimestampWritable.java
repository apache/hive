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
package org.apache.hadoop.hive.serde2.io;

import com.google.code.tempusfugit.concurrency.annotations.*;
import com.google.code.tempusfugit.concurrency.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import org.apache.hadoop.hive.ql.util.TimestampUtils;
import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TestTimestampWritable {

  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  private static ThreadLocal<DateFormat> DATE_FORMAT =
      new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
          return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
      };

  private static final int HAS_DECIMAL_MASK = 0x80000000;

  private static final long MAX_ADDITIONAL_SECONDS_BITS = 0x418937;

  private static long MIN_FOUR_DIGIT_YEAR_MILLIS = parseToMillis("0001-01-01 00:00:00");
  private static long MAX_FOUR_DIGIT_YEAR_MILLIS = parseToMillis("9999-01-01 00:00:00");

  private static int BILLION = 1000 * 1000 * 1000;

  private static long getSeconds(Timestamp ts) {
    // To compute seconds, we first subtract the milliseconds stored in the nanos field of the
    // Timestamp from the result of getTime().
    long seconds = (ts.getTime() - ts.getNanos() / 1000000) / 1000;

    // It should also be possible to calculate this based on ts.getTime() only.
    assertEquals(seconds, TimestampUtils.millisToSeconds(ts.getTime()));

    return seconds;
  }

  private static long parseToMillis(String s) {
    try {
      return DATE_FORMAT.get().parse(s).getTime();
    } catch (ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Before
  public void setUp() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  private static String normalizeTimestampStr(String timestampStr) {
    if (timestampStr.endsWith(".0")) {
      return timestampStr.substring(0, timestampStr.length() - 2);
    }
    return timestampStr;
  }

  private static void assertTSWEquals(TimestampWritable expected, TimestampWritable actual) {
    assertEquals(normalizeTimestampStr(expected.toString()),
                 normalizeTimestampStr(actual.toString()));
    assertEquals(expected, actual);
    assertEquals(expected.getTimestamp(), actual.getTimestamp());
  }

  private static TimestampWritable deserializeFromBytes(byte[] tsBytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(tsBytes);
    DataInputStream dis = new DataInputStream(bais);
    TimestampWritable deserTSW = new TimestampWritable();
    deserTSW.readFields(dis);
    return deserTSW;
  }

  private static int reverseNanos(int nanos) {
    if (nanos == 0) {
      return 0;
    }
    if (nanos < 0 || nanos >= 1000 * 1000 * 1000) {
      throw new IllegalArgumentException("Invalid nanosecond value: " + nanos);
    }

    int x = nanos;
    StringBuilder reversed = new StringBuilder();
    while (x != 0) {
      reversed.append((char)('0' + x % 10));
      x /= 10;
    }

    int result = Integer.parseInt(reversed.toString());
    while (nanos < 100 * 1000 * 1000) {
      result *= 10;
      nanos *= 10;
    }
    return result;
  }

  private static byte[] serializeToBytes(Writable w) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    w.write(dos);
    return baos.toByteArray();
  }

  private static List<Byte> toList(byte[] a) {
    List<Byte> list = new ArrayList<Byte>(a.length);
    for (byte b : a) {
      list.add(b);
    }
    return list;
  }

  /**
   * Pad the given byte array with the given number of bytes in the beginning. The padding bytes
   * deterministically depend on the passed data.
   */
  private static byte[] padBytes(byte[] bytes, int count) {
    byte[] result = new byte[bytes.length + count];
    for (int i = 0; i < count; ++i) {
      // Fill the prefix bytes with deterministic data based on the actual meaningful data.
      result[i] = (byte) (bytes[i % bytes.length] * 37 + 19);
    }
    System.arraycopy(bytes, 0, result, count, bytes.length);
    return result;
  }

  private static TimestampWritable serializeDeserializeAndCheckTimestamp(Timestamp ts)
      throws IOException {
    TimestampWritable tsw = new TimestampWritable(ts);
    assertEquals(ts, tsw.getTimestamp());

    byte[] tsBytes = serializeToBytes(tsw);
    TimestampWritable deserTSW = deserializeFromBytes(tsBytes);
    assertTSWEquals(tsw, deserTSW);
    assertEquals(ts, deserTSW.getTimestamp());
    assertEquals(tsBytes.length, tsw.getTotalLength());

    // Also convert to/from binary-sortable representation.
    int binarySortableOffset = Math.abs(tsw.hashCode()) % 10;
    byte[] binarySortableBytes = padBytes(tsw.getBinarySortable(), binarySortableOffset);
    TimestampWritable fromBinSort = new TimestampWritable();
    fromBinSort.setBinarySortable(binarySortableBytes, binarySortableOffset);
    assertTSWEquals(tsw, fromBinSort);

    long timeSeconds = ts.getTime() / 1000;
    if (0 <= timeSeconds && timeSeconds <= Integer.MAX_VALUE) {
      assertEquals(new Timestamp(timeSeconds * 1000),
        fromIntAndVInts((int) timeSeconds, 0).getTimestamp());

      int nanos = reverseNanos(ts.getNanos());
      assertEquals(ts,
        fromIntAndVInts((int) timeSeconds | (nanos != 0 ? HAS_DECIMAL_MASK : 0),
          nanos).getTimestamp());
    }

    assertEquals(ts.getNanos(), tsw.getNanos());
    assertEquals(getSeconds(ts), tsw.getSeconds());

    // Test various set methods and copy constructors.
    {
      TimestampWritable tsSet1 = new TimestampWritable();
      // make the offset non-zero to keep things interesting.
      int offset = Math.abs(ts.hashCode() % 32);
      byte[] shiftedBytes = padBytes(tsBytes, offset);
      tsSet1.set(shiftedBytes, offset);
      assertTSWEquals(tsw, tsSet1);

      TimestampWritable tswShiftedBytes = new TimestampWritable(shiftedBytes, offset);
      assertTSWEquals(tsw, tswShiftedBytes);
      assertTSWEquals(tsw, deserializeFromBytes(serializeToBytes(tswShiftedBytes)));
    }

    {
      TimestampWritable tsSet2 = new TimestampWritable();
      tsSet2.set(ts);
      assertTSWEquals(tsw, tsSet2);
    }

    {
      TimestampWritable tsSet3 = new TimestampWritable();
      tsSet3.set(tsw);
      assertTSWEquals(tsw, tsSet3);
    }

    {
      TimestampWritable tsSet4 = new TimestampWritable();
      tsSet4.set(deserTSW);
      assertTSWEquals(tsw, tsSet4);
    }

    double expectedDbl = getSeconds(ts) + 1e-9d * ts.getNanos();
    assertTrue(Math.abs(tsw.getDouble() - expectedDbl) < 1e-10d);

    return deserTSW;
  }

  private static int randomNanos(Random rand, int decimalDigits) {
    // Only keep the most significant decimalDigits digits.
    int nanos = rand.nextInt(BILLION);
    return nanos - nanos % (int) Math.pow(10, 9 - decimalDigits);
  }

  private static int randomNanos(Random rand) {
    return randomNanos(rand, rand.nextInt(10));
  }

  private static void checkTimestampWithAndWithoutNanos(Timestamp ts, int nanos)
      throws IOException {
    serializeDeserializeAndCheckTimestamp(ts);

    ts.setNanos(nanos);
    assertEquals(serializeDeserializeAndCheckTimestamp(ts).getNanos(), nanos);
  }

  private static TimestampWritable fromIntAndVInts(int i, long... vints) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(i);
    if ((i & HAS_DECIMAL_MASK) != 0) {
      for (long vi : vints) {
        WritableUtils.writeVLong(dos, vi);
      }
    }
    byte[] bytes = baos.toByteArray();
    TimestampWritable tsw = deserializeFromBytes(bytes);
    assertEquals(toList(bytes), toList(serializeToBytes(tsw)));
    return tsw;
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testReverseNanos() {
    assertEquals(0, reverseNanos(0));
    assertEquals(120000000, reverseNanos(21));
    assertEquals(32100000, reverseNanos(1230));
    assertEquals(5, reverseNanos(500000000));
    assertEquals(987654321, reverseNanos(123456789));
    assertEquals(12345678, reverseNanos(876543210));
  }

  /**
   * Test serializing and deserializing timestamps that can be represented by a number of seconds
   * from 0 to 2147483647 since the UNIX epoch.
   */
  @Test
  @Concurrent(count=4)
  public void testTimestampsWithinPositiveIntRange() throws IOException {
    Random rand = new Random(294722773L);
    for (int i = 0; i < 10000; ++i) {
      long millis = ((long) rand.nextInt(Integer.MAX_VALUE)) * 1000;
      checkTimestampWithAndWithoutNanos(new Timestamp(millis), randomNanos(rand));
    }
  }

  private static long randomMillis(long minMillis, long maxMillis, Random rand) {
    return minMillis + (long) ((maxMillis - minMillis) * rand.nextDouble());
  }

  /**
   * Test timestamps that don't necessarily fit between 1970 and 2038. This depends on HIVE-4525
   * being fixed.
   */
  @Test
  @Concurrent(count=4)
  public void testTimestampsOutsidePositiveIntRange() throws IOException {
    Random rand = new Random(789149717L);
    for (int i = 0; i < 10000; ++i) {
      long millis = randomMillis(MIN_FOUR_DIGIT_YEAR_MILLIS, MAX_FOUR_DIGIT_YEAR_MILLIS, rand);
      checkTimestampWithAndWithoutNanos(new Timestamp(millis), randomNanos(rand));
    }
  }

  @Test
  @Concurrent(count=4)
  public void testTimestampsInFullRange() throws IOException {
    Random rand = new Random(2904974913L);
    for (int i = 0; i < 10000; ++i) {
      checkTimestampWithAndWithoutNanos(new Timestamp(rand.nextLong()), randomNanos(rand));
    }
  }

  @Test
  @Concurrent(count=4)
  public void testToFromDouble() {
    Random rand = new Random(294729777L);
    for (int nanosPrecision = 0; nanosPrecision <= 4; ++nanosPrecision) {
      for (int i = 0; i < 10000; ++i) {
        long millis = randomMillis(MIN_FOUR_DIGIT_YEAR_MILLIS, MAX_FOUR_DIGIT_YEAR_MILLIS, rand);
        Timestamp ts = new Timestamp(millis);
        int nanos = randomNanos(rand, nanosPrecision);
        ts.setNanos(nanos);
        TimestampWritable tsw = new TimestampWritable(ts);
        double asDouble = tsw.getDouble();
        int recoveredNanos =
          (int) (Math.round((asDouble - Math.floor(asDouble)) * Math.pow(10, nanosPrecision)) *
            Math.pow(10, 9 - nanosPrecision));
        assertEquals(String.format("Invalid nanosecond part recovered from %f", asDouble),
          nanos, recoveredNanos);
        assertEquals(ts, TimestampUtils.doubleToTimestamp(asDouble));
        // decimalToTimestamp should be consistent with doubleToTimestamp for this level of
        // precision.
        assertEquals(ts, TimestampUtils.decimalToTimestamp(
            HiveDecimal.create(BigDecimal.valueOf(asDouble))));
      }
    }
  }

  private static HiveDecimal timestampToDecimal(Timestamp ts) {
    BigDecimal d = new BigDecimal(getSeconds(ts));
    d = d.add(new BigDecimal(ts.getNanos()).divide(new BigDecimal(BILLION)));
    return HiveDecimal.create(d);
  }

  @Test
  @Concurrent(count=4)
  public void testDecimalToTimestampRandomly() {
    Random rand = new Random(294729777L);
    for (int i = 0; i < 10000; ++i) {
      Timestamp ts = new Timestamp(
          randomMillis(MIN_FOUR_DIGIT_YEAR_MILLIS, MAX_FOUR_DIGIT_YEAR_MILLIS, rand));
      ts.setNanos(randomNanos(rand, 9));  // full precision
      assertEquals(ts, TimestampUtils.decimalToTimestamp(timestampToDecimal(ts)));
    }
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testDecimalToTimestampCornerCases() {
    Timestamp ts = new Timestamp(parseToMillis("1969-03-04 05:44:33"));
    assertEquals(0, ts.getTime() % 1000);
    for (int nanos : new int[] { 100000, 900000, 999100000, 999900000 }) {
      ts.setNanos(nanos);
      HiveDecimal d = timestampToDecimal(ts);
      assertEquals(ts, TimestampUtils.decimalToTimestamp(d));
      assertEquals(ts, TimestampUtils.doubleToTimestamp(d.bigDecimalValue().doubleValue()));
    }
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testSerializationFormatDirectly() throws IOException {
    assertEquals("1970-01-01 00:00:00", fromIntAndVInts(0).toString());
    assertEquals("1970-01-01 00:00:01", fromIntAndVInts(1).toString());
    assertEquals("1970-01-01 00:05:00", fromIntAndVInts(300).toString());
    assertEquals("1970-01-01 02:00:00", fromIntAndVInts(7200).toString());
    assertEquals("2000-01-02 03:04:05", fromIntAndVInts(946782245).toString());

    // This won't have a decimal part because the HAS_DECIMAL_MASK bit is not set.
    assertEquals("2000-01-02 03:04:05", fromIntAndVInts(946782245, 3210).toString());

    assertEquals("2000-01-02 03:04:05.0123",
      fromIntAndVInts(946782245 | HAS_DECIMAL_MASK, 3210).toString());

    assertEquals("2038-01-19 03:14:07", fromIntAndVInts(Integer.MAX_VALUE).toString());
    assertEquals("2038-01-19 03:14:07.012345678",
      fromIntAndVInts(Integer.MAX_VALUE | HAS_DECIMAL_MASK,  // this is really just -1
        876543210).toString());

    // Timestamps with a second VInt storing additional bits of the seconds field.
    long seconds = 253392390415L;
    assertEquals("9999-09-08 07:06:55",
      fromIntAndVInts((int) (seconds & 0x7fffffff) | (1 << 31), -1L, seconds >> 31).toString());
    assertEquals("9999-09-08 07:06:55.0123",
      fromIntAndVInts((int) (seconds & 0x7fffffff) | (1 << 31),
                      -3210 - 1, seconds >> 31).toString());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testMaxSize() {
    // This many bytes are necessary to store the reversed nanoseconds.
    assertEquals(5, WritableUtils.getVIntSize(999999999));
    assertEquals(5, WritableUtils.getVIntSize(-2 - 999999999));

    // Bytes necessary to store extra bits of the second timestamp if storing a timestamp
    // before 1970 or after 2038.
    assertEquals(3, WritableUtils.getVIntSize(Short.MAX_VALUE));
    assertEquals(3, WritableUtils.getVIntSize(Short.MIN_VALUE));

    // Test that MAX_ADDITIONAL_SECONDS_BITS is really the maximum value of the
    // additional bits (beyond 31 bits) of the seconds-since-epoch part of timestamp.
    assertTrue((((long) MAX_ADDITIONAL_SECONDS_BITS) << 31) * 1000 < Long.MAX_VALUE);
    assertTrue((((double) MAX_ADDITIONAL_SECONDS_BITS + 1) * (1L << 31)) * 1000 >
      Long.MAX_VALUE);

    // This is how many bytes we need to store those additonal bits as a VInt.
    assertEquals(4, WritableUtils.getVIntSize(MAX_ADDITIONAL_SECONDS_BITS));

    // Therefore, the maximum total size of a serialized timestamp is 4 + 5 + 4 = 13.
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testMillisToSeconds() {
    assertEquals(0, TimestampUtils.millisToSeconds(0));
    assertEquals(-1, TimestampUtils.millisToSeconds(-1));
    assertEquals(-1, TimestampUtils.millisToSeconds(-999));
    assertEquals(-1, TimestampUtils.millisToSeconds(-1000));
    assertEquals(-2, TimestampUtils.millisToSeconds(-1001));
    assertEquals(-2, TimestampUtils .millisToSeconds(-1999));
    assertEquals(-2, TimestampUtils .millisToSeconds(-2000));
    assertEquals(-3, TimestampUtils .millisToSeconds(-2001));
    assertEquals(-99, TimestampUtils .millisToSeconds(-99000));
    assertEquals(-100, TimestampUtils .millisToSeconds(-99001));
    assertEquals(-100, TimestampUtils .millisToSeconds(-100000));
    assertEquals(1, TimestampUtils .millisToSeconds(1500));
    assertEquals(19, TimestampUtils .millisToSeconds(19999));
    assertEquals(20, TimestampUtils .millisToSeconds(20000));
  }

  private static int compareEqualLengthByteArrays(byte[] a, byte[] b) {
    assertEquals(a.length, b.length);
    for (int i = 0; i < a.length; ++i) {
      if (a[i] != b[i]) {
        return (a[i] & 0xff) - (b[i] & 0xff);
      }
    }
    return 0;
  }

  private static int normalizeComparisonResult(int result) {
    return result < 0 ? -1 : (result > 0 ? 1 : 0);
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testBinarySortable() {
    Random rand = new Random(5972977L);
    List<TimestampWritable> tswList = new ArrayList<TimestampWritable>();
    for (int i = 0; i < 50; ++i) {
      Timestamp ts = new Timestamp(rand.nextLong());
      ts.setNanos(randomNanos(rand));
      tswList.add(new TimestampWritable(ts));
    }
    for (TimestampWritable tsw1 : tswList) {
      byte[] bs1 = tsw1.getBinarySortable();
      for (TimestampWritable tsw2 : tswList) {
        byte[] bs2 = tsw2.getBinarySortable();
        int binaryComparisonResult =
          normalizeComparisonResult(compareEqualLengthByteArrays(bs1, bs2));
        int comparisonResult = normalizeComparisonResult(tsw1.compareTo(tsw2));
        if (binaryComparisonResult != comparisonResult) {
          throw new AssertionError("TimestampWritables " + tsw1 + " and " + tsw2 + " compare as " +
            comparisonResult + " using compareTo but as " + binaryComparisonResult + " using " +
            "getBinarySortable");
        }
      }
    }
  }

  @Test
  public void testSetTimestamp() {
    // one VInt without nanos
    verifySetTimestamp(1000);

    // one VInt with nanos
    verifySetTimestamp(1001);

    // two VInt without nanos
    verifySetTimestamp((long) Integer.MAX_VALUE * 1000 + 1000);

    // two VInt with nanos
    verifySetTimestamp((long) Integer.MAX_VALUE * 1000 + 1234);
  }

  private static void verifySetTimestamp(long time) {
    Timestamp t1 = new Timestamp(time);
    TimestampWritable writable = new TimestampWritable(t1);
    byte[] bytes = writable.getBytes();
    Timestamp t2 = new Timestamp(0);
    TimestampWritable.setTimestamp(t2, bytes, 0);
    assertEquals(t1, t2);
  }

}
