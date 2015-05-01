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

package org.apache.hadoop.hive.ql.io.orc;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.filters.BloomFilterIO;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.Location;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.TestSearchArgumentImpl;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.Mockito;

public class TestRecordReaderImpl {

  // can add .verboseLogging() to cause Mockito to log invocations
  private final MockSettings settings = Mockito.withSettings().verboseLogging();

  static class BufferInStream
      extends InputStream implements PositionedReadable, Seekable {
    private final byte[] buffer;
    private final int length;
    private int position = 0;

    BufferInStream(byte[] bytes, int length) {
      this.buffer = bytes;
      this.length = length;
    }

    @Override
    public int read() {
      if (position < length) {
        return buffer[position++];
      }
      return -1;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
      int lengthToRead = Math.min(length, this.length - this.position);
      if (lengthToRead >= 0) {
        for(int i=0; i < lengthToRead; ++i) {
          bytes[offset + i] = buffer[position++];
        }
        return lengthToRead;
      } else {
        return -1;
      }
    }

    @Override
    public int read(long position, byte[] bytes, int offset, int length) {
      this.position = (int) position;
      return read(bytes, offset, length);
    }

    @Override
    public void readFully(long position, byte[] bytes, int offset,
                          int length) throws IOException {
      this.position = (int) position;
      while (length > 0) {
        int result = read(bytes, offset, length);
        offset += result;
        length -= result;
        if (result < 0) {
          throw new IOException("Read past end of buffer at " + offset);
        }
      }
    }

    @Override
    public void readFully(long position, byte[] bytes) throws IOException {
      readFully(position, bytes, 0, bytes.length);
    }

    @Override
    public void seek(long position) {
      this.position = (int) position;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public boolean seekToNewSource(long position) throws IOException {
      this.position = (int) position;
      return false;
    }
  }

  @Test
  public void testMaxLengthToReader() throws Exception {
    Configuration conf = new Configuration();
    OrcProto.Type rowType = OrcProto.Type.newBuilder()
        .setKind(OrcProto.Type.Kind.STRUCT).build();
    OrcProto.Footer footer = OrcProto.Footer.newBuilder()
        .setHeaderLength(0).setContentLength(0).setNumberOfRows(0)
        .setRowIndexStride(0).addTypes(rowType).build();
    OrcProto.PostScript ps = OrcProto.PostScript.newBuilder()
        .setCompression(OrcProto.CompressionKind.NONE)
        .setFooterLength(footer.getSerializedSize())
        .setMagic("ORC").addVersion(0).addVersion(11).build();
    DataOutputBuffer buffer = new DataOutputBuffer();
    footer.writeTo(buffer);
    ps.writeTo(buffer);
    buffer.write(ps.getSerializedSize());
    FileSystem fs = Mockito.mock(FileSystem.class, settings);
    FSDataInputStream file =
        new FSDataInputStream(new BufferInStream(buffer.getData(),
            buffer.getLength()));
    Path p = new Path("/dir/file.orc");
    Mockito.when(fs.open(p)).thenReturn(file);
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
    options.filesystem(fs);
    options.maxLength(buffer.getLength());
    Mockito.when(fs.getFileStatus(p))
        .thenReturn(new FileStatus(10, false, 3, 3000, 0, p));
    Reader reader = OrcFile.createReader(p, options);
  }

  @Test
  public void testCompareToRangeInt() throws Exception {
    assertEquals(Location.BEFORE,
        RecordReaderImpl.compareToRange(19L, 20L, 40L));
    assertEquals(Location.AFTER,
        RecordReaderImpl.compareToRange(41L, 20L, 40L));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange(20L, 20L, 40L));
    assertEquals(Location.MIDDLE,
        RecordReaderImpl.compareToRange(21L, 20L, 40L));
    assertEquals(Location.MAX,
        RecordReaderImpl.compareToRange(40L, 20L, 40L));
    assertEquals(Location.BEFORE,
        RecordReaderImpl.compareToRange(0L, 1L, 1L));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange(1L, 1L, 1L));
    assertEquals(Location.AFTER,
        RecordReaderImpl.compareToRange(2L, 1L, 1L));
  }

  @Test
  public void testCompareToRangeString() throws Exception {
    assertEquals(Location.BEFORE,
        RecordReaderImpl.compareToRange("a", "b", "c"));
    assertEquals(Location.AFTER,
        RecordReaderImpl.compareToRange("d", "b", "c"));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange("b", "b", "c"));
    assertEquals(Location.MIDDLE,
        RecordReaderImpl.compareToRange("bb", "b", "c"));
    assertEquals(Location.MAX,
        RecordReaderImpl.compareToRange("c", "b", "c"));
    assertEquals(Location.BEFORE,
        RecordReaderImpl.compareToRange("a", "b", "b"));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange("b", "b", "b"));
    assertEquals(Location.AFTER,
        RecordReaderImpl.compareToRange("c", "b", "b"));
  }

  @Test
  public void testCompareToCharNeedConvert() throws Exception {
    assertEquals(Location.BEFORE,
        RecordReaderImpl.compareToRange("apple", "hello", "world"));
    assertEquals(Location.AFTER,
        RecordReaderImpl.compareToRange("zombie", "hello", "world"));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange("hello", "hello", "world"));
    assertEquals(Location.MIDDLE,
        RecordReaderImpl.compareToRange("pilot", "hello", "world"));
    assertEquals(Location.MAX,
        RecordReaderImpl.compareToRange("world", "hello", "world"));
    assertEquals(Location.BEFORE,
        RecordReaderImpl.compareToRange("apple", "hello", "hello"));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange("hello", "hello", "hello"));
    assertEquals(Location.AFTER,
        RecordReaderImpl.compareToRange("zombie", "hello", "hello"));
  }

  @Test
  public void testGetMin() throws Exception {
    assertEquals(10L, RecordReaderImpl.getMin(
        ColumnStatisticsImpl.deserialize(createIntStats(10L, 100L))));
    assertEquals(10.0d, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                .setMinimum(10.0d).setMaximum(100.0d).build()).build())));
    assertEquals(null, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
            .build())));
    assertEquals("a", RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build())));
    assertEquals("hello", RecordReaderImpl.getMin(ColumnStatisticsImpl
        .deserialize(createStringStats("hello", "world"))));
    assertEquals(HiveDecimal.create("111.1"), RecordReaderImpl.getMin(ColumnStatisticsImpl
        .deserialize(createDecimalStats("111.1", "112.1"))));
  }

  private static OrcProto.ColumnStatistics createIntStats(Long min,
                                                          Long max) {
    OrcProto.IntegerStatistics.Builder intStats =
        OrcProto.IntegerStatistics.newBuilder();
    if (min != null) {
      intStats.setMinimum(min);
    }
    if (max != null) {
      intStats.setMaximum(max);
    }
    return OrcProto.ColumnStatistics.newBuilder()
        .setIntStatistics(intStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createBooleanStats(int n, int trueCount) {
    OrcProto.BucketStatistics.Builder boolStats = OrcProto.BucketStatistics.newBuilder();
    boolStats.addCount(trueCount);
    return OrcProto.ColumnStatistics.newBuilder().setNumberOfValues(n).setBucketStatistics(
        boolStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createIntStats(int min, int max) {
    OrcProto.IntegerStatistics.Builder intStats = OrcProto.IntegerStatistics.newBuilder();
    intStats.setMinimum(min);
    intStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setIntStatistics(intStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDoubleStats(double min, double max) {
    OrcProto.DoubleStatistics.Builder dblStats = OrcProto.DoubleStatistics.newBuilder();
    dblStats.setMinimum(min);
    dblStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDoubleStatistics(dblStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createStringStats(String min, String max,
      boolean hasNull) {
    OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.newBuilder();
    strStats.setMinimum(min);
    strStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setStringStatistics(strStats.build())
        .setHasNull(hasNull).build();
  }

  private static OrcProto.ColumnStatistics createStringStats(String min, String max) {
    OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.newBuilder();
    strStats.setMinimum(min);
    strStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setStringStatistics(strStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDateStats(int min, int max) {
    OrcProto.DateStatistics.Builder dateStats = OrcProto.DateStatistics.newBuilder();
    dateStats.setMinimum(min);
    dateStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDateStatistics(dateStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createTimestampStats(long min, long max) {
    OrcProto.TimestampStatistics.Builder tsStats = OrcProto.TimestampStatistics.newBuilder();
    tsStats.setMinimum(min);
    tsStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setTimestampStatistics(tsStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max) {
    OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.newBuilder();
    decStats.setMinimum(min);
    decStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDecimalStatistics(decStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max,
      boolean hasNull) {
    OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.newBuilder();
    decStats.setMinimum(min);
    decStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDecimalStatistics(decStats.build())
        .setHasNull(hasNull).build();
  }

  @Test
  public void testGetMax() throws Exception {
    assertEquals(100L, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(createIntStats(10L, 100L))));
    assertEquals(100.0d, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                .setMinimum(10.0d).setMaximum(100.0d).build()).build())));
    assertEquals(null, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
            .build())));
    assertEquals("b", RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build())));
    assertEquals("world", RecordReaderImpl.getMax(ColumnStatisticsImpl
        .deserialize(createStringStats("hello", "world"))));
    assertEquals(HiveDecimal.create("112.1"), RecordReaderImpl.getMax(ColumnStatisticsImpl
        .deserialize(createDecimalStats("111.1", "112.1"))));
  }

  @Test
  public void testPredEvalWithBooleanStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", true, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 10), pred, null));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 0), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", "true", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 10), pred, null));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 0), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", "hello", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 10), pred, null));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 0), pred, null));
  }

  @Test
  public void testPredEvalWithIntStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

    // Stats gets converted to column type. "15" is outside of "10" and "100"
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

    // Integer stats will not be converted date because of days/seconds/millis ambiguity
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));
  }

  @Test
  public void testPredEvalWithDoubleStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

    // Stats gets converted to column type. "15.0" is outside of "10.0" and "100.0"
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

    // Double is not converted to date type because of days/seconds/millis ambiguity
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15*1000L), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150*1000L), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));
  }

  @Test
  public void testPredEvalWithStringStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 100, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 100.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "100", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

    // IllegalArgumentException is thrown when converting String to Date, hence YES_NO
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(100).get(), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(100), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(100), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));
  }

  @Test
  public void testPredEvalWithDateStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    // Date to Integer conversion is not possible.
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    // Date to Float conversion is also not possible.
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "1970-01-11", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15.1", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "__a15__1", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "2000-01-16", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "1970-01-16", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(150).get(), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    // Date to Decimal conversion is also not possible.
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15L * 24L * 60L * 60L * 1000L), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));
  }

  @Test
  public void testPredEvalWithDecimalStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

    // "15" out of range of "10.0" and "100.0"
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

    // Decimal to Date not possible.
    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15 * 1000L), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150 * 1000L), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));
  }

  @Test
  public void testPredEvalWithTimestampStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10000, 100000), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", new Timestamp(15).toString(), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10 * 24L * 60L * 60L * 1000L,
            100 * 24L * 60L * 60L * 1000L), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10000, 100000), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10000, 100000), pred, null));
  }

  @Test
  public void testEquals() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 15L), pred, null));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 10L), pred, null));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 15L), pred, null));
  }

  @Test
  public void testNullSafeEquals() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 15L), pred, null));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 10L), pred, null));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 15L), pred, null));
  }

  @Test
  public void testLessThan() throws Exception {
    PredicateLeaf lessThan = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), lessThan, null));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 30L), lessThan, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), lessThan, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 15L), lessThan, null));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 10L), lessThan, null));
  }

  @Test
  public void testLessThanEquals() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), pred, null));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 15L), pred, null));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 10L), pred, null));
  }

  @Test
  public void testIn() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.INTEGER,
            "x", null, args);
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 20L), pred, null));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(30L, 30L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), pred, null));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(12L, 18L), pred, null));
  }

  @Test
  public void testBetween() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.INTEGER,
            "x", null, args);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 5L), pred, null));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(30L, 40L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(5L, 15L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 25L), pred, null));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(5L, 25L), pred, null));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 20L), pred, null));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(12L, 18L), pred, null));
  }

  @Test
  public void testIsNull() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.INTEGER,
            "x", null, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
  }


  @Test
  public void testEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null)); // before
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "c", true), pred, null)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null)); // same
  }

  @Test
  public void testNullSafeEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null)); // before
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "c", true), pred, null)); // max
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null)); // same
  }

  @Test
  public void testLessThanWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null)); // before
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "c", true), pred, null)); // max
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
    assertEquals(TruthValue.NO_NULL, // min, same stats
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null));
  }

  @Test
  public void testLessThanEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null)); // before
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "c", true), pred, null)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null)); // same
  }

  @Test
  public void testInWithNullInStats() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("c");
    args.add("f");
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
            "x", null, args);
    assertEquals(TruthValue.NO_NULL, // before & after
        RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("e", "f", true), pred, null)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null)); // same
  }

  @Test
  public void testBetweenWithNullInStats() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("c");
    args.add("f");
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.STRING,
            "x", null, args);
    assertEquals(TruthValue.YES_NULL, // before & after
        RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null));
    assertEquals(TruthValue.YES_NULL, // before & max
        RecordReaderImpl.evaluatePredicateProto(createStringStats("e", "f", true), pred, null));
    assertEquals(TruthValue.NO_NULL, // before & before
        RecordReaderImpl.evaluatePredicateProto(createStringStats("h", "g", true), pred, null));
    assertEquals(TruthValue.YES_NO_NULL, // before & min
        RecordReaderImpl.evaluatePredicateProto(createStringStats("f", "g", true), pred, null));
    assertEquals(TruthValue.YES_NO_NULL, // before & middle
        RecordReaderImpl.evaluatePredicateProto(createStringStats("e", "g", true), pred, null));

    assertEquals(TruthValue.YES_NULL, // min & after
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "e", true), pred, null));
    assertEquals(TruthValue.YES_NULL, // min & max
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "f", true), pred, null));
    assertEquals(TruthValue.YES_NO_NULL, // min & middle
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "g", true), pred, null));

    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "c", true), pred, null)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
    assertEquals(TruthValue.YES_NULL, // min & after, same stats
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null));
  }

  @Test
  public void testIsNullWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.STRING,
            "x", null, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", false), pred, null));
  }

  @Test
  public void testOverlap() throws Exception {
    assertTrue(!RecordReaderUtils.overlap(0, 10, -10, -1));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 0));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 1));
    assertTrue(RecordReaderUtils.overlap(0, 10, 2, 8));
    assertTrue(RecordReaderUtils.overlap(0, 10, 5, 10));
    assertTrue(RecordReaderUtils.overlap(0, 10, 10, 11));
    assertTrue(RecordReaderUtils.overlap(0, 10, 0, 10));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 11));
    assertTrue(!RecordReaderUtils.overlap(0, 10, 11, 12));
  }

  private static DiskRangeList diskRanges(Integer... points) {
    DiskRangeList head = null, tail = null;
    for(int i = 0; i < points.length; i += 2) {
      DiskRangeList range = new DiskRangeList(points[i], points[i+1]);
      if (tail == null) {
        head = tail = range;
      } else {
        tail = tail.insertAfter(range);
      }
    }
    return head;
  }

  @Test
  public void testGetIndexPosition() throws Exception {
    assertEquals(0, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.PRESENT, true, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(0, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, true, false));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DICTIONARY, OrcProto.Type.Kind.STRING,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(6, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.LENGTH, true, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.LENGTH, false, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(6, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, true, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, false, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(7, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, true, true));
    assertEquals(5, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, false, true));
  }

  @Test
  public void testPartialPlan() throws Exception {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(99000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{true, true, false, false, true, false};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: int, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
                .addSubtypes(1).addSubtypes(2).addFieldNames("x")
                .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(0, 1000, 100, 1000, 400, 1000,
        1000, 11000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        11000, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(0, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));

    // if we read no rows, don't read any bytes
    rowGroups = new boolean[]{false, false, false, false, false, false};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertNull(result);

    // all rows, but only columns 0 and 2.
    rowGroups = null;
    columns = new boolean[]{true, false, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, null, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(100000, 102000, 102000, 200000)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, null, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100000, 200000)));

    rowGroups = new boolean[]{false, true, false, false, false, false};
    indexes[2] = indexes[1];
    indexes[1] = null;
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(100100, 102000,
        112000, 122000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100100, 102000,
        112000, 122000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    indexes[1] = indexes[2];
    columns = new boolean[]{true, true, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000, 100500, 102000,
        152000, 200000)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000, 100500, 102000,
        152000, 200000)));
  }


  @Test
  public void testPartialPlanCompressed() throws Exception {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(99000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{true, true, false, false, true, false};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: int, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
        .addSubtypes(1).addSubtypes(2).addFieldNames("x")
        .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, true, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(0, 1000, 100, 1000,
        400, 1000, 1000, 11000+(2*32771),
        11000, 21000+(2*32771), 41000, 100000)));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, true, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000)));
  }

  @Test
  public void testPartialPlanString() throws Exception {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(94000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.LENGTH)
        .setColumn(1).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DICTIONARY_DATA)
        .setColumn(1).setLength(3000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{false, true, false, false, true, true};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: string, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
        .addSubtypes(1).addSubtypes(2).addFieldNames("x")
        .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(100, 1000, 400, 1000, 500, 1000,
        11000, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        51000, 95000, 95000, 97000, 97000, 100000)));
  }

  @Test
  public void testIntNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createIntStats(10, 100));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testIntEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createIntStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testIntInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(15);
    args.add(19);
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.INTEGER,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createIntStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(19);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(15.0);
    args.add(19.0);
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.FLOAT,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(19.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("str_15");
    args.add("str_19");
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_19");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.DATE, "x",
        new DateWritable(15).get(), null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new DateWritable(i)).getDays());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDateStats(10, 100));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(15)).getDays());
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DATE, "x",
        new DateWritable(15).get(), null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new DateWritable(i)).getDays());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDateStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(15)).getDays());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(new DateWritable(15).get());
    args.add(new DateWritable(19).get());
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DATE,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new DateWritable(i)).getDays());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDateStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(19)).getDays());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(15)).getDays());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testTimestampNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
        new Timestamp(15),
        null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new Timestamp(i)).getTime());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createTimestampStats(10, 100));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new Timestamp(15)).getTime());
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testTimestampEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new Timestamp(i)).getTime());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createTimestampStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new Timestamp(15)).getTime());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testTimestampInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(new Timestamp(15));
    args.add(new Timestamp(19));
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.TIMESTAMP,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new Timestamp(i)).getTime());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createTimestampStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new Timestamp(19)).getTime());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new Timestamp(15)).getTime());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDecimalNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.DECIMAL, "x",
        HiveDecimal.create(15),
        null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDecimalEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15),
        null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDecimalInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(HiveDecimal.create(15));
    args.add(HiveDecimal.create(19));
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(19).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testNullsInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(HiveDecimal.create(15));
    args.add(null);
    args.add(HiveDecimal.create(19));
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200", false));
    // hasNull is false, so bloom filter should return NO
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200", true));
    // hasNull is true, so bloom filter should return YES_NO_NULL
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(19).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }
}
