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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.Location;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.TestSearchArgumentImpl;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
    assertEquals(10L, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(createIntStats(10L, 100L))));
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

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max) {
    OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.newBuilder();
    decStats.setMinimum(min);
    decStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDecimalStatistics(decStats.build()).build();
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
  public void testPredEvalWithIntStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10, 100), pred));
  }

  @Test
  public void testPredEvalWithDoubleStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDoubleStats(10.0, 100.0), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDoubleStats(10.0, 100.0), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDoubleStats(10.0, 100.0), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDoubleStats(10.0, 100.0), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDoubleStats(10.0, 100.0), pred));
  }

  @Test
  public void testPredEvalWithStringStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 100, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("10", "1000"), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 100.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("10", "1000"), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "100", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("10", "1000"), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(100), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("10", "1000"), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(100), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("10", "1000"), pred));
  }

  @Test
  public void testPredEvalWithDateStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "1970-01-11", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15.1", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "__a15__1", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "2000-01-16", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "1970-01-16", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(150), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDateStats(10, 100), pred));

  }

  @Test
  public void testPredEvalWithDecimalStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDecimalStats("10.0", "100.0"), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDecimalStats("10.0", "100.0"), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDecimalStats("10.0", "100.0"), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15), null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createDecimalStats("10.0", "100.0"), pred));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", HiveDecimal.create(15), null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createDecimalStats("10.0", "100.0"), pred));

  }

  @Test
  public void testEquals() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(0L, 10L), pred));
    assertEquals(TruthValue.YES,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 15L), pred));
  }

  @Test
  public void testNullSafeEquals() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(0L, 10L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 15L), pred));
  }

  @Test
  public void testLessThan() throws Exception {
    PredicateLeaf lessThan = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), lessThan));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 30L), lessThan));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), lessThan));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 15L), lessThan));
    assertEquals(TruthValue.YES,
        RecordReaderImpl.evaluatePredicate(createIntStats(0L, 10L), lessThan));
  }

  @Test
  public void testLessThanEquals() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.YES,
        RecordReaderImpl.evaluatePredicate(createIntStats(0L, 10L), pred));
  }

  @Test
  public void testIn() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.INTEGER,
            "x", null, args);
    assertEquals(TruthValue.YES,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 20L), pred));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(30L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(12L, 18L), pred));
  }

  @Test
  public void testBetween() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.INTEGER,
            "x", null, args);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(0L, 5L), pred));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(30L, 40L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(5L, 15L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 25L), pred));
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(5L, 25L), pred));
    assertEquals(TruthValue.YES,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 20L), pred));
    assertEquals(TruthValue.YES,
        RecordReaderImpl.evaluatePredicate(createIntStats(12L, 18L), pred));
  }

  @Test
  public void testIsNull() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.INTEGER,
            "x", null, null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), pred));
  }


  @Test
  public void testEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testNullSafeEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testLessThanWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.NO_NULL, // min, same stats
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "c", true), pred));
  }

  @Test
  public void testLessThanEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "c", true), pred)); // same
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
        RecordReaderImpl.evaluatePredicate(createStringStats("d", "e", true), pred));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("e", "f", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "c", true), pred)); // same
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
        RecordReaderImpl.evaluatePredicate(createStringStats("d", "e", true), pred));
    assertEquals(TruthValue.YES_NULL, // before & max
        RecordReaderImpl.evaluatePredicate(createStringStats("e", "f", true), pred));
    assertEquals(TruthValue.NO_NULL, // before & before
        RecordReaderImpl.evaluatePredicate(createStringStats("h", "g", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // before & min
        RecordReaderImpl.evaluatePredicate(createStringStats("f", "g", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // before & middle
        RecordReaderImpl.evaluatePredicate(createStringStats("e", "g", true), pred));

    assertEquals(TruthValue.YES_NULL, // min & after
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "e", true), pred));
    assertEquals(TruthValue.YES_NULL, // min & max
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "f", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // min & middle
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "g", true), pred));

    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("a", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL, // min & after, same stats
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "c", true), pred));
  }

  @Test
  public void testIsNullWithNullInStats() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.STRING,
            "x", null, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "d", true), pred));
    assertEquals(TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(createStringStats("c", "d", false), pred));
  }

  @Test
  public void testOverlap() throws Exception {
    assertTrue(!RecordReaderImpl.overlap(0, 10, -10, -1));
    assertTrue(RecordReaderImpl.overlap(0, 10, -1, 0));
    assertTrue(RecordReaderImpl.overlap(0, 10, -1, 1));
    assertTrue(RecordReaderImpl.overlap(0, 10, 2, 8));
    assertTrue(RecordReaderImpl.overlap(0, 10, 5, 10));
    assertTrue(RecordReaderImpl.overlap(0, 10, 10, 11));
    assertTrue(RecordReaderImpl.overlap(0, 10, 0, 10));
    assertTrue(RecordReaderImpl.overlap(0, 10, -1, 11));
    assertTrue(!RecordReaderImpl.overlap(0, 10, 11, 12));
  }

  private static List<RecordReaderImpl.DiskRange> diskRanges(Integer... points) {
    List<RecordReaderImpl.DiskRange> result =
        new ArrayList<RecordReaderImpl.DiskRange>();
    for(int i=0; i < points.length; i += 2) {
      result.add(new RecordReaderImpl.DiskRange(points[i], points[i+1]));
    }
    return result;
  }

  @Test
  public void testMergeDiskRanges() throws Exception {
    List<RecordReaderImpl.DiskRange> list = diskRanges();
    RecordReaderImpl.mergeDiskRanges(list);
    assertThat(list, is(diskRanges()));
    list = diskRanges(100, 200, 300, 400, 500, 600);
    RecordReaderImpl.mergeDiskRanges(list);
    assertThat(list, is(diskRanges(100, 200, 300, 400, 500, 600)));
    list = diskRanges(100, 200, 150, 300, 400, 500);
    RecordReaderImpl.mergeDiskRanges(list);
    assertThat(list, is(diskRanges(100, 300, 400, 500)));
    list = diskRanges(100, 200, 300, 400, 400, 500);
    RecordReaderImpl.mergeDiskRanges(list);
    assertThat(list, is(diskRanges(100, 200, 300, 500)));
    list = diskRanges(100, 200, 0, 300);
    RecordReaderImpl.mergeDiskRanges(list);
    assertThat(list, is(diskRanges(0, 300)));
    list = diskRanges(0, 500, 200, 400);
    RecordReaderImpl.mergeDiskRanges(list);
    assertThat(list, is(diskRanges(0, 500)));
    list = diskRanges(0, 100, 100, 200, 200, 300, 300, 400);
    RecordReaderImpl.mergeDiskRanges(list);
    assertThat(list, is(diskRanges(0, 400)));
  }

  @Test
  public void testGetIndexPosition() throws Exception {
    assertEquals(0, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.PRESENT, true, true));
    assertEquals(4, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(0, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, true, false));
    assertEquals(4, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DICTIONARY, OrcProto.Type.Kind.STRING,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(4, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(6, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.LENGTH, true, true));
    assertEquals(4, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.LENGTH, false, true));
    assertEquals(4, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(6, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, true, true));
    assertEquals(4, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, false, true));
    assertEquals(4, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(7, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, true, true));
    assertEquals(5, RecordReaderImpl.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, false, true));
  }

  @Test
  public void testPartialPlan() throws Exception {
    List<RecordReaderImpl.DiskRange> result;

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
        columns, rowGroups, false, encodings, types, 32768);
    assertThat(result, is(diskRanges(0, 1000, 100, 1000, 400, 1000,
        1000, 11000 + RecordReaderImpl.WORST_UNCOMPRESSED_SLOP,
        11000, 21000 + RecordReaderImpl.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderImpl.WORST_UNCOMPRESSED_SLOP)));

    // if we read no rows, don't read any bytes
    rowGroups = new boolean[]{false, false, false, false, false, false};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768);
    assertThat(result, is(diskRanges()));

    // all rows, but only columns 0 and 2.
    rowGroups = null;
    columns = new boolean[]{true, false, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, null, false, encodings, types, 32768);
    assertThat(result, is(diskRanges(100000, 102000, 102000, 200000)));

    rowGroups = new boolean[]{false, true, false, false, false, false};
    indexes[2] = indexes[1];
    indexes[1] = null;
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768);
    assertThat(result, is(diskRanges(100100, 102000,
        112000, 122000 + RecordReaderImpl.WORST_UNCOMPRESSED_SLOP)));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    indexes[1] = indexes[2];
    columns = new boolean[]{true, true, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000, 100500, 102000,
        152000, 200000)));
  }


  @Test
  public void testPartialPlanCompressed() throws Exception {
    List<RecordReaderImpl.DiskRange> result;

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
        columns, rowGroups, true, encodings, types, 32768);
    assertThat(result, is(diskRanges(0, 1000, 100, 1000,
        400, 1000, 1000, 11000+(2*32771),
        11000, 21000+(2*32771), 41000, 100000)));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, true, encodings, types, 32768);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000)));
  }

  @Test
  public void testPartialPlanString() throws Exception {
    List<RecordReaderImpl.DiskRange> result;

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
        columns, rowGroups, false, encodings, types, 32768);
    assertThat(result, is(diskRanges(100, 1000, 400, 1000, 500, 1000,
        11000, 21000 + RecordReaderImpl.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderImpl.WORST_UNCOMPRESSED_SLOP,
        51000, 95000, 95000, 97000, 97000, 100000)));
  }
}
