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

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.TestSearchArgumentImpl;
import org.junit.Test;

import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.Location;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestRecordReaderImpl {

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
  public void testGetMin() throws Exception {
    assertEquals(null, RecordReaderImpl.getMin(createIntStats(null, null)));
    assertEquals(10L, RecordReaderImpl.getMin(createIntStats(10L, 100L)));
    assertEquals(null, RecordReaderImpl.getMin(
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder().build())
            .build()));
    assertEquals(10.0d, RecordReaderImpl.getMin(
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                .setMinimum(10.0d).setMaximum(100.0d).build()).build()));
    assertEquals(null, RecordReaderImpl.getMin(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
            .build()));
    assertEquals("a", RecordReaderImpl.getMin(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build()));
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

  @Test
  public void testGetMax() throws Exception {
    assertEquals(null, RecordReaderImpl.getMax(createIntStats(null, null)));
    assertEquals(100L, RecordReaderImpl.getMax(createIntStats(10L, 100L)));
    assertEquals(null, RecordReaderImpl.getMax(
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder().build())
            .build()));
    assertEquals(100.0d, RecordReaderImpl.getMax(
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                .setMinimum(10.0d).setMaximum(100.0d).build()).build()));
    assertEquals(null, RecordReaderImpl.getMax(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
            .build()));
    assertEquals("b", RecordReaderImpl.getMax(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build()));
  }

  @Test
  public void testEquals() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(0L, 10L), pred));
    assertEquals(TruthValue.YES_NULL,
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
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), lessThan));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 30L), lessThan));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), lessThan));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 15L), lessThan));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(0L, 10L), lessThan));
  }

  @Test
  public void testLessThanEquals() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.INTEGER,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.YES_NULL,
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
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 20L), pred));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(30L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.NO_NULL,
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
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(0L, 5L), pred));
    assertEquals(TruthValue.NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(30L, 40L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(5L, 15L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(15L, 25L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(5L, 25L), pred));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(10L, 20L), pred));
    assertEquals(TruthValue.YES_NULL,
        RecordReaderImpl.evaluatePredicate(createIntStats(12L, 18L), pred));
  }

  @Test
  public void testIsNull() throws Exception {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.INTEGER,
            "x", null, null);
    assertEquals(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(createIntStats(20L, 30L), pred));
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
        columns, rowGroups, false, encodings, types, 32768);
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
        400, 1000, 1000, 11000+32771,
        11000, 21000+32771, 41000, 51000+32771)));

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
