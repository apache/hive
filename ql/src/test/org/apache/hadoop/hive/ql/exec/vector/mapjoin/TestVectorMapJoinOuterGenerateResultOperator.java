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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VoidColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HIVE-29598: verifies {@link VectorMapJoinOuterGenerateResultOperator} clears
 * every small-table slot for unmatched rows, so stale values cannot carry over
 * past the null marking.
 */
class TestVectorMapJoinOuterGenerateResultOperator {

  private static final class TestableOuterOp extends VectorMapJoinOuterGenerateResultOperator {
    @Override
    protected String getLoggingPrefix() {
      throw new UnsupportedOperationException("stubbed only to instantiate abstract class under test");
    }

    @Override
    public void processBatch(VectorizedRowBatch batch) {
      throw new UnsupportedOperationException("stubbed only to instantiate abstract class under test");
    }
  }

  /**
   * Records {@code clearSlotValue} invocations to verify the operator dispatches
   * through {@code clearValue}, not just produces the slot-clearing side effect.
   */
  private static class TrackingLongColumnVector extends LongColumnVector {
    final List<Integer> clearedIndices = new ArrayList<>();

    TrackingLongColumnVector(int size) {
      super(size);
    }

    @Override
    protected void clearSlotValue(int elementNum) {
      super.clearSlotValue(elementNum);
      clearedIndices.add(elementNum);
    }
  }

  @Test
  void generateOuterNullsCallsClearValueOnEachMappedColumnForEachUnmatchedRow() throws HiveException, IOException {
    TestableOuterOp op = new TestableOuterOp();
    op.outerSmallTableKeyColumnMap = new int[] {0};
    op.smallTableValueColumnMap = new int[] {1, 2};

    VectorizedRowBatch batch = new VectorizedRowBatch(3, 4);
    TrackingLongColumnVector keyCol = new TrackingLongColumnVector(4);
    TrackingLongColumnVector valCol1 = new TrackingLongColumnVector(4);
    TrackingLongColumnVector valCol2 = new TrackingLongColumnVector(4);
    keyCol.vector[1] = 99L;
    valCol1.vector[1] = 88L;
    valCol2.vector[3] = 77L;
    batch.cols[0] = keyCol;
    batch.cols[1] = valCol1;
    batch.cols[2] = valCol2;

    int[] noMatchs = new int[] {1, 3};
    op.generateOuterNulls(batch, noMatchs, noMatchs.length);

    assertEquals(Arrays.asList(1, 3), keyCol.clearedIndices);
    assertEquals(Arrays.asList(1, 3), valCol1.clearedIndices);
    assertEquals(Arrays.asList(1, 3), valCol2.clearedIndices);

    assertFalse(keyCol.noNulls);
    assertTrue(keyCol.isNull[1]);
    assertTrue(keyCol.isNull[3]);
    assertFalse(keyCol.isNull[0]);
    assertFalse(keyCol.isNull[2]);

    assertEquals(0L, keyCol.vector[1]);
    assertEquals(0L, valCol1.vector[1]);
    assertEquals(0L, valCol2.vector[3]);
  }

  @Test
  void generateOuterNullsRepeatedAllCallsClearValueAtIndexZeroForEachMappedColumn() throws HiveException {
    TestableOuterOp op = new TestableOuterOp();
    op.outerSmallTableKeyColumnMap = new int[] {0};
    op.smallTableValueColumnMap = new int[] {1};

    VectorizedRowBatch batch = new VectorizedRowBatch(2, 4);
    TrackingLongColumnVector keyCol = new TrackingLongColumnVector(4);
    TrackingLongColumnVector valCol = new TrackingLongColumnVector(4);
    keyCol.vector[0] = 42L;
    valCol.vector[0] = 84L;
    batch.cols[0] = keyCol;
    batch.cols[1] = valCol;

    op.generateOuterNullsRepeatedAll(batch);

    assertEquals(Arrays.asList(0), keyCol.clearedIndices);
    assertEquals(Arrays.asList(0), valCol.clearedIndices);

    // isRepeating is set by the operator, not by clearValue.
    assertFalse(keyCol.noNulls);
    assertTrue(keyCol.isNull[0]);
    assertTrue(keyCol.isRepeating);
    assertFalse(valCol.noNulls);
    assertTrue(valCol.isNull[0]);
    assertTrue(valCol.isRepeating);

    assertEquals(0L, keyCol.vector[0]);
    assertEquals(0L, valCol.vector[0]);
  }

  @Test
  void generateOuterNullsSetsBookkeepingOnTypeWithNoClearSlotValueOverride() throws HiveException, IOException {
    // VoidColumnVector inherits the base no-op clearSlotValue — verifies the
    // operator still drives the null-marking through clearValue() on a type
    // without a per-slot value to zero.
    TestableOuterOp op = new TestableOuterOp();
    op.outerSmallTableKeyColumnMap = new int[] {};
    op.smallTableValueColumnMap = new int[] {0};

    VectorizedRowBatch batch = new VectorizedRowBatch(1, 4);
    VoidColumnVector voidCol = new VoidColumnVector(4);
    batch.cols[0] = voidCol;

    int[] noMatchs = new int[] {1, 3};
    op.generateOuterNulls(batch, noMatchs, noMatchs.length);

    assertFalse(voidCol.noNulls);
    assertTrue(voidCol.isNull[1]);
    assertTrue(voidCol.isNull[3]);
    assertFalse(voidCol.isNull[0]);
    assertFalse(voidCol.isNull[2]);
  }

  /**
   * For each {@link ColumnVector} subclass whose {@code clearSlotValue} is
   * overridden, verifies the operator's call through {@code clearValue} reaches
   * the override and clears the slot to the type's cleared state.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("modifiedColumnVectorTypes")
  void generateOuterNullsClearsSlotForEachModifiedType(
      String typeName,
      ColumnVector cv,
      Runnable preLoad,
      Runnable assertSlotCleared) throws HiveException, IOException {

    TestableOuterOp op = new TestableOuterOp();
    op.outerSmallTableKeyColumnMap = new int[] {};
    op.smallTableValueColumnMap = new int[] {0};

    VectorizedRowBatch batch = new VectorizedRowBatch(1, 4);
    preLoad.run();
    batch.cols[0] = cv;

    int[] noMatchs = new int[] {2};
    op.generateOuterNulls(batch, noMatchs, noMatchs.length);

    assertTrue(cv.isNull[2]);
    assertFalse(cv.noNulls);
    assertSlotCleared.run();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("modifiedColumnVectorTypesAtSlotZero")
  void generateOuterNullsRepeatedAllClearsSlotForEachModifiedType(
      String typeName,
      ColumnVector cv,
      Runnable preLoad,
      Runnable assertSlotCleared) throws HiveException {

    TestableOuterOp op = new TestableOuterOp();
    op.outerSmallTableKeyColumnMap = new int[] {};
    op.smallTableValueColumnMap = new int[] {0};

    VectorizedRowBatch batch = new VectorizedRowBatch(1, 4);
    preLoad.run();
    batch.cols[0] = cv;

    op.generateOuterNullsRepeatedAll(batch);

    assertTrue(cv.isNull[0]);
    assertFalse(cv.noNulls);
    assertTrue(cv.isRepeating);
    assertSlotCleared.run();
  }

  static Stream<Arguments> modifiedColumnVectorTypesAtSlotZero() {
    final LongColumnVector longCv = new LongColumnVector(4);
    final DoubleColumnVector doubleCv = new DoubleColumnVector(4);
    final BytesColumnVector bytesCv = new BytesColumnVector(4);
    final DecimalColumnVector decCv = new DecimalColumnVector(4, 18, 4);
    final Decimal64ColumnVector dec64Cv = new Decimal64ColumnVector(4, 18, 4);
    final TimestampColumnVector tsCv = new TimestampColumnVector(4);
    final IntervalDayTimeColumnVector ivCv = new IntervalDayTimeColumnVector(4);

    return Stream.of(
        Arguments.of(
            "LongColumnVector",
            longCv,
            (Runnable) () -> longCv.vector[0] = 999L,
            (Runnable) () -> assertEquals(0L, longCv.vector[0])),
        Arguments.of(
            "DoubleColumnVector",
            doubleCv,
            (Runnable) () -> doubleCv.vector[0] = 3.14,
            (Runnable) () -> assertEquals(0.0, doubleCv.vector[0])),
        Arguments.of(
            "BytesColumnVector",
            bytesCv,
            (Runnable) () -> {
              bytesCv.vector[0] = "stale".getBytes(StandardCharsets.UTF_8);
              bytesCv.start[0] = 1;
              bytesCv.length[0] = 3;
            },
            (Runnable) () -> {
              assertNull(bytesCv.vector[0]);
              assertEquals(0, bytesCv.start[0]);
              assertEquals(0, bytesCv.length[0]);
            }),
        Arguments.of(
            "DecimalColumnVector",
            decCv,
            (Runnable) () -> decCv.vector[0].setFromLong(999L),
            (Runnable) () -> assertEquals(0L, decCv.vector[0].serialize64(decCv.scale))),
        Arguments.of(
            "Decimal64ColumnVector",
            dec64Cv,
            (Runnable) () -> dec64Cv.vector[0] = 999L,
            (Runnable) () -> assertEquals(0L, dec64Cv.vector[0])),
        Arguments.of(
            "TimestampColumnVector",
            tsCv,
            (Runnable) () -> {
              tsCv.time[0] = 1234567890000L;
              tsCv.nanos[0] = 999;
            },
            (Runnable) () -> {
              assertEquals(0L, tsCv.time[0]);
              assertEquals(1, tsCv.nanos[0]);
            }),
        Arguments.of(
            "IntervalDayTimeColumnVector",
            ivCv,
            (Runnable) () -> ivCv.set(0, new HiveIntervalDayTime(5, 0)),
            (Runnable) () -> {
              assertEquals(0L, ivCv.getTotalSeconds(0));
              assertEquals(1, ivCv.getNanos(0));
            })
    );
  }

  static Stream<Arguments> modifiedColumnVectorTypes() {
    final LongColumnVector longCv = new LongColumnVector(4);
    final DoubleColumnVector doubleCv = new DoubleColumnVector(4);
    final BytesColumnVector bytesCv = new BytesColumnVector(4);
    final DecimalColumnVector decCv = new DecimalColumnVector(4, 18, 4);
    final Decimal64ColumnVector dec64Cv = new Decimal64ColumnVector(4, 18, 4);
    final TimestampColumnVector tsCv = new TimestampColumnVector(4);
    final IntervalDayTimeColumnVector ivCv = new IntervalDayTimeColumnVector(4);

    return Stream.of(
        Arguments.of(
            "LongColumnVector",
            longCv,
            (Runnable) () -> longCv.vector[2] = 999L,
            (Runnable) () -> assertEquals(0L, longCv.vector[2])),
        Arguments.of(
            "DoubleColumnVector",
            doubleCv,
            (Runnable) () -> doubleCv.vector[2] = 3.14,
            (Runnable) () -> assertEquals(0.0, doubleCv.vector[2])),
        Arguments.of(
            "BytesColumnVector",
            bytesCv,
            (Runnable) () -> {
              bytesCv.vector[2] = "stale".getBytes(StandardCharsets.UTF_8);
              bytesCv.start[2] = 1;
              bytesCv.length[2] = 3;
            },
            (Runnable) () -> {
              assertNull(bytesCv.vector[2]);
              assertEquals(0, bytesCv.start[2]);
              assertEquals(0, bytesCv.length[2]);
            }),
        Arguments.of(
            "DecimalColumnVector",
            decCv,
            (Runnable) () -> decCv.vector[2].setFromLong(999L),
            (Runnable) () -> assertEquals(0L, decCv.vector[2].serialize64(decCv.scale))),
        Arguments.of(
            "Decimal64ColumnVector",
            dec64Cv,
            (Runnable) () -> dec64Cv.vector[2] = 999L,
            (Runnable) () -> assertEquals(0L, dec64Cv.vector[2])),
        Arguments.of(
            "TimestampColumnVector",
            tsCv,
            (Runnable) () -> {
              tsCv.time[2] = 1234567890000L;
              tsCv.nanos[2] = 999;
            },
            (Runnable) () -> {
              // setNullValue convention: time = 0, nanos = 1
              assertEquals(0L, tsCv.time[2]);
              assertEquals(1, tsCv.nanos[2]);
            }),
        Arguments.of(
            "IntervalDayTimeColumnVector",
            ivCv,
            (Runnable) () -> ivCv.set(2, new HiveIntervalDayTime(5, 0)),
            (Runnable) () -> {
              // setNullValue convention: totalSeconds = 0, nanos = 1
              assertEquals(0L, ivCv.getTotalSeconds(2));
              assertEquals(1, ivCv.getNanos(2));
            })
    );
  }
}
