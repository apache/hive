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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet.vector.probe;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Pure unit tests for the {@link ParquetProbeFilter} lifecycle: the ProbeDecode path calls
 * {@link ParquetProbeFilter#newBitmap} after probing, per-row column readers query
 * {@link ParquetProbeFilter#isSelected}, and once all columns are read
 * {@link ParquetProbeFilter#compact} materialises the {@code selected[]} that
 * {@code VectorizedRowBatch} consumes.
 */
public class TestParquetProbeFilter {

  @Test
  public void newBitmapRejectsNull() {
    try {
      ParquetProbeFilter.newBitmap(null);
      fail("expected IAE for null bitmap");
    } catch (IllegalArgumentException expected) {
      // ok
    }
  }

  @Test
  public void isSelectedReflectsBitmap() {
    boolean[] bits = { true, false, true, true, false };
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(bits);

    assertTrue(f.isSelected(0));
    assertFalse(f.isSelected(1));
    assertTrue(f.isSelected(2));
    assertTrue(f.isSelected(3));
    assertFalse(f.isSelected(4));
  }

  @Test
  public void isSelectedOutOfBoundsReturnsFalse() {
    // Guards against off-by-one bugs in callers that pass a rowId near the batch tail.
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(new boolean[] { true, true });
    assertFalse(f.isSelected(-1));
    assertFalse(f.isSelected(2));
    assertFalse(f.isSelected(Integer.MAX_VALUE));
  }

  @Test
  public void compactBeforeCallReturnsNullArrays() {
    // Explicit contract: getSelected() / getSelectedSize() are meaningless until compact() runs.
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(new boolean[] { true, false, true });
    assertNull(f.getSelected());
    assertEquals(0, f.getSelectedSize());
  }

  @Test
  public void compactMaterialisesSelected() {
    boolean[] bits = { true, false, true, true, false, true };
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(bits);

    ParquetProbeFilter same = f.compact(bits.length);
    assertSame("compact must return this for chaining", f, same);

    assertArrayEquals(new int[] { 0, 2, 3, 5 }, java.util.Arrays.copyOf(f.getSelected(), f.getSelectedSize()));
    assertEquals(4, f.getSelectedSize());
  }

  @Test
  public void compactHonoursBatchSize() {
    // batchSize can be smaller than the bitmap when the last row group ends mid-batch;
    // entries beyond batchSize must be ignored.
    boolean[] bits = { true, true, true, true };
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(bits);
    f.compact(2);

    assertEquals(2, f.getSelectedSize());
    assertArrayEquals(new int[] { 0, 1 }, java.util.Arrays.copyOf(f.getSelected(), f.getSelectedSize()));
  }

  @Test
  public void compactHandlesEmptyBatch() {
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(new boolean[0]);
    f.compact(0);
    assertEquals(0, f.getSelectedSize());
    assertArrayEquals(new int[0], f.getSelected());
  }

  @Test
  public void compactHandlesAllRejected() {
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(new boolean[] { false, false, false });
    f.compact(3);
    assertEquals(0, f.getSelectedSize());
    // getSelected() is allowed to return an over-provisioned array; only [0, selectedSize) counts.
    assertEquals(3, f.getSelected().length);
  }

  @Test
  public void compactHandlesAllAccepted() {
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(new boolean[] { true, true, true, true });
    f.compact(4);
    assertEquals(4, f.getSelectedSize());
    assertArrayEquals(new int[] { 0, 1, 2, 3 }, f.getSelected());
  }

  @Test
  public void compactIsIdempotent() {
    // VectorizedParquetRecordReader.applyProbeFilterToBatch may call compact() more than once
    // if the same filter is passed to several helpers; a second call must be a no-op.
    boolean[] bits = { true, false, true };
    ParquetProbeFilter f = ParquetProbeFilter.newBitmap(bits);
    f.compact(bits.length);
    int[] firstArray = f.getSelected();
    int firstSize = f.getSelectedSize();

    f.compact(bits.length);
    assertSame("compact must not reallocate on repeat", firstArray, f.getSelected());
    assertEquals(firstSize, f.getSelectedSize());
  }
}
