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
package org.apache.hadoop.hive.llap.chunk;

import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.api.Vector.Type;
import org.apache.hadoop.hive.llap.cache.BufferPool;
import org.apache.hadoop.hive.llap.cache.BufferPool.WeakBuffer;
import org.apache.hadoop.hive.llap.chunk.ChunkWriterImpl;
import org.apache.hadoop.hive.llap.chunk.ChunkWriter.NullsState;
import org.apache.hadoop.hive.llap.loader.BufferInProgress;
import org.apache.hadoop.hive.llap.loader.ChunkPool.Chunk;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestChunkReaderAndWriter {
  private static final Log LOG = LogFactory.getLog(TestChunkReaderAndWriter.class);
  private static final int LARGE_BUFFER = 1024 * 1024;

  @Test
  public void testSimpleValues() throws Exception {
    testValues(0.0);
  }

  @Test
  public void testValuesWithNulls() throws Exception {
    testValues(0.02);
    testValues(0.5);
    testValues(0.98);
  }

  @Test
  public void testNullSegmentTransitions() throws Exception {
    Random random = new Random(1234);
    ChunkWriterImpl writer = new ChunkWriterImpl();
    BufferInProgress buf = new BufferInProgress(createBuffer(LARGE_BUFFER));
    writer.prepare(buf);
    int ncount1 = 1, ncount2 = 3, ncount3 = 60, ncount4 = 3;
    Long[] src = new Long[ncount1 + ncount2 + ncount3 + ncount4 + 1];
    int offset = 0;

    // Try various combinations of repeating and non-repeating segments.
    offset = writeNulls(writer, ncount1, src, offset);
    long[] tmp = new long[1];
    tmp[0] = random.nextLong();
    src[offset] = tmp[0];
    writer.writeLongs(tmp, 0, 1, NullsState.NEXT_NULL);
    ++offset;
    offset = writeNulls(writer, ncount2, src, offset);
    offset = writeNulls(writer, ncount3, src, offset);
    offset = writeNulls(writer, ncount4, src, offset);

    writer.finishCurrentSegment();
    verifyValues(completeChunk(writer, buf), src);
  }

  @Test
  public void testRepeatingSegmentTransitions() throws Exception {
    Random random = new Random(1234);
    ChunkWriterImpl writer = new ChunkWriterImpl();
    BufferInProgress buf = new BufferInProgress(createBuffer(LARGE_BUFFER));
    writer.prepare(buf);
    int repeating1 = 10, rcount1 = 5, repeating2 = -10, rcount2 = 5, repeating3 = 4, rcount3 = 15;
    int nrcount1 = 30, nrcount2 = 30, nrcount3 = 30;
    long[] src = new long[rcount1 + rcount2 + rcount3 + nrcount1 + nrcount2 + nrcount3];
    int offset = 0;

    // Try various combinations of repeating and non-repeating segments.
    offset = writeRepeatedValues(writer, repeating1, rcount1, src, offset);
    offset = writeLongs(random, writer, nrcount1, src, offset);
    offset = writeRepeatedValues(writer, repeating2, rcount2, src, offset);
    offset = writeLongs(random, writer, nrcount2, src, offset);
    offset = writeRepeatedValues(writer, repeating3, rcount3, src, offset);
    offset = writeLongs(random, writer, nrcount3, src, offset);

    writer.finishCurrentSegment();
    verifyValues(completeChunk(writer, buf), src);
  }

  @Test
  public void testBufferBoundary() throws Exception {
    Random random = new Random(1234);
    ChunkWriterImpl writer = new ChunkWriterImpl();
    BufferInProgress buf = new BufferInProgress(createBuffer(128 * 8));
    writer.prepare(buf);
    long[] tmp = new long[122];
    Long[] src = new Long[tmp.length + 2];
    src[0] = tmp[0] = 3;
    // This should start a segment with bitmasks and use up 4 * 8 bytes total.
    // Plus, 8 bytes are already used up for the chunk header.
    writer.writeLongs(tmp, 0, 1, NullsState.NEXT_NULL);
    src[1] = null;
    writer.writeNulls(1, true);
    // Now we have 123 * 8 bytes; 122 values would not fit w/bitmask, but they will w/o one.
    for (int i = 0; i < tmp.length; ++i) {
      src[i + 2] = tmp[i] = random.nextLong();
    }
    writer.writeLongs(tmp, 0, tmp.length, NullsState.NEXT_NULL);
    writer.finishCurrentSegment();
    verifyValues(completeChunk(writer, buf), src);
  }

  @Test
  public void testBoundaryValues() throws Exception {
    ChunkWriterImpl writer = new ChunkWriterImpl();
    BufferInProgress buf = new BufferInProgress(createBuffer(LARGE_BUFFER));
    writer.prepare(buf);
    long[] src = new long[9];
    Arrays.fill(src, 0l);
    src[2] = Long.MIN_VALUE;
    src[6] = Long.MAX_VALUE;
    src[8] = Integer.MIN_VALUE;
    writer.writeLongs(src, 0, src.length, NullsState.NEXT_NULL);
    writer.finishCurrentSegment();
    verifyValues(completeChunk(writer, buf), src);
  }

  /**
   * A method for generic non-scenario tests.
   * @param nullFraction Percentage of nulls in values.
   */
  private void testValues(double nullFraction) throws Exception {
    Random random = new Random(1234);
    ChunkWriterImpl writer = new ChunkWriterImpl();
    WeakBuffer wb = createBuffer(LARGE_BUFFER);
    // Value counts to test
    int[] valueCounts = new int[] { 1, 3, 11, 64, 65, 2048 };
    int totalCount = 0;
    for (int valueCount : valueCounts) {
      LOG.info("Testing for value count " + valueCount);
      BufferInProgress buf = new BufferInProgress(wb);
      writer.prepare(buf);
      totalCount += valueCount;
      Long[] src = new Long[valueCount];
      writeLongs(writer, random, nullFraction, valueCount, src, 0);
      writer.finishCurrentSegment();
      verifyValues(completeChunk(writer, buf), src);
    }

    // Then try all together
    BufferInProgress buf = new BufferInProgress(wb);
    writer.prepare(buf);
    Long[] src = new Long[totalCount];
    int offset = 0;
    LOG.info("Testing for total count " + totalCount);
    for (int valueCount : valueCounts) {
      writeLongs(writer, random, nullFraction, valueCount, src, offset);
      offset += valueCount;
    }
    writer.finishCurrentSegment();
    verifyValues(completeChunk(writer, buf), src);
  }

  private static void writeLongs(ChunkWriterImpl writer,
      Random rdm, double nullFraction, int count, Long[] src, int srcOffset) {
    NullsState nullState = nullFraction == 0 ? NullsState.NO_NULLS : NullsState.NEXT_NULL;
    long[] srcPart = new long[count];
    int offset = 0;
    boolean isFirst = true, isNull = true;
    while (offset < count) {
      int runLength = 0;
      if (!isFirst) {
        isNull = !isNull;
        if (!isNull) {
          srcPart[0] = rdm.nextLong();
        }
        src[srcOffset + offset] = isNull ? null : srcPart[0];
        runLength = 1;
        ++offset;
      }
      while (offset < count) {
        boolean curIsNull = (rdm.nextDouble() <= nullFraction);
        if (!isFirst && curIsNull != isNull) break;
        isNull = curIsNull;
        if (!isNull) {
          srcPart[runLength] = rdm.nextLong();
        }
        src[srcOffset + offset] = isNull ? null : srcPart[runLength];
        ++runLength;
        ++offset;
        isFirst = false;
      }
      if (isNull) {
        LOG.info("Writing " + runLength + " nulls");
        writer.writeNulls(runLength, true);
      } else {
        LOG.info("Writing " + runLength + " values");
        writer.writeLongs(srcPart, 0, runLength, nullState);
      }
    }
  }

  private static int writeLongs(
      Random random, ChunkWriterImpl writer, int count, long[] src, int offset) {
    for (int i = 0; i < count; ++i) {
      src[offset + i] = random.nextLong();
    }
    writer.writeLongs(src, offset, count, NullsState.NO_NULLS);
    return offset + count;
  }

  private static int writeRepeatedValues(
      ChunkWriterImpl writer, int val, int count, long[] src, int offset) {
    for (int i = 0; i < count; ++i) {
      src[offset + i] = val;
    }
    writer.writeRepeatedLongs(val, count, NullsState.NO_NULLS);
    return offset + count;
  }

  private static int writeNulls(
      ChunkWriterImpl writer, int count, Long[] src, int offset) {
    for (int i = 0; i < count; ++i) {
      src[offset + i] = null;
    }
    writer.writeNulls(count, false);
    return offset + count;
  }

  private static void verifyValues(Chunk chunk, Long[] src) throws Exception {
    verifyValues(chunk, src, src.length);
  }

  private static void verifyValues(Chunk chunk, long[] src) throws Exception {
    verifyValues(chunk, src, src.length);
  }

  private static void verifyValues(Chunk chunk, Object src, int srcLength) throws Exception {
    boolean nullable = src instanceof Long[];
    Long[] src0 = nullable ? (Long[])src : null;
    long[] src1 = nullable ? null : (long[])src;
    int[] stepsToVerify = new int[] { srcLength, srcLength / 2, 63, 5, 1 };
    long[] dest = new long[srcLength];
    boolean[] isNull = new boolean[srcLength];
    int lastStep = -1;
    for (int step : stepsToVerify) {
      if (step > srcLength || step == lastStep || step == 0) continue;
      if ((srcLength / step) > 50) continue; // too slow
      LOG.info("Verifying value count " + srcLength + " w/step " + step);
      Arrays.fill(dest, -1);
      int offset = 0;
      ChunkReader reader = new ChunkReader(Type.LONG, chunk);
      while (offset < srcLength) {
        int adjStep = Math.min(srcLength - offset, step);
        try {
          reader.next(adjStep);
          reader.copyLongs(dest, isNull, offset);
        } catch (Exception ex) {
          LOG.error("srcLength " + srcLength + ", step "
              + adjStep + "/" + step + " offset " + offset, ex);
          throw ex;
        }
        offset += adjStep;
      }
      if (nullable) {
        verifyArrays(src0, dest, isNull);
      } else {
        verifyArrays(src1, dest, isNull);
      }
      lastStep = step;
    }
  }

  private static void verifyArrays(Long[] src, long[] dest, boolean[] isNull) {
    for (int i = 0; i < src.length; ++i) {
      boolean curIsNull = (src[i] == null);
      assertTrue(i + ": " + src[i] + " got " + dest[i] + "/" + isNull[i], curIsNull == isNull[i]);
      if (!curIsNull) {
        assertEquals(i + ": " + src[i] + " got " + dest[i], src[i].longValue(), dest[i]);
      }
    }
  }

  private static void verifyArrays(long[] src, long[] dest, boolean[] isNull) {
    for (int i = 0; i < src.length; ++i) {
      assertEquals(i + ": " + src[i] + " got " + dest[i], src[i], dest[i]);
      assertFalse(i + ": unexpected null", isNull[i]);
    }
  }

  private static Chunk completeChunk(ChunkWriterImpl writer, BufferInProgress buf) {
    int rows = buf.getChunkInProgressRows();
    Chunk c = buf.extractChunk();
    writer.finishChunk(c, rows);
    return c;
  }

  private static WeakBuffer createBuffer(int bufferSize) {
    HiveConf hc = new HiveConf();
    hc.setInt(HiveConf.ConfVars.LLAP_BUFFER_SIZE.varname, bufferSize);
    BufferPool bp = new BufferPool(hc, null);
    try {
      return bp.allocateBuffer();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
