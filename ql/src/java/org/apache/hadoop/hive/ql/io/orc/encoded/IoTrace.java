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

package org.apache.hadoop.hive.ql.io.orc.encoded;

import static org.apache.hadoop.hive.common.NumberUtils.getFirstInt;
import static org.apache.hadoop.hive.common.NumberUtils.getSecondInt;
import static org.apache.hadoop.hive.common.NumberUtils.makeIntPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.orc.OrcProto.Stream.Kind;
import org.apache.orc.OrcProto.*;
import org.slf4j.Logger;

/**
 * Single threaded IO trace.
 * Note: this can be made MTT merely by using an AtomicInteger, and storing a thread ID.
 */
public final class IoTrace {
  private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(IoTrace.class);
  private final long[] log;
  private int offset;
  private final boolean isAlwaysDump;
  private boolean hasDumped = false;

  public static enum RangesSrc {
    PLAN, CACHE, DISK, PREREAD
  }

  public IoTrace(int byteSize, boolean isAlwaysDump) {
    log = (byteSize == 0) ? null : new long[byteSize >> 3];
    this.isAlwaysDump = isAlwaysDump;
  }

  // Events.
  public static final int TREE_READER_NEXT_VECTOR = 1, READING_STRIPE = 2,
      SARG_RESULT = 4, RANGES = 5, COLUMN_READ = 6, SKIP_STREAM = 7,
      ADD_STREAM = 8, START_RG = 9, START_COL = 10, START_STRIPE_STREAM = 11,
      START_STREAM = 12, START_READ = 13, UNCOMPRESSED_DATA = 14,
      PARTIAL_UNCOMPRESSED_DATA = 15, VALID_UNCOMPRESSEED_CHUNK = 16, CACHE_COLLISION = 17,
      ORC_CB = 18, INVALID_ORC_CB = 19, PARTIAL_CB = 20, COMPOSITE_ORC_CB = 21, SARG_RESULT2 = 22;

  public void reset() {
    if (isAlwaysDump && !hasDumped) {
      dumpLog(LOG);
    }
    offset = 0;
    hasDumped = false;
  }

  public void dumpLog(Logger logger) {
    hasDumped = true;
    int ix = 0;
    logger.info("Dumping LLAP IO trace; " + (offset << 3) + " bytes");
    while (ix < offset) {
      ix = dumpOneLine(ix, logger, log);
    }
  }

  private static int dumpOneLine(int ix, Logger logger, long[] log) {
    int event = getFirstInt(log[ix]);
    switch (event) {
    case TREE_READER_NEXT_VECTOR: {
      logger.info(ix + ": TreeReader next vector " + getSecondInt(log[ix]));
      return ix + 1;
    }
    case READING_STRIPE: {
      logger.info(ix + ": Reading stripe " + getSecondInt(log[ix])
          + " at " + log[ix + 1] + " length " + log[ix + 2]);
      return ix + 3;
    }
    case SARG_RESULT: {
      logger.info(ix + ": Reading " + log[ix + 1] + " rgs for stripe " + getSecondInt(log[ix]));
      return ix + 2;
    }
    case SARG_RESULT2: {
      int rgsLength = (int) log[ix + 1];
      int elements = (rgsLength >> 6) + ((rgsLength & 63) == 0 ? 0 : 1);
      boolean[] rgs = new boolean[rgsLength];
      int rgsOffset = 0;
      for (int i = 0; i < elements; ++i) {
        long val = log[ix + i + 2];
        int bitsInByte = Math.min(rgsLength - rgsOffset, 64);
        for (int j = 0; j < rgsOffset; ++j, val >>>= 1) {
          rgs[rgsOffset + j] = (val & 1) == 1;
        }
        rgsOffset += bitsInByte;
      }
      logger.info(ix + ": Reading filtered rgs for stripe " + getSecondInt(log[ix])
          + ": " + DebugUtils.toString(rgs));
      return ix + (elements + 2);
    }
    case RANGES: {
      int val = getSecondInt(log[ix]);
      RangesSrc src = RangesSrc.values()[val >>> MAX_ELEMENT_BITS];
      int rangeCount = val & ((1 << MAX_ELEMENT_BITS) - 1);
      int currentOffset = ix + 3;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < rangeCount; ++i, currentOffset += 3) {
        sb.append(printRange(currentOffset, log)).append(", ");
      }
      logger.info(ix + ": Ranges for file " + log[ix + 1] + " (base offset " + log[ix + 2]
          + ") after " + src + ": " + sb.toString());
      return ix + 3 + rangeCount * 3;
    }
    case COLUMN_READ: {
      logger.info(ix + ": Reading column " + getSecondInt(log[ix]) + " (included index "
          + getFirstInt(log[ix + 1]) + "; type "
          + ColumnEncoding.Kind.values()[getSecondInt(log[ix + 1])] + ")");
      return ix + 2;
    }
    case SKIP_STREAM: {
      long streamOffset = log[ix + 1];
      logger.info(ix + ": Skipping stream for col " + getSecondInt(log[ix]) + " [" + streamOffset
          + ", " + (streamOffset + getFirstInt(log[ix + 2])) + ") kind "
          + Kind.values()[getSecondInt(log[ix + 2])]);
      return ix + 3;
    }
    case ADD_STREAM: {
      long streamOffset = log[ix + 1];
      logger.info(ix + ": Adding stream for col " + getSecondInt(log[ix]) + " [" + streamOffset
          + ", " + (streamOffset + getFirstInt(log[ix + 2])) + ") kind "
          + Kind.values()[getSecondInt(log[ix + 2])] + ", index " + getFirstInt(log[ix + 3])
          + ", entire stream " + (getSecondInt(log[ix + 3]) == 1));
      return ix + 4;
    }
    case START_RG: {
      logger.info(ix + ": Starting rg " + getSecondInt(log[ix]));
      return ix + 1;
    }
    case START_COL: {
      logger.info(ix + ": Starting column " + getSecondInt(log[ix]));
      return ix + 1;
    }
    case START_STRIPE_STREAM: {
      logger.info(ix + ": Starting stripe-level stream " + Kind.values()[getSecondInt(log[ix])]);
      return ix + 1;
    }
    case START_STREAM: {
      long offset = log[ix + 1];
      int unlockLen = getFirstInt(log[ix + 2]);
      String unlockStr = (unlockLen == Integer.MAX_VALUE) ? "" : " unlock " + (offset + unlockLen);
      logger.info(ix + ": Starting on stream " + Kind.values()[getSecondInt(log[ix])] + "["
          + offset + ", "  + (offset + getSecondInt(log[ix + 2])) + ") " + unlockStr);
      return ix + 3;
    }
    case START_READ: {
      logger.info(ix + ": Starting read at 0x" + Integer.toHexString(getSecondInt(log[ix])));
      return ix + 1;
    }
    case UNCOMPRESSED_DATA: {
      long offset = log[ix + 1];
      logger.info(ix + ": Uncompressed data ["
          + offset + ", " + (offset + getSecondInt(log[ix])) + ")");
      return ix + 2;
    }
    case PARTIAL_UNCOMPRESSED_DATA: {
      long offset = log[ix + 1];
      logger.info(ix + ": Partial uncompressed data ["
          + offset + ", " + (offset + getSecondInt(log[ix])) + ")");
      return ix + 2;
    }
    case VALID_UNCOMPRESSEED_CHUNK: {
      logger.info(ix + ": Combining uncompressed data for cache buffer of length "
          + getSecondInt(log[ix]) + " from 0x" + Integer.toHexString((int)log[ix + 1]));
      return ix + 2;
    }
    case CACHE_COLLISION: {
      logger.info(ix + ": Replacing " + printRange(ix + 1, log) + " with 0x"
          + Integer.toHexString(getSecondInt(log[ix])));
      return ix + 4;
    }
    case ORC_CB: {
      long offset = log[ix + 1];
      int val = getSecondInt(log[ix]);
      boolean isUncompressed = (val & 1) == 1;
      int cbLength = val >>> 1;
      logger.info(ix + ": Found " + (isUncompressed ? "un" : "") + "compressed ORC CB ["
          + offset + ", " + (offset + cbLength) + ")");
      return ix + 2;
    }
    case INVALID_ORC_CB: {
      long offset = log[ix + 1];
      logger.info(ix + ": Found incomplete ORC CB ["
          + offset + ", " + (offset + getSecondInt(log[ix])) + ")");
      return ix + 2;
    }
    case PARTIAL_CB: {
      logger.info(ix + ": Found buffer with a part of ORC CB " + printRange(ix + 1, log));
      return ix + 4;
    }
    case COMPOSITE_ORC_CB: {
      logger.info(ix + ": Combined ORC CB from multiple buffers " + printRange(ix + 2, log)
          + " last chunk taken " + getSecondInt(log[ix]) + ", remaining " + log[ix + 1]);
      return ix + 5;
    }
    default: throw new AssertionError("Unknown " + event);
    }
  }

  public void logTreeReaderNextVector(int idx) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 1 > log.length) return;
    log[offset] = makeIntPair(TREE_READER_NEXT_VECTOR, idx);
    this.offset += 1;
  }

  public void logReadingStripe(int stripeIx, long stripeOffset, long length) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 3 > log.length) return;
    log[offset] = makeIntPair(READING_STRIPE, stripeIx);
    log[offset + 1] = stripeOffset;
    log[offset + 2] = length;
    this.offset += 3;
  }

  public void logSargResult(int stripeIx, int rgCount) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 2 > log.length) return;
    log[offset] = makeIntPair(SARG_RESULT, stripeIx);
    log[offset + 1] = rgCount;
    this.offset += 2;
  }

  public void logSargResult(int stripeIx, boolean[] rgsToRead) {
    if (log == null) return;
    int offset = this.offset;
    int elements = (rgsToRead.length >> 6) + ((rgsToRead.length & 63) == 0 ? 0 : 1);
    if (offset + elements + 2 > log.length) return;
    log[offset] = makeIntPair(SARG_RESULT2, stripeIx);
    log[offset + 1] = rgsToRead.length;
    for (int i = 0, valOffset = 0; i < elements; ++i, valOffset += 64) {
      long val = 0;
      for (int j = 0; j < 64; ++j) {
        int ix = valOffset + j;
        if (rgsToRead.length == ix) break;
        if (!rgsToRead[ix]) continue;
        val = val | (1 << j);
      }
      log[offset + i + 2] = val;
    }
    this.offset += (elements + 2);
  }

  // Safety limit for potential list bugs.
  private static int MAX_ELEMENT_BITS = 17, MAX_ELEMENTS = (1 << MAX_ELEMENT_BITS) - 1;
  public void logRanges(Object fileKey, long baseOffset, DiskRangeList range, RangesSrc src) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 3 > log.length) return; // At least the header should fit.
    log[offset + 1] = (fileKey instanceof Long) ? (long)fileKey : fileKey.hashCode();
    log[offset + 2] = baseOffset;
    int elementCount = 0;
    int currentOffset = offset + 3;
    while (range != null && elementCount < MAX_ELEMENTS) {
      if (currentOffset + 3 > log.length) break;
      logRange(range, currentOffset);
      currentOffset += 3;
      ++elementCount;
      range = range.next;
    }
    log[offset] = makeIntPair(RANGES, (src.ordinal() << MAX_ELEMENT_BITS) | elementCount);
    this.offset = currentOffset;
  }

  private void logRange(DiskRange range, int currentOffset) {
    log[currentOffset] = range.getOffset();
    log[currentOffset + 1] = range.getEnd();
    log[currentOffset + 2] = range.hasData() ? System.identityHashCode(range.getData()) : 0;
  }

  private static String printRange(int ix, long[] log) {
    return "[" + log[ix] + ", " + log[ix + 1] + "): 0x" + Integer.toHexString((int)log[ix + 2]);
  }


  public void logColumnRead(int colIx, int includedIx, ColumnEncoding.Kind kind) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 2 > log.length) return;
    log[offset] = makeIntPair(COLUMN_READ, colIx);
    log[offset + 1] = makeIntPair(includedIx, kind.ordinal());
    this.offset += 2;
  }

  public void logSkipStream(int colIx, Stream.Kind streamKind, long streamOffset, long length) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 3 > log.length) return;
    log[offset] = makeIntPair(SKIP_STREAM, colIx);
    log[offset + 1] = streamOffset;
    log[offset + 2] = makeIntPair((int)length, streamKind.ordinal());
    this.offset += 3;
  }

  public void logAddStream(int colIx, Stream.Kind streamKind, long streamOffset,
      long length, int indexIx, boolean isEntire) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 4 > log.length) return;
    log[offset] = makeIntPair(ADD_STREAM, colIx);
    log[offset + 1] = streamOffset;
    log[offset + 2] = makeIntPair((int)length, streamKind.ordinal());
    log[offset + 3] = makeIntPair(indexIx, isEntire ? 1 : 0);
    this.offset += 4;
  }

  public void logStartRg(int rgIx) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 1 > log.length) return;
    log[offset] = makeIntPair(START_RG, rgIx);
    this.offset += 1;
  }

  public void logStartCol(int colIx) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 1 > log.length) return;
    log[offset] = makeIntPair(START_COL, colIx);
    this.offset += 1;
  }

  public void logStartStripeStream(Kind kind) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 1 > log.length) return;
    log[offset] = makeIntPair(START_STRIPE_STREAM, kind.ordinal());
    this.offset += 1;
  }

  public void logStartStream(Kind kind, long cOffset, long endCOffset,
      long unlockUntilCOffset) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 3 > log.length) return;
    log[offset] = makeIntPair(START_STREAM, kind.ordinal());
    log[offset + 1] = cOffset;
    long unlockLen = unlockUntilCOffset - cOffset;
    int unlockLenToSave = unlockLen >= 0 && unlockLen < Integer.MAX_VALUE
        ? (int)unlockLen : Integer.MAX_VALUE;
    log[offset + 2] = makeIntPair(unlockLenToSave, (int)(endCOffset - cOffset));
    this.offset += 3;
  }

  public void logStartRead(DiskRangeList current) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 1 > log.length) return;
    log[offset] = makeIntPair(START_READ,
        current.hasData() ? System.identityHashCode(current.getData()) : 0);
    this.offset += 1;
  }

  public void logUncompressedData(long dataOffset, long end) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 2 > log.length) return;
    log[offset] = makeIntPair(UNCOMPRESSED_DATA, (int)(end - dataOffset));
    log[offset + 1] = dataOffset;
    this.offset += 2;
  }

  public void logPartialUncompressedData(long partOffset, long candidateEnd, boolean fromCache) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 2 > log.length) return;
    log[offset] = makeIntPair(PARTIAL_UNCOMPRESSED_DATA, (int)(candidateEnd - partOffset));
    log[offset + 1] = partOffset;
    this.offset += 2;
  }

  public void logValidUncompresseedChunk(int totalLength, DiskRange chunk) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 2 > log.length) return;
    log[offset] = makeIntPair(VALID_UNCOMPRESSEED_CHUNK, totalLength);
    log[offset + 1] = chunk.hasData() ? System.identityHashCode(chunk.getData()) : 0;
    this.offset += 2;
  }

  public void logCacheCollision(DiskRange replacedChunk, MemoryBuffer replacementBuffer) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 4 > log.length) return;
    log[offset] = makeIntPair(CACHE_COLLISION, System.identityHashCode(replacementBuffer));
    logRange(replacedChunk, offset + 1);
    this.offset += 4;
  }

  public void logOrcCb(long cbStartOffset, int cbLength, boolean isUncompressed) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 2 > log.length) return;
    log[offset] = makeIntPair(ORC_CB, (cbLength << 1) | (isUncompressed ? 1 : 0));
    log[offset + 1] = cbStartOffset;
    this.offset += 2;
  }

  public void logInvalidOrcCb(long cbStartOffset, long end) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 2 > log.length) return;
    log[offset] = makeIntPair(INVALID_ORC_CB, (int)(end - cbStartOffset));
    log[offset + 1] = cbStartOffset;
    this.offset += 2;
  }

  public void logPartialCb(DiskRange current) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 4 > log.length) return;
    log[offset] = makeIntPair(PARTIAL_CB, 0);
    logRange(current, offset + 1);
    this.offset += 4;
  }

  public void logCompositeOrcCb(int lastChunkTaken, int lastChunkRemaining, DiskRange cc) {
    if (log == null) return;
    int offset = this.offset;
    if (offset + 5 > log.length) return;
    log[offset] = makeIntPair(COMPOSITE_ORC_CB, lastChunkTaken);
    log[offset + 1] = lastChunkRemaining;
    logRange(cc, offset + 2);
    this.offset += 5;
  }

  public static FixedSizedObjectPool<IoTrace> createTracePool(Configuration conf) {
    final int ioTraceSize = (int)HiveConf.getSizeVar(conf, ConfVars.LLAP_IO_TRACE_SIZE);
    final boolean isAlwaysDump = HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_TRACE_ALWAYS_DUMP);
    int ioThreads = HiveConf.getIntVar(conf, ConfVars.LLAP_IO_THREADPOOL_SIZE);
    return new FixedSizedObjectPool<>(ioThreads, new Pool.PoolObjectHelper<IoTrace>() {
      @Override
      public IoTrace create() {
        return new IoTrace(ioTraceSize, isAlwaysDump);
      }

      @Override
      public void resetBeforeOffer(IoTrace t) {
        t.reset();
      }
    });
  }
}