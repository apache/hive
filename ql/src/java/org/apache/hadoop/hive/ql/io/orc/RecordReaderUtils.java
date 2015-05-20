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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListCreateHelper;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListMutateHelper;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.BufferChunk;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.HadoopShims.ByteBufferPoolShim;
import org.apache.hadoop.hive.shims.HadoopShims.ZeroCopyReaderShim;

import com.google.common.collect.ComparisonChain;

/**
 * Stateless methods shared between RecordReaderImpl and EncodedReaderImpl.
 */
public class RecordReaderUtils {
  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();
  static boolean[] findPresentStreamsByColumn(
      List<OrcProto.Stream> streamList, List<OrcProto.Type> types) {
    boolean[] hasNull = new boolean[types.size()];
    for(OrcProto.Stream stream: streamList) {
      if (stream.hasKind() && (stream.getKind() == OrcProto.Stream.Kind.PRESENT)) {
        hasNull[stream.getColumn()] = true;
      }
    }
    return hasNull;
  }

  /**
   * Does region A overlap region B? The end points are inclusive on both sides.
   * @param leftA A's left point
   * @param rightA A's right point
   * @param leftB B's left point
   * @param rightB B's right point
   * @return Does region A overlap region B?
   */
  static boolean overlap(long leftA, long rightA, long leftB, long rightB) {
    if (leftA <= leftB) {
      return rightA >= leftB;
    }
    return rightB >= leftA;
  }

  static void addEntireStreamToRanges(
      long offset, long length, DiskRangeListCreateHelper list, boolean doMergeBuffers) {
    list.addOrMerge(offset, offset + length, doMergeBuffers, false);
  }

  static void addRgFilteredStreamToRanges(OrcProto.Stream stream,
      boolean[] includedRowGroups, boolean isCompressed, OrcProto.RowIndex index,
      OrcProto.ColumnEncoding encoding, OrcProto.Type type, int compressionSize, boolean hasNull,
      long offset, long length, DiskRangeListCreateHelper list, boolean doMergeBuffers) {
    for (int group = 0; group < includedRowGroups.length; ++group) {
      if (!includedRowGroups[group]) continue;
      int posn = getIndexPosition(
          encoding.getKind(), type.getKind(), stream.getKind(), isCompressed, hasNull);
      long start = index.getEntry(group).getPositions(posn);
      final long nextGroupOffset;
      boolean isLast = group == (includedRowGroups.length - 1);
      nextGroupOffset = isLast ? length : index.getEntry(group + 1).getPositions(posn);

      start += offset;
      long end = offset + estimateRgEndOffset(
          isCompressed, isLast, nextGroupOffset, length, compressionSize);
      list.addOrMerge(start, end, doMergeBuffers, true);
    }
  }

  static long estimateRgEndOffset(boolean isCompressed, boolean isLast,
      long nextGroupOffset, long streamLength, int bufferSize) {
    // figure out the worst case last location
    // if adjacent groups have the same compressed block offset then stretch the slop
    // by factor of 2 to safely accommodate the next compression block.
    // One for the current compression block and another for the next compression block.
    long slop = isCompressed ? 2 * (OutStream.HEADER_SIZE + bufferSize) : WORST_UNCOMPRESSED_SLOP;
    return isLast ? streamLength : Math.min(streamLength, nextGroupOffset + slop);
  }

  private static final int BYTE_STREAM_POSITIONS = 1;
  private static final int RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
  private static final int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;
  private static final int RUN_LENGTH_INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;

  /**
   * Get the offset in the index positions for the column that the given
   * stream starts.
   * @param columnEncoding the encoding of the column
   * @param columnType the type of the column
   * @param streamType the kind of the stream
   * @param isCompressed is the file compressed
   * @param hasNulls does the column have a PRESENT stream?
   * @return the number of positions that will be used for that stream
   */
  public static int getIndexPosition(OrcProto.ColumnEncoding.Kind columnEncoding,
                              OrcProto.Type.Kind columnType,
                              OrcProto.Stream.Kind streamType,
                              boolean isCompressed,
                              boolean hasNulls) {
    if (streamType == OrcProto.Stream.Kind.PRESENT) {
      return 0;
    }
    int compressionValue = isCompressed ? 1 : 0;
    int base = hasNulls ? (BITFIELD_POSITIONS + compressionValue) : 0;
    switch (columnType) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case STRUCT:
      case MAP:
      case LIST:
      case UNION:
        return base;
      case CHAR:
      case VARCHAR:
      case STRING:
        if (columnEncoding == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            columnEncoding == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
          return base;
        } else {
          if (streamType == OrcProto.Stream.Kind.DATA) {
            return base;
          } else {
            return base + BYTE_STREAM_POSITIONS + compressionValue;
          }
        }
      case BINARY:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case DECIMAL:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case TIMESTAMP:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + RUN_LENGTH_INT_POSITIONS + compressionValue;
      default:
        throw new IllegalArgumentException("Unknown type " + columnType);
    }
  }

  // for uncompressed streams, what is the most overlap with the following set
  // of rows (long vint literal group).
  static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;

  /**
   * Is this stream part of a dictionary?
   * @return is this part of a dictionary?
   */
  static boolean isDictionary(OrcProto.Stream.Kind kind,
                              OrcProto.ColumnEncoding encoding) {
    assert kind != OrcProto.Stream.Kind.DICTIONARY_COUNT;
    OrcProto.ColumnEncoding.Kind encodingKind = encoding.getKind();
    return kind == OrcProto.Stream.Kind.DICTIONARY_DATA ||
      (kind == OrcProto.Stream.Kind.LENGTH &&
       (encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
        encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2));
  }

  /**
   * Build a string representation of a list of disk ranges.
   * @param ranges ranges to stringify
   * @return the resulting string
   */
  static String stringifyDiskRanges(DiskRangeList range) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    boolean isFirst = true;
    while (range != null) {
      if (!isFirst) {
        buffer.append(", ");
      }
      isFirst = false;
      buffer.append(range.toString());
      range = range.next;
    }
    buffer.append("]");
    return buffer.toString();
  }

  /**
   * Read the list of ranges from the file.
   * @param file the file to read
   * @param base the base of the stripe
   * @param ranges the disk ranges within the stripe to read
   * @return the bytes read for each disk range, which is the same length as
   *    ranges
   * @throws IOException
   */
  static DiskRangeList readDiskRanges(FSDataInputStream file,
                                 ZeroCopyReaderShim zcr,
                                 long base,
                                 DiskRangeList range,
                                 boolean doForceDirect) throws IOException {
    if (range == null) return null;
    DiskRangeList prev = range.prev;
    if (prev == null) {
      prev = new DiskRangeListMutateHelper(range);
    }
    while (range != null) {
      if (range.hasData()) {
        range = range.next;
        continue;
      }
      int len = (int) (range.getEnd() - range.getOffset());
      long off = range.getOffset();
      file.seek(base + off);
      if (zcr != null) {
        boolean hasReplaced = false;
        while (len > 0) {
          ByteBuffer partial = zcr.readBuffer(len, false);
          BufferChunk bc = new BufferChunk(partial, off);
          if (!hasReplaced) {
            range.replaceSelfWith(bc);
            hasReplaced = true;
          } else {
            range.insertAfter(bc);
          }
          range = bc;
          int read = partial.remaining();
          len -= read;
          off += read;
        }
      } else if (doForceDirect) {
        ByteBuffer directBuf = ByteBuffer.allocateDirect(len);
        readDirect(file, len, directBuf);
        range = range.replaceSelfWith(new BufferChunk(directBuf, range.getOffset()));
      } else {
        byte[] buffer = new byte[len];
        file.readFully(buffer, 0, buffer.length);
        range = range.replaceSelfWith(new BufferChunk(ByteBuffer.wrap(buffer), range.getOffset()));
      }
      range = range.next;
    }
    return prev.next;
  }

  public static void readDirect(FSDataInputStream file,
      int len, ByteBuffer directBuf) throws IOException {
    // TODO: HDFS API is a mess, so handle all kinds of cases.
    // Before 2.7, read() also doesn't adjust position correctly, so track it separately.
    int pos = directBuf.position(), startPos = pos, endPos = pos + len;
    try {
      while (pos < endPos) {
        int count = SHIMS.readByteBuffer(file, directBuf);
        if (count < 0) throw new EOFException();
        assert count != 0 : "0-length read: " + (endPos - pos) + "@" + (pos - startPos);
        pos += count;
        assert pos <= endPos : "Position " + pos + " > " + endPos + " after reading " + count;
        directBuf.position(pos);
      }
    } catch (UnsupportedOperationException ex) {
      assert pos == startPos;
      // Happens in q files and such.
      RecordReaderImpl.LOG.error("Stream does not support direct read; we will copy.");
      byte[] buffer = new byte[len];
      file.readFully(buffer, 0, buffer.length);
      directBuf.put(buffer);
    }
    directBuf.position(startPos);
    directBuf.limit(startPos + len);
  }


  static List<DiskRange> getStreamBuffers(DiskRangeList range, long offset, long length) {
    // This assumes sorted ranges (as do many other parts of ORC code.
    ArrayList<DiskRange> buffers = new ArrayList<DiskRange>();
    if (length == 0) return buffers;
    long streamEnd = offset + length;
    boolean inRange = false;
    while (range != null) {
      if (!inRange) {
        if (range.getEnd() <= offset) {
          range = range.next;
          continue; // Skip until we are in range.
        }
        inRange = true;
        if (range.getOffset() < offset) {
          // Partial first buffer, add a slice of it.
          buffers.add(range.sliceAndShift(offset, Math.min(streamEnd, range.getEnd()), -offset));
          if (range.getEnd() >= streamEnd) break; // Partial first buffer is also partial last buffer.
          range = range.next;
          continue;
        }
      } else if (range.getOffset() >= streamEnd) {
        break;
      }
      if (range.getEnd() > streamEnd) {
        // Partial last buffer (may also be the first buffer), add a slice of it.
        buffers.add(range.sliceAndShift(range.getOffset(), streamEnd, -offset));
        break;
      }
      // Buffer that belongs entirely to one stream.
      // TODO: ideally we would want to reuse the object and remove it from the list, but we cannot
      //       because bufferChunks is also used by clearStreams for zcr. Create a useless dup.
      buffers.add(range.sliceAndShift(range.getOffset(), range.getEnd(), -offset));
      if (range.getEnd() == streamEnd) break;
      range = range.next;
    }
    return buffers;
  }

  static ZeroCopyReaderShim createZeroCopyShim(FSDataInputStream file,
      CompressionCodec codec, ByteBufferAllocatorPool pool) throws IOException {
    if ((codec == null || ((codec instanceof DirectDecompressionCodec)
            && ((DirectDecompressionCodec) codec).isAvailable()))) {
      /* codec is null or is available */
      return ShimLoader.getHadoopShims().getZeroCopyReader(file, pool);
    }
    return null;
  }

  // this is an implementation copied from ElasticByteBufferPool in hadoop-2,
  // which lacks a clear()/clean() operation
  public final static class ByteBufferAllocatorPool implements ByteBufferPoolShim {
    private static final class Key implements Comparable<Key> {
      private final int capacity;
      private final long insertionGeneration;

      Key(int capacity, long insertionGeneration) {
        this.capacity = capacity;
        this.insertionGeneration = insertionGeneration;
      }

      @Override
      public int compareTo(Key other) {
        return ComparisonChain.start().compare(capacity, other.capacity)
            .compare(insertionGeneration, other.insertionGeneration).result();
      }

      @Override
      public boolean equals(Object rhs) {
        if (rhs == null) {
          return false;
        }
        try {
          Key o = (Key) rhs;
          return (compareTo(o) == 0);
        } catch (ClassCastException e) {
          return false;
        }
      }

      @Override
      public int hashCode() {
        return new HashCodeBuilder().append(capacity).append(insertionGeneration)
            .toHashCode();
      }
    }

    private final TreeMap<Key, ByteBuffer> buffers = new TreeMap<Key, ByteBuffer>();

    private final TreeMap<Key, ByteBuffer> directBuffers = new TreeMap<Key, ByteBuffer>();

    private long currentGeneration = 0;

    private final TreeMap<Key, ByteBuffer> getBufferTree(boolean direct) {
      return direct ? directBuffers : buffers;
    }

    public void clear() {
      buffers.clear();
      directBuffers.clear();
    }

    @Override
    public ByteBuffer getBuffer(boolean direct, int length) {
      TreeMap<Key, ByteBuffer> tree = getBufferTree(direct);
      Map.Entry<Key, ByteBuffer> entry = tree.ceilingEntry(new Key(length, 0));
      if (entry == null) {
        return direct ? ByteBuffer.allocateDirect(length) : ByteBuffer
            .allocate(length);
      }
      tree.remove(entry.getKey());
      return entry.getValue();
    }

    @Override
    public void putBuffer(ByteBuffer buffer) {
      TreeMap<Key, ByteBuffer> tree = getBufferTree(buffer.isDirect());
      while (true) {
        Key key = new Key(buffer.capacity(), currentGeneration++);
        if (!tree.containsKey(key)) {
          tree.put(key, buffer);
          return;
        }
        // Buffers are indexed by (capacity, generation).
        // If our key is not unique on the first try, we try again
      }
    }
  }
}
