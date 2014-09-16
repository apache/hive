package org.apache.hadoop.hive.llap.chunk;

import org.apache.hadoop.hive.llap.api.Vector.Type;

public interface ChunkWriter {
  public enum NullsState {
    NEXT_NULL,
    HAS_NULLS,
    NO_NULLS
  }

  // TODO: possible improvement - have chunk format support 1/2/4-sized ints
  //       still vectorizable, but much less space for boolean.
  void writeLongs(long[] values, int offset, int count, NullsState nullsState);

  void writeLongs(byte[] literals, int offset, int toWrite, NullsState nullsState);

  void writeRepeatedLongs(long value, int count, NullsState nullsState);

  void writeNulls(int count, boolean followedByNonNull);

  void finishCurrentSegment();

  int estimateValueCountThatFits(Type type, boolean hasNulls);

  void writeDoubles(double[] src, int srcOffset, int srcCount, NullsState nullsState);
}
