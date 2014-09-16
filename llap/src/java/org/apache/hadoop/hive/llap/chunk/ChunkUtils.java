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

import org.apache.hadoop.hive.llap.api.Vector.Type;

/** Shared and utility methods for ChunkReader and ChunkWriter. */
public class ChunkUtils {
  public final static int BITMASK_SIZE_BYTES = 8;
  public final static int BITMASK_SIZE_BITS = BITMASK_SIZE_BYTES * 8;

  public static final byte FORMAT_VERSION = 0;
  public static final byte[] TYPE_SIZES = new byte[Type.BINARY.value() + 1];
  static {
    TYPE_SIZES[Type.BINARY.value()] = -1; // TODO: add support for binary
    TYPE_SIZES[Type.DECIMAL.value()] = -1; // TODO: add support for decimal
    TYPE_SIZES[Type.LONG.value()] = 8;
    TYPE_SIZES[Type.DOUBLE.value()] = 8;
  }

  public static enum RleSegmentType {
    INVALID(0),
    REPEATING_NULL(1),
    REPEATING_VALUE(2),
    UNIQUE_NOT_NULL(3),
    UNIQUE_NULL_BITMASK(4);

    private byte value;
    private RleSegmentType(int val) {
      assert val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE;
      this.value = (byte)val;
    }
    private static final RleSegmentType[] ints = new RleSegmentType[UNIQUE_NULL_BITMASK.value + 1];
    static {
      for (RleSegmentType type : RleSegmentType.values()) {
        ints[type.value] = type;
      }
    }
    public static RleSegmentType fromInt(int value) {
      return ints[value];
    }
    public byte getValue() {
      return value;
    }
  }

  public static int getSegmentDataSize(Type type, RleSegmentType segmentType, int rowCount) {
    int valueSize = TYPE_SIZES[type.value()];
    switch (segmentType) {
    case REPEATING_NULL: return 0;
    case REPEATING_VALUE: return valueSize;
    case UNIQUE_NOT_NULL: return valueSize * rowCount;
    case UNIQUE_NULL_BITMASK:return valueSize * rowCount + align8(bitMaskSize(rowCount));
    default: throw new AssertionError("Unsupported segment type " + segmentType);
    }
  }

  public static int bitMaskSize(int rowCount) {
    return (rowCount >>> 3) + (((rowCount & 7) > 0) ? 1 : 0);
  }

  public static int align8(int number) {
    int rem = number & 7;
    return number - rem + (rem == 0 ? 0 : 8);
  }

  public static int align64(int number) {
    int rem = number & 63;
    return number - rem + (rem == 0 ? 0 : 64);
  }

  public static int getNonRepeatingValuesOffset(RleSegmentType type, int rowCount) {
    if (type == RleSegmentType.UNIQUE_NULL_BITMASK) {
      return 8 + align8(bitMaskSize(rowCount));
    }
    return 8;
  }

  public static int getFullBitmaskSize(int sizeOf) {
    return BITMASK_SIZE_BYTES * (1 + (sizeOf << 3));
  }
}
