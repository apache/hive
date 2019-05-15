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

package org.apache.hadoop.hive.ql.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Estimation of memory footprint of object
 */
public enum JavaDataModel {

  JAVA32 {
    @Override
    public int object() {
      return JAVA32_OBJECT;
    }

    @Override
    public int array() {
      return JAVA32_ARRAY;
    }

    @Override
    public int ref() {
      return JAVA32_REF;
    }

    @Override
    public int hashMap(int entry) {
      // base  = JAVA32_OBJECT + PRIMITIVES1 * 4 + JAVA32_FIELDREF * 3 + JAVA32_ARRAY;
      // entry = JAVA32_OBJECT + JAVA32_FIELDREF + PRIMITIVES1
      return hashMapBase() + hashMapEntry() * entry;
    }

    @Override
    public int hashMapBase() {
      return 64;
    }

    @Override
    public int hashMapEntry() {
      return 24;
    }

    @Override
    public int hashSet(int entry) {
      // hashMap += JAVA32_OBJECT
      return hashSetBase() + hashSetEntry() * entry;
    }

    @Override
    public int hashSetBase() {
      return 80;
    }

    @Override
    public int hashSetEntry() {
      return 24;
    }

    @Override
    public int linkedHashMap(int entry) {
      // hashMap += JAVA32_FIELDREF + PRIMITIVES1
      // hashMap.entry += JAVA32_FIELDREF * 2
      return 72 + 32 * entry;
    }

    @Override
    public int linkedList(int entry) {
      // base  = JAVA32_OBJECT + PRIMITIVES1 * 2 + JAVA32_FIELDREF;
      // entry = JAVA32_OBJECT + JAVA32_FIELDREF * 2
      return linkedListBase() + linkedListEntry() * entry;
     }

     @Override
     public int linkedListBase() {
       return 28;
     }

     @Override
     public int linkedListEntry() {
       return 24;
     }

    @Override
    public int arrayList() {
      // JAVA32_OBJECT + PRIMITIVES1 * 2 + JAVA32_ARRAY;
      return 44;
    }

    @Override
    public int memoryAlign() {
      return 8;
    }
  }, JAVA64 {
    @Override
    public int object() {
      return JAVA64_OBJECT;
    }

    @Override
    public int array() {
      return JAVA64_ARRAY;
    }

    @Override
    public int ref() {
      return JAVA64_REF;
    }

    @Override
    public int hashMap(int entry) {
      // base  = JAVA64_OBJECT + PRIMITIVES1 * 4 + JAVA64_FIELDREF * 3 + JAVA64_ARRAY;
      // entry = JAVA64_OBJECT + JAVA64_FIELDREF + PRIMITIVES1
      return hashMapBase() + hashMapEntry() * entry;
    }

    @Override
    public int hashMapBase() {
      return 112;
    }


    @Override
    public int hashMapEntry() {
      return 44;
    }

    @Override
    public int hashSet(int entry) {
      // hashMap += JAVA64_OBJECT
      return hashSetBase() + hashSetEntry() * entry;
     }

     @Override
     public int hashSetBase() {
       return 144;
     }

     @Override
     public int hashSetEntry() {
       return 44;
     }

    @Override
    public int linkedHashMap(int entry) {
      // hashMap += JAVA64_FIELDREF + PRIMITIVES1
      // hashMap.entry += JAVA64_FIELDREF * 2
      return 128 + 60 * entry;
    }

    @Override
    public int linkedList(int entry) {
      // base  = JAVA64_OBJECT + PRIMITIVES1 * 2 + JAVA64_FIELDREF;
      // entry = JAVA64_OBJECT + JAVA64_FIELDREF * 2
      return linkedListBase() + linkedListEntry() * entry;
     }

     @Override
     public int linkedListBase() {
       return 48;
     }

     @Override
     public int linkedListEntry() {
       return 48;
     }

    @Override
    public int arrayList() {
      // JAVA64_OBJECT + PRIMITIVES1 * 2 + JAVA64_ARRAY;
      return 80;
    }

    @Override
    public int memoryAlign() {
      return 8;
    }
  };

  public abstract int object();
  public abstract int array();
  public abstract int ref();
  public abstract int hashMap(int entry);
  public abstract int hashMapBase();
  public abstract int hashMapEntry();
  public abstract int hashSetBase();
  public abstract int hashSetEntry();
  public abstract int hashSet(int entry);
  public abstract int linkedHashMap(int entry);
  public abstract int linkedListBase();
  public abstract int linkedListEntry();
  public abstract int linkedList(int entry);
  public abstract int arrayList();
  public abstract int memoryAlign();

  // ascii string
  public long lengthFor(String string) {
    return lengthForStringOfLength(string.length());
  }

  public int lengthForRandom() {
    // boolean + double + AtomicLong
    return object() + primitive1() + primitive2() + object() + primitive2();
  }

  public int primitive1() {
    return PRIMITIVES1;
  }
  public int primitive2() {
    return PRIMITIVES2;
  }

  public static long alignUp(long value, long align) {
    return (value + align - 1L) & ~(align - 1L);
  }

  private static final Logger LOG = LoggerFactory.getLogger(JavaDataModel.class);

  public static final int JAVA32_META = 12;
  public static final int JAVA32_ARRAY_META = 16;
  public static final int JAVA32_REF = 4;
  public static final int JAVA32_OBJECT = 16;   // JAVA32_META + JAVA32_REF
  public static final int JAVA32_ARRAY = 20;    // JAVA32_ARRAY_META + JAVA32_REF

  public static final int JAVA64_META = 24;
  public static final int JAVA64_ARRAY_META = 32;
  public static final int JAVA64_REF = 8;
  public static final int JAVA64_OBJECT = 32;   // JAVA64_META + JAVA64_REF
  public static final int JAVA64_ARRAY = 40;    // JAVA64_ARRAY_META + JAVA64_REF

  public static final int PRIMITIVES1 = 4;      // void, boolean, byte, short, int, float
  public static final int PRIMITIVES2 = 8;      // long, double

  public static final int PRIMITIVE_BYTE = 1;    // byte

  private static final class LazyHolder {
    private static final JavaDataModel MODEL_FOR_SYSTEM = getModelForSystem();
  }

  //@VisibleForTesting
  static JavaDataModel getModelForSystem() {
    String props = null;
    try {
      props = System.getProperty("sun.arch.data.model");
    } catch (Exception e) {
      LOG.warn("Failed to determine java data model, defaulting to 64", e);
    }
    if ("32".equals(props)) {
      return JAVA32;
    }
    // TODO: separate model is needed for compressedOops, which can be guessed from memory size.
    return JAVA64;
  }

  public static JavaDataModel get() {
    return LazyHolder.MODEL_FOR_SYSTEM;
  }

  public static int round(int size) {
    JavaDataModel model = get();
    if (model == JAVA32 || size % 8 == 0) {
      return size;
    }
    return ((size + 8) >> 3) << 3;
  }

  public long lengthForPrimitiveArrayOfSize(int primitiveSize, long length) {
    return alignUp(array() + primitiveSize*length, memoryAlign());
  }

  public long lengthForByteArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(PRIMITIVE_BYTE, length);
  }
  public long lengthForObjectArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(ref(), length);
  }
  public long lengthForLongArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(primitive2(), length);
  }
  public long lengthForDoubleArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(primitive2(), length);
  }
  public long lengthForIntArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(primitive1(), length);
  }
  public long lengthForBooleanArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(PRIMITIVE_BYTE, length);
  }
  public long lengthForTimestampArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(lengthOfTimestamp(), length);
  }
  public long lengthForDateArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(lengthOfDate(), length);
  }
  public long lengthForDecimalArrayOfSize(long length) {
    return lengthForPrimitiveArrayOfSize(lengthOfDecimal(), length);
  }

  public int lengthOfDecimal() {
    // object overhead + 8 bytes for intCompact + 4 bytes for precision
    // + 4 bytes for scale + size of BigInteger
    return object() + 2 * primitive2() + lengthOfBigInteger();
  }

  private int lengthOfBigInteger() {
    // object overhead + 4 bytes for bitCount + 4 bytes for bitLength
    // + 4 bytes for firstNonzeroByteNum + 4 bytes for firstNonzeroIntNum +
    // + 4 bytes for lowestSetBit + 5 bytes for size of magnitude (since max precision
    // is only 38 for HiveDecimal) + 7 bytes of padding (since java memory allocations
    // are 8 byte aligned)
    return object() + 4 * primitive2();
  }

  public int lengthOfTimestamp() {
    // object overhead + 4 bytes for int (nanos) + 4 bytes of padding
    return object() + primitive2();
  }

  public int lengthOfDate() {
    // object overhead + 8 bytes for long (fastTime) + 16 bytes for cdate
    return object() + 3 * primitive2();
  }

  public int lengthForStringOfLength(int strLen) {
    return object() + primitive1() * 3 + array() + strLen;
  }
}
