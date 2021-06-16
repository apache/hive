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

package org.apache.hadoop.hive.ql.exec.util;

import com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

//copy of org.apache.tez.runtime.library.utils.FastByteComparisons
abstract public class FastByteComparisons {
  static final Logger LOG = LoggerFactory.getLogger(FastByteComparisons.class);

  FastByteComparisons() {
  }

  public static int compareTo(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return FastByteComparisons.LexicographicalComparerHolder.BEST_COMPARER.compareTo(b1, s1, l1, b2, s2, l2);
  }

  public static boolean equals(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return FastByteComparisons.LexicographicalComparerHolder.BEST_COMPARER.equals(b1, s1, l1, b2, s2, l2);
  }

  private static FastByteComparisons.Comparer<byte[]> lexicographicalComparerJavaImpl() {
    return FastByteComparisons.LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
  }

  private static class LexicographicalComparerHolder {
    static final String UNSAFE_COMPARER_NAME = FastByteComparisons.LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";
    static final FastByteComparisons.Comparer<byte[]> BEST_COMPARER = getBestComparer();

    private LexicographicalComparerHolder() {
    }

    static FastByteComparisons.Comparer<byte[]> getBestComparer() {
      if (System.getProperty("os.arch").toLowerCase().startsWith("sparc")) {
        if (FastByteComparisons.LOG.isTraceEnabled()) {
          FastByteComparisons.LOG.trace("Lexicographical comparer selected for byte aligned system architecture");
        }

        return FastByteComparisons.lexicographicalComparerJavaImpl();
      } else {
        try {
          Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);
          FastByteComparisons.Comparer<byte[]> comparer = (FastByteComparisons.Comparer)theClass.getEnumConstants()[0];
          if (FastByteComparisons.LOG.isTraceEnabled()) {
            FastByteComparisons.LOG.trace("Unsafe comparer selected for byte unaligned system architecture");
          }

          return comparer;
        } catch (Throwable var2) {
          if (FastByteComparisons.LOG.isTraceEnabled()) {
            FastByteComparisons.LOG.trace(var2.getMessage());
            FastByteComparisons.LOG.trace("Lexicographical comparer selected");
          }

          return FastByteComparisons.lexicographicalComparerJavaImpl();
        }
      }
    }

    private enum UnsafeComparer implements FastByteComparisons.Comparer<byte[]> {
      INSTANCE;

      static final Unsafe theUnsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
        public Object run() {
          try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return f.get(null);
          } catch (NoSuchFieldException | IllegalAccessException var2) {
            throw new Error();
          }
        }
      });
      static final int BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
      static final boolean littleEndian;

      private UnsafeComparer() {
      }

      static boolean lessThanUnsigned(long x1, long x2) {
        return x1 + -9223372036854775808L < x2 + -9223372036854775808L;
      }

      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
        if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
          return 0;
        } else {
          int minLength = Math.min(length1, length2);
          int minWords = minLength / 8;
          int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
          int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

          int i;
          for(i = 0; i < minWords * 8; i += 8) {
            long lw = theUnsafe.getLong(buffer1, (long)offset1Adj + (long)i);
            long rw = theUnsafe.getLong(buffer2, (long)offset2Adj + (long)i);
            long diff = lw ^ rw;
            if (diff != 0L) {
              if (!littleEndian) {
                return lessThanUnsigned(lw, rw) ? -1 : 1;
              }

              int n = 0;
              int x = (int)diff;
              if (x == 0) {
                x = (int)(diff >>> 32);
                n = 32;
              }

              int y = x << 16;
              if (y == 0) {
                n += 16;
              } else {
                x = y;
              }

              y = x << 8;
              if (y == 0) {
                n += 8;
              }

              return (int)((lw >>> n & 255L) - (rw >>> n & 255L));
            }
          }

          for(i = minWords * 8; i < minLength; ++i) {
            int result = UnsignedBytes.compare(buffer1[offset1 + i], buffer2[offset2 + i]);
            if (result != 0) {
              return result;
            }
          }

          return length1 - length2;
        }
      }

      public boolean equals(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
        if (length1 != length2) {
          return false;
        } else if  (buffer1 == buffer2 && offset1 == offset2) {
          return true;
        } else {
          int minWords = length1 / 8;
          int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
          int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

          int i;
          for(i = 0; i < minWords * 8; i += 8) {
            long lw = theUnsafe.getLong(buffer1, (long)offset1Adj + (long)i);
            long rw = theUnsafe.getLong(buffer2, (long)offset2Adj + (long)i);
            if (lw != rw) {
              return false;
            }
          }

          for(i = minWords * 8; i < length1; ++i) {
            int result = UnsignedBytes.compare(buffer1[offset1 + i], buffer2[offset2 + i]);
            if (result != 0) {
              return false;
            }
          }

          return true;
        }
      }

      static {
        if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
          throw new AssertionError();
        } else {
          littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
        }
      }
    }

    private enum PureJavaComparer implements FastByteComparisons.Comparer<byte[]> {
      INSTANCE;

      private PureJavaComparer() {
      }

      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
        if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
          return 0;
        } else {
          int end1 = offset1 + length1;
          int end2 = offset2 + length2;
          int i = offset1;

          for(int j = offset2; i < end1 && j < end2; ++j) {
            int a = buffer1[i] & 255;
            int b = buffer2[j] & 255;
            if (a != b) {
              return a - b;
            }

            ++i;
          }

          return length1 - length2;
        }
      }

      public boolean equals(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
        if (length1 != length2) {
          return false;
        } else if (buffer1 == buffer2 && offset1 == offset2) {
          return true;
        } else {
          int end1 = offset1 + length1;
          int end2 = offset2 + length2;
          int i = offset1;

          for(int j = offset2; i < end1 && j < end2; ++j) {
            int a = buffer1[i] & 255;
            int b = buffer2[j] & 255;
            if (a != b) {
              return false;
            }
            ++i;
          }

          return true;
        }
      }
    }
  }

  private interface Comparer<T> {
    int compareTo(T var1, int var2, int var3, T var4, int var5, int var6);
    boolean equals(T var1, int var2, int var3, T var4, int var5, int var6);
  }
}
