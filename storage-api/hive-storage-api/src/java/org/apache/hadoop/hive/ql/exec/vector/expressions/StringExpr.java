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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;

/**
 * String expression evaluation helper functions.
 */
public class StringExpr {

  /* Compare two strings from two byte arrays each
   * with their own start position and length.
   * Use lexicographic unsigned byte value order.
   * This is what's used for UTF-8 sort order.
   * Return negative value if arg1 < arg2, 0 if arg1 = arg2,
   * positive if arg1 > arg2.
   */
  public static int compare(byte[] arg1, int start1, int len1, byte[] arg2, int start2, int len2) {
    for (int i = 0; i < len1 && i < len2; i++) {
      // Note the "& 0xff" is just a way to convert unsigned bytes to signed integer.
      int b1 = arg1[i + start1] & 0xff;
      int b2 = arg2[i + start2] & 0xff;
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return len1 - len2;
  }

  /* Determine if two strings are equal from two byte arrays each
   * with their own start position and length.
   * Use lexicographic unsigned byte value order.
   * This is what's used for UTF-8 sort order.
   */
  public static boolean equal(byte[] arg1, final int start1, final int len1,
      byte[] arg2, final int start2, final int len2) {
    if (len1 != len2) {
      return false;
    }
    if (len1 == 0) {
      return true;
    }

    // do bounds check for OOB exception
    if (arg1[start1] != arg2[start2]
        || arg1[start1 + len1 - 1] != arg2[start2 + len2 - 1]) {
      return false;
    }

    if (len1 == len2) {
      // prove invariant to the compiler: len1 = len2
      // all array access between (start1, start1+len1) 
      // and (start2, start2+len2) are valid
      // no more OOB exceptions are possible
      final int step = 8;
      final int remainder = len1 % step;
      final int wlen = len1 - remainder;
      // suffix first
      for (int i = wlen; i < len1; i++) {
        if (arg1[start1 + i] != arg2[start2 + i]) {
          return false;
        }
      }
      // SIMD loop
      for (int i = 0; i < wlen; i += step) {
        final int s1 = start1 + i;
        final int s2 = start2 + i;
        boolean neq = false;
        for (int j = 0; j < step; j++) {
          neq = (arg1[s1 + j] != arg2[s2 + j]) || neq;
        }
        if (neq) {
          return false;
        }
      }
    }

    return true;
  }

  public static int characterCount(byte[] bytes) {
    int end = bytes.length;

    // count characters
    int j = 0;
    int charCount = 0;
    while(j < end) {
      // UTF-8 continuation bytes have 2 high bits equal to 0x80.
      if ((bytes[j] & 0xc0) != 0x80) {
        ++charCount;
      }
      j++;
    }
    return charCount;
  }

  public static int characterCount(byte[] bytes, int start, int length) {
    int end = start + length;

    // count characters
    int j = start;
    int charCount = 0;
    while(j < end) {
      // UTF-8 continuation bytes have 2 high bits equal to 0x80.
      if ((bytes[j] & 0xc0) != 0x80) {
        ++charCount;
      }
      j++;
    }
    return charCount;
  }

  // A setVal with the same function signature as rightTrim, leftTrim, truncate, etc, below.
  // Useful for class generation via templates.
  public static void assign(BytesColumnVector outV, int i, byte[] bytes, int start, int length) {
    // set output vector
    outV.setVal(i, bytes, start, length);
  }

  /*
   * Right trim a slice of a byte array and return the new byte length.
   */
  public static int rightTrim(byte[] bytes, int start, int length) {
    // skip trailing blank characters
    int j = start + length - 1;
    while(j >= start && bytes[j] == 0x20) {
      j--;
    }

    return (j - start) + 1;
  }

  /*
   * Right trim a slice of a byte array and place the result into element i of a vector.
   */
  public static void rightTrim(BytesColumnVector outV, int i, byte[] bytes, int start, int length) {
    // skip trailing blank characters
    int j = start + length - 1;
    while(j >= start && bytes[j] == 0x20) {
      j--;
    }

    // set output vector
    outV.setVal(i, bytes, start, (j - start) + 1);
  }

  /*
   * Truncate a slice of a byte array to a maximum number of characters and
   * return the new byte length.
   */
  public static int truncate(byte[] bytes, int start, int length, int maxLength) {
    int end = start + length;

    // count characters forward
    int j = start;
    int charCount = 0;
    while(j < end) {
      // UTF-8 continuation bytes have 2 high bits equal to 0x80.
      if ((bytes[j] & 0xc0) != 0x80) {
        if (charCount == maxLength) {
          break;
        }
        ++charCount;
      }
      j++;
    }
    return (j - start);
  }

  /*
   * Truncate a slice of a byte array to a maximum number of characters and
   * place the result into element i of a vector.
   */
  public static void truncate(BytesColumnVector outV, int i, byte[] bytes, int start, int length, int maxLength) {
    int end = start + length;

    // count characters forward
    int j = start;
    int charCount = 0;
    while(j < end) {
      // UTF-8 continuation bytes have 2 high bits equal to 0x80.
      if ((bytes[j] & 0xc0) != 0x80) {
        if (charCount == maxLength) {
          break;
        }
        ++charCount;
      }
      j++;
    }

    // set output vector
    outV.setVal(i, bytes, start, (j - start));
  }

  /*
   * Truncate a byte array to a maximum number of characters and
   * return a byte array with only truncated bytes.
   */
  public static byte[] truncateScalar(byte[] bytes, int maxLength) {
    int end = bytes.length;

    // count characters forward
    int j = 0;
    int charCount = 0;
    while(j < end) {
      // UTF-8 continuation bytes have 2 high bits equal to 0x80.
      if ((bytes[j] & 0xc0) != 0x80) {
        if (charCount == maxLength) {
          break;
        }
        ++charCount;
      }
      j++;
    }
    if (j == end) {
      return bytes;
    } else {
      return Arrays.copyOf(bytes, j);
    }
  }

  /*
   * Right trim and truncate a slice of a byte array to a maximum number of characters and
   * return the new byte length.
   */
  public static int rightTrimAndTruncate(byte[] bytes, int start, int length, int maxLength) {
    int end = start + length;

    // count characters forward and watch for final run of pads
    int j = start;
    int charCount = 0;
    int padRunStart = -1;
    while(j < end) {
      // UTF-8 continuation bytes have 2 high bits equal to 0x80.
      if ((bytes[j] & 0xc0) != 0x80) {
        if (charCount == maxLength) {
          break;
        }
        if (bytes[j] == 0x20) {
          if (padRunStart == -1) {
            padRunStart = j;
          }
        } else {
          padRunStart = -1;
        }
        ++charCount;
      } else {
        padRunStart = -1;
      }
      j++;
    }
    if (padRunStart != -1) {
      return (padRunStart - start);
    } else {
      return (j - start);
    }
  }

  /*
   * Right trim and truncate a slice of a byte array to a maximum number of characters and
   * place the result into element i of a vector.
   */
  public static void rightTrimAndTruncate(BytesColumnVector outV, int i, byte[] bytes, int start, int length, int maxLength) {
    int end = start + length;

    // count characters forward and watch for final run of pads
    int j = start;
    int charCount = 0;
    int padRunStart = -1;
    while(j < end) {
      // UTF-8 continuation bytes have 2 high bits equal to 0x80.
      if ((bytes[j] & 0xc0) != 0x80) {
        if (charCount == maxLength) {
          break;
        }
        if (bytes[j] == 0x20) {
          if (padRunStart == -1) {
            padRunStart = j;
          }
        } else {
          padRunStart = -1;
        }
        ++charCount;
      } else {
        padRunStart = -1;
      }
      j++;
    }
    // set output vector
    if (padRunStart != -1) {
      outV.setVal(i, bytes, start, (padRunStart - start));
    } else {
      outV.setVal(i, bytes, start, (j - start) );
    }
  }

  /*
   * Right trim and truncate a byte array to a maximum number of characters and
   * return a byte array with only the trimmed and truncated bytes.
   */
  public static byte[] rightTrimAndTruncateScalar(byte[] bytes, int maxLength) {
    int end = bytes.length;

    // count characters forward and watch for final run of pads
    int j = 0;
    int charCount = 0;
    int padRunStart = -1;
    while(j < end) {
      // UTF-8 continuation bytes have 2 high bits equal to 0x80.
      if ((bytes[j] & 0xc0) != 0x80) {
        if (charCount == maxLength) {
          break;
        }
        if (bytes[j] == 0x20) {
          if (padRunStart == -1) {
            padRunStart = j;
          }
        } else {
          padRunStart = -1;
        }
        ++charCount;
      } else {
        padRunStart = -1;
      }
      j++;
    }
    if (padRunStart != -1) {
      return Arrays.copyOf(bytes, padRunStart);
    } else if (j == end) {
      return bytes;
    } else {
      return Arrays.copyOf(bytes, j);
    }
  }

  /*
   * Compiles the given pattern with a proper algorithm.
   */
  public static Finder compile(byte[] pattern) {
    return new BoyerMooreHorspool(pattern);
  }

  /*
   * A finder finds the first index of its pattern in a given byte array.
   * Its thread-safety depends on its implementation.
   */
  public interface Finder {
    int find(byte[] input, int start, int len);
  }

  /*
   * StringExpr uses Boyer Moore Horspool algorithm to find faster.
   * It is thread-safe, because it holds final member instances only.
   * See https://en.wikipedia.org/wiki/Boyer–Moore–Horspool_algorithm .
   */
  private static class BoyerMooreHorspool implements Finder {
    private static final int MAX_BYTE = 0xff;
    private final long[] shift = new long[MAX_BYTE];
    private final byte[] pattern;
    private final int plen;

    public BoyerMooreHorspool(byte[] pattern) {
      this.pattern = pattern;
      this.plen = pattern.length;
      Arrays.fill(shift, plen);
      for (int i = 0; i < plen - 1; i++) {
        shift[pattern[i] & MAX_BYTE] = plen - i - 1;
      }
    }

    public int find(byte[] input, int start, int len) {
      if (pattern.length == 0) {
        return 0;
      }

      final int end = start + len;
      int next = start + plen - 1;
      final int plen = this.plen;
      final byte[] pattern = this.pattern;
      while (next < end) {
        int s_tmp = next;
        int p_tmp = plen - 1;
        while (input[s_tmp] == pattern[p_tmp]) {
          p_tmp--;
          if (p_tmp < 0) {
            return s_tmp;
          }
          s_tmp--;
        }
        next += shift[input[next] & MAX_BYTE];
      }
      return -1;
    }
  }
}
