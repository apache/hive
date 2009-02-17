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
package org.apache.hadoop.hive.serde2.lazy;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;

public class LazyUtils {

  /**
   * Create a lazy primitive class given the java class. 
   */
  public static LazyPrimitive<?> createLazyPrimitiveClass(Class<?> c) {
    if (String.class.equals(c)) {
      return new LazyString();
    } else if (Integer.class.equals(c)) {
      return new LazyInteger();
    } else if (Double.class.equals(c)) {
      return new LazyDouble();
    } else if (Byte.class.equals(c)) {
      return new LazyByte();
    } else if (Short.class.equals(c)) {
      return new LazyShort();
    } else if (Long.class.equals(c)) {
      return new LazyLong();
    } else {
      return null;
    }
  }

  /**
   * Returns the digit represented by character b.
   * @param b  The ascii code of the character
   * @param radix  The radix
   * @return  -1 if it's invalid
   */
  public static int digit(int b, int radix) {
    int r = -1;
    if (b >= '0' && b<='9') {
      r = b - '0';
    } else if (b >= 'A' && b<='Z') {
      r = b - 'A' + 10;
    } else if (b >= 'a' && b <= 'z') {
      r = b - 'a' + 10;
    }
    if (r >= radix) r = -1;
    return r;
  }
  
  /**
   * Returns -1 if the first byte sequence is lexicographically less than the second;
   * returns +1 if the second byte sequence is lexicographically less than the first;
   * otherwise return 0.
   */
  public static int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
    
    int min = Math.min(length1, length2);
    
    for (int i = 0; i < min; i++) {
      if (b1[start1 + i] == b2[start2 + i]) {
        continue;
      }
      if (b1[start1 + i] < b2[start2 + i]) {
        return -1;
      } else {
        return 1;
      }      
    }
    
    if (length1 < length2) return -1;
    if (length1 > length2) return 1;
    return 0;
  }
  
  /**
   * Convert a UTF-8 byte array to String.
   */
  public static String convertToString(byte[] bytes, int start, int length) {
    try {
      return Text.decode(bytes, start, length);
    } catch (CharacterCodingException e) {
      return null;
    }
  }
  
  
}
