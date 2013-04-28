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

package org.apache.hadoop.hive.ql.util;

import org.apache.hadoop.hive.ql.udf.generic.NumDistinctValueEstimator;
import org.apache.hadoop.hive.ql.udf.generic.NumericHistogram;

/**
 * Estimation of memory footprint of object
 */
public enum JavaDataModel {

  JAVA32 {
    public int object() {
      return JAVA32_OBJECT;
    }

    public int array() {
      return JAVA32_ARRAY;
    }

    public int ref() {
      return JAVA32_REF;
    }

    public int hashMap(int entry) {
      // base  = JAVA32_OBJECT + PRIMITIVES1 * 4 + JAVA32_FIELDREF * 3 + JAVA32_ARRAY;
      // entry = JAVA32_OBJECT + JAVA32_FIELDREF + PRIMITIVES1
      return 64 + 24 * entry;
    }

    public int hashSet(int entry) {
      // hashMap += JAVA32_OBJECT
      return 80 + 24 * entry;
    }

    public int linkedHashMap(int entry) {
      // hashMap += JAVA32_FIELDREF + PRIMITIVES1
      // hashMap.entry += JAVA32_FIELDREF * 2
      return 72 + 32 * entry;
    }

    public int linkedList(int entry) {
      // base  = JAVA32_OBJECT + PRIMITIVES1 * 2 + JAVA32_FIELDREF;
      // entry = JAVA32_OBJECT + JAVA32_FIELDREF * 2
      return 28 + 24 * entry;
    }

    public int arrayList() {
      // JAVA32_OBJECT + PRIMITIVES1 * 2 + JAVA32_ARRAY;
      return 44;
    }
  }, JAVA64 {
    public int object() {
      return JAVA64_OBJECT;
    }

    public int array() {
      return JAVA64_ARRAY;
    }

    public int ref() {
      return JAVA64_REF;
    }

    public int hashMap(int entry) {
      // base  = JAVA64_OBJECT + PRIMITIVES1 * 4 + JAVA64_FIELDREF * 3 + JAVA64_ARRAY;
      // entry = JAVA64_OBJECT + JAVA64_FIELDREF + PRIMITIVES1
      return 112 + 44 * entry;
    }

    public int hashSet(int entry) {
      // hashMap += JAVA64_OBJECT
      return 144 + 44 * entry;
    }

    public int linkedHashMap(int entry) {
      // hashMap += JAVA64_FIELDREF + PRIMITIVES1
      // hashMap.entry += JAVA64_FIELDREF * 2
      return 128 + 60 * entry;
    }

    public int linkedList(int entry) {
      // base  = JAVA64_OBJECT + PRIMITIVES1 * 2 + JAVA64_FIELDREF;
      // entry = JAVA64_OBJECT + JAVA64_FIELDREF * 2
      return 48 + 48 * entry;
    }

    public int arrayList() {
      // JAVA64_OBJECT + PRIMITIVES1 * 2 + JAVA64_ARRAY;
      return 80;
    }
  };

  public abstract int object();
  public abstract int array();
  public abstract int ref();
  public abstract int hashMap(int entry);
  public abstract int hashSet(int entry);
  public abstract int linkedHashMap(int entry);
  public abstract int linkedList(int entry);
  public abstract int arrayList();

  // ascii string
  public int lengthFor(String string) {
    return object() + primitive1() * 3 + array() + string.length();
  }

  public int lengthFor(NumericHistogram histogram) {
    int length = object();
    length += primitive1() * 2;       // two int
    int numBins = histogram.getNumBins();
    if (numBins > 0) {
      length += arrayList();   // List<Coord>
      length += numBins * (object() + primitive2() * 2); // Coord holds two doubles
    }
    length += lengthForRandom();      // Random
    return length;
  }

  public int lengthFor(NumDistinctValueEstimator estimator) {
    int length = object();
    length += primitive1() * 2;       // two int
    length += primitive2();           // one double
    length += lengthForRandom() * 2;  // two Random

    int numVector = estimator.getnumBitVectors();
    if (numVector > 0) {
      length += array() * 3;                    // three array
      length += primitive1() * numVector * 2;   // two int array
      length += (object() + array() + primitive1() + primitive2()) * numVector;   // bitset array
    }
    return length;
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

  private static JavaDataModel current;

  public static JavaDataModel get() {
    if (current != null) {
      return current;
    }
    try {
      String props = System.getProperty("sun.arch.data.model");
      if ("32".equals(props)) {
        return current = JAVA32;
      }
    } catch (Exception e) {
      // ignore
    }
    return current = JAVA64;
  }

  public static int round(int size) {
    JavaDataModel model = get();
    if (model == JAVA32 || size % 8 == 0) {
      return size;
    }
    return ((size + 8) >> 3) << 3;
  }
}
