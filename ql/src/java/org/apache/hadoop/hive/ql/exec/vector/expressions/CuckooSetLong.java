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
import java.util.Random;

/**
 * A high-performance set implementation used to support fast set membership testing,
 * using Cuckoo hashing. This is used to support fast tests of the form
 *
 *       column IN ( <list-of-values )
 *
 * For details on the algorithm, see R. Pagh and F. F. Rodler, "Cuckoo Hashing,"
 * Elsevier Science preprint, Dec. 2003. http://www.itu.dk/people/pagh/papers/cuckoo-jour.pdf.
 *
 */
public class CuckooSetLong {
  private long t1[];
  private long t2[];
  private long prev1[] = null; // used for rehashing to get last set of values
  private long prev2[] = null; // " "
  private int n; // current array size
  private static final double PADDING_FACTOR = 1.0/0.40; // have minimum 40% fill factor
  private int salt[] = new int[6];
  private Random gen = new Random(676983475);
  private long blank = Long.MIN_VALUE;
  private int rehashCount = 0;

  // some prime numbers spaced about at powers of 2 in magnitude
  public static int primes[] = {7, 13, 17, 23, 31, 53, 67, 89, 127, 269, 571, 1019, 2089,
      4507, 8263, 16361, 32327, 65437, 131111, 258887, 525961, 999983, 2158909, 4074073,
      8321801, 15485863, 32452867, 67867967, 122949829, 256203221, 553105253, 982451653,
      1645333507, 2147483647};

  /**
   * Allocate a new set to hold expectedSize values. Re-allocation to expand
   * the set is not implemented, so the expected size must be at least the
   * size of the set to be inserteed.
   * @param expectedSize At least the size of the set of values that will be inserted.
   */
  public CuckooSetLong(int expectedSize) {

    // Choose array size. We have two hash tables to hold entries, so the sum
    // of the two should have a bit more than twice as much space as the
    // minimum required.
    n = (int) (expectedSize * PADDING_FACTOR / 2.0);

    // try to get prime number table size to have less dependence on good hash function
    for (int i = 0; i != primes.length; i++) {
      if (n <= primes[i]) {
        n = primes[i];
        break;
      }
    }

    t1 = new long[n];
    t2 = new long[n];
    Arrays.fill(t1, blank);
    Arrays.fill(t2, blank);
    updateHashSalt();
  }

  /**
   * Return true if and only if the value x is present in the set.
   */
  public boolean lookup(long x) {

    /* Must check that x is not blank because otherwise you could
     * get a false positive if the blank value was a value you
     * were legitimately testing to see if it was in the set.
     */
    return x != blank && (t1[h1(x)] == x || t2[h2(x)] == x);
  }

  public void insert(long x) {
    if (x == blank) {
      findNewBlank();
    }
    long temp;
    if (lookup(x)) {
      return;
    }

    // Try to insert up to n times. Rehash if that fails.
    for(int i = 0; i != n; i++) {
      if (t1[h1(x)] == blank) {
        t1[h1(x)] = x;
        return;
      }

      // swap x and t1[h1(x)]
      temp = t1[h1(x)];
      t1[h1(x)] = x;
      x = temp;

      if (t2[h2(x)] == blank) {
        t2[h2(x)] = x;
        return;
      }

      // swap x and t2[h2(x)]
      temp = t2[h2(x)];
      t2[h2(x)] = x;
      x = temp;
    }
    rehash();
    insert(x);
  }

  /**
   * Insert all values in the input array into the set.
   */
  public void load(long[] a) {
    for (Long x : a) {
      insert(x);
    }
  }

  /**
   * Need to change current blank value to something else because it is in
   * the input data set.
   */
  private void findNewBlank() {
    long newBlank = gen.nextLong();
    while(newBlank == blank || lookup(newBlank)) {
      newBlank = gen.nextLong();
    }

    // replace existing blanks with new blanks
    for(int i = 0; i != n; i++) {
      if (t1[i] == blank) {
        t1[i] = newBlank;
      }
      if (t2[i] == blank) {
        t2[i] = newBlank;
      }
    }
    blank = newBlank;
  }

  /**
   * Try to insert with up to n value's "poked out". Return the last value poked out.
   * If the value is not blank then we assume there was a cycle.
   * Don't try to insert the same value twice. This is for use in rehash only,
   * so you won't see the same value twice.
   */
  private long tryInsert(long x) {
    long temp;

    for(int i = 0; i != n; i++) {
      if (t1[h1(x)] == blank) {
        t1[h1(x)] = x;
        return blank;
      }

      // swap x and t1[h1(x)]
      temp = t1[h1(x)];
      t1[h1(x)] = x;
      x = temp;

      if (t2[h2(x)] == blank) {
        t2[h2(x)] = x;
        return blank;
      }

      // swap x and t2[h2(x)]
      temp = t2[h2(x)];
      t2[h2(x)] = x;
      x = temp;
      if (x == blank) {
        break;
      }
    }
    return x;
  }



  /**
   * Variation of Robert Jenkins' hash function.
   */
  private int h1(long y) {
    int x = (int) ((((y >>> 32) ^ y)) & 0xFFFFFFFF);
    x = (x + salt[0]) + (x << 12);
    x = (x ^ salt[1]) ^ (x >> 19);
    x = (x + salt[2]) + (x << 5);
    x = (x + salt[3]) ^ (x << 9);
    x = (x + salt[4]) + (x << 3);
    x = (x ^ salt[5]) ^ (x >> 16);

    // Return value modulo n but always in the positive range (0..n-1).
    // And with the mask to zero the sign bit to make the input to mod positive
    // so the output will definitely be positive.
    return (x & 0x7FFFFFFF) % n;
  }

  /**
   * basic modular hash function
   */
  private int h2(long x) {

    // Return value modulo n but always in the positive range (0..n-1).
    // Since n is prime, this gives good spread for numbers that are multiples
    // of one billion, which is important since timestamps internally
    // are stored as a number of nanoseconds, and the fractional seconds
    // part is often 0.
    return (((int) (x % n)) + n) % n;
  }

  /**
   * In case of rehash, hash function h2 is changed by updating the
   * entries in the salt array with new random values.
   */
  private void updateHashSalt() {
    for (int i = 0; i != 6; i++) {
      salt[i] = gen.nextInt(0x7FFFFFFF);
    }
  }

  private void rehash() {
    rehashCount++;
    if (rehashCount > 20) {
      throw new RuntimeException("Too many rehashes");
    }
    updateHashSalt();

    // Save original values
    if (prev1 == null) {
      prev1 = t1;
      prev1 = t2;
    }
    t1 = new long[n];
    t2 = new long[n];
    Arrays.fill(t1, blank);
    Arrays.fill(t2, blank);
    for (Long v  : prev1) {
      if (v != blank) {
        long x = tryInsert(v);
        if (x != blank) {
          rehash();
          return;
        }
      }
    }
    for (Long v  : prev2) {
      if (v != blank) {
        long x = tryInsert(v);
        if (x != blank) {
          rehash();
          return;
        }
      }
    }

    // We succeeded in adding all the values, so
    // clear the previous values recorded.
    prev1 = null;
    prev2 = null;
  }
}
