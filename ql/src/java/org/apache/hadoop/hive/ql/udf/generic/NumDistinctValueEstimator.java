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
package org.apache.hadoop.hive.ql.udf.generic;
import java.util.Random;

import javolution.util.FastBitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.io.Text;

public class NumDistinctValueEstimator {

  static final Logger LOG = LoggerFactory.getLogger(NumDistinctValueEstimator.class.getName());

  /* We want a,b,x to come from a finite field of size 0 to k, where k is a prime number.
   * 2^p - 1 is prime for p = 31. Hence bitvectorSize has to be 31. Pick k to be 2^p -1.
   * If a,b,x didn't come from a finite field ax1 + b mod k and ax2 + b mod k will not be pair wise
   * independent. As a consequence, the hash values will not distribute uniformly from 0 to 2^p-1
   * thus introducing errors in the estimates.
   */
  private static final int BIT_VECTOR_SIZE = 31;
  private final int numBitVectors;

  // Refer to Flajolet-Martin'86 for the value of phi
  private static final double PHI = 0.77351;

  private final int[] a;
  private final int[] b;
  private final FastBitSet[] bitVector;

  private final Random aValue;
  private final Random bValue;

  /* Create a new distinctValueEstimator
   */
  public NumDistinctValueEstimator(int numBitVectors) {
    this.numBitVectors = numBitVectors;
    bitVector = new FastBitSet[numBitVectors];
    for (int i=0; i< numBitVectors; i++) {
      bitVector[i] = new FastBitSet(BIT_VECTOR_SIZE);
    }

    a = new int[numBitVectors];
    b = new int[numBitVectors];

    /* Use a large prime number as a seed to the random number generator.
     * Java's random number generator uses the Linear Congruential Generator to generate random
     * numbers using the following recurrence relation,
     *
     * X(n+1) = (a X(n) + c ) mod m
     *
     *  where X0 is the seed. Java implementation uses m = 2^48. This is problematic because 2^48
     *  is not a prime number and hence the set of numbers from 0 to m don't form a finite field.
     *  If these numbers don't come from a finite field any give X(n) and X(n+1) may not be pair
     *  wise independent.
     *
     *  However, empirically passing in prime numbers as seeds seems to work better than when passing
     *  composite numbers as seeds. Ideally Java's Random should pick m such that m is prime.
     *
     */
    aValue = new Random(99397);
    bValue = new Random(9876413);

    for (int i = 0; i < numBitVectors; i++) {
      int randVal;
      /* a and b shouldn't be even; If a and b are even, then none of the values
       * will set bit 0 thus introducing errors in the estimate. Both a and b can be even
       * 25% of the times and as a result 25% of the bit vectors could be inaccurate. To avoid this
       * always pick odd values for a and b.
       */
      do {
        randVal = aValue.nextInt();
      } while (randVal % 2 == 0);

      a[i] = randVal;

      do {
        randVal = bValue.nextInt();
      } while (randVal % 2 == 0);

      b[i] = randVal;

      if (a[i] < 0) {
        a[i] = a[i] + (1 << BIT_VECTOR_SIZE - 1);
      }

      if (b[i] < 0) {
        b[i] = b[i] + (1 << BIT_VECTOR_SIZE - 1);
      }
    }
  }

  public NumDistinctValueEstimator(String s, int numBitVectors) {
    this.numBitVectors = numBitVectors;
    FastBitSet bitVectorDeser[] = deserialize(s, numBitVectors);
    bitVector = new FastBitSet[numBitVectors];
    for(int i=0; i <numBitVectors; i++) {
       bitVector[i] = new FastBitSet(BIT_VECTOR_SIZE);
       bitVector[i].clear();
       bitVector[i].or(bitVectorDeser[i]);
    }

    a = null;
    b = null;

    aValue = null;
    bValue = null;
  }

  /**
   * Resets a distinctValueEstimator object to its original state.
   */
  public void reset() {
    for (int i=0; i< numBitVectors; i++) {
      bitVector[i].clear();
    }
  }

  public FastBitSet getBitVector(int index) {
    return bitVector[index];
  }

  public int getnumBitVectors() {
    return numBitVectors;
  }

  public int getBitVectorSize() {
    return BIT_VECTOR_SIZE;
  }

  public void printNumDistinctValueEstimator() {
    String t = new String();

    LOG.debug("NumDistinctValueEstimator");
    LOG.debug("Number of Vectors: {}", numBitVectors);
    LOG.debug("Vector Size: {}", BIT_VECTOR_SIZE);

    for (int i=0; i < numBitVectors; i++) {
      t = t + bitVector[i].toString();
    }

    LOG.debug("Serialized Vectors: ");
    LOG.debug(t);
  }

  /* Serializes a distinctValueEstimator object to Text for transport.
   *
   */
  public Text serialize() {
    String s = new String();
    for(int i=0; i < numBitVectors; i++) {
      s = s + (bitVector[i].toString());
    }
    return new Text(s);
  }

  /* Deserializes from string to FastBitSet; Creates a NumDistinctValueEstimator object and
   * returns it.
   */

  private FastBitSet[] deserialize(String s, int numBitVectors) {
    FastBitSet[] b = new FastBitSet[numBitVectors];
    for (int j=0; j < numBitVectors; j++) {
      b[j] = new FastBitSet(BIT_VECTOR_SIZE);
      b[j].clear();
    }

    int vectorIndex =0;

    /* Parse input string to obtain the indexes that are set in the bitvector.
     * When a toString() is called on a FastBitSet object to serialize it, the serialization
     * adds { and } to the beginning and end of the return String.
     * Skip "{", "}", ",", " " in the input string.
     */
    for(int i=1; i < s.length()-1;) {
      char c = s.charAt(i);
      i = i + 1;

      // Move on to the next bit vector
      if (c == '}') {
         vectorIndex = vectorIndex + 1;
      }

      // Encountered a numeric value; Extract out the entire number
      if (c >= '0' && c <= '9') {
        String t = new String();
        t = t + c;
        c = s.charAt(i);
        i = i + 1;

        while (c != ',' && c!= '}') {
          t = t + c;
          c = s.charAt(i);
          i = i + 1;
        }

        int bitIndex = Integer.parseInt(t);
        assert(bitIndex >= 0);
        assert(vectorIndex < numBitVectors);
        b[vectorIndex].set(bitIndex);
        if (c == '}') {
          vectorIndex =  vectorIndex + 1;
        }
      }
    }
    return b;
  }

  private int generateHash(long v, int hashNum) {
    int mod = (1<<BIT_VECTOR_SIZE) - 1;
    long tempHash = a[hashNum] * v  + b[hashNum];
    tempHash %= mod;
    int hash = (int) tempHash;

    /* Hash function should map the long value to 0...2^L-1.
     * Hence hash value has to be non-negative.
     */
    if (hash < 0) {
      hash = hash + mod;
    }
    return hash;
  }

  private int generateHashForPCSA(long v) {
    int mod = 1 << (BIT_VECTOR_SIZE - 1) - 1;
    long tempHash = a[0] * v + b[0];
    tempHash %= mod;
    int hash = (int) tempHash;

    /* Hash function should map the long value to 0...2^L-1.
     * Hence hash value has to be non-negative.
     */
    if (hash < 0) {
      hash = hash + mod + 1;
    }
    return hash;
  }

  public void addToEstimator(long v) {
    /* Update summary bitVector :
     * Generate hash value of the long value and mod it by 2^bitVectorSize-1.
     * In this implementation bitVectorSize is 31.
     */

    for (int i = 0; i<numBitVectors; i++) {
      int hash = generateHash(v,i);
      int index;

      // Find the index of the least significant bit that is 1
      for (index=0; index<BIT_VECTOR_SIZE; index++) {
        if (hash % 2 != 0) {
          break;
        }
        hash = hash >> 1;
      }

      // Set bitvector[index] := 1
      bitVector[i].set(index);
    }
  }

  public void addToEstimatorPCSA(long v) {
    int hash = generateHashForPCSA(v);
    int rho = hash/numBitVectors;
    int index;

    // Find the index of the least significant bit that is 1
    for (index=0; index<BIT_VECTOR_SIZE; index++) {
      if (rho % 2 != 0) {
        break;
      }
      rho = rho >> 1;
    }

    // Set bitvector[index] := 1
    bitVector[hash%numBitVectors].set(index);
  }

  public void addToEstimator(double d) {
    int v = new Double(d).hashCode();
    addToEstimator(v);
  }

  public void addToEstimatorPCSA(double d) {
    int v = new Double(d).hashCode();
    addToEstimatorPCSA(v);
  }

  public void addToEstimator(HiveDecimal decimal) {
    int v = decimal.hashCode();
    addToEstimator(v);
  }

  public void addToEstimatorPCSA(HiveDecimal decimal) {
    int v = decimal.hashCode();
    addToEstimatorPCSA(v);
  }

  public void mergeEstimators(NumDistinctValueEstimator o) {
    // Bitwise OR the bitvector with the bitvector in the agg buffer
    for (int i=0; i<numBitVectors; i++) {
      bitVector[i].or(o.getBitVector(i));
    }
  }

  public long estimateNumDistinctValuesPCSA() {
    double numDistinctValues = 0.0;
    long S = 0;

    for (int i=0; i < numBitVectors; i++) {
      int index = 0;
      while (bitVector[i].get(index) && index < BIT_VECTOR_SIZE) {
        index = index + 1;
      }
      S = S + index;
    }

    numDistinctValues = ((numBitVectors/PHI) * Math.pow(2.0, S/numBitVectors));
    return ((long)numDistinctValues);
  }

  /* We use the Flajolet-Martin estimator to estimate the number of distinct values.FM uses the
   * location of the least significant zero as an estimate of log2(phi*ndvs).
   */
  public long estimateNumDistinctValues() {
    int sumLeastSigZero = 0;
    double avgLeastSigZero;
    double numDistinctValues;

    for (int i=0; i< numBitVectors; i++) {
      int leastSigZero = bitVector[i].nextClearBit(0);
      sumLeastSigZero += leastSigZero;
    }

    avgLeastSigZero =
        sumLeastSigZero/(numBitVectors * 1.0) - (Math.log(PHI)/Math.log(2.0));
    numDistinctValues = Math.pow(2.0, avgLeastSigZero);
    return ((long)(numDistinctValues));
  }

  @InterfaceAudience.LimitedPrivate(value = { "Hive" })
  static int lengthFor(JavaDataModel model, Integer numVector) {
    int length = model.object();
    length += model.primitive1() * 2;       // two int
    length += model.primitive2();           // one double
    length += model.lengthForRandom() * 2;  // two Random

    if (numVector == null) {
      numVector = 16; // HiveConf hive.stats.ndv.error default produces 16 vectors
    }

    if (numVector > 0) {
      length += model.array() * 3;                    // three array
      length += model.primitive1() * numVector * 2;   // two int array
      length += (model.object() + model.array() + model.primitive1() +
          model.primitive2()) * numVector;   // bitset array
    }
    return length;
  }

  public int lengthFor(JavaDataModel model) {
    return lengthFor(model, getnumBitVectors());
  }
}
