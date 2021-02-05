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
package org.apache.hadoop.hive.common.ndv.fm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javolution.util.FastBitSet;

public class FMSketch implements NumDistinctValueEstimator {

  static final Logger LOG = LoggerFactory.getLogger(FMSketch.class.getName());

  /* We want a,b,x to come from a finite field of size 0 to k, where k is a prime number.
   * 2^p - 1 is prime for p = 31. Hence bitvectorSize has to be 31. Pick k to be 2^p -1.
   * If a,b,x didn't come from a finite field ax1 + b mod k and ax2 + b mod k will not be pair wise
   * independent. As a consequence, the hash values will not distribute uniformly from 0 to 2^p-1
   * thus introducing errors in the estimates.
   */
  public static final int BIT_VECTOR_SIZE = 31;

  // Refer to Flajolet-Martin'86 for the value of phi
  private static final double PHI = 0.77351;

  private final int[] a;
  private final int[] b;
  private final FastBitSet[] bitVector;

  private final Random aValue;
  private final Random bValue;
  
  private int numBitVectors;

  /* Create a new distinctValueEstimator
   */
  public FMSketch(int numBitVectors) {
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

  /**
   * Resets a distinctValueEstimator object to its original state.
   */
  @Override
  public void reset() {
    for (int i=0; i< numBitVectors; i++) {
      bitVector[i].clear();
    }
  }

  public FastBitSet getBitVector(int index) {
    return bitVector[index];
  }

  public FastBitSet setBitVector(FastBitSet fastBitSet, int index) {
    return bitVector[index] = fastBitSet;
  }

  public int getNumBitVectors() {
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

  @Override
  public byte[] serialize() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    // write bytes to bos ...
    try {
      FMSketchUtils.serializeFM(bos, this);
      final byte[] result = bos.toByteArray();
      bos.close();
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public NumDistinctValueEstimator deserialize(byte[] buf) {
    InputStream is = new ByteArrayInputStream(buf);
    try {
      NumDistinctValueEstimator n = FMSketchUtils.deserializeFM(is);
      is.close();
      return n;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  @Override
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

  @Override
  public void addToEstimator(double d) {
    int v = Double.hashCode(d);
    addToEstimator(v);
  }

  public void addToEstimatorPCSA(double d) {
    int v = Double.hashCode(d);
    addToEstimatorPCSA(v);
  }

  @Override
  public void addToEstimator(HiveDecimal decimal) {
    int v = decimal.hashCode();
    addToEstimator(v);
  }

  public void addToEstimatorPCSA(HiveDecimal decimal) {
    int v = decimal.hashCode();
    addToEstimatorPCSA(v);
  }

  public void mergeEstimators(FMSketch o) {
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
  @Override
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

  @InterfaceAudience.LimitedPrivate(value = {"Hive" })
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

  @Override
  public int lengthFor(JavaDataModel model) {
    return lengthFor(model, getNumBitVectors());
  }

  // the caller needs to gurrantee that they are the same type based on numBitVectors
  @Override
  public void mergeEstimators(NumDistinctValueEstimator o) {
    // Bitwise OR the bitvector with the bitvector in the agg buffer
    for (int i = 0; i < numBitVectors; i++) {
      bitVector[i].or(((FMSketch) o).getBitVector(i));
    }
  }

  @Override
  public void addToEstimator(String s) {
    int v = s.hashCode();
    addToEstimator(v);
  }

  @Override
  public void addToEstimator(byte[] value, int offset, int length) {
    throw new UnsupportedOperationException("cannot add byte values to fmsketch");
  }

  @Override
  public boolean canMerge(NumDistinctValueEstimator o) {
    return o instanceof FMSketch && this.numBitVectors == ((FMSketch) o).numBitVectors;
  }
}
