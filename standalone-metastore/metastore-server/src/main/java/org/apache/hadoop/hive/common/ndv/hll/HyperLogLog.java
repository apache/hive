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

package org.apache.hadoop.hive.common.ndv.hll;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hive.common.util.Murmur3;

import com.google.common.annotations.VisibleForTesting;

/**
 * <pre>
 * This is an implementation of the following variants of hyperloglog (HLL)
 * algorithm
 * Original  - Original HLL algorithm from Flajolet et. al from
 *             http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 * HLLNoBias - Google's implementation of bias correction based on lookup table
 *             http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 * HLL++     - Google's implementation of HLL++ algorithm that uses SPARSE registers
 *             http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 *
 * Following are the constructor parameters that determines which algorithm is
 * used
 * <b>numRegisterIndexBits</b> - number of LSB hashcode bits to be used as register index.
 *                        <i>Default is 14</i>. min = 4 and max = 16
 * <b>numHashBits</b> - number of bits for hashcode. <i>Default is 64</i>. min = 32 and max = 128
 * <b>encoding</b> - Type of encoding to use (SPARSE or DENSE). The algorithm automatically
 *            switches to DENSE beyond a threshold. <i>Default: SPARSE</i>
 * <b>enableBitPacking</b> - To enable bit packing or not. Bit packing improves compression
 *                    at the cost of more CPU cycles. <i>Default: true</i>
 * <b>noBias</b> - Use Google's bias table lookup for short range bias correction.
 *          Enabling this will highly improve the estimation accuracy for short
 *          range values. <i>Default: true</i>
 *
 * </pre>
 */
public class HyperLogLog implements NumDistinctValueEstimator {
  private final static int DEFAULT_HASH_BITS = 64;
  private final static long HASH64_ZERO = Murmur3.hash64(new byte[] {0});
  private final static long HASH64_ONE = Murmur3.hash64(new byte[] {1});

  public enum EncodingType {
    SPARSE, DENSE
  }

  // number of bits to address registers
  private final int p;

  // number of registers - 2^p
  private final int m;

  // refer paper
  private float alphaMM;

  // enable/disable bias correction using table lookup
  private final boolean noBias;

  // enable/disable bitpacking
  private final boolean bitPacking;

  // Not making it configurable for perf reasons (avoid checks)
  private final int chosenHashBits = DEFAULT_HASH_BITS;

  private HLLDenseRegister denseRegister;
  private HLLSparseRegister sparseRegister;

  // counts are cached to avoid repeated complex computation. If register value
  // is updated the count will be computed again.
  private long cachedCount;
  private boolean invalidateCount;

  private EncodingType encoding;

  // threshold to switch from SPARSE to DENSE encoding
  private int encodingSwitchThreshold;

  private HyperLogLog(HyperLogLogBuilder hllBuilder) {
    if (hllBuilder.numRegisterIndexBits < HLLConstants.MIN_P_VALUE
        || hllBuilder.numRegisterIndexBits > HLLConstants.MAX_P_VALUE) {
      throw new IllegalArgumentException("p value should be between " + HLLConstants.MIN_P_VALUE
          + " to " + HLLConstants.MAX_P_VALUE);
    }
    this.p = hllBuilder.numRegisterIndexBits;
    this.m = 1 << p;
    this.noBias = hllBuilder.noBias;
    this.bitPacking = hllBuilder.bitPacking;

    // the threshold should be less than 12K bytes for p = 14.
    // The reason to divide by 5 is, in sparse mode after serialization the
    // entriesin sparse map are compressed, and delta encoded as varints. The
    // worst case size of varints are 5 bytes. Hence, 12K/5 ~= 2400 entries in
    // sparse map.
    if (bitPacking) {
      this.encodingSwitchThreshold = ((m * 6) / 8) / 5;
    } else {
      // if bitpacking is disabled, all register values takes 8 bits and hence
      // we can be more flexible with the threshold. For p=14, 16K/5 = 3200
      // entries in sparse map can be allowed.
      this.encodingSwitchThreshold = m / 3;
    }

    // initializeAlpha(DEFAULT_HASH_BITS);
    // alphaMM value for 128 bits hash seems to perform better for default 64 hash bits
    this.alphaMM = 0.7213f / (1 + 1.079f / m);
    // For efficiency alpha is multiplied by m^2
    this.alphaMM = this.alphaMM * m * m;

    this.cachedCount = -1;
    this.invalidateCount = false;
    this.encoding = hllBuilder.encoding;
    if (encoding.equals(EncodingType.SPARSE)) {
      this.sparseRegister = new HLLSparseRegister(p, HLLConstants.P_PRIME_VALUE,
          HLLConstants.Q_PRIME_VALUE);
      this.denseRegister = null;
    } else {
      this.sparseRegister = null;
      this.denseRegister = new HLLDenseRegister(p, bitPacking);
    }
  }

  public static HyperLogLogBuilder builder() {
    return new HyperLogLogBuilder();
  }

  public static class HyperLogLogBuilder {
    private int numRegisterIndexBits = 14;
    private EncodingType encoding = EncodingType.SPARSE;
    private boolean bitPacking = true;
    private boolean noBias = true;

    public HyperLogLogBuilder() {
    }

    public HyperLogLogBuilder setNumRegisterIndexBits(int b) {
      this.numRegisterIndexBits = b;
      return this;
    }

    public HyperLogLogBuilder setSizeOptimized() {
      // allowing this to be increased via config breaks the merge impl
      // p=10 = ~1kb per vector or smaller
      this.numRegisterIndexBits = 10;
      return this;
    }

    public HyperLogLogBuilder setEncoding(EncodingType enc) {
      this.encoding = enc;
      return this;
    }

    public HyperLogLogBuilder enableBitPacking(boolean b) {
      this.bitPacking = b;
      return this;
    }

    public HyperLogLogBuilder enableNoBias(boolean nb) {
      this.noBias = nb;
      return this;
    }

    public HyperLogLog build() {
      return new HyperLogLog(this);
    }
  }

  // see paper for alpha initialization.
  private void initializeAlpha(final int hashBits) {
    if (hashBits <= 16) {
      alphaMM = 0.673f;
    } else if (hashBits <= 32) {
      alphaMM = 0.697f;
    } else if (hashBits <= 64) {
      alphaMM = 0.709f;
    } else {
      alphaMM = 0.7213f / (1 + 1.079f / m);
    }

    // For efficiency alpha is multiplied by m^2
    alphaMM = alphaMM * m * m;
  }

  public void addBoolean(boolean val) {
    add(val ? HASH64_ONE : HASH64_ZERO);
  }

  public void addByte(byte val) {
    add(Murmur3.hash64(new byte[] {val}));
  }

  public void addBytes(byte[] val) {
    add(Murmur3.hash64(val));
  }

  public void addShort(short val) {
    add(Murmur3.hash64(val));
  }

  public void addInt(int val) {
    add(Murmur3.hash64(val));
  }

  public void addLong(long val) {
    add(Murmur3.hash64(val));
  }

  public void addFloat(float val) {
    add(Murmur3.hash64(Float.floatToIntBits(val)));
  }

  public void addDouble(double val) {
    add(Murmur3.hash64(Double.doubleToLongBits(val)));
  }

  public void addChar(char val) {
    add(Murmur3.hash64((short)val));
  }

  /**
   * Java's default charset will be used for strings.
   * @param val
   *          - input string
   */
  public void addString(String val) {
    add(Murmur3.hash64(val.getBytes()));
  }

  public void addBytes(byte[] value, int offset, int length) {
    add(Murmur3.hash64(value, offset, length));
  }

  public void addString(String val, Charset charset) {
    add(Murmur3.hash64(val.getBytes(charset)));
  }

  public void add(long hashcode) {
    if (encoding.equals(EncodingType.SPARSE)) {
      if (sparseRegister.add(hashcode)) {
        invalidateCount = true;
      }

      // if size of sparse map excess the threshold convert the sparse map to
      // dense register and switch to DENSE encoding
      if (sparseRegister.isSizeGreaterThan(encodingSwitchThreshold)) {
        encoding = EncodingType.DENSE;
        denseRegister = sparseToDenseRegister(sparseRegister);
        sparseRegister = null;
        invalidateCount = true;
      }
    } else {
      if (denseRegister.add(hashcode)) {
        invalidateCount = true;
      }
    }
  }

  public long estimateNumDistinctValues() {
    // FMSketch treats the ndv of all nulls as 1 but hll treates the ndv as 0.
    // In order to get rid of divide by 0 problem, we follow FMSketch
    return count() > 0 ? count() : 1;
  }

  public long count() {
    // compute count only if the register values are updated else return the
    // cached count
    if (invalidateCount || cachedCount < 0) {
      if (encoding.equals(EncodingType.SPARSE)) {

        // if encoding is still SPARSE use linear counting with increase
        // accuracy (as we use pPrime bits for register index)
        int mPrime = 1 << sparseRegister.getPPrime();
        cachedCount = linearCount(mPrime, mPrime - sparseRegister.getSparseMap().size());
      } else {

        // for DENSE encoding, use bias table lookup for HLLNoBias algorithm
        // else fallback to HLLOriginal algorithm
        double sum = denseRegister.getSumInversePow2();
        long numZeros = denseRegister.getNumZeroes();

        // cardinality estimate from normalized bias corrected harmonic mean on
        // the registers
        cachedCount = (long) (alphaMM * (1.0 / sum));
        long pow = (long) Math.pow(2, chosenHashBits);

        // when bias correction is enabled
        if (noBias) {
          cachedCount = cachedCount <= 5 * m ? (cachedCount - estimateBias(cachedCount))
              : cachedCount;
          long h = cachedCount;
          if (numZeros != 0) {
            h = linearCount(m, numZeros);
          }

          if (h < getThreshold()) {
            cachedCount = h;
          }
        } else {
          // HLL algorithm shows stronger bias for values in (2.5 * m) range.
          // To compensate for this short range bias, linear counting is used
          // for values before this short range. The original paper also says
          // similar bias is seen for long range values due to hash collisions
          // in range >1/30*(2^32). For the default case, we do not have to
          // worry about this long range bias as the paper used 32-bit hashing
          // and we use 64-bit hashing as default. 2^64 values are too high to
          // observe long range bias (hash collisions).
          if (cachedCount <= 2.5 * m) {

            // for short range use linear counting
            if (numZeros != 0) {
              cachedCount = linearCount(m, numZeros);
            }
          } else if (chosenHashBits < 64 && cachedCount > (0.033333 * pow)) {

            // long range bias for 32-bit hashcodes
            if (cachedCount > (1 / 30) * pow) {
              cachedCount = (long) (-pow * Math.log(1.0 - (double) cachedCount / (double) pow));
            }
          }
        }
      }
      invalidateCount = false;
    }

    return cachedCount;
  }

  private long getThreshold() {
    return (long) (HLLConstants.thresholdData[p - 4] + 0.5);
  }

  /**
   * Estimate bias from lookup table
   * @param count
   *          - cardinality before bias correction
   * @return cardinality after bias correction
   */
  private long estimateBias(long count) {
    double[] rawEstForP = HLLConstants.rawEstimateData[p - 4];

    // compute distance and store it in sorted map
    TreeMap<Double,Integer> estIndexMap = new TreeMap<>();
    double distance = 0;
    for (int i = 0; i < rawEstForP.length; i++) {
      distance = Math.pow(count - rawEstForP[i], 2);
      estIndexMap.put(distance, i);
    }

    // take top-k closest neighbors and compute the bias corrected cardinality
    long result = 0;
    double[] biasForP = HLLConstants.biasData[p - 4];
    double biasSum = 0;
    int kNeighbors = HLLConstants.K_NEAREST_NEIGHBOR;
    for (Map.Entry<Double, Integer> entry : estIndexMap.entrySet()) {
      biasSum += biasForP[entry.getValue()];
      kNeighbors--;
      if (kNeighbors <= 0) {
        break;
      }
    }

    // 0.5 added for rounding off
    result = (long) ((biasSum / HLLConstants.K_NEAREST_NEIGHBOR) + 0.5);
    return result;
  }

  public void setCount(long count) {
    this.cachedCount = count;
    this.invalidateCount = true;
  }

  private long linearCount(int mVal, long numZeros) {
    return (Math.round(mVal * Math.log(mVal / ((double) numZeros))));
  }

  // refer paper
  public double getStandardError() {
    return 1.04 / Math.sqrt(m);
  }

  public HLLDenseRegister getHLLDenseRegister() {
    return denseRegister;
  }

  public HLLSparseRegister getHLLSparseRegister() {
    return sparseRegister;
  }

  /**
   * Reconstruct sparse map from serialized integer list
   * @param reg
   *          - uncompressed and delta decoded integer list
   */
  public void setHLLSparseRegister(int[] reg) {
    for (int i : reg) {
      int key = i >>> HLLConstants.Q_PRIME_VALUE;
      byte value = (byte) (i & 0x3f);
      sparseRegister.set(key, value);
    }
  }

  /**
   * Reconstruct dense registers from byte array
   * @param reg
   *          - unpacked byte array
   */
  public void setHLLDenseRegister(byte[] reg) {
    int i = 0;
    for (byte b : reg) {
      denseRegister.set(i, b);
      i++;
    }
  }

  /**
   * Merge the specified hyperloglog to the current one. Encoding switches
   * automatically after merge if the encoding switch threshold is exceeded.
   * @param hll
   *          - hyperloglog to be merged
   * @throws IllegalArgumentException
   */
  public void merge(HyperLogLog hll) {
    if (chosenHashBits != hll.chosenHashBits) {
      throw new IllegalArgumentException(
          "HyperLogLog cannot be merged as either p or hashbits are different. Current: "
              + toString() + " Provided: " + hll.toString());
    }

    if (p > hll.p) {
      throw new IllegalArgumentException(
          "HyperLogLog cannot merge a smaller p into a larger one : "
              + toString() + " Provided: " + hll.toString());
    }

    if (p != hll.p) {
      // invariant: p > hll.p
      hll = hll.squash(p);
    }

    EncodingType otherEncoding = hll.getEncoding();

    if (encoding.equals(EncodingType.SPARSE) && otherEncoding.equals(EncodingType.SPARSE)) {
      sparseRegister.merge(hll.getHLLSparseRegister());
      // if after merge the sparse switching threshold is exceeded then change
      // to dense encoding
      if (sparseRegister.isSizeGreaterThan(encodingSwitchThreshold)) {
        encoding = EncodingType.DENSE;
        denseRegister = sparseToDenseRegister(sparseRegister);
        sparseRegister = null;
      }
    } else if (encoding.equals(EncodingType.DENSE) && otherEncoding.equals(EncodingType.DENSE)) {
      denseRegister.merge(hll.getHLLDenseRegister());
    } else if (encoding.equals(EncodingType.SPARSE) && otherEncoding.equals(EncodingType.DENSE)) {
      denseRegister = sparseToDenseRegister(sparseRegister);
      denseRegister.merge(hll.getHLLDenseRegister());
      sparseRegister = null;
      encoding = EncodingType.DENSE;
    } else if (encoding.equals(EncodingType.DENSE) && otherEncoding.equals(EncodingType.SPARSE)) {
      HLLDenseRegister otherDenseRegister = sparseToDenseRegister(hll.getHLLSparseRegister());
      denseRegister.merge(otherDenseRegister);
    }

    invalidateCount = true;
  }

  /**
   * Reduces the accuracy of the HLL provided to a smaller size
   * @param p0
   *         - new p size for the new HyperLogLog (smaller or no change)
   * @return reduced (or same) HyperLogLog instance
   */
  public HyperLogLog squash(final int p0) {
    if (p0 > p) {
      throw new IllegalArgumentException(
          "HyperLogLog cannot be be squashed to be bigger. Current: "
              + toString() + " Provided: " + p0);
    }

    if (p0 == p) {
      return this;
    }

    final HyperLogLog hll = new HyperLogLogBuilder()
        .setNumRegisterIndexBits(p0).setEncoding(EncodingType.DENSE)
        .enableNoBias(noBias).build();
    final HLLDenseRegister result = hll.denseRegister;

    if (encoding == EncodingType.SPARSE) {
      sparseRegister.extractLowBitsTo(result);
    } else if (encoding == EncodingType.DENSE) {
      denseRegister.extractLowBitsTo(result);
    }
    return hll;
  }

  /**
   * Converts sparse to dense hll register.
   * @param sparseRegister
   *          - sparse register to be converted
   * @return converted dense register
   */
  private HLLDenseRegister sparseToDenseRegister(HLLSparseRegister sparseRegister) {
    if (sparseRegister == null) {
      return null;
    }
    int p = sparseRegister.getP();
    int pMask = (1 << p) - 1;
    HLLDenseRegister result = new HLLDenseRegister(p, bitPacking);
    for (Map.Entry<Integer, Byte> entry : sparseRegister.getSparseMap().entrySet()) {
      int key = entry.getKey();
      int idx = key & pMask;
      result.set(idx, entry.getValue());
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Encoding: ");
    sb.append(encoding);
    sb.append(", p: ");
    sb.append(p);
    sb.append(", estimatedCardinality: ");
    sb.append(estimateNumDistinctValues());
    return sb.toString();
  }

  public String toStringExtended() {
    if (encoding.equals(EncodingType.DENSE)) {
      return toString() + ", " + denseRegister.toExtendedString();
    } else if (encoding.equals(EncodingType.SPARSE)) {
      return toString() + ", " + sparseRegister.toExtendedString();
    }

    return toString();
  }

  public int getNumRegisterIndexBits() {
    return p;
  }

  public EncodingType getEncoding() {
    return encoding;
  }

  public void setEncoding(EncodingType encoding) {
    this.encoding = encoding;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HyperLogLog)) {
      return false;
    }

    HyperLogLog other = (HyperLogLog) obj;
    long count = estimateNumDistinctValues();
    long otherCount = other.estimateNumDistinctValues();
    boolean result = p == other.p && chosenHashBits == other.chosenHashBits
        && encoding.equals(other.encoding) && count == otherCount;
    if (encoding.equals(EncodingType.DENSE)) {
      result = result && denseRegister.equals(other.getHLLDenseRegister());
    }

    if (encoding.equals(EncodingType.SPARSE)) {
      result = result && sparseRegister.equals(other.getHLLSparseRegister());
    }
    return result;
  }

  @Override
  public int hashCode() {
    int hashcode = 0;
    hashcode += 31 * p;
    hashcode += 31 * chosenHashBits;
    hashcode += encoding.hashCode();
    hashcode += 31 * estimateNumDistinctValues();
    if (encoding.equals(EncodingType.DENSE)) {
      hashcode += 31 * denseRegister.hashCode();
    }

    if (encoding.equals(EncodingType.SPARSE)) {
      hashcode += 31 * sparseRegister.hashCode();
    }
    return hashcode;
  }

  @Override
  public void reset() {
  }

  @Override
  public byte[] serialize() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    // write bytes to bos ...
    try {
      HyperLogLogUtils.serializeHLL(bos, this);
      byte[] result = bos.toByteArray();
      bos.close();
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public NumDistinctValueEstimator deserialize(byte[] buf) {
    return HyperLogLogUtils.deserializeHLL(buf);
  }

  @Override
  public void addToEstimator(long v) {
    addLong(v);
  }

  @Override
  public void addToEstimator(String s) {
    addString(s);
  }

  @Override
  public void addToEstimator(double d) {
    addDouble(d);
  }

  @Override
  public void addToEstimator(byte[] value, int offset, int length) {
    addBytes(value, offset, length);
  }

  @Override
  public void addToEstimator(HiveDecimal decimal) {
    addDouble(decimal.doubleValue());
  }

  @Override
  public void mergeEstimators(NumDistinctValueEstimator o) {
    merge((HyperLogLog) o);
  }

  @Override
  public int lengthFor(JavaDataModel model) {
    // 5 is the head, 1<<p means the number of bytes for register
    return (5 + (1 << p));
  }

  @Override
  public boolean canMerge(NumDistinctValueEstimator o) {
    return o instanceof HyperLogLog;
  }

  @VisibleForTesting
  public int getEncodingSwitchThreshold() {
    return encodingSwitchThreshold;
  }

}
