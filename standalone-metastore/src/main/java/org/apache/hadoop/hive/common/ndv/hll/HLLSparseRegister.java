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

import java.util.Map;
import java.util.TreeMap;

public class HLLSparseRegister implements HLLRegister {

  private TreeMap<Integer,Byte> sparseMap;

  // for a better insertion performance values are added to temporary unsorted
  // list which will be merged to sparse map after a threshold
  private int[] tempList;
  private int tempListIdx;

  // number of register bits
  private final int p;

  // new number of register bits for higher accuracy
  private final int pPrime;

  // number of bits to store the number of zero runs
  private final int qPrime;

  // masks for quicker extraction of p, pPrime, qPrime values
  private final int mask;
  private final int pPrimeMask;
  private final int qPrimeMask;

  public HLLSparseRegister(int p, int pp, int qp) {
    this.p = p;
    this.sparseMap = new TreeMap<>();
    this.tempList = new int[HLLConstants.TEMP_LIST_DEFAULT_SIZE];
    this.tempListIdx = 0;
    this.pPrime = pp;
    this.qPrime = qp;
    this.mask = ((1 << pPrime) - 1) ^ ((1 << p) - 1);
    this.pPrimeMask = ((1 << pPrime) - 1);
    this.qPrimeMask = (1 << qPrime) - 1;
  }

  public boolean add(long hashcode) {
    boolean updated = false;

    // fill the temp list before merging to sparse map
    if (tempListIdx < tempList.length) {
      int encodedHash = encodeHash(hashcode);
      tempList[tempListIdx++] = encodedHash;
      updated = true;
    } else {
      updated = mergeTempListToSparseMap();
    }

    return updated;
  }

  /**
   * Adds temp list to sparse map. The key for sparse map entry is the register
   * index determined by pPrime and value is the number of trailing zeroes.
   * @return
   */
  private boolean mergeTempListToSparseMap() {
    boolean updated = false;
    for (int i = 0; i < tempListIdx; i++) {
      int encodedHash = tempList[i];
      int key = encodedHash & pPrimeMask;
      byte value = (byte) (encodedHash >>> pPrime);
      byte nr = 0;
      // if MSB is set to 1 then next qPrime MSB bits contains the value of
      // number of zeroes.
      // if MSB is set to 0 then number of zeroes is contained within pPrime - p
      // bits.
      if (encodedHash < 0) {
        nr = (byte) (value & qPrimeMask);
      } else {
        nr = (byte) (Integer.numberOfTrailingZeros(encodedHash >>> p) + 1);
      }
      updated = set(key, nr);
    }

    // reset temp list index
    tempListIdx = 0;
    return updated;
  }

  /**
   * <pre>
   * <b>Input:</b> 64 bit hashcode
   * 
   * |---------w-------------| |------------p'----------|
   * 10101101.......1010101010 10101010101 01010101010101
   *                                       |------p-----|
   *                                       
   * <b>Output:</b> 32 bit int
   * 
   * |b| |-q'-|  |------------p'----------|
   *  1  010101  01010101010 10101010101010
   *                         |------p-----|
   *                    
   * 
   * The default values of p', q' and b are 25, 6, 1 (total 32 bits) respectively.
   * This function will return an int encoded in the following format
   * 
   * p  - LSB p bits represent the register index
   * p' - LSB p' bits are used for increased accuracy in estimation
   * q' - q' bits after p' are left as such from the hashcode if b = 0 else
   *      q' bits encodes the longest trailing zero runs from in (w-p) input bits
   * b  - 0 if longest trailing zero run is contained within (p'-p) bits
   *      1 if longest trailing zero run is computeed from (w-p) input bits and
   *      its value is stored in q' bits
   * </pre>
   * @param hashcode
   * @return
   */
  public int encodeHash(long hashcode) {
    // x = p' - p
    int x = (int) (hashcode & mask);
    if (x == 0) {
      // more bits should be considered for finding q (longest zero runs)
      // set MSB to 1
      int ntr = Long.numberOfTrailingZeros(hashcode >> p) + 1;
      long newHashCode = hashcode & pPrimeMask;
      newHashCode |= ntr << pPrime;
      newHashCode |= 0x80000000;
      return (int) newHashCode;
    } else {
      // q is contained within p' - p
      // set MSB to 0
      return (int) (hashcode & 0x7FFFFFFF);
    }
  }

  public int getSize() {

    // merge temp list before getting the size of sparse map
    if (tempListIdx != 0) {
      mergeTempListToSparseMap();
    }
    return sparseMap.size();
  }

  public void merge(HLLRegister hllRegister) {
    if (hllRegister instanceof HLLSparseRegister) {
      HLLSparseRegister hsr = (HLLSparseRegister) hllRegister;

      // retain only the largest value for a register index
      for (Map.Entry<Integer, Byte> entry : hsr.getSparseMap().entrySet()) {
        int key = entry.getKey();
        byte value = entry.getValue();
        set(key, value);
      }
    } else {
      throw new IllegalArgumentException("Specified register not instance of HLLSparseRegister");
    }
  }

  public boolean set(int key, byte value) {
    boolean updated = false;

    // retain only the largest value for a register index
    if (sparseMap.containsKey(key)) {
      byte containedVal = sparseMap.get(key);
      if (value > containedVal) {
        sparseMap.put(key, value);
        updated = true;
      }
    } else {
      sparseMap.put(key, value);
      updated = true;
    }
    return updated;
  }

  public TreeMap<Integer,Byte> getSparseMap() {
    return sparseMap;
  }

  public TreeMap<Integer,Byte> getMergedSparseMap() {
    if (tempListIdx != 0) {
      mergeTempListToSparseMap();
    }
    return sparseMap;
  }

  public int getP() {
    return p;
  }

  public int getPPrime() {
    return pPrime;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HLLSparseRegister - ");
    sb.append("p: ");
    sb.append(p);
    sb.append(" pPrime: ");
    sb.append(pPrime);
    sb.append(" qPrime: ");
    sb.append(qPrime);
    return sb.toString();
  }

  public String toExtendedString() {
    return toString() + " register: " + sparseMap.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HLLSparseRegister)) {
      return false;
    }
    HLLSparseRegister other = (HLLSparseRegister) obj;
    boolean result = p == other.p && pPrime == other.pPrime && qPrime == other.qPrime
        && tempListIdx == other.tempListIdx;
    if (result) {
      for (int i = 0; i < tempListIdx; i++) {
        if (tempList[i] != other.tempList[i]) {
          return false;
        }
      }

      result = result && sparseMap.equals(other.sparseMap);
    }
    return result;
  }

  @Override
  public int hashCode() {
    int hashcode = 0;
    hashcode += 31 * p;
    hashcode += 31 * pPrime;
    hashcode += 31 * qPrime;
    for (int i = 0; i < tempListIdx; i++) {
      hashcode += 31 * tempList[tempListIdx];
    }
    hashcode += sparseMap.hashCode();
    return hashcode;
  }

}
