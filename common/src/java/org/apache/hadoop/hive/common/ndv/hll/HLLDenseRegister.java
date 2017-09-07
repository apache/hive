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

package org.apache.hadoop.hive.common.ndv.hll;

import java.util.Arrays;

public class HLLDenseRegister implements HLLRegister {

  // 2^p number of bytes for register
  private byte[] register;

  // max value stored in registered is cached to determine the bit width for
  // bit packing
  private int maxRegisterValue;

  // keep count of number of zeroes in registers
  private int numZeroes;

  // compute and cache inverse power of 2 for register values
  private double[] invPow2Register;

  // number of register bits
  private int p;

  // m = 2^p
  private int m;

  public HLLDenseRegister(int p) {
    this(p, true);
  }

  public HLLDenseRegister(int p, boolean bitPack) {
    this.p = p;
    this.m = 1 << p;
    this.register = new byte[m];
    this.invPow2Register = new double[m];
    Arrays.fill(invPow2Register, 1.0);
    this.maxRegisterValue = 0;
    this.numZeroes = m;
    if (bitPack == false) {
      this.maxRegisterValue = 0xff;
    }
  }

  public boolean add(long hashcode) {

    // LSB p bits
    final int registerIdx = (int) (hashcode & (m - 1));

    // MSB 64 - p bits
    final long w = hashcode >>> p;

    // longest run of trailing zeroes
    final int lr = Long.numberOfTrailingZeros(w) + 1;
    return set(registerIdx, (byte) lr);
  }

  public boolean set(int idx, byte value) {
    boolean updated = false;
    if (idx < register.length && value > register[idx]) {

      // update max register value
      if (value > maxRegisterValue) {
        maxRegisterValue = value;
      }

      // update number of zeros
      if (register[idx] == 0 && value > 0) {
        numZeroes--;
      }

      // set register value and compute inverse pow of 2 for register value
      register[idx] = value;
      invPow2Register[idx] = Math.pow(2, -value);

      updated = true;
    }
    return updated;
  }

  public int size() {
    return register.length;
  }

  public int getNumZeroes() {
    return numZeroes;
  }

  public void merge(HLLRegister hllRegister) {
    if (hllRegister instanceof HLLDenseRegister) {
      HLLDenseRegister hdr = (HLLDenseRegister) hllRegister;
      byte[] inRegister = hdr.getRegister();

      // merge only if the register length matches
      if (register.length != inRegister.length) {
        throw new IllegalArgumentException(
            "The size of register sets of HyperLogLogs to be merged does not match.");
      }

      // compare register values and store the max register value
      for (int i = 0; i < inRegister.length; i++) {
        if (inRegister[i] > register[i]) {
          if (register[i] == 0) {
            numZeroes--;
          }
          register[i] = inRegister[i];
          invPow2Register[i] = Math.pow(2, -inRegister[i]);
        }
      }

      // update max register value
      if (hdr.getMaxRegisterValue() > maxRegisterValue) {
        maxRegisterValue = hdr.getMaxRegisterValue();
      }
    } else {
      throw new IllegalArgumentException("Specified register is not instance of HLLDenseRegister");
    }
  }

  public byte[] getRegister() {
    return register;
  }

  public void setRegister(byte[] register) {
    this.register = register;
  }

  public int getMaxRegisterValue() {
    return maxRegisterValue;
  }

  public double getSumInversePow2() {
    double sum = 0;
    for (double d : invPow2Register) {
      sum += d;
    }
    return sum;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HLLDenseRegister - ");
    sb.append("p: ");
    sb.append(p);
    sb.append(" numZeroes: ");
    sb.append(numZeroes);
    sb.append(" maxRegisterValue: ");
    sb.append(maxRegisterValue);
    return sb.toString();
  }

  public String toExtendedString() {
    return toString() + " register: " + Arrays.toString(register);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HLLDenseRegister)) {
      return false;
    }
    HLLDenseRegister other = (HLLDenseRegister) obj;
    return numZeroes == other.numZeroes && maxRegisterValue == other.maxRegisterValue
        && Arrays.equals(register, other.register);
  }

  @Override
  public int hashCode() {
    int hashcode = 0;
    hashcode += 31 * numZeroes;
    hashcode += 31 * maxRegisterValue;
    hashcode += Arrays.hashCode(register);
    return hashcode;
  }

}
