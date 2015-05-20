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

package org.apache.hadoop.hive.ql.io.filters;

import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hive.common.util.BloomFilter;

import com.google.common.primitives.Longs;

public class BloomFilterIO extends BloomFilter {

  public BloomFilterIO(long expectedEntries) {
    super(expectedEntries, DEFAULT_FPP);
  }

  public BloomFilterIO(long expectedEntries, double fpp) {
    super(expectedEntries, fpp);
  }

/**
 * Initializes the BloomFilter from the given Orc BloomFilter
 */
  public BloomFilterIO(OrcProto.BloomFilter bloomFilter) {
    this.bitSet = new BitSet(Longs.toArray(bloomFilter.getBitsetList()));
    this.numHashFunctions = bloomFilter.getNumHashFunctions();
    this.numBits = (int) this.bitSet.bitSize();
  }
}
