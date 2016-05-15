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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

import com.google.common.annotations.VisibleForTesting;

/*
 * An multi-key value hash map optimized for vector map join.
 *
 * The key is stored as the provided bytes (uninterpreted).
 */
public class VectorMapJoinFastMultiKeyHashMap
        extends VectorMapJoinFastBytesHashMap {

  /*
   * A Unit Test convenience method for putting key and value into the hash table using the
   * actual types.
   */
  @VisibleForTesting
  public void testPutRow(byte[] currentKey, byte[] currentValue) throws HiveException, IOException {
    if (testKeyBytesWritable == null) {
      testKeyBytesWritable = new BytesWritable();
      testValueBytesWritable = new BytesWritable();
    }
    testKeyBytesWritable.set(currentKey, 0, currentKey.length);
    testValueBytesWritable.set(currentValue, 0, currentValue.length);
    putRow(testKeyBytesWritable, testValueBytesWritable);
  }

  public VectorMapJoinFastMultiKeyHashMap(
        boolean isOuterJoin,
        int initialCapacity, float loadFactor, int writeBuffersSize) {
    super(initialCapacity, loadFactor, writeBuffersSize);
  }
}