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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.CheckFastHashTable.VerifyFastBytesHashMap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

/*
 * An multi-key value hash map optimized for vector map join.
 *
 * The key is uninterpreted bytes.
 */
public class TestVectorMapJoinFastBytesHashMapNonMatched extends CommonFastHashTable {

  @Test
  public void testOneKey() throws Exception {
    random = new Random(82733);

    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false,CAPACITY, LOAD_FACTOR, WB_SIZE, -1);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    byte[] key = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key);
    byte[] value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
    random.nextBytes(value);

    map.testPutRow(key, value);
    verifyTable.add(key, value);

    // Second value.
    value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
    random.nextBytes(value);
    map.testPutRow(key, value);
    verifyTable.add(key, value);

    // Third value.
    value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
    random.nextBytes(value);
    map.testPutRow(key, value);
    verifyTable.add(key, value);

    verifyTable.verifyNonMatched(map, random);
  }

  @Test
  public void testMultipleKeysSingleValue() throws Exception {
    random = new Random(29383);

    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false,CAPACITY, LOAD_FACTOR, WB_SIZE, -1);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    int keyCount = 100 + random.nextInt(1000);
    for (int i = 0; i < keyCount; i++) {
      byte[] key = new byte[random.nextInt(MAX_KEY_LENGTH)];
      random.nextBytes(key);
      if (!verifyTable.contains(key)) {
        // Unique keys for this test.
        break;
      }
      byte[] value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
      random.nextBytes(value);

      map.testPutRow(key, value);
      verifyTable.add(key, value);
    }

    verifyTable.verifyNonMatched(map, random);
  }

  public void addAndVerifyMultipleKeyMultipleValue(int keyCount,
      VectorMapJoinFastMultiKeyHashMap map, VerifyFastBytesHashMap verifyTable)
          throws HiveException, IOException {
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable, MAX_KEY_LENGTH, -1);
  }

  public void addAndVerifyMultipleKeyMultipleValue(int keyCount,
      VectorMapJoinFastMultiKeyHashMap map, VerifyFastBytesHashMap verifyTable,
      int maxKeyLength, int fixedValueLength)
          throws HiveException, IOException {
    for (int i = 0; i < keyCount; i++) {
      byte[] value;
      if (fixedValueLength == -1) {
        value = new byte[generateLargeCount() - 1];
      } else {
        value = new byte[fixedValueLength];
      }
      random.nextBytes(value);

      // Add a new key or add a value to an existing key?
      if (random.nextBoolean() || verifyTable.getCount() == 0) {
        byte[] key;
        while (true) {
          key = new byte[random.nextInt(maxKeyLength)];
          random.nextBytes(key);
          if (!verifyTable.contains(key)) {
            // Unique keys for this test.
            break;
          }
        }

        map.testPutRow(key, value);
        verifyTable.add(key, value);
      } else {
        byte[] randomExistingKey = verifyTable.addRandomExisting(value, random);
        map.testPutRow(randomExistingKey, value);
      }
    }

    verifyTable.verifyNonMatched(map, random);
  }

  @Test
  public void testMultipleKeysMultipleValue() throws Exception {
    random = new Random(9332);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false,LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    int keyCount = 100;
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable);
  }

  @Test
  public void testReallyBig() throws Exception {
    random = new Random(42662);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false, LARGE_CAPACITY, LOAD_FACTOR, MODERATE_WB_SIZE, -1);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    int keyCount = 100;
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable);
  }
}
