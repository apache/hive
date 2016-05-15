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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.CheckFastHashTable.VerifyFastBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

/*
 * An multi-key value hash map optimized for vector map join.
 *
 * The key is uninterpreted bytes.
 */
public class TestVectorMapJoinFastBytesHashMap extends CommonFastHashTable {

  @Test
  public void testOneKey() throws Exception {
    random = new Random(82733);

    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false,CAPACITY, LOAD_FACTOR, WB_SIZE);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    byte[] key = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key);
    byte[] value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
    random.nextBytes(value);

    map.testPutRow(key, value);
    verifyTable.add(key, value);
    verifyTable.verify(map);

    // Second value.
    value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
    random.nextBytes(value);
    map.testPutRow(key, value);
    verifyTable.add(key, value);
    verifyTable.verify(map);

    // Third value.
    value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
    random.nextBytes(value);
    map.testPutRow(key, value);
    verifyTable.add(key, value);
    verifyTable.verify(map);
  }

  @Test
  public void testMultipleKeysSingleValue() throws Exception {
    random = new Random(29383);

    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false,CAPACITY, LOAD_FACTOR, WB_SIZE);

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
      verifyTable.verify(map);
    }
  }

  @Test
  public void testGetNonExistent() throws Exception {
    random = new Random(1002);

    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false,CAPACITY, LOAD_FACTOR, WB_SIZE);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    byte[] key1 = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key1);
    byte[] value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
    random.nextBytes(value);

    map.testPutRow(key1, value);
    verifyTable.add(key1, value);
    verifyTable.verify(map);

    byte[] key2 = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key2);
    VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
    JoinUtil.JoinResult joinResult = map.lookup(key2, 0, key2.length, hashMapResult);
    assertTrue(joinResult == JoinUtil.JoinResult.NOMATCH);
    assertTrue(!hashMapResult.hasRows());

    map.testPutRow(key2, value);
    verifyTable.add(key2, value);
    verifyTable.verify(map);

    byte[] key3 = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key3);
    hashMapResult = map.createHashMapResult();
    joinResult = map.lookup(key3, 0, key3.length, hashMapResult);
    assertTrue(joinResult == JoinUtil.JoinResult.NOMATCH);
    assertTrue(!hashMapResult.hasRows());
  }

  @Test
  public void testFullMap() throws Exception {
    random = new Random(200001);

    // Make sure the map does not expand; should be able to find space.
    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(false,CAPACITY, 1f, WB_SIZE);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    for (int i = 0; i < CAPACITY; i++) {
      byte[] key;
      while (true) {
        key = new byte[random.nextInt(MAX_KEY_LENGTH)];
        random.nextBytes(key);
        if (!verifyTable.contains(key)) {
          // Unique keys for this test.
          break;
        }
      }
      byte[] value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
      random.nextBytes(value);

      map.testPutRow(key, value);
      verifyTable.add(key, value);
      // verifyTable.verify(map);
    }
    verifyTable.verify(map);

    byte[] anotherKey;
    while (true) {
      anotherKey = new byte[random.nextInt(MAX_KEY_LENGTH)];
      random.nextBytes(anotherKey);
      if (!verifyTable.contains(anotherKey)) {
        // Unique keys for this test.
        break;
      }
    }

    VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
    JoinUtil.JoinResult joinResult = map.lookup(anotherKey, 0, anotherKey.length, hashMapResult);
    assertTrue(joinResult == JoinUtil.JoinResult.NOMATCH);
  }

  @Test
  public void testExpand() throws Exception {
    random = new Random(99221);

    // Start with capacity 1; make sure we expand on every put.
    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(false,1, 0.0000001f, WB_SIZE);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    for (int i = 0; i < 18; ++i) {
      byte[] key;
      while (true) {
        key = new byte[random.nextInt(MAX_KEY_LENGTH)];
        random.nextBytes(key);
        if (!verifyTable.contains(key)) {
          // Unique keys for this test.
          break;
        }
      }
      byte[] value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
      random.nextBytes(value);

      map.testPutRow(key, value);
      verifyTable.add(key, value);
      // verifyTable.verify(map);
    }
    verifyTable.verify(map);
    // assertEquals(1 << 18, map.getCapacity());
  }

  public void addAndVerifyMultipleKeyMultipleValue(int keyCount,
      VectorMapJoinFastMultiKeyHashMap map, VerifyFastBytesHashMap verifyTable)
          throws HiveException, IOException {
    for (int i = 0; i < keyCount; i++) {
      byte[] value = new byte[generateLargeCount() - 1];
      random.nextBytes(value);

      // Add a new key or add a value to an existing key?
      if (random.nextBoolean() || verifyTable.getCount() == 0) {
        byte[] key;
        while (true) {
          key = new byte[random.nextInt(MAX_KEY_LENGTH)];
          random.nextBytes(key);
          if (!verifyTable.contains(key)) {
            // Unique keys for this test.
            break;
          }
        }

        map.testPutRow(key, value);
        verifyTable.add(key, value);
        // verifyTable.verify(map);
      } else {
        byte[] randomExistingKey = verifyTable.addRandomExisting(value, random);
        map.testPutRow(randomExistingKey, value);
        // verifyTable.verify(map);
      }
    }
    verifyTable.verify(map);
  }
  @Test
  public void testMultipleKeysMultipleValue() throws Exception {
    random = new Random(9332);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false,LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    int keyCount = 1000;
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable);
  }

  @Test
  public void testLargeAndExpand() throws Exception {
    random = new Random(21111);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(
            false,MODERATE_CAPACITY, LOAD_FACTOR, MODERATE_WB_SIZE);

    VerifyFastBytesHashMap verifyTable = new VerifyFastBytesHashMap();

    int keyCount = 1000;
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable);
  }
}
