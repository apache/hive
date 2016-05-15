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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.CheckFastHashTable.VerifyFastBytesHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

public class TestVectorMapJoinFastBytesHashMultiSet extends CommonFastHashTable {

  @Test
  public void testOneKey() throws Exception {
    random = new Random(5255);

    VectorMapJoinFastMultiKeyHashMultiSet map =
        new VectorMapJoinFastMultiKeyHashMultiSet(
            false,CAPACITY, LOAD_FACTOR, WB_SIZE);

    VerifyFastBytesHashMultiSet verifyTable = new VerifyFastBytesHashMultiSet();

    byte[] key = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key);

    map.testPutRow(key);
    verifyTable.add(key);
    verifyTable.verify(map);

    // Second time.
    map.testPutRow(key);
    verifyTable.add(key);
    verifyTable.verify(map);

    // Third time.
    map.testPutRow(key);
    verifyTable.add(key);
    verifyTable.verify(map);
  }

  @Test
  public void testMultipleKeysSingleValue() throws Exception {
    random = new Random(2374);

    VectorMapJoinFastMultiKeyHashMultiSet map =
        new VectorMapJoinFastMultiKeyHashMultiSet(
            false,CAPACITY, LOAD_FACTOR, WB_SIZE);

    VerifyFastBytesHashMultiSet verifyTable = new VerifyFastBytesHashMultiSet();

    int keyCount = 100 + random.nextInt(1000);
    for (int i = 0; i < keyCount; i++) {
      byte[] key = new byte[random.nextInt(MAX_KEY_LENGTH)];
      random.nextBytes(key);
      if (!verifyTable.contains(key)) {
        // Unique keys for this test.
        break;
      }

      map.testPutRow(key);
      verifyTable.add(key);
      // verifyTable.verify(map);
    }
    verifyTable.verify(map);
  }

  @Test
  public void testGetNonExistent() throws Exception {
    random = new Random(98222);

    VectorMapJoinFastMultiKeyHashMultiSet map =
        new VectorMapJoinFastMultiKeyHashMultiSet(
            false,CAPACITY, LOAD_FACTOR, WB_SIZE);

    VerifyFastBytesHashMultiSet verifyTable = new VerifyFastBytesHashMultiSet();

    byte[] key1 = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key1);

    map.testPutRow(key1);
    verifyTable.add(key1);
    verifyTable.verify(map);

    byte[] key2 = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key2);
    VectorMapJoinHashMultiSetResult hashMultiSetResult = map.createHashMultiSetResult();
    JoinUtil.JoinResult joinResult = map.contains(key2, 0, key2.length, hashMultiSetResult);
    assertTrue(joinResult == JoinUtil.JoinResult.NOMATCH);

    map.testPutRow(key2);
    verifyTable.add(key2);
    verifyTable.verify(map);

    byte[] key3 = new byte[random.nextInt(MAX_KEY_LENGTH)];
    random.nextBytes(key3);
    hashMultiSetResult = map.createHashMultiSetResult();
    joinResult = map.contains(key3, 0, key3.length, hashMultiSetResult);
    assertTrue(joinResult == JoinUtil.JoinResult.NOMATCH);
    assertEquals(hashMultiSetResult.count(), 0);
  }

  @Test
  public void testFullMap() throws Exception {
    random = new Random(9024);

    // Make sure the map does not expand; should be able to find space.
    VectorMapJoinFastMultiKeyHashMultiSet map =
        new VectorMapJoinFastMultiKeyHashMultiSet(false,CAPACITY, 1f, WB_SIZE);

    VerifyFastBytesHashMultiSet verifyTable = new VerifyFastBytesHashMultiSet();

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

      map.testPutRow(key);
      verifyTable.add(key);
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

    VectorMapJoinHashMultiSetResult hashMultiSetResult = map.createHashMultiSetResult();
    JoinUtil.JoinResult joinResult = map.contains(anotherKey, 0, anotherKey.length, hashMultiSetResult);
    assertTrue(joinResult == JoinUtil.JoinResult.NOMATCH);
  }

  @Test
  public void testExpand() throws Exception {
    random = new Random(2933);

    // Start with capacity 1; make sure we expand on every put.
    VectorMapJoinFastMultiKeyHashMultiSet map =
        new VectorMapJoinFastMultiKeyHashMultiSet(false,1, 0.0000001f, WB_SIZE);

    VerifyFastBytesHashMultiSet verifyTable = new VerifyFastBytesHashMultiSet();

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

      map.testPutRow(key);
      verifyTable.add(key);
      // verifyTable.verify(map);
    }
    verifyTable.verify(map);
    // assertEquals(1 << 18, map.getCapacity());
  }

  public void addAndVerifyMultipleKeyMultipleValue(int keyCount,
      VectorMapJoinFastMultiKeyHashMultiSet map, VerifyFastBytesHashMultiSet verifyTable)
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

        map.testPutRow(key);
        verifyTable.add(key);
        // verifyTable.verify(map);
      } else {
        byte[] randomExistingKey = verifyTable.addRandomExisting(value, random);
        map.testPutRow(randomExistingKey);
        // verifyTable.verify(map);
      }
    }
    verifyTable.verify(map);
  }
  @Test
  public void testMultipleKeysMultipleValue() throws Exception {
    random = new Random(5445);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMultiSet map =
        new VectorMapJoinFastMultiKeyHashMultiSet(
            false,LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE);

    VerifyFastBytesHashMultiSet verifyTable = new VerifyFastBytesHashMultiSet();

    int keyCount = 1000;
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable);
  }

  @Test
  public void testLargeAndExpand() throws Exception {
    random = new Random(5637);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMultiSet map =
        new VectorMapJoinFastMultiKeyHashMultiSet(
            false,MODERATE_CAPACITY, LOAD_FACTOR, MODERATE_WB_SIZE);

    VerifyFastBytesHashMultiSet verifyTable = new VerifyFastBytesHashMultiSet();

    int keyCount = 1000;
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable);
  }
}
