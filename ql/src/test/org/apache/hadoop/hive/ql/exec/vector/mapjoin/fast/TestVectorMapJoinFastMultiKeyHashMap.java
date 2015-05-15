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

import java.util.Random;

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastMultiKeyHashMap;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestVectorMapJoinFastMultiKeyHashMap extends CommonFastHashTable {

  @Test
  public void testPutGetOne() throws Exception {
    random = new Random(47496);

    VectorMapJoinFastMultiKeyHashMap map =
        new VectorMapJoinFastMultiKeyHashMap(false, CAPACITY, LOAD_FACTOR, WB_SIZE);

    RandomByteArrayStream randomByteArrayKeyStream = new RandomByteArrayStream(random);
    RandomByteArrayStream randomByteArrayValueStream = new RandomByteArrayStream(random);

    byte[] key = randomByteArrayKeyStream.next();
    byte[] value = randomByteArrayValueStream.next();
    map.putRow(key, value);
    verifyHashMapResult(map, key, randomByteArrayValueStream.get(0));

    key = randomByteArrayKeyStream.next();
    value = randomByteArrayValueStream.next();
    map.putRow(key, value);
    verifyHashMapResult(map, key, randomByteArrayValueStream.get(1));
  }

  @Test
  public void testPutGetMultiple() throws Exception {
    random = new Random(2990);

    VectorMapJoinFastMultiKeyHashMap map = new VectorMapJoinFastMultiKeyHashMap(false, CAPACITY, LOAD_FACTOR, WB_SIZE);

    RandomByteArrayStream randomByteArrayKeyStream = new RandomByteArrayStream(random);
    RandomByteArrayStream randomByteArrayValueStream = new RandomByteArrayStream(random);

    byte[] key = randomByteArrayKeyStream.next();
    byte[] value = randomByteArrayValueStream.next();
    map.putRow(key, value);
    verifyHashMapResult(map, key, value);

    // Same key, multiple values.
    for (int i = 0; i < 3; ++i) {
      value = randomByteArrayValueStream.next();
      map.putRow(key, value);
      verifyHashMapResult(map, key, randomByteArrayValueStream);
    }
  }

  @Test
  public void testGetNonExistent() throws Exception {
    random = new Random(16916);

    VectorMapJoinFastMultiKeyHashMap map = new VectorMapJoinFastMultiKeyHashMap(false, CAPACITY, LOAD_FACTOR, WB_SIZE);

    RandomByteArrayStream randomByteArrayKeyStream = new RandomByteArrayStream(random);
    RandomByteArrayStream randomByteArrayValueStream = new RandomByteArrayStream(random);

    byte[] key = randomByteArrayKeyStream.next();
    byte[] value = randomByteArrayValueStream.next();
    map.putRow(key, value);

    key[0] = (byte) (key[0] + 1);
    map.putRow(key, value);

    key[0] = (byte) (key[0] + 1);
    VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
    JoinUtil.JoinResult joinResult = map.lookup(key, 0, key.length, hashMapResult);
    assertTrue(joinResult == JoinUtil.JoinResult.NOMATCH);
    assertTrue(!hashMapResult.hasRows());
  }

  @Test
  public void testPutWithFullMap() throws Exception {
    random = new Random(26078);

    // Make sure the map does not expand; should be able to find space.
    VectorMapJoinFastMultiKeyHashMap map = new VectorMapJoinFastMultiKeyHashMap(false, CAPACITY, 1f, WB_SIZE);

    RandomByteArrayStream randomByteArrayKeyStream = new RandomByteArrayStream(random);
    RandomByteArrayStream randomByteArrayValueStream = new RandomByteArrayStream(random);
    for (int i = 0; i < CAPACITY; ++i) {
      byte[] key = randomByteArrayKeyStream.next();
      byte[] value = randomByteArrayValueStream.next();
      map.putRow(key, value);
    }
    for (int i = 0; i < randomByteArrayKeyStream.size(); ++i) {
      verifyHashMapResult(map, randomByteArrayKeyStream.get(i), randomByteArrayValueStream.get(i));
    }
    // assertEquals(CAPACITY, map.getCapacity());
    // Get of non-existent key should terminate..
    byte[] anotherKey = randomByteArrayKeyStream.next();
    VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
    JoinUtil.JoinResult joinResult = map.lookup(anotherKey, 0, anotherKey.length, hashMapResult);
    assertTrue(joinResult == JoinUtil.JoinResult.NOMATCH);
  }

  @Test
  public void testExpand() throws Exception {
    random = new Random(22470);

    // Start with capacity 1; make sure we expand on every put.
    VectorMapJoinFastMultiKeyHashMap map = new VectorMapJoinFastMultiKeyHashMap(false, 1, 0.0000001f, WB_SIZE);

    RandomByteArrayStream randomByteArrayKeyStream = new RandomByteArrayStream(random);
    RandomByteArrayStream randomByteArrayValueStream = new RandomByteArrayStream(random);

    for (int i = 0; i < 18; ++i) {
      byte[] key = randomByteArrayKeyStream.next();
      byte[] value = randomByteArrayValueStream.next();
      map.putRow(key, value);
      for (int j = 0; j <= i; ++j) {
        verifyHashMapResult(map, randomByteArrayKeyStream.get(j), randomByteArrayValueStream.get(j));
      }
    }
    // assertEquals(1 << 18, map.getCapacity());
  }

  @Test
  public void testLarge() throws Exception {
    random = new Random(5231);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMap map = new VectorMapJoinFastMultiKeyHashMap(false, LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE);

    RandomByteArrayStream randomByteArrayKeyStream = new RandomByteArrayStream(random, 10);

    final int largeSize = 1000;
    RandomByteArrayStream[] randomByteArrayValueStreams = new RandomByteArrayStream[largeSize];
    for (int i = 0; i < largeSize; i++) {
      randomByteArrayValueStreams[i] = new RandomByteArrayStream(random);
      int count = generateLargeCount();
      byte[] key = randomByteArrayKeyStream.next();
      VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
      JoinUtil.JoinResult joinResult = map.lookup(key, 0, key.length, hashMapResult);
      if (joinResult == JoinUtil.JoinResult.MATCH) {
        // A problem or need different random seed / longer key?
        assertTrue(false);
      }
      for (int v = 0; v < count; v++) {
        byte[] value = randomByteArrayValueStreams[i].next();
        map.putRow(key, value);
      }
    }
    for (int i = 0; i < largeSize; i++) {
      verifyHashMapResult(map, randomByteArrayKeyStream.get(i), randomByteArrayValueStreams[i]);
    }
  }

  @Test
  public void testLargeAndExpand() throws Exception {
    random = new Random(46809);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMap map = new VectorMapJoinFastMultiKeyHashMap(false, MODERATE_CAPACITY, LOAD_FACTOR, MODERATE_WB_SIZE);

    RandomByteArrayStream randomByteArrayKeyStream = new RandomByteArrayStream(random, 10);

    final int largeSize = 1000;
    RandomByteArrayStream[] randomByteArrayValueStreams = new RandomByteArrayStream[largeSize];
    for (int i = 0; i < largeSize; i++) {
      randomByteArrayValueStreams[i] = new RandomByteArrayStream(random);
      int count = generateLargeCount();
      byte[] key = randomByteArrayKeyStream.next();
      VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
      JoinUtil.JoinResult joinResult = map.lookup(key, 0, key.length, hashMapResult);
      if (joinResult == JoinUtil.JoinResult.MATCH) {
        // A problem or need different random seed / longer key?
        assertTrue(false);
      }
      for (int v = 0; v < count; v++) {
        byte[] value = randomByteArrayValueStreams[i].next();
        map.putRow(key, value);
      }
    }
    for (int i = 0; i < largeSize; i++) {
      verifyHashMapResult(map, randomByteArrayKeyStream.get(i), randomByteArrayValueStreams[i]);
    }
  }

  private void verifyHashMapResult(VectorMapJoinFastMultiKeyHashMap map, byte[] key,
          RandomByteArrayStream randomByteArrayValueStream) {

    VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
    JoinUtil.JoinResult joinResult = map.lookup(key, 0, key.length, hashMapResult);
    if (joinResult != JoinUtil.JoinResult.MATCH) {
      assertTrue(false);
    }

    CommonFastHashTable.verifyHashMapResult(hashMapResult, randomByteArrayValueStream);
  }

  private void verifyHashMapResult(VectorMapJoinFastMultiKeyHashMap map, byte[] key,
      byte[] valueBytes) {

    VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
    JoinUtil.JoinResult joinResult = map.lookup(key, 0, key.length, hashMapResult);
    if (joinResult != JoinUtil.JoinResult.MATCH) {
      assertTrue(false);
    }

    CommonFastHashTable.verifyHashMapResult(hashMapResult, valueBytes);
  }

}
