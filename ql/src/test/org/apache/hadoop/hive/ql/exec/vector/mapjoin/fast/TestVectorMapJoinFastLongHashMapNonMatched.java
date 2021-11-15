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
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.CheckFastHashTable.VerifyFastLongHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastLongHashMap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestVectorMapJoinFastLongHashMapNonMatched extends CommonFastHashTable {

  public static final Properties ANY_TABLE_PROPERTIES = new Properties();
  private static TableDesc tableDesc = new TableDesc();

  @Before
  public void setUp() throws Exception {
    tableDesc.setProperties(ANY_TABLE_PROPERTIES);
  }

  @Test
  public void testOneKey() throws Exception {
    random = new Random(33221);

    VectorMapJoinFastLongHashMap map =
        new VectorMapJoinFastLongHashMap(
            false, false, HashTableKeyType.LONG, CAPACITY, LOAD_FACTOR, WB_SIZE, -1, tableDesc);

    VerifyFastLongHashMap verifyTable = new VerifyFastLongHashMap();

    long key = random.nextLong();
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
    random = new Random(900);

    VectorMapJoinFastLongHashMap map =
        new VectorMapJoinFastLongHashMap(
            false, false, HashTableKeyType.LONG, CAPACITY, LOAD_FACTOR, WB_SIZE, -1, tableDesc);

    VerifyFastLongHashMap verifyTable = new VerifyFastLongHashMap();

    int keyCount = 100 + random.nextInt(1000);
    for (int i = 0; i < keyCount; i++) {
      long key;
      while (true) {
        key = random.nextLong();
        if (!verifyTable.contains(key)) {
          // Unique keys for this test.
          break;
        }
      }
      byte[] value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
      random.nextBytes(value);

      map.testPutRow(key, value);
      verifyTable.add(key, value);
    }

    verifyTable.verifyNonMatched(map, random);
  }

  @Test
  public void testExpand() throws Exception {
    random = new Random(5227);

    // Start with capacity 1; make sure we expand on every put.
    VectorMapJoinFastLongHashMap map =
        new VectorMapJoinFastLongHashMap(
            false, false, HashTableKeyType.LONG, 1, 0.0000001f, WB_SIZE, -1, tableDesc);

    VerifyFastLongHashMap verifyTable = new VerifyFastLongHashMap();

    for (int i = 0; i < 6; ++i) {
      long key;
      while (true) {
        key = random.nextLong();
        if (!verifyTable.contains(key)) {
          // Unique keys for this test.
          break;
        }
      }
      byte[] value = new byte[random.nextInt(MAX_VALUE_LENGTH)];
      random.nextBytes(value);

      map.testPutRow(key, value);
      verifyTable.add(key, value);
    }

    verifyTable.verifyNonMatched(map, random);
    // assertEquals(1 << 18, map.getCapacity());
  }

  public void addAndVerifyMultipleKeyMultipleValue(int keyCount,
      VectorMapJoinFastLongHashMap map, VerifyFastLongHashMap verifyTable)
          throws HiveException, IOException {
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable, -1);
  }

  public void addAndVerifyMultipleKeyMultipleValue(int keyCount,
      VectorMapJoinFastLongHashMap map, VerifyFastLongHashMap verifyTable, int fixedValueLength)
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
        long key;
        while (true) {
          key = random.nextLong();
          if (!verifyTable.contains(key)) {
            // Unique keys for this test.
            break;
          }
        }

        map.testPutRow(key, value);
        verifyTable.add(key, value);
      } else {
        long randomExistingKey = verifyTable.addRandomExisting(value, random);
        map.testPutRow(randomExistingKey, value);
      }
    }

    verifyTable.verifyNonMatched(map, random);
  }

  @Test
  public void testMultipleKeysMultipleValue() throws Exception {
    random = new Random(8);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMap map =
        new VectorMapJoinFastLongHashMap(
            false, false, HashTableKeyType.LONG, LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc);

    VerifyFastLongHashMap verifyTable = new VerifyFastLongHashMap();

    int keyCount = 100;
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable);
  }

  @Test
  public void testLargeAndExpand() throws Exception {
    random = new Random(20);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMap map =
        new VectorMapJoinFastLongHashMap(
            false, false, HashTableKeyType.LONG, MODERATE_CAPACITY, LOAD_FACTOR, MODERATE_WB_SIZE, -1, tableDesc);

    VerifyFastLongHashMap verifyTable = new VerifyFastLongHashMap();

    int keyCount = 100;
    addAndVerifyMultipleKeyMultipleValue(keyCount, map, verifyTable);
  }
}
