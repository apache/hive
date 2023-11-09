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

import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.HashCodeUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestVectorMapJoinFastHashMapContainerNonMatched {
  private static final int numHashTable = 2;
  private static final int initialCapacity = 8;
  private static final float loadFactor = 0.9f;
  private static final int writeBufferSize = 1024 * 1024;
  private static final int estimatedKeyCount = -1;

  private BytesWritable serializeLong(long value, Properties properties) throws Exception {
    BinarySortableSerializeWrite serializeWrite = BinarySortableSerializeWrite.with(properties, 1);
    ByteStream.Output output = new ByteStream.Output();
    serializeWrite.set(output);
    serializeWrite.writeLong(value);

    BytesWritable writable = new BytesWritable();
    writable.set(output.getData(), 0, output.getLength());

    return writable;
  }

  private void addToHashMap(VectorMapJoinFastLongHashMapContainer hashMap, long value, Properties properties)
      throws Exception {
    BytesWritable keyWritable = serializeLong(value, properties);
    BytesWritable valueWritable = new BytesWritable(keyWritable.copyBytes());
    hashMap.putRow(HashCodeUtil.calculateLongHashCode(value), keyWritable, valueWritable);
  }

  private long getHashCode(String key) {
    Text keyWritable = new Text(key);
    return HashCodeUtil.murmurHash(keyWritable.getBytes(), 0, keyWritable.getLength());
  }

  private BytesWritable serializeString(String value, Properties properties) throws Exception {
    BinarySortableSerializeWrite serializeWrite = BinarySortableSerializeWrite.with(properties, 1);
    ByteStream.Output output = new ByteStream.Output();
    serializeWrite.set(output);

    Text text = new Text(value);
    serializeWrite.writeString(text.getBytes(), 0, text.getLength());

    BytesWritable writable = new BytesWritable();
    writable.set(output.getData(), 0, output.getLength());

    return writable;
  }

  private void addToHashMap(
      VectorMapJoinFastStringHashMapContainer hashMap, String value, Properties properties) throws Exception {
    BytesWritable keyWritable = serializeString(value, properties);
    BytesWritable valueWritable = new BytesWritable(keyWritable.copyBytes());
    hashMap.putRow(getHashCode(value), keyWritable, valueWritable);
  }

  private BytesWritable createRandomMultiKey(Random random, BinarySortableSerializeWrite serializeWrite)
      throws Exception {
    ByteStream.Output output = new ByteStream.Output();
    serializeWrite.set(output);

    serializeWrite.writeLong(random.nextLong());
    serializeWrite.writeLong(random.nextLong());

    BytesWritable writable = new BytesWritable();
    writable.set(output.getData(), 0, output.getLength());

    return writable;
  }

  private long getHashCode(BytesWritable key) {
    return HashCodeUtil.murmurHash(key.getBytes(), 0, key.getLength());
  }

  private void addToHashMap(
      VectorMapJoinFastMultiKeyHashMapContainer hashMap, BytesWritable key) throws Exception {
    BytesWritable value = new BytesWritable(key.copyBytes());
    hashMap.putRow(getHashCode(key), key, value);
  }

  @Test
  public void testLongHashMapContainer() throws Exception {
    Random random = new Random();
    long keyA = random.nextLong();
    while ((HashCodeUtil.calculateLongHashCode(keyA) & (initialCapacity - 1)) != 0) {
      keyA = random.nextLong();
    }

    long keyB = random.nextLong();
    while ((HashCodeUtil.calculateLongHashCode(keyB) & (initialCapacity - 1)) != 0 || keyB == keyA) {
      keyB = random.nextLong();
    }

    long keyC = random.nextLong();
    while ((HashCodeUtil.calculateLongHashCode(keyC) & (initialCapacity - 1)) != 1) {
      keyC = random.nextLong();
    }

    TableDesc tableDesc = new TableDesc();
    Properties properties = new Properties();
    tableDesc.setProperties(properties);

    VectorMapJoinFastLongHashMapContainer hashMapContainer =
        new VectorMapJoinFastLongHashMapContainer(
            true, /* isFullOuter */
            false, /* minMaxEnabled */
            VectorMapJoinDesc.HashTableKeyType.LONG,
            initialCapacity,
            loadFactor,
            writeBufferSize,
            estimatedKeyCount,
            tableDesc,
            numHashTable);

    addToHashMap(hashMapContainer, keyA, properties);
    addToHashMap(hashMapContainer, keyB, properties);
    addToHashMap(hashMapContainer, keyC, properties);

    MatchTracker matchTracker = hashMapContainer.createMatchTracker();
    VectorMapJoinHashMapResult hashMapResult = hashMapContainer.createHashMapResult();

    hashMapContainer.lookup(keyB, hashMapResult, matchTracker);

    VectorMapJoinNonMatchedIterator nonMatchedIterator =
        hashMapContainer.createNonMatchedIterator(matchTracker);
    nonMatchedIterator.init();

    ArrayList<Long> nonMatchedList = new ArrayList<Long>();
    while (nonMatchedIterator.findNextNonMatched()) {
      boolean isNull = !nonMatchedIterator.readNonMatchedLongKey();
      assertFalse(isNull);

      long key = nonMatchedIterator.getNonMatchedLongKey();
      nonMatchedList.add(key);
    }

    assertEquals(2, nonMatchedList.size());
    assertTrue(nonMatchedList.contains(keyA));
    assertTrue(nonMatchedList.contains(keyC));
  }

  @Test
  public void testStringHashMapContainer() throws Exception {
    Random random = new Random();

    String keyA = VectorRandomRowSource.getRandString(random, 5, false);
    while ((getHashCode(keyA) & (initialCapacity - 1)) != 0) {
      keyA = VectorRandomRowSource.getRandString(random, 5, false);
    }

    String keyB = VectorRandomRowSource.getRandString(random, 5, false);
    while ((getHashCode(keyB) & (initialCapacity - 1)) != 0 || keyB.equals(keyA)) {
      keyB = VectorRandomRowSource.getRandString(random, 5, false);
    }

    String keyC = VectorRandomRowSource.getRandString(random, 5, false);
    while ((getHashCode(keyC) & (initialCapacity - 1)) != 1) {
      keyC = VectorRandomRowSource.getRandString(random, 5, false);
    }

    TableDesc tableDesc = new TableDesc();
    Properties properties = new Properties();
    tableDesc.setProperties(properties);

    VectorMapJoinFastStringHashMapContainer hashMapContainer =
        new VectorMapJoinFastStringHashMapContainer(
            true, /* isFullOuter */
            initialCapacity,
            loadFactor,
            writeBufferSize,
            estimatedKeyCount,
            tableDesc,
            numHashTable);

    addToHashMap(hashMapContainer, keyA, properties);
    addToHashMap(hashMapContainer, keyB, properties);
    addToHashMap(hashMapContainer, keyC, properties);

    MatchTracker matchTracker = hashMapContainer.createMatchTracker();
    VectorMapJoinHashMapResult hashMapResult = hashMapContainer.createHashMapResult();

    Text keyTextB = new Text(keyB);
    hashMapContainer.lookup(keyTextB.getBytes(), 0, keyTextB.getLength(), hashMapResult, matchTracker);

    VectorMapJoinNonMatchedIterator nonMatchedIterator =
        hashMapContainer.createNonMatchedIterator(matchTracker);
    nonMatchedIterator.init();

    ArrayList<String> nonMatchedList = new ArrayList<String>();
    while (nonMatchedIterator.findNextNonMatched()) {
      boolean isNull = !nonMatchedIterator.readNonMatchedBytesKey();
      assertFalse(isNull);

      byte[] keyBytes = nonMatchedIterator.getNonMatchedBytes();
      int keyOffset = nonMatchedIterator.getNonMatchedBytesOffset();
      int keyLength = nonMatchedIterator.getNonMatchedBytesLength();

      byte[] array = new byte[keyLength];
      System.arraycopy(keyBytes, keyOffset, array, 0, keyLength);
      Text key = new Text(array);

      nonMatchedList.add(key.toString());
    }

    assertEquals(2, nonMatchedList.size());
    assertTrue(nonMatchedList.contains(keyA));
    assertTrue(nonMatchedList.contains(keyC));
  }

  @Test
  public void testMultiKeyHashMapContainer() throws Exception {
    Random random = new Random();
    BinarySortableSerializeWrite serializeWrite =
        BinarySortableSerializeWrite.with(new Properties(), 2);

    BytesWritable keyA = createRandomMultiKey(random, serializeWrite);
    while ((getHashCode(keyA) & (initialCapacity - 1)) != 0) {
      keyA = createRandomMultiKey(random, serializeWrite);
    }

    BytesWritable keyB = createRandomMultiKey(random, serializeWrite);
    while ((getHashCode(keyB) & (initialCapacity - 1)) != 0 || keyB == keyA) {
      keyB = createRandomMultiKey(random, serializeWrite);
    }

    BytesWritable keyC = createRandomMultiKey(random, serializeWrite);
    while ((getHashCode(keyC) & (initialCapacity - 1)) != 1) {
      keyC = createRandomMultiKey(random, serializeWrite);
    }

    VectorMapJoinFastMultiKeyHashMapContainer hashMapContainer =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            true, /* isFullOuter */
            initialCapacity,
            loadFactor,
            writeBufferSize,
            estimatedKeyCount,
            numHashTable);

    addToHashMap(hashMapContainer, keyA);
    addToHashMap(hashMapContainer, keyB);
    addToHashMap(hashMapContainer, keyC);

    MatchTracker matchTracker = hashMapContainer.createMatchTracker();
    VectorMapJoinHashMapResult hashMapResult = hashMapContainer.createHashMapResult();

    hashMapContainer.lookup(keyB.getBytes(), 0, keyB.getLength(), hashMapResult, matchTracker);

    VectorMapJoinNonMatchedIterator nonMatchedIterator =
        hashMapContainer.createNonMatchedIterator(matchTracker);
    nonMatchedIterator.init();

    ArrayList<byte[]> nonMatchedList = new ArrayList<byte[]>();
    while (nonMatchedIterator.findNextNonMatched()) {
      boolean isNull = !nonMatchedIterator.readNonMatchedBytesKey();
      assertFalse(isNull);

      byte[] keyBytes = nonMatchedIterator.getNonMatchedBytes();
      int keyOffset = nonMatchedIterator.getNonMatchedBytesOffset();
      int keyLength = nonMatchedIterator.getNonMatchedBytesLength();

      byte[] array = new byte[keyLength];
      System.arraycopy(keyBytes, keyOffset, array, 0, keyLength);

      nonMatchedList.add(array);
    }

    final BytesWritable finalKeyA = keyA;
    final BytesWritable finalKeyC = keyC;

    assertEquals(2, nonMatchedList.size());
    assertTrue(nonMatchedList.stream().anyMatch(arr -> {
      return Arrays.equals(arr, finalKeyA.copyBytes());
    }));
    assertTrue(nonMatchedList.stream().anyMatch(arr -> {
      return Arrays.equals(arr, finalKeyC.copyBytes());
    }));  }
}

