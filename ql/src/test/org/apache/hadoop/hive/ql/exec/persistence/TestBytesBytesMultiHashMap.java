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
package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestBytesBytesMultiHashMap {
  private static final float LOAD_FACTOR = 0.75f;
  private static final int CAPACITY = 8;
  private static final int WB_SIZE = 128; // Make sure we cross some buffer boundaries...

  @Test
  public void testCapacityValidation() {
    BytesBytesMultiHashMap map = new BytesBytesMultiHashMap(CAPACITY, LOAD_FACTOR, WB_SIZE);
    assertEquals(CAPACITY, map.getCapacity());
    map = new BytesBytesMultiHashMap(9, LOAD_FACTOR, WB_SIZE);
    assertEquals(16, map.getCapacity());
  }

  @Test
  public void testPutGetOne() throws Exception {
    BytesBytesMultiHashMap map = new BytesBytesMultiHashMap(CAPACITY, LOAD_FACTOR, WB_SIZE);
    RandomKvSource kv = new RandomKvSource(0, 0);
    map.put(kv, -1);
    verifyHashMapResult(map, kv.getLastKey(), kv.getLastValue());
    kv = new RandomKvSource(10, 100);
    map.put(kv, -1);
    verifyHashMapResult(map, kv.getLastKey(), kv.getLastValue());
  }

  @Test
  public void testPutGetMultiple() throws Exception {
    BytesBytesMultiHashMap map = new BytesBytesMultiHashMap(CAPACITY, LOAD_FACTOR, WB_SIZE);
    RandomKvSource kv = new RandomKvSource(0, 100);
    map.put(kv, -1);
    verifyHashMapResult(map, kv.getLastKey(), kv.getLastValue());
    FixedKeyKvSource kv2 = new FixedKeyKvSource(kv.getLastKey(), 0, 100);
    kv2.values.add(kv.getLastValue());
    for (int i = 0; i < 3; ++i) {
      map.put(kv2, -1);
      verifyHashMapResult(map, kv2.key, kv2.values.toArray(new byte[kv2.values.size()][]));
    }
  }

  @Test
  public void testGetNonExistent() throws Exception {
    BytesBytesMultiHashMap map = new BytesBytesMultiHashMap(CAPACITY, LOAD_FACTOR, WB_SIZE);
    RandomKvSource kv = new RandomKvSource(1, 100);
    map.put(kv, -1);
    byte[] key = kv.getLastKey();
    key[0] = (byte)(key[0] + 1);
    FixedKeyKvSource kv2 = new FixedKeyKvSource(kv.getLastKey(), 0, 100);
    map.put(kv2, -1);
    key[0] = (byte)(key[0] + 1);
    BytesBytesMultiHashMap.Result hashMapResult = new BytesBytesMultiHashMap.Result();
    map.getValueResult(key, 0, key.length, hashMapResult);
    assertTrue(!hashMapResult.hasRows());
    map.getValueResult(key, 0, 0, hashMapResult);
    assertTrue(!hashMapResult.hasRows());
  }

  @Test
  public void testPutWithFullMap() throws Exception {
    // Make sure the map does not expand; should be able to find space.
    BytesBytesMultiHashMap map = new BytesBytesMultiHashMap(CAPACITY, 1f, WB_SIZE);
    UniqueKeysKvSource kv = new UniqueKeysKvSource();
    for (int i = 0; i < CAPACITY; ++i) {
      map.put(kv, -1);
    }
    for (int i = 0; i < kv.keys.size(); ++i) {
      verifyHashMapResult(map, kv.keys.get(i), kv.values.get(i));
    }
    assertEquals(CAPACITY, map.getCapacity());
    // Get of non-existent key should terminate..
    BytesBytesMultiHashMap.Result hashMapResult = new BytesBytesMultiHashMap.Result();
    map.getValueResult(new byte[0], 0, 0, hashMapResult);
  }

  @Test
  public void testExpand() throws Exception {
    // Start with capacity 1; make sure we expand on every put.
    BytesBytesMultiHashMap map = new BytesBytesMultiHashMap(1, 0.0000001f, WB_SIZE);
    UniqueKeysKvSource kv = new UniqueKeysKvSource();
    for (int i = 0; i < 18; ++i) {
      map.put(kv, -1);
      for (int j = 0; j <= i; ++j) {
        verifyHashMapResult(map, kv.keys.get(j), kv.values.get(j));
      }
    }
    assertEquals(1 << 18, map.getCapacity());
  }

  private void verifyHashMapResult(BytesBytesMultiHashMap map, byte[] key, byte[]... values) {
    BytesBytesMultiHashMap.Result hashMapResult = new BytesBytesMultiHashMap.Result();
    byte state = map.getValueResult(key, 0, key.length, hashMapResult);
    HashSet<ByteBuffer> hs = new HashSet<ByteBuffer>();
    int count = 0;
    if (hashMapResult.hasRows()) {
      WriteBuffers.ByteSegmentRef ref = hashMapResult.first();
      while (ref != null) {
        count++;
        hs.add(ref.copy());
        ref = hashMapResult.next();
      }
    } else {
      assertTrue(hashMapResult.isEof());
    }
    assertEquals(state, count);
    assertEquals(values.length, count);
    for (int i = 0; i < values.length; ++i) {
      assertTrue(hs.contains(ByteBuffer.wrap(values[i])));
    }
  }

  private static class FixedKeyKvSource extends RandomKvSource {
    private byte[] key;

    public FixedKeyKvSource(byte[] key, int minLength, int maxLength) {
      super(minLength, maxLength);
      this.key = key;
    }

    @Override
    public void writeKey(RandomAccessOutput dest) throws SerDeException {
      try {
        dest.write(key);
      } catch (IOException e) {
        e.printStackTrace();
        fail("Thrown " + e.getMessage());
      }
    }
  }

  private static class UniqueKeysKvSource extends RandomKvSource {
    private long lastKey = -1;
    private byte[] buffer = new byte[9];
    private byte[] lastBuffer;

    public UniqueKeysKvSource() {
      super(0, 0);
    }

    @Override
    public void writeKey(RandomAccessOutput dest) throws SerDeException {
      lastKey += 465623573;
      int len = LazyBinaryUtils.writeVLongToByteArray(buffer, lastKey);
      lastBuffer = Arrays.copyOf(buffer, len);
      keys.add(lastBuffer);
      writeLastBuffer(dest);
    }

    private void writeLastBuffer(RandomAccessOutput dest) {
      try {
        dest.write(lastBuffer);
      } catch (IOException e) {
        e.printStackTrace();
        fail("Thrown " + e.getMessage());
      }
    }

    @Override
    public void writeValue(RandomAccessOutput dest) throws SerDeException {
      // Assumes value is written after key.
      values.add(lastBuffer);
      writeLastBuffer(dest);
    }
  }

  private static class RandomKvSource implements BytesBytesMultiHashMap.KvSource {
    private int minLength, maxLength;
    private final Random rdm = new Random(43);
    public List<byte[]> keys = new ArrayList<byte[]>(), values = new ArrayList<byte[]>();

    public RandomKvSource(int minLength, int maxLength) {
      this.minLength = minLength;
      this.maxLength = maxLength;
    }

    public byte[] getLastValue() {
      return values.get(values.size() - 1);
    }

    public byte[] getLastKey() {
      return keys.get(keys.size() - 1);
    }

    @Override
    public void writeKey(RandomAccessOutput dest) throws SerDeException {
      keys.add(write(dest));
    }

    @Override
    public void writeValue(RandomAccessOutput dest) throws SerDeException {
      values.add(write(dest));
    }

    protected byte[] write(RandomAccessOutput dest) {
      byte[] bytes = new byte[minLength + rdm.nextInt(maxLength - minLength + 1)];
      rdm.nextBytes(bytes);
      try {
        dest.write(bytes);
      } catch (IOException e) {
        e.printStackTrace();
        fail("Thrown " + e.getMessage());
      }
      return bytes;
    }

    @Override
    public byte updateStateByte(Byte previousValue) {
      return (byte)(previousValue == null ? 1 : previousValue + 1);
    }
  }
}
