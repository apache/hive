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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.base.Preconditions;

import static org.junit.Assert.*;

public class CheckFastHashTable {

  public static boolean findMatch(int valueIndex, byte[] valueBytes, List<byte[]> actualValues,
      int actualCount, boolean[] actualTaken, int[] actualToValueMap) {
    for (int i = 0; i < actualCount; i++) {
      if (!actualTaken[i]) {
        byte[] actualBytes = actualValues.get(i);
        if (StringExpr.compare(valueBytes, 0, valueBytes.length, actualBytes, 0, actualBytes.length) == 0) {
          actualToValueMap[i] = valueIndex;
          actualTaken[i] = true;
          return true;
        }
      }
    }
    return false;
  }

  public static int[] verifyHashMapValues(VectorMapJoinHashMapResult hashMapResult,
      List<byte[]> values) {

    int valueCount = values.size();

    WriteBuffers.ByteSegmentRef ref = hashMapResult.first();

    // Read through all values.
    List<byte[]> actualValues = new ArrayList<byte[]>();
    while (true) {
      byte[] bytes = ref.getBytes();
      int offset = (int) ref.getOffset();
      int length = ref.getLength();

      if (length == 0) {
        actualValues.add(new byte[0]);
      } else {
        actualValues.add(Arrays.copyOfRange(bytes, offset, offset + length));
      }
      ref = hashMapResult.next();
      if (ref == null) {
        break;
      }
    }

    int actualCount = actualValues.size();

    if (valueCount != actualCount) {
      TestCase.fail("values.size() " + valueCount + " does not match actualCount " + actualCount);
    }

    boolean[] actualTaken = new boolean[actualCount];
    int[] actualToValueMap = new int[actualCount];

    for (int i = 0; i < actualCount; i++) {
      byte[] valueBytes = values.get(i);

      if (!findMatch(i, valueBytes, actualValues, actualCount, actualTaken, actualToValueMap)) {
        List<Integer> availableLengths = new ArrayList<Integer>();
        for (int a = 0; a < actualCount; a++) {
          if (!actualTaken[a]) {
            availableLengths.add(actualValues.get(a).length);
          }
        }
        TestCase.fail("No match for actual value (valueBytes length " + valueBytes.length +
            ", availableLengths " + availableLengths.toString() + " of " + actualCount + " total)");
      }
    }
    return actualToValueMap;
  }

  /*
   * Element for Key: Long x Hash Table: HashMap
   */
  public static class FastLongHashMapElement {
    private long key;
    private List<byte[]> values;

    public FastLongHashMapElement(long key, byte[] firstValue) {
      this.key = key;
      values = new ArrayList<byte[]>();
      values.add(firstValue);
    }

    public long getKey() {
      return key;
    }

    public int getValueCount() {
      return values.size();
    }

    public List<byte[]> getValues() {
      return values;
    }

    public void addValue(byte[] value) {
      values.add(value);
    }
  }

  /*
   * Verify table for Key: Long x Hash Table: HashMap
   */
  public static class VerifyFastLongHashMap {

    private int count;

    private FastLongHashMapElement[] array;

    private HashMap<Long, Integer> keyValueMap;

    public VerifyFastLongHashMap() {
      count = 0;
      array = new FastLongHashMapElement[50];
      keyValueMap = new HashMap<Long, Integer>();
    }

    public int getCount() {
      return count;
    }

    public boolean contains(long key) {
      return keyValueMap.containsKey(key);
    }

    public void add(long key, byte[] value) {
      if (keyValueMap.containsKey(key)) {
        int index = keyValueMap.get(key);
        array[index].addValue(value);
      } else {
        if (count >= array.length) {
          // Grow.
          FastLongHashMapElement[] newArray = new FastLongHashMapElement[array.length * 2];
          System.arraycopy(array, 0, newArray, 0, count);
          array = newArray;
        }
        array[count] = new FastLongHashMapElement(key, value);
        keyValueMap.put(key, count);
        count++;
      }
    }

    public long addRandomExisting(byte[] value, Random r) {
      Preconditions.checkState(count > 0);
      int index = r.nextInt(count);
      array[index].addValue(value);
      return array[index].getKey();
    }

    public long getKey(int index) {
      return array[index].getKey();
    }

    public List<byte[]> getValues(int index) {
      return array[index].getValues();
    }

    public void verify(VectorMapJoinFastLongHashMap map) {
      int mapSize = map.size();
      if (mapSize != count) {
        TestCase.fail("map.size() does not match expected count");
      }

      for (int index = 0; index < count; index++) {
        FastLongHashMapElement element = array[index];
        long key = element.getKey();
        List<byte[]> values = element.getValues();

        VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
        JoinUtil.JoinResult joinResult = map.lookup(key, hashMapResult);
        if (joinResult != JoinUtil.JoinResult.MATCH) {
          assertTrue(false);
        }

        verifyHashMapValues(hashMapResult, values);
      }
    }
  }

  /*
   * Element for Key: byte[] x Hash Table: HashMap
   */
  public static class FastBytesHashMapElement {
    private byte[] key;
    private List<byte[]> values;

    public FastBytesHashMapElement(byte[] key, byte[] firstValue) {
      this.key = key;
      values = new ArrayList<byte[]>();
      values.add(firstValue);
    }

    public byte[] getKey() {
      return key;
    }

    public int getValueCount() {
      return values.size();
    }

    public List<byte[]> getValues() {
      return values;
    }

    public void addValue(byte[] value) {
      values.add(value);
    }
  }

  /*
   * Verify table for Key: byte[] x Hash Table: HashMap
   */
  public static class VerifyFastBytesHashMap {

    private int count;

    private FastBytesHashMapElement[] array;

    private TreeMap<BytesWritable, Integer> keyValueMap;

    public VerifyFastBytesHashMap() {
      count = 0;
      array = new FastBytesHashMapElement[50];

      // We use BytesWritable because it supports Comparable for our TreeMap.
      keyValueMap = new TreeMap<BytesWritable, Integer>();
    }

    public int getCount() {
      return count;
    }

    public boolean contains(byte[] key) {
      BytesWritable keyBytesWritable = new BytesWritable(key, key.length);
      return keyValueMap.containsKey(keyBytesWritable);
    }

    public void add(byte[] key, byte[] value) {
      BytesWritable keyBytesWritable = new BytesWritable(key, key.length);
      if (keyValueMap.containsKey(keyBytesWritable)) {
        int index = keyValueMap.get(keyBytesWritable);
        array[index].addValue(value);
      } else {
        if (count >= array.length) {
          // Grow.
          FastBytesHashMapElement[] newArray = new FastBytesHashMapElement[array.length * 2];
          System.arraycopy(array, 0, newArray, 0, count);
          array = newArray;
        }
        array[count] = new FastBytesHashMapElement(key, value);
        keyValueMap.put(keyBytesWritable, count);
        count++;
      }
    }

    public byte[] addRandomExisting(byte[] value, Random r) {
      Preconditions.checkState(count > 0);
      int index = r.nextInt(count);
      array[index].addValue(value);
      return array[index].getKey();
    }

    public byte[] getKey(int index) {
      return array[index].getKey();
    }

    public List<byte[]> getValues(int index) {
      return array[index].getValues();
    }

    public void verify(VectorMapJoinFastBytesHashMap map) {
      int mapSize = map.size();
      if (mapSize != count) {
        TestCase.fail("map.size() does not match expected count");
      }

      for (int index = 0; index < count; index++) {
        FastBytesHashMapElement element = array[index];
        byte[] key = element.getKey();
        List<byte[]> values = element.getValues();

        VectorMapJoinHashMapResult hashMapResult = map.createHashMapResult();
        JoinUtil.JoinResult joinResult = map.lookup(key, 0, key.length, hashMapResult);
        if (joinResult != JoinUtil.JoinResult.MATCH) {
          assertTrue(false);
        }

        verifyHashMapValues(hashMapResult, values);
      }
    }
  }

  /*
   * Element for Key: Long x Hash Table: HashMultiSet
   */
  public static class FastLongHashMultiSetElement {
    private long key;
    private int multiSetCount;

    public FastLongHashMultiSetElement(long key) {
      this.key = key;
      multiSetCount = 1;
    }

    public long getKey() {
      return key;
    }

    public int getMultiSetCount() {
      return multiSetCount;
    }

    public void incrementMultiSetCount() {
      multiSetCount++;
    }
  }

  /*
   * Verify table for Key: Long x Hash Table: HashMultiSet
   */
  public static class VerifyFastLongHashMultiSet {

    private int count;

    private FastLongHashMultiSetElement[] array;

    private HashMap<Long, Integer> keyValueMap;

    public VerifyFastLongHashMultiSet() {
      count = 0;
      array = new FastLongHashMultiSetElement[50];
      keyValueMap = new HashMap<Long, Integer>();
    }

    public int getCount() {
      return count;
    }

    public boolean contains(long key) {
      return keyValueMap.containsKey(key);
    }

    public void add(long key) {
      if (keyValueMap.containsKey(key)) {
        int index = keyValueMap.get(key);
        array[index].incrementMultiSetCount();
      } else {
        if (count >= array.length) {
          // Grow.
          FastLongHashMultiSetElement[] newArray = new FastLongHashMultiSetElement[array.length * 2];
          System.arraycopy(array, 0, newArray, 0, count);
          array = newArray;
        }
        array[count] = new FastLongHashMultiSetElement(key);
        keyValueMap.put(key, count);
        count++;
      }
    }

    public long addRandomExisting(byte[] value, Random r) {
      Preconditions.checkState(count > 0);
      int index = r.nextInt(count);
      array[index].incrementMultiSetCount();
      return array[index].getKey();
    }

    public long getKey(int index) {
      return array[index].getKey();
    }

    public int getMultiSetCount(int index) {
      return array[index].getMultiSetCount();
    }

    public void verify(VectorMapJoinFastLongHashMultiSet map) {
      int mapSize = map.size();
      if (mapSize != count) {
        TestCase.fail("map.size() does not match expected count");
      }

      for (int index = 0; index < count; index++) {
        FastLongHashMultiSetElement element = array[index];
        long key = element.getKey();
        int multiSetCount = element.getMultiSetCount();

        VectorMapJoinHashMultiSetResult hashMultiSetResult = map.createHashMultiSetResult();
        JoinUtil.JoinResult joinResult = map.contains(key, hashMultiSetResult);
        if (joinResult != JoinUtil.JoinResult.MATCH) {
          assertTrue(false);
        }

        assertEquals(hashMultiSetResult.count(), multiSetCount);
      }
    }
  }

  /*
   * Element for Key: byte[] x Hash Table: HashMultiSet
   */
  public static class FastBytesHashMultiSetElement {
    private byte[] key;
    private int multiSetCount;

    public FastBytesHashMultiSetElement(byte[] key) {
      this.key = key;
      multiSetCount = 1;
    }

    public byte[] getKey() {
      return key;
    }

    public int getMultiSetCount() {
      return multiSetCount;
    }

    public void incrementMultiSetCount() {
      multiSetCount++;
    }
  }

  /*
   * Verify table for Key: byte[] x Hash Table: HashMultiSet
   */
  public static class VerifyFastBytesHashMultiSet {

    private int count;

    private FastBytesHashMultiSetElement[] array;

    private TreeMap<BytesWritable, Integer> keyValueMap;

    public VerifyFastBytesHashMultiSet() {
      count = 0;
      array = new FastBytesHashMultiSetElement[50];

      // We use BytesWritable because it supports Comparable for our TreeMap.
      keyValueMap = new TreeMap<BytesWritable, Integer>();
    }

    public int getCount() {
      return count;
    }

    public boolean contains(byte[] key) {
      BytesWritable keyBytesWritable = new BytesWritable(key, key.length);
      return keyValueMap.containsKey(keyBytesWritable);
    }

    public void add(byte[] key) {
      BytesWritable keyBytesWritable = new BytesWritable(key, key.length);
      if (keyValueMap.containsKey(keyBytesWritable)) {
        int index = keyValueMap.get(keyBytesWritable);
        array[index].incrementMultiSetCount();
      } else {
        if (count >= array.length) {
          // Grow.
          FastBytesHashMultiSetElement[] newArray = new FastBytesHashMultiSetElement[array.length * 2];
          System.arraycopy(array, 0, newArray, 0, count);
          array = newArray;
        }
        array[count] = new FastBytesHashMultiSetElement(key);
        keyValueMap.put(keyBytesWritable, count);
        count++;
      }
    }

    public byte[] addRandomExisting(byte[] value, Random r) {
      Preconditions.checkState(count > 0);
      int index = r.nextInt(count);
      array[index].incrementMultiSetCount();
      return array[index].getKey();
    }

    public byte[] getKey(int index) {
      return array[index].getKey();
    }

    public int getMultiSetCount(int index) {
      return array[index].getMultiSetCount();
    }

    public void verify(VectorMapJoinFastBytesHashMultiSet map) {
      int mapSize = map.size();
      if (mapSize != count) {
        TestCase.fail("map.size() does not match expected count");
      }

      for (int index = 0; index < count; index++) {
        FastBytesHashMultiSetElement element = array[index];
        byte[] key = element.getKey();
        int multiSetCount = element.getMultiSetCount();

        VectorMapJoinHashMultiSetResult hashMultiSetResult = map.createHashMultiSetResult();
        JoinUtil.JoinResult joinResult = map.contains(key, 0, key.length, hashMultiSetResult);
        if (joinResult != JoinUtil.JoinResult.MATCH) {
          assertTrue(false);
        }

        assertEquals(hashMultiSetResult.count(), multiSetCount);
      }
    }
  }

  /*
   * Element for Key: Long x Hash Table: HashSet
   */
  public static class FastLongHashSetElement {
    private long key;

    public FastLongHashSetElement(long key) {
      this.key = key;
    }

    public long getKey() {
      return key;
    }
  }

  /*
   * Verify table for Key: Long x Hash Table: HashSet
   */
  public static class VerifyFastLongHashSet {

    private int count;

    private FastLongHashSetElement[] array;

    private HashMap<Long, Integer> keyValueMap;

    public VerifyFastLongHashSet() {
      count = 0;
      array = new FastLongHashSetElement[50];
      keyValueMap = new HashMap<Long, Integer>();
    }

    public int getCount() {
      return count;
    }

    public boolean contains(long key) {
      return keyValueMap.containsKey(key);
    }

    public void add(long key) {
      if (keyValueMap.containsKey(key)) {
        // Already exists.
      } else {
        if (count >= array.length) {
          // Grow.
          FastLongHashSetElement[] newArray = new FastLongHashSetElement[array.length * 2];
          System.arraycopy(array, 0, newArray, 0, count);
          array = newArray;
        }
        array[count] = new FastLongHashSetElement(key);
        keyValueMap.put(key, count);
        count++;
      }
    }

    public long addRandomExisting(byte[] value, Random r) {
      Preconditions.checkState(count > 0);
      int index = r.nextInt(count);

      // Exists aleady.

      return array[index].getKey();
    }

    public long getKey(int index) {
      return array[index].getKey();
    }

    public void verify(VectorMapJoinFastLongHashSet map) {
      int mapSize = map.size();
      if (mapSize != count) {
        TestCase.fail("map.size() does not match expected count");
      }

      for (int index = 0; index < count; index++) {
        FastLongHashSetElement element = array[index];
        long key = element.getKey();

        VectorMapJoinHashSetResult hashSetResult = map.createHashSetResult();
        JoinUtil.JoinResult joinResult = map.contains(key, hashSetResult);
        if (joinResult != JoinUtil.JoinResult.MATCH) {
          assertTrue(false);
        }
      }
    }
  }

  /*
   * Element for Key: byte[] x Hash Table: HashSet
   */
  public static class FastBytesHashSetElement {
    private byte[] key;

    public FastBytesHashSetElement(byte[] key) {
      this.key = key;
    }

    public byte[] getKey() {
      return key;
    }
  }

  /*
   * Verify table for Key: byte[] x Hash Table: HashSet
   */
  public static class VerifyFastBytesHashSet {

    private int count;

    private FastBytesHashSetElement[] array;

    private TreeMap<BytesWritable, Integer> keyValueMap;

    public VerifyFastBytesHashSet() {
      count = 0;
      array = new FastBytesHashSetElement[50];

      // We use BytesWritable because it supports Comparable for our TreeMap.
      keyValueMap = new TreeMap<BytesWritable, Integer>();
    }

    public int getCount() {
      return count;
    }

    public boolean contains(byte[] key) {
      BytesWritable keyBytesWritable = new BytesWritable(key, key.length);
      return keyValueMap.containsKey(keyBytesWritable);
    }

    public void add(byte[] key) {
      BytesWritable keyBytesWritable = new BytesWritable(key, key.length);
      if (keyValueMap.containsKey(keyBytesWritable)) {
        // Already exists.
      } else {
        if (count >= array.length) {
          // Grow.
          FastBytesHashSetElement[] newArray = new FastBytesHashSetElement[array.length * 2];
          System.arraycopy(array, 0, newArray, 0, count);
          array = newArray;
        }
        array[count] = new FastBytesHashSetElement(key);
        keyValueMap.put(keyBytesWritable, count);
        count++;
      }
    }

    public byte[] addRandomExisting(byte[] value, Random r) {
      Preconditions.checkState(count > 0);
      int index = r.nextInt(count);

      // Already exists.

      return array[index].getKey();
    }

    public byte[] getKey(int index) {
      return array[index].getKey();
    }

    public void verify(VectorMapJoinFastBytesHashSet map) {
      int mapSize = map.size();
      if (mapSize != count) {
        TestCase.fail("map.size() does not match expected count");
      }

      for (int index = 0; index < count; index++) {
        FastBytesHashSetElement element = array[index];
        byte[] key = element.getKey();

        VectorMapJoinHashSetResult hashSetResult = map.createHashSetResult();
        JoinUtil.JoinResult joinResult = map.contains(key, 0, key.length, hashSetResult);
        if (joinResult != JoinUtil.JoinResult.MATCH) {
          assertTrue(false);
        }
      }
    }
  }
}