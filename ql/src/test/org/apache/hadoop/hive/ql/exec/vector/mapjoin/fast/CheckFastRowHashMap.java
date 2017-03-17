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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

public class CheckFastRowHashMap extends CheckFastHashTable {

  public static void verifyHashMapRows(List<Object[]> rows, int[] actualToValueMap,
      VectorMapJoinHashMapResult hashMapResult, TypeInfo[] typeInfos) throws IOException {

    final int count = rows.size();
    final int columnCount = typeInfos.length;

    WriteBuffers.ByteSegmentRef ref = hashMapResult.first();

    for (int a = 0; a < count; a++) {

      int valueIndex = actualToValueMap[a];

      Object[] row = rows.get(valueIndex);

      byte[] bytes = ref.getBytes();
      int offset = (int) ref.getOffset();
      int length = ref.getLength();

      LazyBinaryDeserializeRead lazyBinaryDeserializeRead =
          new LazyBinaryDeserializeRead(
              typeInfos,
              /* useExternalBuffer */ false);

      lazyBinaryDeserializeRead.set(bytes, offset, length);

      for (int index = 0; index < columnCount; index++) {
        Writable writable = (Writable) row[index];
        VerifyFastRow.verifyDeserializeRead(lazyBinaryDeserializeRead, (PrimitiveTypeInfo) typeInfos[index], writable);
      }
      TestCase.assertTrue(lazyBinaryDeserializeRead.isEndOfInputReached());

      ref = hashMapResult.next();
      if (a == count - 1) {
        TestCase.assertTrue (ref == null);
      } else {
        TestCase.assertTrue (ref != null);
      }
    }
  }

  public static void verifyHashMapRowsMore(List<Object[]> rows, int[] actualToValueMap,
      VectorMapJoinHashMapResult hashMapResult, TypeInfo[] typeInfos,
      int clipIndex, boolean useExactBytes) throws IOException {
    String debugExceptionMessage = null;
    StackTraceElement[] debugStackTrace = null;

    final int count = rows.size();
    final int columnCount = typeInfos.length;

    WriteBuffers.ByteSegmentRef ref = hashMapResult.first();

    for (int a = 0; a < count; a++) {

      int valueIndex = actualToValueMap[a];

      Object[] row = rows.get(valueIndex);

      byte[] bytes = ref.getBytes();
      int offset = (int) ref.getOffset();
      int length = ref.getLength();
      if (a == clipIndex) {
        length--;
      }

      if (useExactBytes) {
        // Use exact byte array which might generate array out of bounds...
        bytes = Arrays.copyOfRange(bytes, offset, offset + length);
        offset = 0;
      }

      LazyBinaryDeserializeRead lazyBinaryDeserializeRead =
          new LazyBinaryDeserializeRead(
              typeInfos,
              /* useExternalBuffer */ false);

      lazyBinaryDeserializeRead.set(bytes, offset, length);

      boolean thrown = false;
      Exception saveException = null;
      int index = 0;
      try {
        for (index = 0; index < columnCount; index++) {
          Writable writable = (Writable) row[index];
          VerifyFastRow.verifyDeserializeRead(lazyBinaryDeserializeRead, (PrimitiveTypeInfo) typeInfos[index], writable);
        }
      } catch (Exception e) {
        thrown = true;
        saveException = e;
        lazyBinaryDeserializeRead.getDetailedReadPositionString();

        hashMapResult.getDetailedHashMapResultPositionString();

        debugExceptionMessage = saveException.getMessage();
        debugStackTrace = saveException.getStackTrace();
      }
      if (a == clipIndex) {
        if (!thrown) {
          TestCase.fail("Expecting an exception to be thrown for the clipped case...");
        } else {
          TestCase.assertTrue(saveException != null);
          if (saveException instanceof EOFException) {
            // This is the one we are expecting.
          } else if (saveException instanceof ArrayIndexOutOfBoundsException) {
          } else {
            TestCase.fail("Expecting an EOFException to be thrown for the clipped case...");
          }
        }
      } else {
        if (thrown) {
          TestCase.fail("Not expecting an exception to be thrown for the non-clipped case... " +
              " exception message " + debugExceptionMessage +
              " stack trace " + getStackTraceAsSingleLine(debugStackTrace));
        }
        TestCase.assertTrue(lazyBinaryDeserializeRead.isEndOfInputReached());
      }

      ref = hashMapResult.next();
      if (a == count - 1) {
        TestCase.assertTrue (ref == null);
      } else {
        TestCase.assertTrue (ref != null);
      }
    }
  }

  /*
   * Element for Key: row and byte[] x Hash Table: HashMap
   */
  public static class FastRowHashMapElement {
    private byte[] key;
    private Object[] keyRow;
    private List<byte[]> values;
    private List<Object[]> valueRows;

    public FastRowHashMapElement(byte[] key, Object[] keyRow, byte[] firstValue,
        Object[] valueRow) {
      this.key = key;
      this.keyRow = keyRow;
      values = new ArrayList<byte[]>();
      values.add(firstValue);
      valueRows = new ArrayList<Object[]>();
      valueRows.add(valueRow);
    }

    public byte[] getKey() {
      return key;
    }

    public Object[] getKeyRow() {
      return keyRow;
    }

    public int getCount() {
      return values.size();
    }

    public List<byte[]> getValues() {
      return values;
    }

    public List<Object[]> getValueRows() {
      return valueRows;
    }

    public void add(byte[] value, Object[] valueRow) {
      values.add(value);
      valueRows.add(valueRow);
    }
  }

  /*
   * Verify table for Key: row and byte[] x Hash Table: HashMap
   */
  public static class VerifyFastRowHashMap {

    private int count;

    private FastRowHashMapElement[] array;

    private TreeMap<BytesWritable, Integer> keyValueMap;

    public VerifyFastRowHashMap() {
      count = 0;
      array = new FastRowHashMapElement[50];

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

    public void add(byte[] key, Object[] keyRow, byte[] value, Object[] valueRow) {
      BytesWritable keyBytesWritable = new BytesWritable(key, key.length);
      if (keyValueMap.containsKey(keyBytesWritable)) {
        int index = keyValueMap.get(keyBytesWritable);
        array[index].add(value, valueRow);
      } else {
        if (count >= array.length) {
          // Grow.
          FastRowHashMapElement[] newArray = new FastRowHashMapElement[array.length * 2];
          System.arraycopy(array, 0, newArray, 0, count);
          array = newArray;
        }
        array[count] = new FastRowHashMapElement(key, keyRow, value, valueRow);
        keyValueMap.put(keyBytesWritable, count);
        count++;
      }
    }

    public byte[] addRandomExisting(byte[] value, Object[] valueRow, Random r) {
      Preconditions.checkState(count > 0);
      int index = r.nextInt(count);
      array[index].add(value, valueRow);
      return array[index].getKey();
    }

    public byte[] getKey(int index) {
      return array[index].getKey();
    }

    public List<byte[]> getValues(int index) {
      return array[index].getValues();
    }

    public void verify(VectorMapJoinFastHashTable map,
        HashTableKeyType hashTableKeyType,
        PrimitiveTypeInfo[] valuePrimitiveTypeInfos, boolean doClipping,
        boolean useExactBytes, Random random) throws IOException {
      int mapSize = map.size();
      if (mapSize != count) {
        TestCase.fail("map.size() does not match expected count");
      }

      for (int index = 0; index < count; index++) {
        FastRowHashMapElement element = array[index];

        List<byte[]> values = element.getValues();

        VectorMapJoinHashMapResult hashMapResult = null;
        JoinUtil.JoinResult joinResult = JoinUtil.JoinResult.NOMATCH;
        switch (hashTableKeyType) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          {
            Object[] keyRow = element.getKeyRow();
            Object keyObject = keyRow[0];
            VectorMapJoinFastLongHashMap longHashMap = (VectorMapJoinFastLongHashMap) map;
            hashMapResult = longHashMap.createHashMapResult();
            long longKey;
            switch (hashTableKeyType) {
            case BOOLEAN:
              longKey = ((BooleanWritable) keyObject).get() ? 1 : 0;
              break;
            case BYTE:
              longKey = ((ByteWritable) keyObject).get();
              break;
            case SHORT:
              longKey = ((ShortWritable) keyObject).get();
              break;
            case INT:
              longKey = ((IntWritable) keyObject).get();
              break;
            case LONG:
              longKey = ((LongWritable) keyObject).get();
              break;
            default:
              throw new RuntimeException("Unexpected hash table key type " + hashTableKeyType.name());
            }
            joinResult = longHashMap.lookup(longKey, hashMapResult);
            if (joinResult != JoinUtil.JoinResult.MATCH) {
              assertTrue(false);
            }
          }
          break;
        case STRING:
          {
            Object[] keyRow = element.getKeyRow();
            Object keyObject = keyRow[0];
            VectorMapJoinFastStringHashMap stringHashMap = (VectorMapJoinFastStringHashMap) map;
            hashMapResult = stringHashMap.createHashMapResult();
            Text text = (Text) keyObject;
            byte[] bytes = text.getBytes();
            int length = text.getLength();
            joinResult = stringHashMap.lookup(bytes, 0, length, hashMapResult);
            if (joinResult != JoinUtil.JoinResult.MATCH) {
              assertTrue(false);
            }
          }
          break;
        case MULTI_KEY:
          {
            byte[] keyBytes = element.getKey();
            VectorMapJoinFastMultiKeyHashMap stringHashMap = (VectorMapJoinFastMultiKeyHashMap) map;
            hashMapResult = stringHashMap.createHashMapResult();
            joinResult = stringHashMap.lookup(keyBytes, 0, keyBytes.length, hashMapResult);
            if (joinResult != JoinUtil.JoinResult.MATCH) {
              assertTrue(false);
            }
          }
          break;
        default:
          throw new RuntimeException("Unexpected hash table key type " + hashTableKeyType.name());
        }

        int[] actualToValueMap = verifyHashMapValues(hashMapResult, values);

        List<Object[]> rows = element.getValueRows();
        if (!doClipping && !useExactBytes) {
          verifyHashMapRows(rows, actualToValueMap, hashMapResult, valuePrimitiveTypeInfos);
        } else {
          int clipIndex = random.nextInt(rows.size());
          verifyHashMapRowsMore(rows, actualToValueMap, hashMapResult, valuePrimitiveTypeInfos,
              clipIndex, useExactBytes);
        }
      }
    }
  }

  static final int STACK_LENGTH_LIMIT = 20;
  public static String getStackTraceAsSingleLine(StackTraceElement[] stackTrace) {
    StringBuilder sb = new StringBuilder();
    sb.append("Stack trace: ");
    int length = stackTrace.length;
    boolean isTruncated = false;
    if (length > STACK_LENGTH_LIMIT) {
      length = STACK_LENGTH_LIMIT;
      isTruncated = true;
    }
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(stackTrace[i]);
    }
    if (isTruncated) {
      sb.append(", ...");
    }

    return sb.toString();
  }
}