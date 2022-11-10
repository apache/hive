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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.CheckFastRowHashMap.VerifyFastRowHashMap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

/*
 * Multi-key value hash map optimized for vector map join.
 *
 * The key is uninterpreted bytes.
 */
public class TestVectorMapJoinFastRowHashMap extends CommonFastHashTable {

  public static final Properties ANY_TABLE_PROPERTIES = new Properties();
  private static TableDesc tableDesc = new TableDesc();

  @Before
  public void setUp() throws Exception {
    tableDesc.setProperties(ANY_TABLE_PROPERTIES);
  }

  private void addAndVerifyRows(VectorRandomRowSource valueSource, Object[][] rows,
                                VectorMapJoinFastHashTableContainerBase map, HashTableKeyType hashTableKeyType,
                                VerifyFastRowHashMap verifyTable, String[] keyTypeNames,
                                boolean doClipping, boolean useExactBytes)
          throws HiveException, IOException {

    final int keyCount = keyTypeNames.length;
    PrimitiveTypeInfo[] keyPrimitiveTypeInfos = new PrimitiveTypeInfo[keyCount];
    PrimitiveCategory[] keyPrimitiveCategories = new PrimitiveCategory[keyCount];
    ArrayList<ObjectInspector> keyPrimitiveObjectInspectorList =
        new ArrayList<ObjectInspector>(keyCount);

    for (int i = 0; i < keyCount; i++) {
      PrimitiveTypeInfo primitiveTypeInfo =
          (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(keyTypeNames[i]);
      keyPrimitiveTypeInfos[i] = primitiveTypeInfo;
      PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
      keyPrimitiveCategories[i] = primitiveCategory;
      keyPrimitiveObjectInspectorList.add(
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveTypeInfo));
    }

    boolean[] keyColumnSortOrderIsDesc = new boolean[keyCount];
    Arrays.fill(keyColumnSortOrderIsDesc, false);
    byte[] keyColumnNullMarker = new byte[keyCount];
    Arrays.fill(keyColumnNullMarker, BinarySortableSerDe.ZERO);
    byte[] keyColumnNotNullMarker = new byte[keyCount];
    Arrays.fill(keyColumnNotNullMarker, BinarySortableSerDe.ONE);

    BinarySortableSerializeWrite keySerializeWrite =
        new BinarySortableSerializeWrite(keyColumnSortOrderIsDesc,
            keyColumnNullMarker, keyColumnNotNullMarker);


    TypeInfo[] valueTypeInfos = valueSource.typeInfos();
    final int columnCount = valueTypeInfos.length;

    SerializeWrite valueSerializeWrite = new LazyBinarySerializeWrite(columnCount);

    final int count = rows.length;
    for (int i = 0; i < count; i++) {

      Object[] valueRow = rows[i];

      Output valueOutput = new Output();
        ((LazyBinarySerializeWrite) valueSerializeWrite).set(valueOutput);

      for (int index = 0; index < columnCount; index++) {
        VerifyFastRow.serializeWrite(valueSerializeWrite, valueTypeInfos[index], valueRow[index]);
      }

      byte[] value = Arrays.copyOf(valueOutput.getData(), valueOutput.getLength());

      // Add a new key or add a value to an existing key?
      byte[] key;
      if (random.nextBoolean() || verifyTable.getCount() == 0) {
        Object[] keyRow =
            VectorRandomRowSource.randomWritablePrimitiveRow(keyCount, random, keyPrimitiveTypeInfos);

        Output keyOutput = new Output();
        keySerializeWrite.set(keyOutput);

        for (int index = 0; index < keyCount; index++) {
          VerifyFastRow.serializeWrite(keySerializeWrite, keyPrimitiveTypeInfos[index], keyRow[index]);
        }

        key = Arrays.copyOf(keyOutput.getData(), keyOutput.getLength());

        verifyTable.add(key, keyRow, value, valueRow);
      } else {
        key = verifyTable.addRandomExisting(value, valueRow, random);
      }

      // Serialize keyRow into key bytes.
      BytesWritable keyWritable = new BytesWritable(key);
      BytesWritable valueWritable = new BytesWritable(value);
      long hashcode = map.getHashCode(keyWritable);
      map.putRow(hashcode, keyWritable, valueWritable);
      // verifyTable.verify(map);
    }
    verifyTable.verify(map, hashTableKeyType, valueTypeInfos,
        doClipping, useExactBytes, random);
  }

  @Test
  public void testBigIntRows() throws Exception {
    random = new Random(927337);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMapContainer map =
        new VectorMapJoinFastLongHashMapContainer(
            false, false, HashTableKeyType.LONG,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.LONG, verifyTable,
        new String[] { "bigint" },
        /* doClipping */ false, /* useExactBytes */ false);
  }

  @Test
  public void testIntRows() throws Exception {
    random = new Random(927337);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMapContainer map =
        new VectorMapJoinFastLongHashMapContainer(
            false, false, HashTableKeyType.INT,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.INT, verifyTable,
        new String[] { "int" },
        /* doClipping */ false, /* useExactBytes */ false);
  }

  @Test
  public void testStringRows() throws Exception {
    random = new Random(927337);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastStringHashMapContainer map =
        new VectorMapJoinFastStringHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.STRING, verifyTable,
        new String[] { "string" },
        /* doClipping */ false, /* useExactBytes */ false);
  }

  @Test
  public void testMultiKeyRows1() throws Exception {
    random = new Random(833);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "int", "int" },
        /* doClipping */ false, /* useExactBytes */ false);
  }

  @Test
  public void testMultiKeyRows2() throws Exception {
    random = new Random(833099);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "string", "string" },
        /* doClipping */ false, /* useExactBytes */ false);
  }

  @Test
  public void testMultiKeyRows3() throws Exception {
    random = new Random(833099);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "bigint", "timestamp", "double" },
        /* doClipping */ false, /* useExactBytes */ false);
  }

  @Test
  public void testBigIntRowsClipped() throws Exception {
    random = new Random(326232);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMapContainer map =
        new VectorMapJoinFastLongHashMapContainer(
            false, false, HashTableKeyType.LONG,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.LONG, verifyTable,
        new String[] { "bigint" },
        /* doClipping */ true, /* useExactBytes */ false);
  }

  @Test
  public void testIntRowsClipped() throws Exception {
    random = new Random(326232);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMapContainer map =
        new VectorMapJoinFastLongHashMapContainer(
            false, false, HashTableKeyType.INT,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.INT, verifyTable,
        new String[] { "int" },
        /* doClipping */ true, /* useExactBytes */ false);
  }

  @Test
  public void testStringRowsClipped() throws Exception {
    random = new Random(326232);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastStringHashMapContainer map =
        new VectorMapJoinFastStringHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.STRING, verifyTable,
        new String[] { "string" },
        /* doClipping */ true, /* useExactBytes */ false);
  }

  @Test
  public void testMultiKeyRowsClipped1() throws Exception {
    random = new Random(2331);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "varchar(20)", "date", "interval_day_time" },
        /* doClipping */ true, /* useExactBytes */ false);
  }

  @Test
  public void testMultiKeyRowsClipped2() throws Exception {
    random = new Random(7403);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "varchar(20)", "varchar(40)" },
        /* doClipping */ true, /* useExactBytes */ false);
  }

  @Test
  public void testMultiKeyRowsClipped3() throws Exception {
    random = new Random(99);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "float", "tinyint" },
        /* doClipping */ true, /* useExactBytes */ false);
  }


  @Test
  public void testBigIntRowsExact() throws Exception {
    random = new Random(27722);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMapContainer map =
        new VectorMapJoinFastLongHashMapContainer(
            false, false, HashTableKeyType.LONG,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.LONG, verifyTable,
        new String[] { "bigint" },
        /* doClipping */ false, /* useExactBytes */ true);
  }

  @Test
  public void testIntRowsExact() throws Exception {
    random = new Random(8238383);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMapContainer map =
        new VectorMapJoinFastLongHashMapContainer(
            false, false, HashTableKeyType.INT,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.INT, verifyTable,
        new String[] { "int" },
        /* doClipping */ false, /* useExactBytes */ true);
  }

  @Test
  public void testStringRowsExact() throws Exception {
    random = new Random(8235);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastStringHashMapContainer map =
        new VectorMapJoinFastStringHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.STRING, verifyTable,
        new String[] { "string" },
        /* doClipping */ false, /* useExactBytes */ true);
  }

  @Test
  public void testMultiKeyRowsExact1() throws Exception {
    random = new Random(8235);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "string", "string", "string", "string" },
        /* doClipping */ false, /* useExactBytes */ true);
  }

  @Test
  public void testMultiKeyRowsExact2() throws Exception {
    random = new Random(8235);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "smallint" },
        /* doClipping */ false, /* useExactBytes */ true);
  }

  @Test
  public void testMultiKeyRowsExact3() throws Exception {
    random = new Random(8235);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "int", "binary" },
        /* doClipping */ false, /* useExactBytes */ true);
  }

  @Test
  public void testBigIntRowsClippedExact() throws Exception {
    random = new Random(2122);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMapContainer map =
        new VectorMapJoinFastLongHashMapContainer(
            false, false, HashTableKeyType.LONG,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.LONG, verifyTable,
        new String[] { "bigint" },
        /* doClipping */ true, /* useExactBytes */ true);
  }

  @Test
  public void testIntRowsClippedExact() throws Exception {
    random = new Random(7520);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastLongHashMapContainer map =
        new VectorMapJoinFastLongHashMapContainer(
            false, false, HashTableKeyType.INT,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.INT, verifyTable,
        new String[] { "int" },
        /* doClipping */ true, /* useExactBytes */ true);
  }

  @Test
  public void testStringRowsClippedExact() throws Exception {
    random = new Random(7539);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastStringHashMapContainer map =
        new VectorMapJoinFastStringHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, tableDesc, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.STRING, verifyTable,
        new String[] { "string" },
        /* doClipping */ true, /* useExactBytes */ true);
  }

  @Test
  public void testMultiKeyRowsClippedExact1() throws Exception {
    random = new Random(13);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "interval_year_month", "decimal(12,8)" },
        /* doClipping */ true, /* useExactBytes */ true);
  }

  @Test
  public void testMultiKeyRowsClippedExact2() throws Exception {
    random = new Random(12);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "bigint", "string", "int" },
        /* doClipping */ true, /* useExactBytes */ true);
  }

  @Test
  public void testMultiKeyRowsClippedExact3() throws Exception {
    random = new Random(7);

    // Use a large capacity that doesn't require expansion, yet.
    VectorMapJoinFastMultiKeyHashMapContainer map =
        new VectorMapJoinFastMultiKeyHashMapContainer(
            false,
            LARGE_CAPACITY, LOAD_FACTOR, LARGE_WB_SIZE, -1, 4);

    VerifyFastRowHashMap verifyTable = new VerifyFastRowHashMap();

    VectorRandomRowSource valueSource = new VectorRandomRowSource();

    valueSource.init(
        random, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    int rowCount = 1000;
    Object[][] rows = valueSource.randomRows(rowCount);

    addAndVerifyRows(valueSource, rows,
        map, HashTableKeyType.MULTI_KEY, verifyTable,
        new String[] { "bigint", "string", "varchar(5000)" },
        /* doClipping */ true, /* useExactBytes */ true);
  }
}
