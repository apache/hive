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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.util.DescriptionTest;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class MapJoinTestDescription extends DescriptionTest {

  public static enum MapJoinPlanVariation {
    DYNAMIC_PARTITION_HASH_JOIN,
    SHARED_SMALL_TABLE
  }

  public static class SmallTableGenerationParameters {

    public static enum ValueOption {
      NO_RESTRICTION,
      ONLY_ONE,
      NO_REGULAR_SMALL_KEYS
    }

    private ValueOption valueOption;

    private int keyOutOfAThousand;
    private int noMatchKeyOutOfAThousand;

    public SmallTableGenerationParameters() {
      valueOption = ValueOption.NO_RESTRICTION;
      keyOutOfAThousand = 50;         // 5%
      noMatchKeyOutOfAThousand = 50;  // 5%
    }

    public void setValueOption(ValueOption valueOption) {
      this.valueOption = valueOption;
    }

    public ValueOption getValueOption() {
      return valueOption;
    }

    public void setKeyOutOfAThousand(int keyOutOfAThousand) {
      this.keyOutOfAThousand = keyOutOfAThousand;
    }

    public int getKeyOutOfAThousand() {
      return keyOutOfAThousand;
    }

    public void setNoMatchKeyOutOfAThousand(int noMatchKeyOutOfAThousand) {
      this.noMatchKeyOutOfAThousand = noMatchKeyOutOfAThousand;
    }

    public int getNoMatchKeyOutOfAThousand() {
      return noMatchKeyOutOfAThousand;
    }
  }

  final VectorMapJoinVariation vectorMapJoinVariation;

  // Adjustable.
  public String[] bigTableKeyColumnNames;
  public TypeInfo[] bigTableTypeInfos;

  public int[] bigTableKeyColumnNums;

  public TypeInfo[] smallTableValueTypeInfos;

  public int[] smallTableRetainKeyColumnNums;

  public SmallTableGenerationParameters smallTableGenerationParameters;

  // Derived.

  public int[] bigTableColumnNums;
  public String[] bigTableColumnNames;
  public List<String> bigTableColumnNameList;
  public ObjectInspector[] bigTableObjectInspectors;
  public List<ObjectInspector> bigTableObjectInspectorList;

  public TypeInfo[] bigTableKeyTypeInfos;

  public List<String> smallTableKeyColumnNameList;
  public String[] smallTableKeyColumnNames;
  public TypeInfo[] smallTableKeyTypeInfos;
  public ObjectInspector[] smallTableKeyObjectInspectors;
  public List<ObjectInspector> smallTableKeyObjectInspectorList;

  public List<String> smallTableValueColumnNameList;
  public String[] smallTableValueColumnNames;
  public ObjectInspector[] smallTableValueObjectInspectors;
  public List<ObjectInspector> smallTableValueObjectInspectorList;

  public int[] bigTableRetainColumnNums;
  public int[] smallTableRetainValueColumnNums;

  public String[] smallTableColumnNames;
  public List<String> smallTableColumnNameList;
  public TypeInfo[] smallTableTypeInfos;
  public List<ObjectInspector> smallTableObjectInspectorList;

  public StandardStructObjectInspector bigTableStandardObjectInspector;
  public StandardStructObjectInspector smallTableStandardObjectInspector;
  public ObjectInspector[] inputObjectInspectors;

  public String[] outputColumnNames;
  public TypeInfo[] outputTypeInfos;
  public ObjectInspector[] outputObjectInspectors;

  final MapJoinPlanVariation mapJoinPlanVariation;

  public MapJoinTestDescription (
      HiveConf hiveConf,
      VectorMapJoinVariation vectorMapJoinVariation,
      TypeInfo[] bigTableTypeInfos,
      int[] bigTableKeyColumnNums,
      TypeInfo[] smallTableValueTypeInfos,
      int[] smallTableRetainKeyColumnNums,
      SmallTableGenerationParameters smallTableGenerationParameters,
      MapJoinPlanVariation mapJoinPlanVariation) {
    this(
        hiveConf,
        vectorMapJoinVariation,
        /* bigTableColumnNames */ null,
        bigTableTypeInfos,
        bigTableKeyColumnNums,
        smallTableValueTypeInfos,
        smallTableRetainKeyColumnNums,
        smallTableGenerationParameters,
        mapJoinPlanVariation);
  }

  public MapJoinTestDescription (
    HiveConf hiveConf,
    VectorMapJoinVariation vectorMapJoinVariation,
    String[] bigTableColumnNames,
    TypeInfo[] bigTableTypeInfos,
    int[] bigTableKeyColumnNums,
    TypeInfo[] smallTableValueTypeInfos,
    int[] smallTableRetainKeyColumnNums,
    SmallTableGenerationParameters smallTableGenerationParameters,
    MapJoinPlanVariation mapJoinPlanVariation) {

    super(hiveConf);

    this.vectorMapJoinVariation = vectorMapJoinVariation;

    this.bigTableColumnNames = bigTableColumnNames;
    this.bigTableTypeInfos = bigTableTypeInfos;
    this.bigTableKeyColumnNums = bigTableKeyColumnNums;

    this.smallTableValueTypeInfos = smallTableValueTypeInfos;

    this.smallTableRetainKeyColumnNums = smallTableRetainKeyColumnNums;;

    this.smallTableGenerationParameters = smallTableGenerationParameters;

    this.mapJoinPlanVariation = mapJoinPlanVariation;

    computeDerived();
  }

  public SmallTableGenerationParameters getSmallTableGenerationParameters() {
    return smallTableGenerationParameters;
  }

  public void computeDerived() {

    final int bigTableSize = bigTableTypeInfos.length;

    if (bigTableKeyColumnNames == null) {

      // Automatically populate.
      bigTableColumnNames = new String[bigTableSize];
      for (int i = 0; i < bigTableSize; i++) {
        bigTableColumnNames[i] = HiveConf.getColumnInternalName(i);
      }
    }

    // Automatically populate.
    bigTableColumnNums = new int[bigTableSize];

    for (int i = 0; i < bigTableSize; i++) {
      bigTableColumnNums[i] = i;
    }

    // Automatically populate.
    bigTableRetainColumnNums = new int[bigTableSize];
    for (int i = 0; i < bigTableSize; i++) {
      bigTableRetainColumnNums[i] = i;
    }

    /*
     * Big Table key information.
     */
    final int keySize = bigTableKeyColumnNums.length;

    bigTableKeyColumnNames = new String[keySize];
    bigTableKeyTypeInfos = new TypeInfo[keySize];
    for (int i = 0; i < keySize; i++) {
      final int bigTableKeyColumnNum = bigTableKeyColumnNums[i];
      bigTableKeyColumnNames[i] = bigTableColumnNames[bigTableKeyColumnNum];
      bigTableKeyTypeInfos[i] = bigTableTypeInfos[bigTableKeyColumnNum];
    }

    /*
     * Big Table object inspectors.
     */
    bigTableObjectInspectors = new ObjectInspector[bigTableSize];
    for (int i = 0; i < bigTableSize; i++) {
      bigTableObjectInspectors[i] =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
              (PrimitiveTypeInfo) bigTableTypeInfos[i]);
    }
    bigTableColumnNameList = Arrays.asList(bigTableColumnNames);
    bigTableObjectInspectorList = Arrays.asList(bigTableObjectInspectors);

    /*
     * Small Table key object inspectors are derived directly from the Big Table key information.
     */
    smallTableKeyColumnNames = new String[keySize];
    smallTableKeyTypeInfos = Arrays.copyOf(bigTableKeyTypeInfos, keySize);
    smallTableKeyObjectInspectors = new ObjectInspector[keySize];
    for (int i = 0; i < keySize; i++) {
      smallTableKeyColumnNames[i] = HiveConf.getColumnInternalName(i);
      final int bigTableKeyColumnNum = bigTableKeyColumnNums[i];
      smallTableKeyObjectInspectors[i] = bigTableObjectInspectors[bigTableKeyColumnNum];
    }
    smallTableKeyColumnNameList = Arrays.asList(smallTableKeyColumnNames);
    smallTableKeyObjectInspectorList = Arrays.asList(smallTableKeyObjectInspectors);

    // First part of Small Table information is the key information.
    smallTableColumnNameList = new ArrayList<String>(smallTableKeyColumnNameList);
    List<TypeInfo> smallTableTypeInfoList =
        new ArrayList<TypeInfo>(Arrays.asList(smallTableKeyTypeInfos));
    smallTableObjectInspectorList = new ArrayList<ObjectInspector>();
    smallTableObjectInspectorList.addAll(smallTableKeyObjectInspectorList);

    final int valueSize = smallTableValueTypeInfos.length;

    // Automatically populate.
    smallTableValueColumnNames = new String[valueSize];
    for (int i = 0; i < valueSize; i++) {
      smallTableValueColumnNames[i] = HiveConf.getColumnInternalName(keySize + i);
    }

    smallTableValueObjectInspectors = new ObjectInspector[valueSize];
    for (int i = 0; i < valueSize; i++) {
      smallTableValueObjectInspectors[i] =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
              (PrimitiveTypeInfo) smallTableValueTypeInfos[i]);
    }
    smallTableValueColumnNameList = Arrays.asList(smallTableValueColumnNames);
    smallTableTypeInfoList.addAll(Arrays.asList(smallTableValueTypeInfos));
    smallTableValueObjectInspectorList = Arrays.asList(smallTableValueObjectInspectors);

    smallTableColumnNameList.addAll(smallTableValueColumnNameList);
    smallTableColumnNames = smallTableColumnNameList.toArray(new String[0]);
    smallTableTypeInfos = smallTableTypeInfoList.toArray(new TypeInfo[0]);

    smallTableObjectInspectorList.addAll(smallTableValueObjectInspectorList);

    /*
     * The inputObjectInspectors describe the keys and values of the Big Table and Small Table.
     */
    bigTableStandardObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            bigTableColumnNameList, bigTableObjectInspectorList);
    smallTableStandardObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            smallTableColumnNameList, smallTableObjectInspectorList);

    inputObjectInspectors =
        new ObjectInspector[] {
            bigTableStandardObjectInspector, smallTableStandardObjectInspector };

    // For now, we always retain the Small Table values...
    // Automatically populate.
    smallTableRetainValueColumnNums = new int[valueSize];
    for (int i = 0; i < valueSize; i++) {
      smallTableRetainValueColumnNums[i] = i;
    }

    int outputLength =
        bigTableRetainColumnNums.length +
        smallTableRetainKeyColumnNums.length +
        smallTableRetainValueColumnNums.length;
    outputColumnNames = createOutputColumnNames(outputLength);

    outputTypeInfos = new TypeInfo[outputLength];
    int outputIndex = 0;
    final int bigTableRetainSize = bigTableRetainColumnNums.length;
    for (int i = 0; i < bigTableRetainSize; i++) {
      outputTypeInfos[outputIndex++] = bigTableTypeInfos[bigTableRetainColumnNums[i]];
    }
    for (int i = 0; i < smallTableRetainKeyColumnNums.length; i++) {
      outputTypeInfos[outputIndex++] = smallTableKeyTypeInfos[smallTableRetainKeyColumnNums[i]];
    }
    for (int i = 0; i < smallTableRetainValueColumnNums.length; i++) {
      outputTypeInfos[outputIndex++] = smallTableValueTypeInfos[smallTableRetainValueColumnNums[i]];
    }

    outputObjectInspectors = new ObjectInspector[outputLength];
    for (int i = 0; i < outputLength; i++) {
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) outputTypeInfos[i];
      outputObjectInspectors[i] =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveTypeInfo);
    }
  }

  private String[] createOutputColumnNames(int outputColumnCount) {
    String[] outputColumnNames = new String[outputColumnCount];
    int counter = 1;
    for (int i = 0; i < outputColumnCount; i++) {
      outputColumnNames[i] = "out" + counter++;
    }
    return outputColumnNames;
  }
}