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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.util.DescriptionTest;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class MapJoinTestDescription extends DescriptionTest {

  public static class SmallTableGenerationParameters {

    public static enum ValueOption {
      NO_RESTRICTION,
      ONLY_ONE,
      ONLY_TWO,
      AT_LEAST_TWO
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
  public String[] bigTableColumnNames;
  public TypeInfo[] bigTableTypeInfos;
  public int[] bigTableKeyColumnNums;
  public String[] smallTableValueColumnNames;
  public TypeInfo[] smallTableValueTypeInfos;
  public int[] bigTableRetainColumnNums;
  public int[] smallTableRetainKeyColumnNums;
  public int[] smallTableRetainValueColumnNums;

  public SmallTableGenerationParameters smallTableGenerationParameters;

  // Derived.
  public List<String> bigTableColumnNamesList;
  public String[] bigTableKeyColumnNames;
  public TypeInfo[] bigTableKeyTypeInfos;
  public List<String> smallTableValueColumnNamesList;
  public ObjectInspector[] bigTableObjectInspectors;
  public List<ObjectInspector> bigTableObjectInspectorsList;
  public StandardStructObjectInspector bigTableStandardObjectInspector;
  public PrimitiveTypeInfo[] smallTableValuePrimitiveTypeInfos;
  public ObjectInspector[] smallTableObjectInspectors;
  public PrimitiveCategory[] smallTablePrimitiveCategories;
  public List<ObjectInspector> smallTableObjectInspectorsList;
  public StandardStructObjectInspector smallTableStandardObjectInspector;
  public ObjectInspector[] inputObjectInspectors;
  public String[] outputColumnNames;
  public TypeInfo[] outputTypeInfos;
  public ObjectInspector[] outputObjectInspectors;

  public MapJoinTestDescription (
    HiveConf hiveConf,
    VectorMapJoinVariation vectorMapJoinVariation,
    String[] bigTableColumnNames, TypeInfo[] bigTableTypeInfos,
    int[] bigTableKeyColumnNums,
    String[] smallTableValueColumnNames, TypeInfo[] smallTableValueTypeInfos,
    int[] bigTableRetainColumnNums,
    int[] smallTableRetainKeyColumnNums, int[] smallTableRetainValueColumnNums,
    SmallTableGenerationParameters smallTableGenerationParameters) {

    super(hiveConf);
    this.vectorMapJoinVariation = vectorMapJoinVariation;

    this.bigTableColumnNames = bigTableColumnNames;
    this.bigTableTypeInfos = bigTableTypeInfos;
    this.bigTableKeyColumnNums = bigTableKeyColumnNums;
    this.smallTableValueColumnNames = smallTableValueColumnNames;
    this.smallTableValueTypeInfos = smallTableValueTypeInfos;
    this.bigTableRetainColumnNums = bigTableRetainColumnNums;
    this.smallTableRetainKeyColumnNums = smallTableRetainKeyColumnNums;
    this.smallTableRetainValueColumnNums = smallTableRetainValueColumnNums;

    this.smallTableGenerationParameters = smallTableGenerationParameters;

    switch (vectorMapJoinVariation) {
    case INNER_BIG_ONLY:
    case LEFT_SEMI:
      trimAwaySmallTableValueInfo();
      break;
    case INNER:
    case OUTER:
      break;
    default:
      throw new RuntimeException("Unknown operator variation " + vectorMapJoinVariation);
    }

    computeDerived();
  }

  public SmallTableGenerationParameters getSmallTableGenerationParameters() {
    return smallTableGenerationParameters;
  }

  public void computeDerived() {
    bigTableColumnNamesList = Arrays.asList(bigTableColumnNames);

    bigTableKeyColumnNames = new String[bigTableKeyColumnNums.length];
    bigTableKeyTypeInfos = new TypeInfo[bigTableKeyColumnNums.length];
    for (int i = 0; i < bigTableKeyColumnNums.length; i++) {
      bigTableKeyColumnNames[i] = bigTableColumnNames[bigTableKeyColumnNums[i]];
      bigTableKeyTypeInfos[i] = bigTableTypeInfos[bigTableKeyColumnNums[i]];
    }

    smallTableValueColumnNamesList = Arrays.asList(smallTableValueColumnNames);

    bigTableObjectInspectors = new ObjectInspector[bigTableTypeInfos.length];
    for (int i = 0; i < bigTableTypeInfos.length; i++) {
      bigTableObjectInspectors[i] =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector((PrimitiveTypeInfo) bigTableTypeInfos[i]);
    }
    bigTableObjectInspectorsList = Arrays.asList(bigTableObjectInspectors);

    smallTableObjectInspectors = new ObjectInspector[smallTableValueTypeInfos.length];
    smallTablePrimitiveCategories = new PrimitiveCategory[smallTableValueTypeInfos.length];
    smallTableValuePrimitiveTypeInfos = new PrimitiveTypeInfo[smallTableValueTypeInfos.length];
    for (int i = 0; i < smallTableValueTypeInfos.length; i++) {
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) smallTableValueTypeInfos[i];
      smallTableObjectInspectors[i] =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveTypeInfo);
      smallTablePrimitiveCategories[i] = primitiveTypeInfo.getPrimitiveCategory();
      smallTableValuePrimitiveTypeInfos[i] = primitiveTypeInfo;
    }
    smallTableObjectInspectorsList = Arrays.asList(smallTableObjectInspectors);

    bigTableStandardObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            bigTableColumnNamesList, Arrays.asList((ObjectInspector[]) bigTableObjectInspectors));
    smallTableStandardObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            smallTableValueColumnNamesList, Arrays.asList((ObjectInspector[]) smallTableObjectInspectors));

    inputObjectInspectors =
        new ObjectInspector[] { bigTableStandardObjectInspector, smallTableStandardObjectInspector };

    int outputLength =
        bigTableRetainColumnNums.length +
        smallTableRetainKeyColumnNums.length +
        smallTableRetainValueColumnNums.length;
    outputColumnNames = createOutputColumnNames(outputLength);

    outputTypeInfos = new TypeInfo[outputLength];
    int outputIndex = 0;
    for (int i = 0; i < bigTableRetainColumnNums.length; i++) {
      outputTypeInfos[outputIndex++] = bigTableTypeInfos[bigTableRetainColumnNums[i]];
    }
    // for (int i = 0; i < smallTableRetainKeyColumnNums.length; i++) {
    //   outputTypeInfos[outputIndex++] = smallTableTypeInfos[smallTableRetainKeyColumnNums[i]];
    // }
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

  public void trimAwaySmallTableValueInfo() {
    smallTableValueColumnNames = new String[] {};
    smallTableValueTypeInfos = new TypeInfo[] {};
    smallTableRetainKeyColumnNums = new int[] {};
    smallTableRetainValueColumnNums = new int[] {};
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