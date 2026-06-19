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

package org.apache.hive.benchmark.vectorization.mapjoin;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters.ValueOption;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public abstract class MapJoinOneLongKeyBenchBase extends AbstractMapJoin {
  
  public void doSetup(VectorMapJoinVariation vectorMapJoinVariation,
      MapJoinTestImplementation mapJoinImplementation) throws Exception {
    
    HiveConf hiveConf = new HiveConf();

    long seed = 2543;

    int rowCount = 10000000;  // 10,000,000.

    String[] bigTableColumnNames = new String[] {"number1"};
    TypeInfo[] bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.longTypeInfo};
    int[] bigTableKeyColumnNums = new int[] {0};

    String[] smallTableValueColumnNames = new String[] {"sv1", "sv2"};
    TypeInfo[] smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.dateTypeInfo, TypeInfoFactory.stringTypeInfo};

    int[] bigTableRetainColumnNums = new int[] {0};

    int[] smallTableRetainKeyColumnNums = new int[] {};
    int[] smallTableRetainValueColumnNums = new int[] {0, 1};

    SmallTableGenerationParameters smallTableGenerationParameters = new SmallTableGenerationParameters();
    smallTableGenerationParameters.setValueOption(ValueOption.ONLY_ONE);

    setupMapJoin(hiveConf, seed, rowCount,
        vectorMapJoinVariation, mapJoinImplementation,
        bigTableColumnNames, bigTableTypeInfos,
        bigTableKeyColumnNums,
        smallTableValueColumnNames, smallTableValueTypeInfos,
        bigTableRetainColumnNums,
        smallTableRetainKeyColumnNums, smallTableRetainValueColumnNums,
        smallTableGenerationParameters);
  }
}