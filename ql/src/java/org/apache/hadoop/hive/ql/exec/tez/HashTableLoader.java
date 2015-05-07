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
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableConf;
import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * HashTableLoader for Tez constructs the hashtable from records read from
 * a broadcast edge.
 */
public class HashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Log LOG = LogFactory.getLog(HashTableLoader.class.getName());

  private Configuration hconf;
  private MapJoinDesc desc;
  private TezContext tezContext;

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext, Configuration hconf,
      MapJoinOperator joinOp) {
    this.tezContext = (TezContext) mrContext;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
  }

  @Override
  public void load(MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes)
      throws HiveException {

    Map<Integer, String> parentToInput = desc.getParentToInput();
    Map<Integer, Long> parentKeyCounts = desc.getParentKeyCounts();

    boolean useOptimizedTables = HiveConf.getBoolVar(
        hconf, HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDTABLE);
    boolean useHybridGraceHashJoin = desc.isHybridHashJoin();
    boolean isFirstKey = true;
    // TODO remove this after memory manager is in
    long noConditionalTaskThreshold = HiveConf.getLongVar(
        hconf, HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);

    // Only applicable to n-way Hybrid Grace Hash Join
    HybridHashTableConf nwayConf = null;
    long totalSize = 0;
    int biggest = 0;                                // position of the biggest small table
    if (useHybridGraceHashJoin && mapJoinTables.length > 2) {
      // Create a Conf for n-way HybridHashTableContainers
      nwayConf = new HybridHashTableConf();

      // Find the biggest small table; also calculate total data size of all small tables
      long maxSize = 0; // the size of the biggest small table
      for (int pos = 0; pos < mapJoinTables.length; pos++) {
        if (pos == desc.getPosBigTable()) {
          continue;
        }
        totalSize += desc.getParentDataSizes().get(pos);
        biggest = desc.getParentDataSizes().get(pos) > maxSize ? pos : biggest;
        maxSize = desc.getParentDataSizes().get(pos) > maxSize ? desc.getParentDataSizes().get(pos)
                                                               : maxSize;
      }

      // Using biggest small table, calculate number of partitions to create for each small table
      float percentage = (float) maxSize / totalSize;
      long memory = (long) (noConditionalTaskThreshold * percentage);
      int numPartitions = 0;
      try {
        numPartitions = HybridHashTableContainer.calcNumPartitions(memory,
            desc.getParentDataSizes().get(biggest),
            HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINNUMPARTITIONS),
            HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINWBSIZE),
            nwayConf);
      } catch (IOException e) {
        throw new HiveException(e);
      }
      nwayConf.setNumberOfPartitions(numPartitions);
    }

    for (int pos = 0; pos < mapJoinTables.length; pos++) {
      if (pos == desc.getPosBigTable()) {
        continue;
      }

      String inputName = parentToInput.get(pos);
      LogicalInput input = tezContext.getInput(inputName);

      try {
        input.start();
        tezContext.getTezProcessorContext().waitForAnyInputReady(
            Collections.<Input> singletonList(input));
      } catch (Exception e) {
        throw new HiveException(e);
      }

      try {
        KeyValueReader kvReader = (KeyValueReader) input.getReader();
        MapJoinObjectSerDeContext keyCtx = mapJoinTableSerdes[pos].getKeyContext(),
          valCtx = mapJoinTableSerdes[pos].getValueContext();
        if (useOptimizedTables) {
          ObjectInspector keyOi = keyCtx.getSerDe().getObjectInspector();
          if (!MapJoinBytesTableContainer.isSupportedKey(keyOi)) {
            if (isFirstKey) {
              useOptimizedTables = false;
              LOG.info(describeOi("Not using optimized hash table. " +
                           "Only a subset of mapjoin keys is supported. Unsupported key: ", keyOi));
            } else {
              throw new HiveException(describeOi(
                  "Only a subset of mapjoin keys is supported. Unsupported key: ", keyOi));
            }
          }
        }
        isFirstKey = false;
        Long keyCountObj = parentKeyCounts.get(pos);
        long keyCount = (keyCountObj == null) ? -1 : keyCountObj.longValue();

        long memory = 0;
        if (useHybridGraceHashJoin) {
          if (mapJoinTables.length > 2) {
            // Allocate n-way join memory proportionally
            float percentage = (float) desc.getParentDataSizes().get(pos) / totalSize;
            memory = (long) (noConditionalTaskThreshold * percentage);
          } else {  // binary join
            memory = noConditionalTaskThreshold;
          }
        }

        MapJoinTableContainer tableContainer = useOptimizedTables
            ? (useHybridGraceHashJoin ? new HybridHashTableContainer(hconf, keyCount,
                                            memory, desc.getParentDataSizes().get(pos), nwayConf)
                                      : new MapJoinBytesTableContainer(hconf, valCtx, keyCount, 0))
            : new HashMapWrapper(hconf, keyCount);
        LOG.info("Using tableContainer " + tableContainer.getClass().getSimpleName());

        while (kvReader.next()) {
          tableContainer.putRow(keyCtx, (Writable)kvReader.getCurrentKey(),
              valCtx, (Writable)kvReader.getCurrentValue());
        }
        tableContainer.seal();
        mapJoinTables[pos] = tableContainer;
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }

  private String describeOi(String desc, ObjectInspector keyOi) {
    for (StructField field : ((StructObjectInspector)keyOi).getAllStructFieldRefs()) {
      ObjectInspector oi = field.getFieldObjectInspector();
      String cat = oi.getCategory().toString();
      if (oi.getCategory() == Category.PRIMITIVE) {
        cat = ((PrimitiveObjectInspector)oi).getPrimitiveCategory().toString();
      }
      desc += field.getFieldName() + ":" + cat + ", ";
    }
    return desc;
  }
}
