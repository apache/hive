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
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
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

  private static final Logger LOG = LoggerFactory.getLogger(HashTableLoader.class.getName());

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

    boolean isCrossProduct = false;
    List<ExprNodeDesc> joinExprs = desc.getKeys().values().iterator().next();
    if (joinExprs.size() == 0) {
      isCrossProduct = true;
    }

    boolean useOptimizedTables = HiveConf.getBoolVar(
        hconf, HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDTABLE);
    boolean useHybridGraceHashJoin = desc.isHybridHashJoin();
    boolean isFirstKey = true;

    // Get the total available memory from memory manager
    long totalMapJoinMemory = desc.getMemoryNeeded();
    LOG.info("Memory manager allocates " + totalMapJoinMemory + " bytes for the loading hashtable.");
    if (totalMapJoinMemory <= 0) {
      totalMapJoinMemory = HiveConf.getLongVar(
        hconf, HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);
    }

    long processMaxMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    if (totalMapJoinMemory > processMaxMemory) {
      float hashtableMemoryUsage = HiveConf.getFloatVar(
          hconf, HiveConf.ConfVars.HIVEHASHTABLEFOLLOWBYGBYMAXMEMORYUSAGE);
      LOG.warn("totalMapJoinMemory value of " + totalMapJoinMemory +
          " is greater than the max memory size of " + processMaxMemory);
      // Don't want to attempt to grab more memory than we have available .. percentage is a bit arbitrary
      totalMapJoinMemory = (long) (processMaxMemory * hashtableMemoryUsage);
    }

    // Only applicable to n-way Hybrid Grace Hash Join
    HybridHashTableConf nwayConf = null;
    long totalSize = 0;
    int biggest = 0;                                // position of the biggest small table
    Map<Integer, Long> tableMemorySizes = null;
    if (useHybridGraceHashJoin && mapJoinTables.length > 2) {
      // Create a Conf for n-way HybridHashTableContainers
      nwayConf = new HybridHashTableConf();
      LOG.info("N-way join: " + (mapJoinTables.length - 1) + " small tables.");

      // Find the biggest small table; also calculate total data size of all small tables
      long maxSize = Long.MIN_VALUE; // the size of the biggest small table
      for (int pos = 0; pos < mapJoinTables.length; pos++) {
        if (pos == desc.getPosBigTable()) {
          continue;
        }
        long smallTableSize = desc.getParentDataSizes().get(pos);
        totalSize += smallTableSize;
        if (maxSize < smallTableSize) {
          maxSize = smallTableSize;
          biggest = pos;
        }
      }

      tableMemorySizes = divideHybridHashTableMemory(mapJoinTables, desc,
          totalSize, totalMapJoinMemory);
      // Using biggest small table, calculate number of partitions to create for each small table
      long memory = tableMemorySizes.get(biggest);
      int numPartitions = 0;
      try {
        numPartitions = HybridHashTableContainer.calcNumPartitions(memory, maxSize,
            HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINNUMPARTITIONS),
            HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINWBSIZE));
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
            memory = tableMemorySizes.get(pos);
          } else {  // binary join
            memory = totalMapJoinMemory;
          }
        }

        MapJoinTableContainer tableContainer;
        if (useOptimizedTables) {
          if (!useHybridGraceHashJoin || isCrossProduct) {
            tableContainer = new MapJoinBytesTableContainer(hconf, valCtx, keyCount, 0);
          } else {
            tableContainer = new HybridHashTableContainer(hconf, keyCount, memory,
                desc.getParentDataSizes().get(pos), nwayConf);
          }
        } else {
          tableContainer = new HashMapWrapper(hconf, keyCount);
        }

        LOG.info("Using tableContainer " + tableContainer.getClass().getSimpleName());

        tableContainer.setSerde(keyCtx, valCtx);
        while (kvReader.next()) {
          tableContainer.putRow(
              (Writable)kvReader.getCurrentKey(), (Writable)kvReader.getCurrentValue());
        }
        tableContainer.seal();
        LOG.info("Finished loading hashtable using " + tableContainer.getClass() + ". Small table position: " + pos);
        mapJoinTables[pos] = tableContainer;
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }

  private static Map<Integer, Long> divideHybridHashTableMemory(
      MapJoinTableContainer[] mapJoinTables, MapJoinDesc desc,
      long totalSize, long totalHashTableMemory) {
    int smallTableCount = Math.max(mapJoinTables.length - 1, 1);
    Map<Integer, Long> tableMemorySizes = new HashMap<Integer, Long>();
    // If any table has bad size estimate, we need to fall back to sizing each table equally
    boolean fallbackToEqualProportions = totalSize <= 0;

    if (!fallbackToEqualProportions) {
      for (Map.Entry<Integer, Long> tableSizeEntry : desc.getParentDataSizes().entrySet()) {
        if (tableSizeEntry.getKey() == desc.getPosBigTable()) {
          continue;
        }

        long tableSize = tableSizeEntry.getValue();
        if (tableSize <= 0) {
          fallbackToEqualProportions = true;
          break;
        }
        float percentage = (float) tableSize / totalSize;
        long tableMemory = (long) (totalHashTableMemory * percentage);
        tableMemorySizes.put(tableSizeEntry.getKey(), tableMemory);
      }
    }

    if (fallbackToEqualProportions) {
      // Just give each table the same amount of memory.
      long equalPortion = totalHashTableMemory / smallTableCount;
      for (Integer pos : desc.getParentDataSizes().keySet()) {
        if (pos == desc.getPosBigTable()) {
          break;
        }
        tableMemorySizes.put(pos, equalPortion);
      }
    }

    return tableMemorySizes;
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
