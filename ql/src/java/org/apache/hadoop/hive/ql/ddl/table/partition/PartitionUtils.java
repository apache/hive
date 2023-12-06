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

package org.apache.hadoop.hive.ql.ddl.table.partition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Utilities for partition related DDL operations.
 */
public final class PartitionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionUtils.class);

  private PartitionUtils() {
    throw new UnsupportedOperationException("PartitionUtils should not be instantiated");
  }

  /**
   * Certain partition values are are used by hive. e.g. the default partition in dynamic partitioning and the
   * intermediate partition values used in the archiving process. Naturally, prohibit the user from creating partitions
   * with these reserved values. The check that this function is more restrictive than the actual limitation, but it's
   * simpler. Should be okay since the reserved names are fairly long and uncommon.
   */
  public static void validatePartitions(HiveConf conf, Map<String, String> partitionSpec) throws SemanticException {
    Set<String> reservedPartitionValues = new HashSet<>();
    // Partition can't have this name
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.DEFAULT_PARTITION_NAME));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME));
    // Partition value can't end in this suffix
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_ORIGINAL));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_ARCHIVED));
    reservedPartitionValues.add(HiveConf.getVar(conf, ConfVars.METASTORE_INT_EXTRACTED));

    for (Entry<String, String> e : partitionSpec.entrySet()) {
      for (String s : reservedPartitionValues) {
        String value = e.getValue();
        if (value != null && value.contains(s)) {
          throw new SemanticException(ErrorMsg.RESERVED_PART_VAL.getMsg(
              "(User value: " + e.getValue() + " Reserved substring: " + s + ")"));
        }
      }
    }
  }

  public static ExprNodeGenericFuncDesc makeBinaryPredicate(String fn, ExprNodeDesc left, ExprNodeDesc right)
      throws SemanticException {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getFunctionInfo(fn).getGenericUDF(), Lists.newArrayList(left, right));
  }

  public static ExprNodeGenericFuncDesc makeUnaryPredicate(String fn, ExprNodeDesc arg) throws SemanticException {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getFunctionInfo(fn).getGenericUDF(), Lists.newArrayList(arg));
  }

  public static Partition getPartition(Hive db, Table table, Map<String, String> partitionSpec, boolean throwException)
      throws SemanticException {
    Partition partition;
    try {
      partition = db.getPartition(table, partitionSpec, false);
    } catch (Exception e) {
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partitionSpec), e);
    }
    if (partition == null && throwException) {
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partitionSpec));
    }
    return partition;
  }

  public static List<Partition> getPartitions(Hive db, Table table, Map<String, String> partitionSpec,
      boolean throwException) throws SemanticException {
    List<Partition> partitions;
    try {
      partitions = partitionSpec == null ? db.getPartitions(table) : db.getPartitions(table, partitionSpec);
    } catch (Exception e) {
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partitionSpec), e);
    }
    if (partitions.isEmpty() && throwException) {
      throw new SemanticException(toMessage(ErrorMsg.INVALID_PARTITION, partitionSpec));
    }
    return partitions;
  }

  private static String toMessage(ErrorMsg message, Object detail) {
    return detail == null ? message.getMsg() : message.getMsg(detail.toString());
  }

  /**
   * Add the table partitions to be modified in the output, so that it is available for the pre-execution hook.
   */
  public static void addTablePartsOutputs(Hive db, Set<WriteEntity> outputs, Table table,
      List<Map<String, String>> partitionSpecs, boolean allowMany, WriteEntity.WriteType writeType)
          throws SemanticException {
    for (Map<String, String> partitionSpec : partitionSpecs) {
      List<Partition> parts = null;
      if (allowMany) {
        try {
          parts = db.getPartitions(table, partitionSpec);
        } catch (HiveException e) {
          LOG.error("Got HiveException during obtaining list of partitions", e);
          throw new SemanticException(e.getMessage(), e);
        }
      } else {
        parts = new ArrayList<Partition>();
        try {
          Partition p = db.getPartition(table, partitionSpec, false);
          if (p != null) {
            parts.add(p);
          }
        } catch (HiveException e) {
          LOG.debug("Wrong specification", e);
          throw new SemanticException(e.getMessage(), e);
        }
      }
      for (Partition p : parts) {
        // Don't request any locks here, as the table has already been locked.
        outputs.add(new WriteEntity(p, writeType));
      }
    }
  }
}
