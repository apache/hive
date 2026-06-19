/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.udf.example;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for testing only, will try to alter the partition's parameters with the input key value pairs.
 */
public class AlterPartitionParamsExample extends GenericUDF {

  private static final Logger LOG = LoggerFactory.getLogger(AlterPartitionParamsExample.class);

  private transient ObjectInspectorConverters.Converter[] converters;
  private transient BooleanWritable ret = new BooleanWritable(false);
  private transient Table table;

  // table, partition name, param_key, param_value
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 4) {
      throw new UDFArgumentLengthException(
          "Requires 4 argument, got " + arguments.length);
    }
    if (!(arguments[0] instanceof ConstantObjectInspector)) {
      throw new UDFArgumentException(
          "The first argument should be a string constant, got " + arguments[0].getTypeName());
    }
    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 1; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }
    String tableName = ((ConstantObjectInspector) arguments[0]).getWritableConstantValue().toString();
    try {
      table = Hive.get().getTable(tableName);
      if (!table.isPartitioned()) {
        throw new UDFArgumentException("The input table: " + table + " isn't a partitioned table!");
      }
    } catch (Exception e) {
      if (e instanceof UDFArgumentException) {
        throw (UDFArgumentException) e;
      }
      throw new UDFArgumentException(e);
    }
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    ret.set(false);
    if (arguments[1].get() == null || arguments[2].get() == null ||
        arguments[3].get() == null) {
      return ret;
    }
    String partName   = converters[1].convert(arguments[1].get()).toString();
    String paramKey   = converters[2].convert(arguments[2].get()).toString();
    String paramValue = converters[3].convert(arguments[3].get()).toString();
    try {
      List<Partition> partitionList = Hive.get()
          .getPartitionsByNames(table, Arrays.asList(partName));
      if (partitionList == null || partitionList.isEmpty()) {
        return ret;
      }
      Partition partition = partitionList.get(0);
      partition.getParameters().put(paramKey, paramValue);
      Hive.get()
          .alterPartition(table.getCatName(), table.getDbName(), table.getTableName(), partition, null, true);
      ret.set(true);
    } catch (Exception e) {
      LOG.debug("Error while altering the partition's parameters", e);
    }
    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("alter_partition_params", children);
  }

}
