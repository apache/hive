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

package org.apache.hadoop.hive.ql.ddl.table.partition.alter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Operation process of altering a partition to a table.
 */
public class AlterTableAlterPartitionOperation extends DDLOperation<AlterTableAlterPartitionDesc> {
  public AlterTableAlterPartitionOperation(DDLOperationContext context, AlterTableAlterPartitionDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Table tbl = context.getDb().getTable(desc.getTableName(), true);

    check(tbl);
    setNewPartitionKeys(tbl);
    alterTable(tbl);

    return 0;
  }

  private void check(Table tbl) throws HiveException {
    assert(tbl.isPartitioned());
    try {
      int colIndex = getColumnIndex(tbl);
      checkPartitionValues(tbl, colIndex);
    } catch(Exception e) {
      throw new HiveException("Exception while checking type conversion of existing partition values to " +
          desc.getPartKeySpec() + " : " + e.getMessage());
    }
  }

  private int getColumnIndex(Table tbl) throws HiveException {
    int colIndex = -1;
    for (FieldSchema col : tbl.getTTable().getPartitionKeys()) {
      colIndex++;
      if (col.getName().compareTo(desc.getPartKeyName()) == 0) {
        return colIndex;
      }
    }

    throw new HiveException("Cannot find partition column " + desc.getPartKeyName());
  }

  /**
   * Check if the existing partition values can be type casted to the new column type
   * with a non null value before trying to alter the partition column type.
   */
  private void checkPartitionValues(Table tbl, int colIndex) throws HiveException {
    TypeInfo expectedType = TypeInfoUtils.getTypeInfoFromTypeString(desc.getPartKeyType());
    ObjectInspector outputOI = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(expectedType);
    Converter converter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);

    List<String> partNames = context.getDb().getPartitionNames(tbl.getDbName(),
        tbl.getTableName(), (short) -1);
    for (String partName : partNames) {
      try {
        List<String> values = Warehouse.getPartValuesFromPartName(partName);
        String value = values.get(colIndex);
        if (value.equals(context.getConf().getVar(HiveConf.ConfVars.DEFAULT_PARTITION_NAME))) {
          continue;
        }
        Object convertedValue = converter.convert(value);
        if (convertedValue == null) {
          throw new HiveException(" Converting from " + TypeInfoFactory.stringTypeInfo + " to " + expectedType +
              " for value : " + value + " resulted in NULL object");
        }
      } catch (Exception e) {
        throw new HiveException("Exception while converting " + TypeInfoFactory.stringTypeInfo + " to " +
            expectedType + " for partition : " + partName + ", index: " + colIndex);
      }
    }
  }

  private void setNewPartitionKeys(Table tbl) {
    List<FieldSchema> newPartitionKeys = new ArrayList<FieldSchema>();
    for (FieldSchema col : tbl.getTTable().getPartitionKeys()) {
      if (col.getName().compareTo(desc.getPartKeyName()) == 0) {
        newPartitionKeys.add(desc.getPartKeySpec());
      } else {
        newPartitionKeys.add(col);
      }
    }

    tbl.getTTable().setPartitionKeys(newPartitionKeys);
  }

  private void alterTable(Table tbl) throws HiveException {
    context.getDb().alterTable(tbl, false, null, true);
    context.getWork().getInputs().add(new ReadEntity(tbl));
    // We've already locked the table as the input, don't relock it as the output.
    DDLUtils.addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK), context);
  }
}
