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

package org.apache.hadoop.hive.ql.ddl.table.partition.show;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Operation process of showing the partitions of a table.
 */
public class ShowPartitionsOperation extends DDLOperation<ShowPartitionsDesc> {
  public ShowPartitionsOperation(DDLOperationContext context, ShowPartitionsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Table tbl = context.getDb().getTable(desc.getTabName());
    List<String> parts;
    if (tbl.isNonNative() && tbl.getStorageHandler().supportsPartitionTransform()) {
      parts = tbl.getStorageHandler().showPartitions(context, tbl);
    } else if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, desc.getTabName());
    } else if (desc.getCond() != null || desc.getOrder() != null) {
      parts = getPartitionNames(tbl);
    } else if (desc.getPartSpec() != null) {
      parts = context.getDb().getPartitionNames(tbl.getDbName(), tbl.getTableName(),
          desc.getPartSpec(), desc.getLimit());
    } else {
      parts = context.getDb().getPartitionNames(tbl.getDbName(), tbl.getTableName(), desc.getLimit());
    }

    // write the results in the file
    try (DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      ShowPartitionsFormatter formatter = ShowPartitionsFormatter.getFormatter(context.getConf());
      formatter.showTablePartitions(outStream, parts);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show partitions for table " + desc.getTabName());
    }

    return 0;
  }

  // Get partition names if order or filter is specified.
  private List<String> getPartitionNames(Table tbl) throws HiveException {
    List<String> partNames;
    ExprNodeDesc predicate = desc.getCond();
    if (desc.getPartSpec() != null) {
      List<FieldSchema> fieldSchemas = tbl.getPartitionKeys();
      Map<String, String> colTypes = new HashMap<String, String>();
      for (FieldSchema fs : fieldSchemas) {
        colTypes.put(fs.getName().toLowerCase(), fs.getType());
      }
      for (Map.Entry<String, String> entry : desc.getPartSpec().entrySet()) {
        String type = colTypes.get(entry.getKey().toLowerCase());
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        Object val = entry.getValue();
        if (!pti.equals(TypeInfoFactory.stringTypeInfo)) {
          Object converted = ObjectInspectorConverters.getConverter(
              TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoFactory.stringTypeInfo),
              TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(pti))
              .convert(val);
          if (converted == null) {
            throw new HiveException("Cannot convert to " + type + " from string, value: " + val);
          }
          val = converted;
        }
        List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
        children.add(new ExprNodeColumnDesc(pti, entry.getKey().toLowerCase(), null, true));
        children.add(new ExprNodeConstantDesc(pti, val));
        ExprNodeDesc exprNodeDesc = ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPEqual(), children);
        predicate = (predicate == null) ? exprNodeDesc :
            ExprNodeDescUtils.mergePredicates(exprNodeDesc, predicate);
      }
    }

    partNames = context.getDb().getPartitionNames(tbl, (ExprNodeGenericFuncDesc) predicate,
        desc.getOrder(), desc.getLimit());
    return partNames;
  }

}

