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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class PartExprEvalUtils {
  /**
   * Evaluate expression with partition columns
   *
   * @param expr
   * @return value returned by the expression
   * @throws HiveException
   */
  static public Object evalExprWithPart(ExprNodeDesc expr, Partition p) throws HiveException {
    Map<String, String> partSpec = p.getSpec();
    Properties partProps = p.getSchema();
    
    boolean icebergTable = DDLUtils.isIcebergTable(p.getTable());
    String defaultPartitionName = HiveConf.getVar(SessionState.getSessionConf(),
        HiveConf.ConfVars.DEFAULT_PARTITION_NAME);

    List<String> partNames = new ArrayList<>();
    List<Object> partValues = new ArrayList<>();
    List<ObjectInspector> partObjectInspectors = new ArrayList<>();

    if (icebergTable && p.getTable().hasNonNativePartitionSupport()) {
      if (!populateIcebergPcrRow(expr, p, partSpec, defaultPartitionName, partNames, partValues,
          partObjectInspectors)) {
        return null;
      }
    } else {
      String[] partKeyTypes;
      if (p.getTable().hasNonNativePartitionSupport()) {
        if (!partSpec.keySet().containsAll(expr.getCols())) {
          return null;
        }
        partKeyTypes = p.getTable().getPartCols().stream().map(FieldSchema::getType)
            .toArray(String[]::new);
      } else {
        String pcolTypes = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
        partKeyTypes = pcolTypes.trim().split(":");
      }
      if (partSpec.size() != partKeyTypes.length) {
        throw new HiveException("Internal error : Partition Spec size, " + partSpec.size() +
            " doesn't match partition key definition size, " + partKeyTypes.length);
      }
      int i = 0;
      for (Map.Entry<String, String> entry : partSpec.entrySet()) {
        partNames.add(entry.getKey());
        String partitionValue = entry.getValue();
        ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector
            (TypeInfoFactory.getPrimitiveTypeInfo(partKeyTypes[i++]));
        if (partitionValue.equals(defaultPartitionName)) {
          partValues.add(null); // Null for default partition.
        } else {
          partValues.add(ObjectInspectorConverters.getConverter(
              PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi)
              .convert(partitionValue));
        }
        partObjectInspectors.add(oi);
      }
    }

    StructObjectInspector partObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(partNames, partObjectInspectors);

    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory
        .get(expr);
    ObjectInspector evaluateResultOI = evaluator
        .initialize(partObjectInspector);
    Object evaluateResultO = evaluator.evaluate(partValues);
    
    return ((PrimitiveObjectInspector) evaluateResultOI)
        .getPrimitiveJavaObject(evaluateResultO);
  }

  private static boolean populateIcebergPcrRow(ExprNodeDesc expr, Partition p, Map<String, String> pathSpec,
      String defaultPartitionName, List<String> partNames, List<Object> partValues,
      List<ObjectInspector> partObjectInspectors) throws HiveException {
    List<String> exprPartCols = expr.getCols();
    if (exprPartCols == null || exprPartCols.isEmpty()) {
      return false;
    }
    Table table = p.getTable();
    for (String colName : exprPartCols) {
      FieldSchema partCol = table.getPartColByName(colName);
      Integer pathIdx = null;
      if (partCol == null) {
        pathIdx = pathSpecKeyIndex(pathSpec, colName);
        if (pathIdx == null) {
          return false;
        }
        List<FieldSchema> partCols = table.getPartCols();
        if (pathIdx >= partCols.size()) {
          return false;
        }
        partCol = partCols.get(pathIdx);
      }
      String partitionValue = pathSpecValue(table, partCol, pathSpec, pathIdx);
      if (partitionValue == null) {
        return false;
      }
      partNames.add(colName);
      PrimitiveTypeInfo partTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(partCol.getType());
      ObjectInspector partOi =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(partTypeInfo);
      partObjectInspectors.add(partOi);
      if (partitionValue.equals(defaultPartitionName)) {
        partValues.add(null);
      } else {
        try {
          Object javaValue = table.getStorageHandler().parsePartitionLiteralForExpr(
              table, partCol, partitionValue);
          partValues.add(ObjectInspectorConverters.getConverter(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(partTypeInfo),
              partOi).convert(javaValue));
        } catch (SemanticException e) {
          throw new HiveException(e);
        }
      }
    }
    return true;
  }

  /** Index of a path segment key, or null if no case-insensitive match. */
  private static Integer pathSpecKeyIndex(Map<String, String> pathSpec, String key) {
    int idx = 0;
    for (String pathKey : pathSpec.keySet()) {
      if (pathKey.equalsIgnoreCase(key)) {
        return idx;
      }
      idx++;
    }
    return null;
  }

  /** Literal from a path spec for a logical partition column (handles renames). */
  private static String pathSpecValue(Table table, FieldSchema partCol, Map<String, String> pathSpec,
      Integer knownPathIdx) {
    for (Map.Entry<String, String> entry : pathSpec.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(partCol.getName())) {
        return entry.getValue();
      }
    }
    int idx = knownPathIdx != null ? knownPathIdx : partColIndex(table, partCol);
    if (idx < 0) {
      return null;
    }
    int seg = 0;
    for (String pathKey : pathSpec.keySet()) {
      if (seg == idx) {
        return pathSpec.get(pathKey);
      }
      seg++;
    }
    return null;
  }

  private static int partColIndex(Table table, FieldSchema partCol) {
    List<FieldSchema> partCols = table.getPartCols();
    for (int i = 0; i < partCols.size(); i++) {
      if (partCols.get(i).getName().equalsIgnoreCase(partCol.getName())) {
        return i;
      }
    }
    return -1;
  }

  public static Pair<PrimitiveObjectInspector, ExprNodeEvaluator> prepareExpr(
      ExprNodeDesc expr, List<String> partColumnNames,
      List<PrimitiveTypeInfo> partColumnTypeInfos) throws HiveException {
    // Create the row object
    List<ObjectInspector> partObjectInspectors = new ArrayList<>();
    for (int i = 0; i < partColumnNames.size(); i++) {
      partObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
        partColumnTypeInfos.get(i)));
    }
    StructObjectInspector objectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(partColumnNames, partObjectInspectors);

    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(expr);
    ObjectInspector evaluateResultOI = evaluator.initialize(objectInspector);
    return Pair.of((PrimitiveObjectInspector)evaluateResultOI, evaluator);
  }

  static public Object evaluateExprOnPart(
      Pair<PrimitiveObjectInspector, ExprNodeEvaluator> pair, Object partColValues)
          throws HiveException {
    return pair.getLeft().getPrimitiveJavaObject(pair.getRight().evaluate(partColValues));
  }
}
