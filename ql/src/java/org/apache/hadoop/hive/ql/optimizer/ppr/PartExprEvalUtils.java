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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
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
    LinkedHashMap<String, String> partSpec = p.getSpec();
    Properties partProps = p.getSchema();
    
    String[] partKeyTypes;
    if (p.getTable().hasNonNativePartitionSupport()) {
      if (!partSpec.keySet().containsAll(expr.getCols())) {
        return null;
      }
      partKeyTypes = p.getTable().getStorageHandler().getPartitionKeys(p.getTable()).stream()
          .map(FieldSchema::getType).toArray(String[]::new);
    } else {
      String pcolTypes = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
      partKeyTypes = pcolTypes.trim().split(":");
    }
    
    if (partSpec.size() != partKeyTypes.length) {
      if (DDLUtils.isIcebergTable(p.getTable())) {
        return null;
      }
      throw new HiveException("Internal error : Partition Spec size, " + partSpec.size() +
          " doesn't match partition key definition size, " + partKeyTypes.length);
    }
    // Create the row object
    List<String> partNames = new ArrayList<>();
    List<Object> partValues = new ArrayList<>();
    List<ObjectInspector> partObjectInspectors = new ArrayList<>();
    int i = 0;
    for (Map.Entry<String, String> entry : partSpec.entrySet()) {
      partNames.add(entry.getKey());
      ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector
          (TypeInfoFactory.getPrimitiveTypeInfo(partKeyTypes[i++]));
      partValues.add(ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi)
          .convert(entry.getValue()));
      partObjectInspectors.add(oi);
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

  public static Pair<PrimitiveObjectInspector, ExprNodeEvaluator> prepareExpr(
      ExprNodeDesc expr, List<String> partColumnNames,
      List<PrimitiveTypeInfo> partColumnTypeInfos) throws HiveException {
    // Create the row object
    List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
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
