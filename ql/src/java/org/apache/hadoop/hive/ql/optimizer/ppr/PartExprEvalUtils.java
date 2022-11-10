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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
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
   * @param rowObjectInspector
   * @return value returned by the expression
   * @throws HiveException
   */
  static public Object evalExprWithPart(ExprNodeDesc expr,
      Partition p, List<VirtualColumn> vcs,
      StructObjectInspector rowObjectInspector) throws HiveException {
    LinkedHashMap<String, String> partSpec = p.getSpec();
    Properties partProps = p.getSchema();
    String pcolTypes = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
    String[] partKeyTypes = pcolTypes.trim().split(":");

    if (partSpec.size() != partKeyTypes.length) {
        throw new HiveException("Internal error : Partition Spec size, " + partSpec.size() +
                " doesn't match partition key definition size, " + partKeyTypes.length);
    }
    boolean hasVC = vcs != null && !vcs.isEmpty();
    Object[] rowWithPart = new Object[hasVC ? 3 : 2];
    // Create the row object
    ArrayList<String> partNames = new ArrayList<String>();
    ArrayList<Object> partValues = new ArrayList<Object>();
    ArrayList<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
    int i=0;
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

    rowWithPart[1] = partValues;
    ArrayList<StructObjectInspector> ois = new ArrayList<StructObjectInspector>(2);
    ois.add(rowObjectInspector);
    ois.add(partObjectInspector);
    if (hasVC) {
      ois.add(VirtualColumn.getVCSObjectInspector(vcs));
    }
    StructObjectInspector rowWithPartObjectInspector = ObjectInspectorFactory
        .getUnionStructObjectInspector(ois);

    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory
        .get(expr);
    ObjectInspector evaluateResultOI = evaluator
        .initialize(rowWithPartObjectInspector);
    Object evaluateResultO = evaluator.evaluate(rowWithPart);

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
