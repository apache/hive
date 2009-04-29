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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeIndexDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;

/**
 * This class can evaluate index operators on both list(array) and map.
 */
public class ExprNodeIndexEvaluator extends ExprNodeEvaluator {

  protected exprNodeIndexDesc expr;
  transient ExprNodeEvaluator mainEvaluator;
  transient ExprNodeEvaluator indexEvaluator;

  transient ObjectInspector mainInspector;
  transient PrimitiveObjectInspector indexInspector;
  
  public ExprNodeIndexEvaluator(exprNodeIndexDesc expr) {
    this.expr = expr;
    mainEvaluator = ExprNodeEvaluatorFactory.get(expr.getDesc());
    indexEvaluator = ExprNodeEvaluatorFactory.get(expr.getIndex());
  }

  public ObjectInspector initialize(ObjectInspector rowInspector)
      throws HiveException {
    mainInspector = mainEvaluator.initialize(rowInspector);
    indexInspector = (PrimitiveObjectInspector)indexEvaluator.initialize(rowInspector);
    
    if (mainInspector.getCategory() == Category.LIST) {
      return ((ListObjectInspector)mainInspector).getListElementObjectInspector();
    } else if (mainInspector.getCategory() == Category.MAP) {
      return ((MapObjectInspector)mainInspector).getMapValueObjectInspector();
    } else {
      // Should never happen because we checked this in SemanticAnalyzer.getXpathOrFuncExprNodeDesc
      throw new RuntimeException("Hive 2 Internal error: cannot evaluate index expression on "
          + mainInspector.getTypeName());
    }
  }
  
  public Object evaluate(Object row) throws HiveException {
    
    Object main = mainEvaluator.evaluate(row);
    Object index = indexEvaluator.evaluate(row);

    if (mainInspector.getCategory() == Category.LIST) {
      int intIndex = PrimitiveObjectInspectorUtils.getInt(index, indexInspector);
      ListObjectInspector loi = (ListObjectInspector)mainInspector;
      return loi.getListElement(main, intIndex);
    } else if (mainInspector.getCategory() == Category.MAP) {
      MapObjectInspector moi = (MapObjectInspector)mainInspector;
      Object indexObject;
      if (((PrimitiveObjectInspector)moi.getMapKeyObjectInspector()).isWritable()) {
        indexObject = indexInspector.getPrimitiveWritableObject(index);
      } else {
        indexObject = indexInspector.getPrimitiveJavaObject(index);
      }
      return moi.getMapValueElement(main, indexObject);
    }
    else {
      // Should never happen because we checked this in SemanticAnalyzer.getXpathOrFuncExprNodeDesc
      throw new RuntimeException("Hive 2 Internal error: cannot evaluate index expression on "
          + mainInspector.getTypeName());
    }
  }

}
