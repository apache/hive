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

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.util.ReflectionUtils;

public class ExprNodeFuncEvaluator extends ExprNodeEvaluator {

  private static final Log LOG = LogFactory.getLog(ExprNodeFuncEvaluator.class.getName());
  
  protected exprNodeFuncDesc expr;
  transient ExprNodeEvaluator[] paramEvaluators;
  transient InspectableObject[] paramInspectableObjects;
  transient boolean[] paramIsPrimitiveWritable;
  transient Object[] paramValues;
  transient UDF udf;
  transient Method udfMethod;
  transient ObjectInspector outputObjectInspector;
  
  public ExprNodeFuncEvaluator(exprNodeFuncDesc expr) {
    this.expr = expr;
    assert(expr != null);
    Class<?> c = expr.getUDFClass();
    udfMethod = expr.getUDFMethod();
    LOG.debug(c.toString());
    LOG.debug(udfMethod.toString());
    udf = (UDF)ReflectionUtils.newInstance(expr.getUDFClass(), null);
    int paramNumber = expr.getChildren().size();
    paramEvaluators = new ExprNodeEvaluator[paramNumber];
    paramInspectableObjects  = new InspectableObject[paramNumber];
    paramIsPrimitiveWritable = new boolean[paramNumber];
    for(int i=0; i<paramNumber; i++) {
      paramEvaluators[i] = ExprNodeEvaluatorFactory.get(expr.getChildExprs().get(i));
      paramInspectableObjects[i] = new InspectableObject();
      paramIsPrimitiveWritable[i] = PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(udfMethod.getParameterTypes()[i]);
    }
    paramValues = new Object[expr.getChildren().size()];
    // The return type of a function can be of either Java Primitive Type/Class or Writable Class.
    if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(udfMethod.getReturnType())) {
      outputObjectInspector = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveWritableClass(udfMethod.getReturnType()).primitiveCategory);
    } else {
      outputObjectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJavaClass(udfMethod.getReturnType()).primitiveCategory);
    }
  }

  public void evaluate(Object row, ObjectInspector rowInspector,
      InspectableObject result) throws HiveException {
    if (result == null) {
      throw new HiveException("result cannot be null.");
    }
    // Evaluate all children first
    for(int i=0; i<paramEvaluators.length; i++) {
      paramEvaluators[i].evaluate(row, rowInspector, paramInspectableObjects[i]);
      Category c = paramInspectableObjects[i].oi.getCategory();
      // TODO: Both getList and getMap are not very efficient.
      // We should convert them to UDFTemplate - UDFs that accepts Object with 
      // ObjectInspectors when needed.
      switch(c) {
        case LIST: {
          // Need to pass a Java List for List type
          paramValues[i] = ((ListObjectInspector)paramInspectableObjects[i].oi)
              .getList(paramInspectableObjects[i].o);
          break;
        }
        case MAP: {
          // Need to pass a Java Map for Map type
          paramValues[i] = ((MapObjectInspector)paramInspectableObjects[i].oi)
              .getMap(paramInspectableObjects[i].o);
          break;
        }
        case PRIMITIVE: {
          PrimitiveObjectInspector poi = (PrimitiveObjectInspector)paramInspectableObjects[i].oi;
          paramValues[i] = (paramIsPrimitiveWritable[i]
              ? poi.getPrimitiveWritableObject(paramInspectableObjects[i].o)
              : poi.getPrimitiveJavaObject(paramInspectableObjects[i].o));
          break;
        }
        default: {
          // STRUCT
          paramValues[i] = paramInspectableObjects[i].o;
        }
      }
    }
    result.o = FunctionRegistry.invoke(udfMethod, udf, paramValues);
    result.oi = outputObjectInspector;
  }

  public ObjectInspector evaluateInspector(ObjectInspector rowInspector)
      throws HiveException {
    return outputObjectInspector;
  }
}
