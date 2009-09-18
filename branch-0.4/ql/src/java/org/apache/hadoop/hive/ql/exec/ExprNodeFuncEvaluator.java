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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.util.ReflectionUtils;

public class ExprNodeFuncEvaluator extends ExprNodeEvaluator {

  private static final Log LOG = LogFactory.getLog(ExprNodeFuncEvaluator.class.getName());
  
  protected exprNodeFuncDesc expr;
  transient ExprNodeEvaluator[] paramEvaluators;
  transient ObjectInspector[] paramInspectors;
  transient boolean[] paramIsPrimitiveWritable;
  transient Object[] paramValues;
  transient UDF udf;
  transient Method udfMethod;
  
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
    paramInspectors  = new ObjectInspector[paramNumber];
    paramIsPrimitiveWritable = new boolean[paramNumber];
    for(int i=0; i<paramNumber; i++) {
      paramEvaluators[i] = ExprNodeEvaluatorFactory.get(expr.getChildExprs().get(i));
      paramIsPrimitiveWritable[i] = PrimitiveObjectInspectorUtils
          .isPrimitiveWritableClass(udfMethod.getParameterTypes()[i]);
    }
    paramValues = new Object[expr.getChildren().size()];
  }

  @Override
  public ObjectInspector initialize(ObjectInspector rowInspector)
    throws HiveException {
    for (int i=0; i<paramEvaluators.length; i++) {
      paramInspectors[i] = paramEvaluators[i].initialize(rowInspector);
    }
    
    // The return type of a function can be either Java Primitive or Writable.
    if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(
        udfMethod.getReturnType())) {
      PrimitiveCategory pc = PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveWritableClass(udfMethod.getReturnType())
          .primitiveCategory;
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(pc);
    } else {
      PrimitiveCategory pc = PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaClass(udfMethod.getReturnType())
          .primitiveCategory;
      return PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(pc);
    }
  }
  
  @Override
  public Object evaluate(Object row) throws HiveException {

    // Evaluate all children first
    for(int i=0; i<paramEvaluators.length; i++) {
      
      Object thisParam = paramEvaluators[i].evaluate(row);
      Category c = paramInspectors[i].getCategory();
      
      // TODO: Both getList and getMap are not very efficient.
      // We should convert UDFSize and UDFIsNull to ExprNodeEvaluator. 
      switch(c) {
        case LIST: {
          // Need to pass a Java List for List type
          paramValues[i] = ((ListObjectInspector)paramInspectors[i])
              .getList(thisParam);
          break;
        }
        case MAP: {
          // Need to pass a Java Map for Map type
          paramValues[i] = ((MapObjectInspector)paramInspectors[i])
              .getMap(thisParam);
          break;
        }
        case PRIMITIVE: {
          PrimitiveObjectInspector poi = (PrimitiveObjectInspector)paramInspectors[i];
          paramValues[i] = (paramIsPrimitiveWritable[i]
              ? poi.getPrimitiveWritableObject(thisParam)
              : poi.getPrimitiveJavaObject(thisParam));
          break;
        }
        default: {
          // STRUCT
          paramValues[i] = thisParam;
        }
      }
    }
    
    return FunctionRegistry.invoke(udfMethod, udf, paramValues);
  }

}
