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

package org.apache.hadoop.hive.ql.udf.generic;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;

/**
 * common class for reflective UDFs
 */
public abstract class AbstractGenericUDFReflect extends GenericUDF {

  private transient PrimitiveObjectInspector[] parameterOIs;
  private transient PrimitiveTypeEntry[] parameterTypes;
  private transient Class[] parameterClasses;

  private transient Object[] parameterJavaValues;

  void setupParameterOIs(ObjectInspector[] arguments, int start) throws UDFArgumentTypeException {
    int length = arguments.length - start;
    parameterOIs = new PrimitiveObjectInspector[length];
    parameterTypes = new PrimitiveTypeEntry[length];
    parameterClasses = new Class[length];
    parameterJavaValues = new Object[length];
    for (int i = 0; i < length; i++) {
      if (arguments[i + start].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i,
            "The parameters of GenericUDFReflect(class,method[,arg1[,arg2]...])"
            + " must be primitive (int, double, string, etc).");
      }
      parameterOIs[i] = (PrimitiveObjectInspector)arguments[i + start];
      parameterTypes[i] = PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(
          parameterOIs[i].getPrimitiveCategory());
      parameterClasses[i] = parameterTypes[i].primitiveJavaType == null ?
          parameterTypes[i].primitiveJavaClass : parameterTypes[i].primitiveJavaType;
    }
  }

  Object[] setupParameters(DeferredObject[] arguments, int start) throws HiveException {
      // Get the parameter values
    for (int i = 0; i < parameterOIs.length; i++) {
      Object argument = arguments[i + start].get();
      parameterJavaValues[i] = parameterOIs[i].getPrimitiveJavaObject(argument);
    }
    return parameterJavaValues;
  }

  // a(string,int,int) can be matched with methods like
  // a(string,int,int), a(string,int,Integer), a(string,Integer,int) and a(string,Integer,Integer)
  // and accepts the first one clazz.getMethods() returns
  Method findMethod(Class clazz, String name, Class<?> retType, boolean memberOnly)
      throws Exception {
    for (Method method : clazz.getMethods()) {
      if (!method.getName().equals(name) ||
          (retType != null && !retType.isAssignableFrom(method.getReturnType())) ||
          (memberOnly && Modifier.isStatic(method.getReturnType().getModifiers())) ||
          method.getParameterTypes().length != parameterTypes.length) {
        continue;
      }
      // returns first one matches all of the params
      boolean match = true;
      Class<?>[] types = method.getParameterTypes();
      for (int i = 0; i < parameterTypes.length; i++) {
        if (types[i] != parameterTypes[i].primitiveJavaType &&
            types[i] != parameterTypes[i].primitiveJavaClass &&
            !types[i].isAssignableFrom(parameterTypes[i].primitiveJavaClass)) {
          match = false;
          break;
        }
      }
      if (match) {
        return method;
      }
    }
    // tried all, back to original code (for error message)
    return clazz.getMethod(name, parameterClasses);
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(functionName(), children, ",");
  }

  protected abstract String functionName();
}
