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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A simple generic udf to call java static functions via reflection.
 */
@Description(name = "reflect",
  value = "_FUNC_(class,method[,arg1[,arg2..]]) calls method with reflection",
  extended = "Use this UDF to call Java methods by matching the argument signature\n")
@UDFType(deterministic = false)
public class GenericUDFReflect extends GenericUDF {

  PrimitiveObjectInspector[] argumentOIs;
  StringObjectInspector classNameOI;
  StringObjectInspector methodNameOI;
  
  Class<?>[] parameterJavaClasses; // Classes are Integer, Double, String
  Class<?>[] parameterJavaTypes;   // Types are int, double, etc
  Object[] parameterJavaValues;
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
  throws UDFArgumentException {
    
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function GenericUDFReflect(class,method[,arg1[,arg2]...])"
          + " accepts 2 or more arguments.");
    }

    for (int i = 0; i < 2; i++) {
      if (!(arguments[i] instanceof StringObjectInspector)) {
        throw new UDFArgumentTypeException(i,
            "The first 2 parameters of GenericUDFReflect(class,method[,arg1[,arg2]...])"
            + " should be string.");
      }
    }
    
    classNameOI = (StringObjectInspector)
        ObjectInspectorUtils.getStandardObjectInspector(arguments[0]);
    methodNameOI = (StringObjectInspector)
        ObjectInspectorUtils.getStandardObjectInspector(arguments[1]);
    
    parameterJavaClasses = new Class[arguments.length - 2];
    parameterJavaTypes = new Class[arguments.length - 2];
    for (int i = 2; i < arguments.length; i++) {
      if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i,
            "The parameters of GenericUDFReflect(class,method[,arg1[,arg2]...])"
            + " must be primitive (int, double, string, etc).");
      }
      PrimitiveCategory category =
          ((PrimitiveObjectInspector)arguments[i]).getPrimitiveCategory();
      PrimitiveTypeEntry t =
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(category);
      parameterJavaClasses[i - 2] = t.primitiveJavaClass;
      parameterJavaTypes[i - 2] = t.primitiveJavaType;
    }
    
    parameterJavaValues = new Object[arguments.length - 2];

    argumentOIs = new PrimitiveObjectInspector[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      argumentOIs[i] = (PrimitiveObjectInspector)arguments[i];
    }
    
    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
        PrimitiveCategory.STRING);
  }

  Class<?> c;
  Object o;
  Method m;
  Object className;
  Object methodName;
  String result;
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    
    // Try to find the class
    // Skip class loading if the class name didn't change
    boolean classNameChanged = false;
    
    ObjectInspector newClassNameOI = argumentOIs[0];
    Object newClassName = arguments[0].get();
    
    // We compare class name/method name using ObjectInspectorUtils.compare(...), to avoid
    // any object conversion (which may cause object creation) in most cases, when the class
    // name/method name is constant Java String, or constant Text (StringWritable).
    if (className == null || ObjectInspectorUtils.compare(className, classNameOI, newClassName,
        newClassNameOI) != 0) {
      className = ObjectInspectorUtils.copyToStandardObject(newClassName, newClassNameOI);
      String classNameString = classNameOI.getPrimitiveJavaObject(className);
      try {
        c = Class.forName(classNameString);
      } catch (ClassNotFoundException ex) {
        throw new HiveException("UDFReflect evaluate ", ex);
      }
      try {
        o = null;
        o = ReflectionUtils.newInstance(c, null);
      } catch (Exception e) {
        // ignored
      }
      classNameChanged = true;
    }
    
    // Try to find the method
    // Skip method finding if the method name didn't change, and class name didn't change.
    ObjectInspector newMethodNameOI = argumentOIs[1];
    Object newMethodName = arguments[1].get();
    
    if (methodName == null || ObjectInspectorUtils.compare(methodName, methodNameOI, newMethodName,
        newMethodNameOI) != 0 || classNameChanged) {
      methodName = ObjectInspectorUtils.copyToStandardObject(newMethodName, newMethodNameOI);
      String methodNameString = methodNameOI.getPrimitiveJavaObject(methodName);
      try {
        m = c.getMethod(methodNameString, parameterJavaClasses);
      } catch (SecurityException e) {
        throw new HiveException("UDFReflect getMethod ", e);
      } catch (NoSuchMethodException e) {
        try {
          m = c.getMethod(methodNameString, parameterJavaTypes);
        } catch (SecurityException ex) {
          throw new HiveException("UDFReflect getMethod ", ex);
        } catch (NoSuchMethodException ex) {
          throw new HiveException("UDFReflect getMethod ", ex);
        }
      }
    }
    
    // Get the parameter values
    for (int i = 2; i < arguments.length; i++) {
      parameterJavaValues[i - 2] = argumentOIs[i].getPrimitiveJavaObject(arguments[i].get());
    }

    try {
      result = String.valueOf(m.invoke(o, parameterJavaValues));
      return result;
    } catch (IllegalArgumentException e1) {
      System.err.println("UDFReflect evaluate "+ e1 + " method = " + m + " args = " +
          Arrays.asList(parameterJavaValues));
    } catch (IllegalAccessException e1) {
      System.err.println("UDFReflect evaluate "+ e1 + " method = " + m + " args = " +
          Arrays.asList(parameterJavaValues));
    } catch (InvocationTargetException e1) {
      System.err.println("UDFReflect evaluate "+ e1 + " method = " + m + " args = " +
          Arrays.asList(parameterJavaValues));
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("reflect(");
    for (int i = 0; i < children.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(children[i]);
    }
    sb.append(')');
    return sb.toString();
  }
  
}
