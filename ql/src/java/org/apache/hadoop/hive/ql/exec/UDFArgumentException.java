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
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * exception class, thrown when udf argument have something wrong.
 */
public class UDFArgumentException extends SemanticException {

  public UDFArgumentException() {
    super();
  }

  public UDFArgumentException(String message) {
    super(message);
  }

  public UDFArgumentException(Throwable cause) {
    super(cause);
  }

  
  /**
   * Constructor.
   * 
   * @param funcClass
   *          The UDF or UDAF class.
   * @param argTypeInfos
   *          The list of argument types that lead to an ambiguity.
   * @param methods
   *          All potential matches.
   */
  public UDFArgumentException(String message,
      Class<?> funcClass,
      List<TypeInfo> argTypeInfos,
      List<Method> methods) {
    super(getMessage(message, funcClass, argTypeInfos, methods));
    this.funcClass = funcClass;
    this.argTypeInfos = argTypeInfos;
    this.methods = methods;
  }
  
  private static String getMessage(String message, 
      Class<?> funcClass,
      List<TypeInfo> argTypeInfos,
      List<Method> methods) {
    StringBuilder sb = new StringBuilder();
    sb.append(message);
    if (methods != null) {
      // Sort the methods before omitting them.
      sortMethods(methods);
      sb.append(". Possible choices: ");
      for (Method m: methods) {
        Type[] types = m.getGenericParameterTypes();
        sb.append("_FUNC_(");
        List<String> typeNames = new ArrayList<String>(types.length);
        for (int t = 0; t < types.length; t++) {
          if (t > 0) {
            sb.append(", ");
          }
          sb.append(ObjectInspectorUtils.getTypeNameFromJavaClass(types[t]));
        }
        sb.append(")  ");
      }
    }
    return sb.toString();
  }
  
  private static void sortMethods(List<Method> methods) {
    Collections.sort( methods, new Comparator<Method>(){

      @Override
      public int compare(Method m1, Method m2) {
        int result = m1.getName().compareTo(m2.getName());
        if (result != 0)
          return result;
        Type[] types1 = m1.getGenericParameterTypes();
        Type[] types2 = m2.getGenericParameterTypes();
        for (int i = 0; i < types1.length && i < types2.length; i++) {
          String type1 = ObjectInspectorUtils.getTypeNameFromJavaClass(types1[i]);
          String type2 = ObjectInspectorUtils.getTypeNameFromJavaClass(types2[i]);
          if ((result = type1.compareTo(type2)) != 0)
            return result;
        }
        return types1.length - types2.length;
      }

    });
  }
  
  /**
   * The UDF or UDAF class that has the ambiguity.
   */
  private Class<?> funcClass;

  /**
   * The list of parameter types.
   */
  private List<TypeInfo> argTypeInfos;
  
  /**
   * The list of matched methods.
   */
  private List<Method> methods;

  public Class<?> getFunctionClass() {
    return funcClass;
  }

  public List<TypeInfo> getArgTypeList() {
    return argTypeInfos;
  }
  
  public List<Method> getMethods() {
    return methods;
  }
  
}
