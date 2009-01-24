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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * The class implements the method resolution for operators like (+, -, *, /, %, |, &, ^). The
 * resolution logic is as follows:
 * 1. The resolver first tries to find an exact parameter match.
 * 2. If 1 fails then it returns the evaluate(Double, Double) method.
 */
public class NumericOpMethodResolver implements UDFMethodResolver {

  /**
   * The udfclass for which resolution is needed.
   */
  Class<? extends UDF> udfClass;
  
  /**
   * Constuctor.
   */
  public NumericOpMethodResolver(Class<? extends UDF> udfClass) {
    this.udfClass = udfClass;
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.exec.UDFMethodResolver#getEvalMethod(java.util.List)
   */
  @Override
  public Method getEvalMethod(List<Class<?>> argClasses)
  throws AmbiguousMethodException {
    assert(argClasses.size() == 2);

    List<Class<?>> pClasses = null;
    if (argClasses.get(0) == Void.class ||
        argClasses.get(1) == Void.class) {
      pClasses = new ArrayList<Class<?>>();
      pClasses.add(Double.class);
      pClasses.add(Double.class);      
    } else if (argClasses.get(0) == String.class ||
        argClasses.get(1) == String.class) {
      pClasses = new ArrayList<Class<?>>();
      pClasses.add(Double.class);
      pClasses.add(Double.class);
    } else if (argClasses.get(0) == argClasses.get(1)) {
      pClasses = argClasses;
    } else {
      pClasses = new ArrayList<Class<?>>();
      pClasses.add(Double.class);
      pClasses.add(Double.class);
    }

    Method udfMethod = null;

    for(Method m: Arrays.asList(udfClass.getMethods())) {
      if (m.getName().equals("evaluate")) {

        Class<?>[] argumentTypeInfos = m.getParameterTypes();

        boolean match = (argumentTypeInfos.length == pClasses.size());

        for(int i=0; i<pClasses.size() && match; i++) {
          Class<?> accepted = ObjectInspectorUtils.generalizePrimitive(argumentTypeInfos[i]);
          if (accepted != pClasses.get(i)) {
            match = false;
          }
        }

        if (match) {
          if (udfMethod != null) {
            throw new AmbiguousMethodException(udfClass, argClasses);
          }
          else {
            udfMethod = m;
          }
        }
      }
    }
    return udfMethod;      
  }
}
