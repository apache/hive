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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * The class implements the method resolution for overloaded comparison operators. The
 * resolution logic is as follows:
 * 1. The resolver first tries to find an exact parameter match.
 * 2. If 1 fails and any of the parameters is a date, it converts the other to the date.
 * 3. If 1 and 3 fail then it returns the evaluate(Double, Double) method.
 */
public class ComparisonOpMethodResolver implements UDFMethodResolver {

  /**
   * The udfclass for which resolution is needed.
   */
  private Class<? extends UDF> udfClass;
  
  /**
   * Constuctor.
   */
  public ComparisonOpMethodResolver(Class<? extends UDF> udfClass) {
    this.udfClass = udfClass;
  }
  

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.exec.UDFMethodResolver#getEvalMethod(java.util.List)
   */
  @Override
  public Method getEvalMethod(List<TypeInfo> argTypeInfos)
      throws AmbiguousMethodException {
    assert(argTypeInfos.size() == 2);

    List<TypeInfo> pTypeInfos = null;
    if (argTypeInfos.get(0).equals(TypeInfoFactory.voidTypeInfo) ||
        argTypeInfos.get(1).equals(TypeInfoFactory.voidTypeInfo)) {
      pTypeInfos = new ArrayList<TypeInfo>();
      pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
      pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
    }
    else if (argTypeInfos.get(0) == argTypeInfos.get(1)) {
      pTypeInfos = argTypeInfos;
    }
    else {
      pTypeInfos = new ArrayList<TypeInfo>();
      pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
      pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
    }

    Method udfMethod = null;

    for(Method m: Arrays.asList(udfClass.getMethods())) {
      if (m.getName().equals("evaluate")) {

        List<TypeInfo> acceptedTypeInfos = TypeInfoUtils.getParameterTypeInfos(m);

        boolean match = (acceptedTypeInfos.size() == pTypeInfos.size());

        for(int i=0; i<pTypeInfos.size() && match; i++) {
          TypeInfo accepted = acceptedTypeInfos.get(i);
          if (accepted != pTypeInfos.get(i)) {
            match = false;
          }
        }

        if (match) {
          if (udfMethod != null) {
            throw new AmbiguousMethodException(udfClass, argTypeInfos);
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
