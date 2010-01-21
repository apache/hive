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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * The class implements the method resolution for operators like (+, -, *, %).
 * The resolution logic is as follows:
 * 
 * 1. If one of the parameters is a string, then it resolves to evaluate(double,
 * double) 2. If one of the parameters is null, then it resolves to evaluate(T,
 * T) where T is the other non-null parameter type. 3. If both of the parameters
 * are null, then it resolves to evaluate(byte, byte) 4. Otherwise, it resolves
 * to evaluate(T, T), where T is the type resulting from calling
 * FunctionRegistry.getCommonClass() on the two arguments.
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

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.exec.UDFMethodResolver#getEvalMethod(java.util
   * .List)
   */
  @Override
  public Method getEvalMethod(List<TypeInfo> argTypeInfos)
      throws AmbiguousMethodException, UDFArgumentException {
    assert (argTypeInfos.size() == 2);

    List<TypeInfo> pTypeInfos = null;
    List<TypeInfo> modArgTypeInfos = new ArrayList<TypeInfo>();

    // If either argument is a string, we convert to a double because a number
    // in string form should always be convertible into a double
    if (argTypeInfos.get(0).equals(TypeInfoFactory.stringTypeInfo)
        || argTypeInfos.get(1).equals(TypeInfoFactory.stringTypeInfo)) {
      modArgTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
      modArgTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
    } else {
      // If it's a void, we change the type to a byte because once the types
      // are run through getCommonClass(), a byte and any other type T will
      // resolve to type T
      for (int i = 0; i < 2; i++) {
        if (argTypeInfos.get(i).equals(TypeInfoFactory.voidTypeInfo)) {
          modArgTypeInfos.add(TypeInfoFactory.byteTypeInfo);
        } else {
          modArgTypeInfos.add(argTypeInfos.get(i));
        }
      }
    }

    TypeInfo commonType = FunctionRegistry.getCommonClass(modArgTypeInfos
        .get(0), modArgTypeInfos.get(1));

    if (commonType == null) {
      throw new UDFArgumentException("Unable to find a common class between"
          + "types " + modArgTypeInfos.get(0).getTypeName() + " and "
          + modArgTypeInfos.get(1).getTypeName());
    }

    pTypeInfos = new ArrayList<TypeInfo>();
    pTypeInfos.add(commonType);
    pTypeInfos.add(commonType);

    Method udfMethod = null;

    for (Method m : Arrays.asList(udfClass.getMethods())) {
      if (m.getName().equals("evaluate")) {

        List<TypeInfo> argumentTypeInfos = TypeInfoUtils.getParameterTypeInfos(
            m, pTypeInfos.size());
        if (argumentTypeInfos == null) {
          // null means the method does not accept number of arguments passed.
          continue;
        }

        boolean match = (argumentTypeInfos.size() == pTypeInfos.size());

        for (int i = 0; i < pTypeInfos.size() && match; i++) {
          TypeInfo accepted = argumentTypeInfos.get(i);
          if (!accepted.equals(pTypeInfos.get(i))) {
            match = false;
          }
        }

        if (match) {
          if (udfMethod != null) {
            throw new AmbiguousMethodException(udfClass, argTypeInfos);
          } else {
            udfMethod = m;
          }
        }
      }
    }
    return udfMethod;
  }
}
