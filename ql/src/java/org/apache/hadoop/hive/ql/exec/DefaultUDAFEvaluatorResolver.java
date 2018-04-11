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

package org.apache.hadoop.hive.ql.exec;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * The default UDAF Method resolver. This resolver is used for resolving the
 * UDAF methods are used for partial and final evaluation given the list of the
 * argument types. The getEvalMethod goes through all the evaluate methods and
 * returns the one that matches the argument signature or is the closest match.
 * Closest match is defined as the one that requires the least number of
 * arguments to be converted. In case more than one matches are found, the
 * method throws an ambiguous method exception.
 */
public class DefaultUDAFEvaluatorResolver implements UDAFEvaluatorResolver {

  /**
   * The class of the UDAF.
   */
  private final Class<? extends UDAF> udafClass;

  /**
   * Constructor. This constructor sets the resolver to be used for comparison
   * operators. See {@link UDAFEvaluatorResolver}
   */
  public DefaultUDAFEvaluatorResolver(Class<? extends UDAF> udafClass) {
    this.udafClass = udafClass;
  }

  /**
   * Gets the evaluator class for the UDAF given the parameter types.
   * 
   * @param argClasses
   *          The list of the parameter types.
   */
  public Class<? extends UDAFEvaluator> getEvaluatorClass(
      List<TypeInfo> argClasses) throws UDFArgumentException {

    ArrayList<Class<? extends UDAFEvaluator>> classList =
        new ArrayList<Class<? extends UDAFEvaluator>>();

    // Add all the public member classes that implement an evaluator
    for (Class<?> enclClass : udafClass.getClasses()) {
      if (UDAFEvaluator.class.isAssignableFrom(enclClass)) {
        classList.add((Class<? extends UDAFEvaluator>) enclClass);
      }
    }

    // Next we locate all the iterate methods for each of these classes.
    ArrayList<Method> mList = new ArrayList<Method>();
    ArrayList<Class<? extends UDAFEvaluator>> cList =
        new ArrayList<Class<? extends UDAFEvaluator>>();
    for (Class<? extends UDAFEvaluator> evaluator : classList) {
      for (Method m : evaluator.getMethods()) {
        if (m.getName().equalsIgnoreCase("iterate")) {
          mList.add(m);
          cList.add(evaluator);
        }
      }
    }

    Method m = FunctionRegistry.getMethodInternal(udafClass, mList, false, argClasses);

    // Find the class that has this method.
    // Note that Method.getDeclaringClass() may not work here because the method
    // can be inherited from a base class.
    int found = -1;
    for (int i = 0; i < mList.size(); i++) {
      if (mList.get(i) == m) {
        if (found == -1) {
          found = i;
        } else {
          throw new AmbiguousMethodException(udafClass, argClasses, mList);
        }
      }
    }
    assert (found != -1);

    return cList.get(found);
  }

}
