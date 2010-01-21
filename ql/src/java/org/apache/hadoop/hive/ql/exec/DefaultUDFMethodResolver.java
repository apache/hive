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
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * The default UDF Method resolver. This resolver is used for resolving the UDF
 * method that is to be used for evaluation given the list of the argument
 * types. The getEvalMethod goes through all the evaluate methods and returns
 * the one that matches the argument signature or is the closest match. Closest
 * match is defined as the one that requires the least number of arguments to be
 * converted. In case more than one matches are found, the method throws an
 * ambiguous method exception.
 */
public class DefaultUDFMethodResolver implements UDFMethodResolver {

  /**
   * The class of the UDF.
   */
  private final Class<? extends UDF> udfClass;

  /**
   * Constructor. This constructor sets the resolver to be used for comparison
   * operators. See {@link UDFMethodResolver}
   */
  public DefaultUDFMethodResolver(Class<? extends UDF> udfClass) {
    this.udfClass = udfClass;
  }

  /**
   * Gets the evaluate method for the UDF given the parameter types.
   * 
   * @param argClasses
   *          The list of the argument types that need to matched with the
   *          evaluate function signature.
   */
  @Override
  public Method getEvalMethod(List<TypeInfo> argClasses)
      throws AmbiguousMethodException {
    Method m = FunctionRegistry.getMethodInternal(udfClass, "evaluate", false,
        argClasses);
    if (m == null) {
      throw new AmbiguousMethodException(udfClass, argClasses);
    }
    return m;
  }
}
