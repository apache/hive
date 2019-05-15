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
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Exception thrown by the UDF and UDAF method resolvers in case a unique method
 * is not found.
 * 
 */
public class AmbiguousMethodException extends UDFArgumentException {

  private static final long serialVersionUID = 1L;

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
  public AmbiguousMethodException(Class<?> funcClass,
      List<TypeInfo> argTypeInfos, List<Method> methods) {
    super("Ambiguous method for " + funcClass + " with "
        + argTypeInfos.toString().replace('[', '(').replace(']', ')'), 
        funcClass, argTypeInfos, methods);
  }
}
