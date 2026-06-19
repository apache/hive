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
 * The UDF Method resolver interface. A user can plugin a resolver to their UDF
 * by implementing the functions in this interface. Note that the resolver is
 * stored in the UDF class as an instance variable. We did not use a static
 * variable because many resolvers maintain the class of the enclosing UDF as
 * state and are called from a base class e.g. UDFBaseCompare. This makes it
 * very easy to write UDFs that want to do resolution similar to the comparison
 * operators. Such UDFs just need to extend UDFBaseCompare and do not have to
 * care about the UDFMethodResolver interface. Same is true for UDFs that want
 * to do resolution similar to that done by the numeric operators. Such UDFs
 * simply have to extend UDFBaseNumericOp class. For the default resolution the
 * UDF implementation simply needs to extend the UDF class.
 */
@Deprecated
public interface UDFMethodResolver {

  /**
   * Gets the evaluate method for the UDF given the parameter types.
   * 
   * @param argClasses
   *          The list of the argument types that need to matched with the
   *          evaluate function signature.
   */
  Method getEvalMethod(List<TypeInfo> argClasses) throws UDFArgumentException;
}
